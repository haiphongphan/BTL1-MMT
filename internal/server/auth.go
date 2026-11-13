package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

// UserAccount là một dòng trong users.json. Không lưu mật khẩu, chỉ lưu hash có salt riêng cho từng user.
type UserAccount struct {
	Salt string `json:"salt"`
	Hash string `json:"hash"` // hex(sha256(salt + password))
}

// Server gồm: danh sách tài khoản, registry các session đang online, và Mux của tầng transport
// (một socket UDP chung cho mọi client, tách gói theo conn_id).
type Server struct {
	reg        *Registry
	users      map[string]UserAccount
	nextConnID atomic.Uint32
	udpPort    int
	mux        *transport.Mux
}

// NewServer đọc users.json và tạo server (chưa mở socket nào).
func NewServer(usersFilePath string, udpPort int) (*Server, error) {
	data, err := os.ReadFile(usersFilePath)
	if err != nil {
		return nil, err
	}
	var users map[string]UserAccount
	if err := json.Unmarshal(data, &users); err != nil {
		return nil, err
	}

	srv := &Server{
		reg:     NewRegistry(),
		users:   users,
		udpPort: udpPort,
	}
	srv.nextConnID.Store(1000) // conn_id bắt đầu từ 1001
	return srv, nil
}

// ListenUDP mở socket UDP dữ liệu. loss/jitter > 0 thì bọc socket bằng LossyConn để giả lập mạng xấu
// ở chiều gửi của server (demo độ tin cậy trên localhost).
func (s *Server) ListenUDP(loss float64, jitter time.Duration) error {
	sock, err := net.ListenUDP("udp", &net.UDPAddr{Port: s.udpPort})
	if err != nil {
		return err
	}
	var pc transport.PacketConn = sock
	if loss > 0 || jitter > 0 {
		pc = &transport.LossyConn{UDPConn: sock, Loss: loss, Jitter: jitter}
	}
	s.mux = transport.NewMux(pc)
	log.Printf("[Server] UDP transport running on :%d (loss %.0f%%, jitter %v)", s.udpPort, loss*100, jitter)
	return nil
}

// ListenTCP nhận kết nối handshake; mỗi kết nối được xử lý trong một goroutine riêng nên một client
// chậm/treo không chặn người khác đăng nhập.
func (s *Server) ListenTCP(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("[Server] TCP Handshake server running on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[Server] Accept err: %v", err)
			continue
		}
		go s.handleHandshake(conn)
	}
}

// handleHandshake chạy HELLO → KEYX → AUTH → AUTH_OK/AUTH_FAIL trên một kết nối TCP (docs/protocol.md 3.2).
// Thành công thì tạo kết nối transport cho conn_id mới và đóng TCP; mọi dữ liệu sau đó đi qua UDP.
func (s *Server) handleHandshake(c net.Conn) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second)) // client im lặng giữa chừng không giữ goroutine mãi

	// 1. HELLO: khóa công khai X25519 của client
	var hello protocol.Hello
	if err := protocol.ReadMsg(c, &hello); err != nil {
		log.Printf("[Server] Read HELLO err: %v", err)
		return
	}

	// Sinh cặp khóa của server, tính k_tcp (mã hóa AUTH) và k_udp (mã hóa mọi gói UDP)
	priv, err := transport.NewKeyPair()
	if err != nil {
		return
	}
	connID := s.nextConnID.Add(1)

	kTCP, kUDP, err := transport.DeriveKeys(priv, hello.ClientPub)
	if err != nil {
		log.Printf("[Server] DeriveKeys err: %v", err)
		return
	}

	// 2. KEYX: khóa công khai của server, conn_id và cổng UDP
	keyx := protocol.KeyX{
		Type:      "KEYX",
		ServerPub: priv.PublicKey().Bytes(),
		ConnID:    connID,
		UDPPort:   s.udpPort,
	}
	if err := protocol.WriteMsg(c, keyx); err != nil {
		return
	}

	// 3. AUTH: {user, pass} đã mã hóa AES-GCM bằng k_tcp
	var auth protocol.Auth
	if err := protocol.ReadMsg(c, &auth); err != nil {
		return
	}
	aead, err := transport.NewAEAD(kTCP)
	if err != nil {
		return
	}
	plain, err := aead.Open(nil, auth.Nonce, auth.Sealed, nil)
	if err != nil {
		protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: "Giải mã thông tin thất bại"})
		return
	}
	var cred struct {
		User string `json:"user"`
		Pass string `json:"pass"`
	}
	if err := json.Unmarshal(plain, &cred); err != nil {
		return
	}

	// 4. Kiểm tra mật khẩu và đăng nhập trùng. User lạ và sai mật khẩu trả cùng một lý do để không lộ
	// user nào tồn tại.
	if !s.checkPassword(cred.User, cred.Pass) {
		protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: "Tài khoản hoặc mật khẩu không chính xác"})
		return
	}
	sess, err := s.reg.AddPending(cred.User, connID, c.RemoteAddr().String())
	if err != nil {
		protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: "Tài khoản đang online ở máy khác"})
		return
	}
	log.Printf("[Server] User '%s' authenticated from %s (ConnID: %d)", cred.User, c.RemoteAddr(), connID)

	// Tạo kết nối transport ngay (trước khi trả AUTH_OK) để PING đầu tiên của client tới là có người nhận.
	// Nếu client không bao giờ gửi UDP, idle timeout 10 s của transport đóng kết nối và dọn session.
	conn, err := s.mux.Accept(connID, kUDP)
	if err != nil {
		s.reg.Remove(sess)
		protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: "Lỗi tạo kết nối UDP"})
		return
	}
	s.AttachEndpoint(connID, conn)

	// 5. AUTH_OK kèm danh sách user đang online
	protocol.WriteMsg(c, protocol.AuthResult{
		Type:  "AUTH_OK",
		Users: s.reg.Names(),
	})
}

// Close đóng mọi kết nối UDP (gửi CLOSE để client biết ngay thay vì chờ idle timeout 10 s) rồi đóng socket.
// Dùng khi server tắt chủ động (Ctrl+C).
func (s *Server) Close() {
	if s.mux != nil {
		s.mux.Close()
	}
}

// checkPassword so sha256(salt ‖ password) với hash đã lưu.
func (s *Server) checkPassword(username, password string) bool {
	acc, ok := s.users[username]
	if !ok {
		return false
	}
	h := sha256.Sum256([]byte(acc.Salt + password))
	return hex.EncodeToString(h[:]) == acc.Hash
}
