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

type UserAccount struct {
	Salt string `json:"salt"`
	Hash string `json:"hash"` // sha256(salt + password)
}

type Server struct {
	reg *Registry
	users map[string]UserAccount
	nextConnID atomic.Uint32
	udpPort int
}

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
		reg: NewRegistry(),
		users: users,
		udpPort: udpPort,
	}
	srv.nextConnID.Store(1000) // Khởi tạo ID bắt đầu từ 1000
	return srv, nil
}

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
		go s.handleHandshake(conn) // Mỗi client 1 goroutine độc lập
	}
}

func (s *Server) handleHandshake(c net.Conn) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second)) // Timeout chống treo goroutine

	// Step 1: Nhận HELLO
	var hello protocol.Hello
	if err := protocol.ReadMsg(c, &hello); err != nil {
		log.Printf("[Server] Read HELLO err: %v", err)
		return
	}

	// Sinh khóa ECDH
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

	// Step 2: Gửi KEYX
	keyx := protocol.KeyX{
		Type: "KEYX",
		ServerPub: priv.PublicKey().Bytes(),
		ConnID: connID,
		UDPPort: s.udpPort,
	}
	if err := protocol.WriteMsg(c, keyx); err != nil {
		return
	}

	// Step 3: Nhận AUTH
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

	// Step 4: Kiểm tra Auth & Trùng lặp
	if !s.checkPassword(cred.User, cred.Pass) {
		protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: "Tài khoản hoặc mật khẩu không chính xác"})
		return
	}

	if s.reg.GetByName(cred.User) != nil {
		protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: "Tài khoản đang online ở máy khác"})
		return
	}

	// Thêm vào Pending Registry
	s.reg.AddPending(cred.User, connID, kUDP)
	log.Printf("[Server] User '%s' authenticated successfully (ConnID: %d)", cred.User, connID)

	// Trả AUTH_OK
	protocol.WriteMsg(c, protocol.AuthResult{
		Type:  "AUTH_OK",
		Users: s.reg.Names(),
	})
}

func (s *Server) checkPassword(username, password string) bool {
	acc, ok := s.users[username]
	if !ok {
		return false
	}
	h := sha256.Sum256([]byte(acc.Salt + password))
	return hex.EncodeToString(h[:]) == acc.Hash
}