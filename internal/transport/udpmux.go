package transport

import (
	"errors"
	"net"
	"sync"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

// readLoop đọc gói từ sock, tìm Conn theo conn_id trong header rồi đẩy vào Conn.in.
// Dùng chung cho server (Mux, nhiều Conn trên 1 socket) và client (Dial, 1 Conn).
// Ở đây chỉ kiểm tra header; giải mã/xác thực do Conn.run() làm.
func readLoop(sock PacketConn, lookup func(connID uint32) *Conn) {
	buf := make([]byte, 2048)
	for {
		n, addr, err := sock.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return // socket đã đóng -> dừng
			}
			continue // lỗi tạm thời (vd ICMP port unreachable) -> đọc tiếp
		}

		h, err := protocol.ParseHeader(buf[:n])
		if err != nil || h.Magic != protocol.Magic || h.Version != protocol.Version {
			continue // gói rác, không phải giao thức này
		}

		c := lookup(h.ConnID)
		if c == nil {
			continue // conn_id không tồn tại
		}

		raw := append([]byte(nil), buf[:n]...) // copy: buf bị ghi đè ở lần đọc sau
		select {
		case c.in <- inPacket{raw: raw, addr: addr}:
		default: // Conn quá tải -> bỏ gói (như router thật); bên gửi sẽ phát hiện mất và gửi lại
		}
	}
}

// ========== Server ==========

// Mux cho 1 socket UDP phục vụ nhiều Conn, phân kênh theo conn_id (không theo IP:port, nên client
// đổi mạng, ví dụ Wi-Fi sang 4G, vẫn giữ được kết nối).
type Mux struct {
	sock  PacketConn
	mu    sync.RWMutex
	conns map[uint32]*Conn
}

// NewMux tạo bộ phân kênh phía server trên 1 socket UDP: gói được chuyển tới Conn theo conn_id trong header.
func NewMux(sock PacketConn) *Mux {
	m := &Mux{
		sock:  sock,
		conns: map[uint32]*Conn{},
	}
	go readLoop(sock, m.lookup)
	return m
}

// lookup tìm Conn theo conn_id; nil nếu không có.
func (m *Mux) lookup(id uint32) *Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[id]
}

// Accept tạo Conn phía server sau khi handshake TCP xong (đã biết conn_id và k_udp).
// Lúc này chưa biết địa chỉ UDP của client; Conn học được từ gói đầu tiên (PING) client gửi.
func (m *Mux) Accept(connID uint32, key [32]byte) (*Conn, error) {
	c, err := newConn(m.sock, nil, connID, key, true)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.conns[connID] = c
	m.mu.Unlock()
	go func() { // Conn kết thúc -> xóa khỏi bảng
		<-c.Done()
		m.mu.Lock()
		if m.conns[connID] == c {
			delete(m.conns, connID)
		}
		m.mu.Unlock()
	}()
	return c, nil
}

// Close đóng mọi Conn (gửi CLOSE để bên kia biết ngay, không phải đợi 10s idle timeout) rồi mới đóng socket.
func (m *Mux) Close() error {
	// copy danh sách rồi thả lock: Conn.Close() đợi run() thoát, còn goroutine dọn dẹp trong Accept cũng cần m.mu
	m.mu.RLock()
	conns := make([]*Conn, 0, len(m.conns))
	for _, c := range m.conns {
		conns = append(conns, c)
	}
	m.mu.RUnlock()

	for _, c := range conns {
		c.Close()
	}
	return m.sock.Close()
}

// ========== Client ==========

// Dial tạo Conn phía client trên socket riêng của client; Conn.run() gửi PING đầu tiên ở tick đầu tiên.
// sock thường là net.ListenUDP("udp", nil). Conn đóng thì sock cũng bị đóng.
func Dial(sock PacketConn, server *net.UDPAddr, connID uint32, key [32]byte) (*Conn, error) {
	c, err := newConn(sock, server, connID, key, false)
	if err != nil {
		return nil, err
	}

	go readLoop(sock, func(id uint32) *Conn {
		if id == connID {
			return c
		}
		return nil
	})

	go func() {
		<-c.Done()
		sock.Close()
	}()

	return c, nil
}
