package server

import (
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

// sendQueueSize là số tin tối đa chờ gửi cho một client. Server không bỏ tin (bỏ tin là mất dữ liệu, trái
// yêu cầu truyền tin cậy): hàng đợi đầy nghĩa là client đó không đọc kịp trong thời gian dài, server ngắt kết
// nối của riêng client đó (xem Session.Send) và client tự kết nối lại.
const sendQueueSize = 4096

// Session là một user đã đăng nhập. Vòng đời:
//
//	pending: đã AUTH_OK, có trong Registry nhưng EP == nil
//	active:  AttachEndpoint gắn kết nối transport, 2 goroutine serveSession + writeLoop chạy
//	removed: Registry.Remove khi kết nối đóng (CLOSE, idle timeout 10 s, hoặc bị ngắt vì quá chậm)
type Session struct {
	Username string
	ConnID   uint32
	TCPAddr  string             // "ip:port" của kết nối TCP lúc handshake
	EP       transport.Endpoint // kết nối UDP tin cậy; chỉ gán một lần trong Registry.Attach

	// sendCh: hàng đợi tin gửi tới client này. Chỉ writeLoop đọc, nên thứ tự gửi = thứ tự vào hàng đợi.
	sendCh chan protocol.AppMsg
	// inSeq sắp lại tin client này gửi lên theo Seq. Chỉ goroutine serveSession dùng.
	inSeq protocol.SeqBuffer
	// kicked bật khi server đã quyết định ngắt session vì hàng đợi đầy (để chỉ ngắt và ghi log một lần).
	kicked atomic.Bool
}

// Addr trả về địa chỉ hiện tại của client: "ip:port" UDP mà transport đang gửi tới, hoặc IP lúc handshake TCP
// nếu server chưa nhận được gói UDP nào của client.
func (sess *Session) Addr() string {
	if sess.EP != nil {
		if p := sess.EP.Stats().Peer; p != "" {
			return p
		}
	}
	if host, _, err := net.SplitHostPort(sess.TCPAddr); err == nil {
		return host
	}
	return sess.TCPAddr
}

// Registry là nguồn sự thật về ai đang online. Tra theo tên (định tuyến DM, chặn đăng nhập trùng) và theo
// conn_id (gắn kết nối UDP vào đúng session). Mọi hàm chỉ thao tác map rồi thả khóa; không bao giờ gửi mạng
// trong lúc giữ khóa.
type Registry struct {
	mu       sync.RWMutex
	byName   map[string]*Session
	byConnID map[uint32]*Session
}

// NewRegistry tạo registry rỗng.
func NewRegistry() *Registry {
	return &Registry{
		byName:   make(map[string]*Session),
		byConnID: make(map[uint32]*Session),
	}
}

// ErrAlreadyOnline: tên này đang có session (pending hoặc active).
var ErrAlreadyOnline = errors.New("registry: user đang online")

// AddPending thêm session mới sau khi kiểm tra mật khẩu. Kiểm tra "đã online chưa" và "thêm vào" nằm trong
// cùng một lần giữ khóa, nên hai client cùng tên đăng nhập đồng thời thì chỉ một người vào được.
func (r *Registry) AddPending(username string, connID uint32, tcpAddr string) (*Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.byName[username]; ok {
		return nil, ErrAlreadyOnline
	}
	sess := &Session{
		Username: username,
		ConnID:   connID,
		TCPAddr:  tcpAddr,
		sendCh:   make(chan protocol.AppMsg, sendQueueSize),
	}
	r.byName[username] = sess
	r.byConnID[connID] = sess
	return sess, nil
}

// GetByName trả về session của user (nil nếu không online).
func (r *Registry) GetByName(name string) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byName[name]
}

// Attach gắn kết nối transport vào session pending có conn_id tương ứng. Trả nil nếu không có session đó
// hoặc session đã được gắn rồi.
func (r *Registry) Attach(connID uint32, ep transport.Endpoint) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	sess := r.byConnID[connID]
	if sess == nil || sess.EP != nil {
		return nil
	}
	sess.EP = ep
	return sess
}

// Remove xóa session nếu nó vẫn là session hiện tại của user đó. So sánh con trỏ để kết nối cũ chết muộn
// không xóa nhầm session mới của user vừa đăng nhập lại. Trả về true nếu thật sự đã xóa (gọi nhiều lần an toàn).
func (r *Registry) Remove(sess *Session) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.byName[sess.Username] != sess {
		return false
	}
	delete(r.byName, sess.Username)
	delete(r.byConnID, sess.ConnID)
	return true
}

// Names trả về tên mọi user đang online (kể cả pending), đã sắp xếp.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Addrs trả về địa chỉ hiện tại của từng user đang online (xem Session.Addr).
func (r *Registry) Addrs() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]string, len(r.byName))
	for name, sess := range r.byName {
		out[name] = sess.Addr()
	}
	return out
}

// Snapshot trả về bản sao danh sách session đã active, để gửi tin mà không giữ khóa.
func (r *Registry) Snapshot() []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*Session, 0, len(r.byName))
	for _, sess := range r.byName {
		if sess.EP != nil {
			list = append(list, sess)
		}
	}
	return list
}
