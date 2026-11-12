package server

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

type Session struct {
	Username string
	ConnID   uint32
	KUDP     [32]byte
	UDPAddr  *net.UDPAddr
	LastSeen time.Time
	EP       transport.Endpoint

	// sendCh: hàng đợi tin gửi tới client này. Chỉ writeLoop đọc, nó đánh Seq rồi mới gửi, nên tin bị bỏ
	// vì hàng đợi đầy không "ăn" mất một số Seq (client sẽ không chờ một tin không bao giờ tới).
	sendCh chan protocol.AppMsg
	// inSeq sắp lại tin client này gửi lên theo Seq. Chỉ serveSession dùng.
	inSeq protocol.SeqBuffer
}

type Registry struct {
	mu       sync.RWMutex
	byName   map[string]*Session
	byConnID map[uint32]*Session
}

func NewRegistry() *Registry {
	return &Registry{
		byName:   make(map[string]*Session),
		byConnID: make(map[uint32]*Session),
	}
}

// sendQueueSize: số tin tối đa chờ gửi cho một client. Đầy thì tin mới bị bỏ (xem Session.Send).
const sendQueueSize = 64

var ErrAlreadyOnline = errors.New("registry: user đang online")

func (r *Registry) AddPending(username string, connID uint32, kUDP [32]byte) (*Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.byName[username]; ok {
		return nil, ErrAlreadyOnline
	}

	sess := &Session{
		Username: username,
		ConnID:   connID,
		KUDP:     kUDP,
		LastSeen: time.Now(),
		sendCh:   make(chan protocol.AppMsg, sendQueueSize),
	}
	r.byName[username] = sess
	r.byConnID[connID] = sess
	return sess, nil
}

func (r *Registry) GetByName(name string) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byName[name]
}

func (r *Registry) GetByConnID(connID uint32) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byConnID[connID]
}

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

func (r *Registry) RemoveIfPending(sess *Session) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sess.EP != nil || r.byName[sess.Username] != sess {
		return false
	}
	delete(r.byName, sess.Username)
	delete(r.byConnID, sess.ConnID)
	return true
}

func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	return names
}

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
