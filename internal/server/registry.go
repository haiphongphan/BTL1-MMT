package server

import (
	"errors"
	"github.com/phongphanhai06/go-collab/internal/transport"
	"net"
	"sync"
	"time"
)

type Session struct {
	Username string
	ConnID   uint32
	KUDP     [32]byte
	UDPAddr  *net.UDPAddr
	LastSeen time.Time
	EP       transport.Endpoint
	sendCh   chan []byte
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
		sendCh:   make(chan []byte, 64),
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
