package server

import (
	"net"
	"sync"
	"time"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

type Session struct {
	Username string
	ConnID uint32
	KUDP [32]byte
	UDPAddr *net.UDPAddr
	LastSeen time.Time
	EP transport.Endpoint
}

type Registry struct {
	mu sync.RWMutex
	byName map[string]*Session
	byConnID map[uint32]*Session
}

func NewRegistry() *Registry {
	return &Registry{
		byName: make(map[string]*Session),
		byConnID: make(map[uint32]*Session),
	}
}

func (r *Registry) AddPending(username string, connID uint32, kUDP [32]byte) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()

	sess := &Session{
		Username: username,
		ConnID: connID,
		KUDP: kUDP,
		LastSeen: time.Now(),
	}
	r.byName[username] = sess
	r.byConnID[connID] = sess
	return sess
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

func (r *Registry) Remove(username string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if sess, ok := r.byName[username]; ok {
		delete(r.byName, username)
		delete(r.byConnID, sess.ConnID)
	}
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
		list = append(list, sess)
	}
	return list
}