package transport

import (
	"errors"
	"net"
	"sync"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

// readLoop reads packet from sock, find Conn by conn_id, then push into Conn.in
// Both server(Mux, many Conns) and client(Dial, 1 Conn) use this
func readLoop(sock PacketConn, lookup func(connID uint32) *Conn) {
	buf := make([]byte, 2048)
	for {
		n, addr, err := sock.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return // socket closed -> stop
			}
			continue // temporary err (ICMP port unreachable...)
		}

		h, err := protocol.ParseHeader(buf[:n])
		if err != nil || h.Magic != protocol.Magic || h.Version != protocol.Version {
			continue // trash packet
		}

		c := lookup(h.ConnID)
		if c == nil {
			continue // conn_id not existed
		}

		raw := append([]byte(nil), buf[:n]...) // copy, buf will be overwritten
		select {
		case c.in <- inPacket{raw: raw, addr: addr}:
		default: // conn is overload -> drop packet (like real router), sender retrans
		}
	}
}

//=========== Server ===========

// Mux: 1 socket UDP serves many Conn, demux by conn_id (not IP:port).
type Mux struct {
	sock  PacketConn
	mu    sync.RWMutex
	conns map[uint32]*Conn
}

func NewMux(sock PacketConn) *Mux {
	m := &Mux{
		sock:  sock,
		conns: map[uint32]*Conn{},
	}
	go readLoop(sock, m.lookup)
	return m
}

func (m *Mux) lookup(id uint32) *Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[id]
}

// Accept creates a Conn on server side after TCP handshake (conn_id and k_udp are known)
// Client's UDP address is unknown; Conn learns it from the first packet (PING) the client sends.
func (m *Mux) Accept(connID uint32, key [32]byte) (*Conn, error) {
	c, err := newConn(m.sock, nil, connID, key, true)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.conns[connID] = c
	m.mu.Unlock()
	go func() { // conn dies -> remove from table
		<-c.Done()
		m.mu.Lock()
		if m.conns[connID] == c {
			delete(m.conns, connID)
		}
		m.mu.Unlock()
	}()
	return c, nil
}

func (m *Mux) Close() error {
	return m.sock.Close()
}

//========= Client =========

// Dial creates Conn on client side on client's private socket; Conn.run() sends the first PING.
// sock is normally net.ListenUDP("udp", nil)
// Conn closes -> sock closes
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
