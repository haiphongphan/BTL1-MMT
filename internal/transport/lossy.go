package transport

import (
	"math/rand/v2"
	"net"
	"time"
)

type PacketConn interface {
	ReadFromUDP (b []byte) (int, *net.UDPAddr, error)
	WriteToUDP (b []byte, addr *net.UDPAddr) (int, error)
	Close() error
}

// LossyConn wraps a real UDPConn, intentionally lose/delay packet from the sending end
type LossyConn struct {
	*net.UDPConn
	Loss float64
	Delay time.Duration
	Jitter time.Duration
}

func (l *LossyConn) WriteToUDP (b []byte, addr *net.UDPAddr) (int, error) {
	if rand.Float64() < l.Loss {
		return len(b), nil // lose package, pretend to finish sending
	}

	if l.Delay == 0 && l.Jitter == 0 {
		return l.UDPConn.WriteToUDP(b, addr)
	}

	cp := append([]byte(nil), b...) // copy, caller can reuse b
	d := l.Delay

	if l.Jitter > 0 {
		d += time.Duration(rand.Int64N(int64(l.Jitter)))
	}

	time.AfterFunc(d, func() { l.UDPConn.WriteToUDP(cp, addr) }) // err (closed socket) then skip

	return len(b), nil
}

