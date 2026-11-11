package transport

import (
	"errors"
	"io"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

var ErrClosed = errors.New("stream is closed")

/*======================RECEIVER======================*/

// recvStream reassembles a stream's data in offset orser
// Each stream has 1 independent recvStram so that stream chat is not blocked even though there are stream file erros (no HoL)
type recvStream struct {
	next     uint32            // the next offset needs to be handed for app
	pending  map[uint32][]byte // early incoming data, waiting for fill in hols (key = offset)
	buffered int               // total bytes in pending to calculate cwnd
	finAt    int64             // stream's length when FIN is known, -1 if not
}

func newRecvStream() *recvStream {
	return &recvStream{
		pending: map[uint32][]byte{},
		finAt:   -1,
	}
}

// push receives 1 STREAM frame, returns the completed segments and hand them to app, done = true when app successfully receives correct bytes to FIN.
func (s *recvStream) push(off uint32, data []byte, fin bool) (out [][]byte, done bool) {
	if fin {
		s.finAt = int64(off) + int64(len(data))
	}
	end := off + uint32(len(data))
	switch {
	case end <= s.next: // retransmit -> skip

	case off > s.next:
		// early arrival, holes ahead -> pending (skip if identical)
		if _, ok := s.pending[off]; !ok {
			s.pending[off] = data
			s.buffered += len(data)
		}
	default:
		// off <= next < end: fit with next (or partly overlap) -> ship
		data = data[s.next-off:]
		out = append(out, data)
		s.next += uint32(len(data))
		for { // fill holes + communicate with segments waiting for connection
			d, ok := s.pending[s.next]
			if !ok {
				break
			}

			delete(s.pending, s.next)
			s.buffered -= len(d)

			if len(d) > 0 { // check if it is empty FIN -> nothing to ship
				out = append(out, d)
				s.next += uint32(len(d))
			}
		}
	}
	done = s.finAt >= 0 && int64(s.next) == s.finAt

	return
}

/*======================SENDER======================*/

// SendStream is io.WriteCloser, safe to be called from goroutine except for run().
// 1 SendStream should be used by only 1 goroutine at a time
type SendStream struct {
	c      *Conn  // Connection that contains this stream
	id     uint32 // streamID
	off    uint32 // offset of the next byte needs to be sent
	urgent bool   // true = chat, false = bulk (file)
	closed bool   // call Close()?
}

func (s *SendStream) Write(p []byte) (int, error) {
	if s.closed {
		return 0, ErrClosed
	} // Write after close -> err

	total := len(p)
	for len(p) > 0 {
		n := min(len(p), protocol.MaxStreamData) // max 1000 bytes/frame
		data := append([]byte(nil), p[:n]...)    // copy
		f := &protocol.StreamFrame{
			StreamID: s.id,
			Offset:   s.off,
			Data:     data,
		}

		if err := s.c.enqueue(f, s.urgent); err != nil {
			return total - len(p), err // return the sent bytes
		}
		s.off += uint32(n)
		p = p[n:]
	}

	return total, nil
}

// Close send 1 empty frame containing FIN
func (s *SendStream) Close() error {
	if s.closed {
		return nil
	} // idempotent

	s.closed = true
	return s.c.enqueue(&protocol.StreamFrame{
		StreamID: s.id,
		Offset:   s.off,
		Fin:      true,
	}, s.urgent)
}

// enqueue give frame to goroutine run(). Block when queue is full -> app slows down (backpressure)
func (c *Conn) enqueue(f *protocol.StreamFrame, urgent bool) error {
	ch := c.bulkCh

	if urgent {
		ch = c.urgentCh
	}

	select {
	case ch <- f:
		return nil // avai -> enqueue
	case <-c.done:
		return ErrClosed // closed connection -> exit
	}
}

// OpenStream opens a new stream, Client uses odd id, server use even id
func (c *Conn) OpenStream(urgent bool) io.WriteCloser {
	return &SendStream{
		c:      c,
		id:     c.nextStream.Add(2),
		urgent: urgent,
	}
}

// SendMessage: 1 message = 1 high priority stream, close after write
func (c *Conn) SendMessage(b []byte) error {
	s := c.OpenStream(true) // message -> high priority
	if _, err := s.Write(b); err != nil {
		return err
	}
	return s.Close()
}
