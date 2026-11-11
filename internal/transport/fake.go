package transport

import (
	"io"
	"sync"
)

// FakeEndpoint: transport giả, nối 2 đầu bằng channel trong CÙNG process. Không mất gói, không mã hóa.
type FakeEndpoint struct {
	peer      *FakeEndpoint
	recvCh    chan Delivery
	doneCh    chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	nextStrm  uint32
}

type fakeStream struct {
	ep *FakeEndpoint
	id uint32
}

func FakePair() (Endpoint, Endpoint) {
	ep1 := &FakeEndpoint{recvCh: make(chan Delivery, 128), doneCh: make(chan struct{}), nextStrm: 1}
	ep2 := &FakeEndpoint{recvCh: make(chan Delivery, 128), doneCh: make(chan struct{}), nextStrm: 2}
	ep1.peer, ep2.peer = ep2, ep1
	return ep1, ep2
}

// deliver đẩy d sang đầu kia; trả lỗi nếu kết nối đã đóng thay vì block mãi.
func (e *FakeEndpoint) deliver(d Delivery) error {
	select {
	case <-e.doneCh:
		return io.ErrClosedPipe
	default:
	}
	select {
	case e.peer.recvCh <- d:
		return nil
	case <-e.doneCh:
		return io.ErrClosedPipe
	}
}

func (s *fakeStream) Write(p []byte) (int, error) {
	cp := append([]byte(nil), p...) // copy: caller được phép dùng lại p
	if err := s.ep.deliver(Delivery{StreamID: s.id, Data: cp}); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *fakeStream) Close() error {
	return s.ep.deliver(Delivery{StreamID: s.id, Fin: true})
}

func (e *FakeEndpoint) SendMessage(b []byte) error {
	strm := e.OpenStream(true)
	if _, err := strm.Write(b); err != nil {
		return err
	}
	return strm.Close()
}

func (e *FakeEndpoint) OpenStream(urgent bool) io.WriteCloser {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := e.nextStrm
	e.nextStrm += 2 // ep1 dùng id lẻ, ep2 dùng id chẵn → không trùng
	return &fakeStream{ep: e, id: id}
}

func (e *FakeEndpoint) Recv() <-chan Delivery { return e.recvCh }
func (e *FakeEndpoint) Done() <-chan struct{} { return e.doneCh }
func (e *FakeEndpoint) Stats() Stats          { return Stats{State: "fake"} }

// Close đóng CẢ 2 đầu, giống mất kết nối thật → đầu kia thấy Done().
func (e *FakeEndpoint) Close() error {
	e.closeOnce.Do(func() { close(e.doneCh) })
	e.peer.closeOnce.Do(func() { close(e.peer.doneCh) })
	return nil
}
