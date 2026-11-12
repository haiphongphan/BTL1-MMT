package transport

import (
	"errors"
	"io"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

// ErrClosed: ghi vào stream đã Close(), hoặc kết nối đã đóng.
var ErrClosed = errors.New("stream is closed")

// ========== Phía nhận ==========

// recvStream ghép lại dữ liệu của 1 stream theo offset.
// Mỗi stream có recvStream riêng, nên gói của stream file bị mất chỉ làm stream file phải chờ,
// stream chat vẫn được giao bình thường (không có Head-of-Line blocking giữa các stream).
// Chỉ goroutine run() dùng nên không cần khóa.
type recvStream struct {
	next     uint32            // offset tiếp theo cần giao cho app; mọi byte trước next đã giao
	pending  map[uint32][]byte // dữ liệu tới sớm (phía trước còn chỗ trống), key = offset
	buffered int               // tổng số byte trong pending, tính vào cửa sổ nhận (flow control)
	finAt    int64             // độ dài stream khi đã biết FIN, -1 nếu chưa
}

// newRecvStream tạo recvStream rỗng, chưa biết FIN.
func newRecvStream() *recvStream {
	return &recvStream{
		pending: map[uint32][]byte{},
		finAt:   -1,
	}
}

// push nhận 1 STREAM frame, trả về các đoạn đã liền mạch (giao ngay cho app, đúng thứ tự).
// done = true khi đã giao đủ mọi byte tới FIN, tức là stream hoàn tất.
//
// 3 trường hợp, với [off, end) là khoảng byte của frame:
//   - end <= next:  toàn bộ đã giao rồi (gói gửi lại) -> bỏ
//   - off > next:   tới sớm, phía trước còn chỗ trống -> cất vào pending
//   - còn lại:      nối tiếp được next (có thể chồng một phần) -> giao phần mới, rồi giao tiếp các đoạn
//     trong pending vừa liền mạch
func (s *recvStream) push(off uint32, data []byte, fin bool) (out [][]byte, done bool) {
	if fin {
		s.finAt = int64(off) + int64(len(data))
	}
	end := off + uint32(len(data))
	switch {
	case end <= s.next: // gói gửi lại, đã giao rồi -> bỏ

	case off > s.next:
		// tới sớm, phía trước còn chỗ trống -> cất vào pending (đã có đoạn cùng offset thì bỏ, đó là bản trùng)
		if _, ok := s.pending[off]; !ok {
			s.pending[off] = data
			s.buffered += len(data)
		}
	default:
		// off <= next < end: nối tiếp được (hoặc chồng 1 phần lên đoạn đã giao) -> cắt phần đã giao, giao phần còn lại
		data = data[s.next-off:]
		out = append(out, data)
		s.next += uint32(len(data))
		for { // chỗ trống vừa được lấp -> giao tiếp các đoạn trong pending bắt đầu đúng tại next
			d, ok := s.pending[s.next]
			if !ok {
				break
			}

			delete(s.pending, s.next)
			s.buffered -= len(d)

			if len(d) > 0 { // frame FIN rỗng thì không có gì để giao
				out = append(out, d)
				s.next += uint32(len(d))
			}
		}
	}
	done = s.finAt >= 0 && int64(s.next) == s.finAt

	return
}

// ========== Phía gửi ==========

// SendStream là 1 stream gửi đi (io.WriteCloser). Gọi được từ bất kỳ goroutine nào trừ run(),
// nhưng mỗi SendStream chỉ nên được 1 goroutine dùng tại 1 thời điểm.
type SendStream struct {
	c      *Conn  // kết nối chứa stream này
	id     uint32 // stream_id
	off    uint32 // offset của byte tiếp theo sẽ gửi
	urgent bool   // true = chat (hàng đợi ưu tiên), false = bulk (file)
	closed bool   // đã gọi Close() chưa
}

// Write cắt p thành các frame ≤ 1000 byte và xếp vào hàng đợi gửi; bị chặn khi hàng đợi bulk đầy (backpressure).
func (s *SendStream) Write(p []byte) (int, error) {
	if s.closed {
		return 0, ErrClosed // Write sau Close -> lỗi
	}

	total := len(p)
	for len(p) > 0 {
		n := min(len(p), protocol.MaxStreamData) // tối đa 1000 byte/frame
		data := append([]byte(nil), p[:n]...)    // copy: frame còn nằm trong hàng đợi (và có thể gửi lại) sau khi Write trả về
		f := &protocol.StreamFrame{
			StreamID: s.id,
			Offset:   s.off,
			Data:     data,
		}

		if err := s.c.enqueue(f, s.urgent); err != nil {
			return total - len(p), err // trả về số byte đã xếp hàng được
		}
		s.off += uint32(n)
		p = p[n:]
	}

	return total, nil
}

// Close gửi 1 frame rỗng có cờ FIN. Gọi nhiều lần cũng chỉ gửi 1 lần.
func (s *SendStream) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	return s.c.enqueue(&protocol.StreamFrame{
		StreamID: s.id,
		Offset:   s.off,
		Fin:      true,
	}, s.urgent)
}

// enqueue chuyển frame cho goroutine run() qua channel. Bị chặn khi hàng đợi đầy -> app tự chậm lại
// (backpressure); trả về ErrClosed nếu kết nối đã đóng.
func (c *Conn) enqueue(f *protocol.StreamFrame, urgent bool) error {
	ch := c.bulkCh

	if urgent {
		ch = c.urgentCh
	}

	select {
	case <-c.done: // kiểm tra trước: ch có buffer nên select dưới đây có thể chọn nhánh gửi dù conn đã đóng
		return ErrClosed
	default:
	}

	select {
	case ch <- f:
		return nil // còn chỗ -> đã xếp hàng
	case <-c.done:
		return ErrClosed // kết nối đóng trong lúc đang chờ -> thoát, không treo mãi
	}
}

// OpenStream mở stream gửi mới. Client dùng stream_id lẻ, server dùng id chẵn nên 2 bên không bao giờ trùng.
// urgent = true: frame vào hàng đợi ưu tiên (chat), luôn được gửi trước dữ liệu file.
func (c *Conn) OpenStream(urgent bool) io.WriteCloser {
	return &SendStream{
		c:      c,
		id:     c.nextStream.Add(2),
		urgent: urgent,
	}
}

// SendMessage gửi b thành 1 stream ưu tiên cao riêng (1 message = 1 stream), đóng ngay sau khi ghi.
func (c *Conn) SendMessage(b []byte) error {
	s := c.OpenStream(true)
	if _, err := s.Write(b); err != nil {
		return err
	}
	return s.Close()
}
