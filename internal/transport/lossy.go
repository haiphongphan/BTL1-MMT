package transport

import (
	"math/rand/v2"
	"net"
	"sync"
	"time"
)

type PacketConn interface {
	ReadFromUDP(b []byte) (int, *net.UDPAddr, error)
	WriteToUDP(b []byte, addr *net.UDPAddr) (int, error)
	Close() error
}

const (
	// lossyQueueMax: số gói tối đa đang "trên đường" trong hàng đợi giả lập; đầy thì bỏ gói (giống router tràn bộ đệm).
	lossyQueueMax = 4096
	// defaultQueueBytes: bộ đệm của nút cổ chai khi bật Rate mà không đặt QueueBytes.
	defaultQueueBytes = 64 * 1024
)

// LossyConn wraps a real UDPConn, intentionally lose/delay packet from the sending end.
//
//   - Loss:   bỏ ngẫu nhiên (mất gói không liên quan tới nghẽn, vd sóng Wi-Fi yếu).
//   - Delay/Jitter: độ trễ đường truyền. Giả lập hàng đợi FIFO của router: độ trễ mỗi gói thay đổi nhưng gói sau
//     KHÔNG BAO GIỜ tới trước gói trước (giống mạng thật). Bật Reorder để mỗi gói được hẹn giờ độc lập, tức là
//     cố ý đảo thứ tự gói (Reorder bỏ qua Rate).
//   - Rate/QueueBytes: nút cổ chai băng thông Rate byte/s với bộ đệm QueueBytes. Gửi nhanh hơn Rate thì gói xếp hàng
//     (RTT tăng); hàng đợi đầy thì bỏ gói ở đuôi (tail drop). Đây là "mất gói do nghẽn" mà congestion control
//     được thiết kế để phát hiện, khác với Loss.
type LossyConn struct {
	*net.UDPConn
	Loss       float64
	Delay      time.Duration
	Jitter     time.Duration
	Reorder    bool
	Rate       int // byte/giây, 0 = không giới hạn
	QueueBytes int // bộ đệm nút cổ chai, 0 = defaultQueueBytes

	once       sync.Once
	mu         sync.Mutex
	lastDue    time.Time // thời điểm phát của gói được xếp hàng gần nhất
	lastDepart time.Time // thời điểm gói gần nhất rời nút cổ chai
	q          chan delayedPkt
	done       chan struct{}
	closed     sync.Once
}

type delayedPkt struct {
	due  time.Time
	b    []byte
	addr *net.UDPAddr
}

// WriteToUDP gửi gói qua bộ giả lập: có thể bỏ (Loss), trễ (Delay/Jitter, giữ FIFO) hoặc xếp hàng
// ở nút cổ chai (Rate/QueueBytes).
func (l *LossyConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	if rand.Float64() < l.Loss {
		return len(b), nil // lose package, pretend to finish sending
	}

	if l.Delay == 0 && l.Jitter == 0 && l.Rate == 0 {
		return l.UDPConn.WriteToUDP(b, addr)
	}

	cp := append([]byte(nil), b...) // copy, caller can reuse b
	d := l.Delay

	if l.Jitter > 0 {
		d += time.Duration(rand.Int64N(int64(l.Jitter)))
	}

	if l.Reorder && l.Rate == 0 {
		time.AfterFunc(d, func() { l.UDPConn.WriteToUDP(cp, addr) }) // err (closed socket) then skip
		return len(b), nil
	}

	l.once.Do(l.start)

	l.mu.Lock()
	now := time.Now()
	due := now.Add(d)

	if l.Rate > 0 {
		// nút cổ chai: gói rời link sau khi các gói trước rời hết + thời gian "đẩy" chính nó lên dây (len/Rate)
		start := now
		if l.lastDepart.After(now) {
			start = l.lastDepart
		}
		queued := int(start.Sub(now).Seconds() * float64(l.Rate)) // số byte đang chờ trong hàng đợi
		limit := l.QueueBytes
		if limit == 0 {
			limit = defaultQueueBytes
		}
		if queued+len(b) > limit {
			l.mu.Unlock()
			return len(b), nil // hàng đợi đầy -> tail drop
		}
		l.lastDepart = start.Add(time.Duration(float64(len(b)) / float64(l.Rate) * float64(time.Second)))
		due = l.lastDepart.Add(d)
	}

	if due.Before(l.lastDue) {
		due = l.lastDue // FIFO: không được phát trước gói đã xếp hàng trước đó
	}
	l.lastDue = due
	l.mu.Unlock()

	select {
	case l.q <- delayedPkt{due: due, b: cp, addr: addr}:
	case <-l.done:
	default: // hàng đợi đầy -> bỏ gói
	}

	return len(b), nil
}

// start chạy 1 goroutine phát gói duy nhất -> thứ tự phát đúng thứ tự xếp hàng.
func (l *LossyConn) start() {
	l.q = make(chan delayedPkt, lossyQueueMax)
	l.done = make(chan struct{})

	go func() {
		t := time.NewTimer(0)
		defer t.Stop()

		for {
			var p delayedPkt
			select {
			case p = <-l.q:
			case <-l.done:
				return
			}

			if wait := time.Until(p.due); wait > 0 {
				t.Reset(wait)
				select {
				case <-t.C:
				case <-l.done:
					return
				}
			}

			l.UDPConn.WriteToUDP(p.b, p.addr) // err (closed socket) then skip
		}
	}()
}

// Close dừng goroutine phát gói (nếu có) rồi đóng socket thật.
func (l *LossyConn) Close() error {
	l.once.Do(func() {}) // chưa từng start -> chặn start về sau
	l.closed.Do(func() {
		if l.done != nil {
			close(l.done)
		}
	})
	return l.UDPConn.Close()
}
