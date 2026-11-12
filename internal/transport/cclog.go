package transport

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// cclog: ghi diễn biến congestion control ra CSV để vẽ đồ thị cwnd (go run ./scripts/plotcc file.csv > cwnd.svg).
//
// Logger là biến toàn cục của package, bật 1 lần trong main bằng EnableCCLog; mọi Conn tạo ra sau đó đều ghi vào.
// Server có nhiều Conn cùng ghi nên dùng mutex; ghi qua bufio và flush định kỳ nên run() gần như không bị chậm.

const ccLogFlushEvery = 500 * time.Millisecond

type ccLogger struct {
	mu        sync.Mutex
	f         *os.File
	w         *bufio.Writer
	start     time.Time
	lastFlush time.Time
}

var ccLog atomic.Pointer[ccLogger]

// EnableCCLog mở file CSV và bật ghi log congestion control. Trả về hàm đóng (flush + close file).
func EnableCCLog(path string) (func() error, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	l := &ccLogger{f: f, w: bufio.NewWriter(f), start: now, lastFlush: now}
	fmt.Fprintln(l.w, "time_ms,conn_id,event,cwnd,ssthresh,rtt_us,rto_ms,inflight,sent,lost")
	ccLog.Store(l)

	return func() error {
		ccLog.CompareAndSwap(l, nil)
		l.mu.Lock()
		defer l.mu.Unlock()
		if err := l.w.Flush(); err != nil {
			l.f.Close()
			return err
		}
		return l.f.Close()
	}, nil
}

// logCC ghi 1 dòng. "tick" chỉ ghi khi có dữ liệu đang bay hoặc cwnd vừa đổi (rảnh thì không ghi).
func (c *Conn) logCC(now time.Time, event string) {
	l := ccLog.Load()
	if l == nil {
		return
	}

	cwnd := c.cc.Window()
	if event == "tick" && c.bytesInFlight == 0 && cwnd == c.lastLogCwnd {
		return
	}
	c.lastLogCwnd = cwnd

	ssthresh := -1
	if s, ok := c.cc.(interface{ Ssthresh() int }); ok {
		ssthresh = s.Ssthresh()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, "%.1f,%d,%s,%d,%d,%d,%d,%d,%d,%d\n",
		float64(now.Sub(l.start).Microseconds())/1000, c.id, event, cwnd, ssthresh,
		c.rtt.srtt.Microseconds(), c.rto().Milliseconds(), c.bytesInFlight, c.sentPkts, c.lostPkts)

	if now.Sub(l.lastFlush) >= ccLogFlushEvery {
		l.w.Flush()
		l.lastFlush = now
	}
}
