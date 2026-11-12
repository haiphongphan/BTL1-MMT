package transport

// Các phép đo dài để lấy số liệu cho báo cáo. Không chạy mặc định (mất vài phút):
//
//	BTL_LONG=1 go test -run Long -v -timeout 30m ./internal/transport/
//
// Đồ thị cwnd: thêm BTL_CCLOG=cwnd.csv rồi  go run ./scripts/plotcc internal/transport/cwnd.csv > cwnd.svg

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
)

func needLong(t *testing.T) {
	if os.Getenv("BTL_LONG") == "" {
		t.Skip("đặt BTL_LONG=1 để chạy phép đo dài")
	}
}

type result struct {
	size    int
	elapsed time.Duration
	stats   Stats
	chat    []time.Duration // độ trễ từng tin chat gửi trong lúc truyền file
}

// transferWithChat gửi `size` byte trên 1 stream bulk, đồng thời cứ chatEvery lại gửi 1 tin chat mang timestamp.
// chatUrgent = false: chat đi chung hàng đợi bulk với file, tức là giả lập transport KHÔNG có hàng đợi ưu tiên.
func transferWithChat(t *testing.T, nc netCond, size int, chatEvery time.Duration, chatUrgent bool) result {
	t.Helper()
	p := newPair(t, nc)
	data := make([]byte, size)
	rand.Read(data)

	fs := p.cli.OpenStream(false)
	fileID := fs.(*SendStream).id
	go func() {
		fs.Write(data)
		fs.Close()
	}()

	stopChat := make(chan struct{})
	if chatEvery > 0 {
		go func() {
			tk := time.NewTicker(chatEvery)
			defer tk.Stop()
			for {
				select {
				case <-stopChat:
					return
				case <-tk.C:
					ts := binary.BigEndian.AppendUint64(nil, uint64(time.Now().UnixNano()))
					s := p.cli.OpenStream(chatUrgent)
					s.Write(ts)
					s.Close()
				}
			}
		}()
	}
	defer close(stopChat)

	start := time.Now()
	var got []byte
	res := result{size: size}
	chatBuf := map[uint32][]byte{}
	deadline := time.After(20 * time.Minute)
	for {
		select {
		case d := <-p.srv.Recv():
			if d.StreamID == fileID {
				got = append(got, d.Data...)
				if d.Fin {
					res.elapsed = time.Since(start)
					res.stats = p.cli.Stats()
					if !bytes.Equal(got, data) {
						t.Fatalf("dữ liệu sai: %d/%d byte", len(got), len(data))
					}
					return res
				}
				continue
			}
			chatBuf[d.StreamID] = append(chatBuf[d.StreamID], d.Data...)
			if d.Fin {
				sent := time.Unix(0, int64(binary.BigEndian.Uint64(chatBuf[d.StreamID])))
				res.chat = append(res.chat, time.Since(sent))
				delete(chatBuf, d.StreamID)
			}
		case <-deadline:
			t.Fatalf("hết giờ, nhận %d/%d byte", len(got), size)
		}
	}
}

func (r result) String() string {
	return fmt.Sprintf("%v, %.2f MB/s, gửi %d gói, mất %d (%.1f%%), SRTT %v",
		r.elapsed.Round(time.Millisecond), float64(r.size)/1e6/r.elapsed.Seconds(), r.stats.SentPkts, r.stats.LostPkts,
		100*float64(r.stats.LostPkts)/float64(max(r.stats.SentPkts, 1)), r.stats.RTT.Round(10*time.Microsecond))
}

func latencySummary(l []time.Duration) string {
	if len(l) == 0 {
		return "không có mẫu"
	}
	s := slices.Clone(l)
	slices.Sort(s)
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	q := func(p float64) time.Duration { return s[int(p*float64(len(s)-1))].Round(100 * time.Microsecond) }
	return fmt.Sprintf("n=%d  trung bình %v  p50 %v  p95 %v  max %v",
		len(s), (sum / time.Duration(len(s))).Round(100*time.Microsecond), q(0.5), q(0.95), q(1))
}

// Phase 3: stream dài 50 MB qua mạng loss 10% + jitter 20 ms, so sánh 2 thuật toán congestion control.
func TestLong50MB(t *testing.T) {
	needLong(t)
	nc := netCond{loss: 0.1, jitter: 20 * time.Millisecond}

	withCC(t, "fixed")
	r := transferWithChat(t, nc, 50<<20, 0, true)
	t.Logf("[fixed 64KB] 50 MB, loss 10%%, jitter 20ms: %v", r)

	// CUBIC ở loss 10% ngẫu nhiên giữ cwnd sát đáy -> chậm hơn nhiều, đo 5 MB cho đỡ lâu
	withCC(t, "cubic")
	r = transferWithChat(t, nc, 5<<20, 0, true)
	t.Logf("[cubic]       5 MB, loss 10%%, jitter 20ms: %v", r)

	// không mất ngẫu nhiên, chỉ jitter: trước khi sửa LossyConn thì 89%% gói bị coi là mất oan vì đảo thứ tự
	r = transferWithChat(t, netCond{jitter: 20 * time.Millisecond}, 20<<20, 0, true)
	t.Logf("[cubic]      20 MB, loss 0%%,  jitter 20ms: %v", r)
}

// Phase 3: chứng minh không có Head-of-Line blocking. File chạy qua nút cổ chai 1 MB/s (RTT ≈ 40 ms),
// cứ 100 ms gửi 1 tin chat; so sánh chat đi hàng đợi ưu tiên (urgent) với chat xếp chung hàng bulk.
func TestLongHoL(t *testing.T) {
	needLong(t)
	nc := netCond{delay: 20 * time.Millisecond, rate: 1 << 20, queue: 64 << 10, loss: 0.01}
	r := transferWithChat(t, nc, 6<<20, 100*time.Millisecond, true)
	t.Logf("[urgent] file 6 MB: %v", r)
	t.Logf("[urgent] độ trễ chat: %s", latencySummary(r.chat))

	r = transferWithChat(t, nc, 6<<20, 100*time.Millisecond, false)
	t.Logf("[bulk]   file 6 MB: %v", r)
	t.Logf("[bulk]   độ trễ chat: %s", latencySummary(r.chat))

	idle := newPair(t, nc) // mốc: không có file, chỉ có độ trễ đường truyền
	var lat []time.Duration
	for i := 0; i < 20; i++ {
		st := time.Now()
		idle.cli.SendMessage([]byte("x"))
		recvAll(t, idle.srv, 1, 5*time.Second)
		lat = append(lat, time.Since(st))
	}
	t.Logf("[mốc]    độ trễ chat khi không có file: %s", latencySummary(lat))
}

// Phase 4: đồ thị cwnd. Nút cổ chai 2 MB/s, trễ 20 ms mỗi chiều, bộ đệm 64 KB, không mất ngẫu nhiên:
// mọi lần mất gói đều do hàng đợi tràn (nghẽn thật) -> thấy rõ răng cưa và đường bậc 3.
func TestLongCCGraph(t *testing.T) {
	needLong(t)
	nc := netCond{delay: 20 * time.Millisecond, rate: 2 << 20, queue: 64 << 10}

	if path := os.Getenv("BTL_CCLOG"); path != "" {
		closeLog, err := EnableCCLog(path)
		if err != nil {
			t.Fatal(err)
		}
		defer closeLog()
	}

	withCC(t, "cubic")
	r := transferWithChat(t, nc, 60<<20, 0, true)
	t.Logf("[cubic] 60 MB qua link 2 MB/s, RTT ≥ 40 ms: %v", r)

	if os.Getenv("BTL_CCLOG") != "" {
		ccLog.Store(nil) // chỉ vẽ CUBIC
	}
	withCC(t, "fixed")
	r = transferWithChat(t, nc, 20<<20, 0, true)
	t.Logf("[fixed] 20 MB qua link 2 MB/s, RTT ≥ 40 ms: %v", r)
}
