package transport

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

// netCond: điều kiện mạng giả lập, áp dụng cho CẢ 2 chiều.
type netCond struct {
	loss          float64
	delay, jitter time.Duration
	rate, queue   int // nút cổ chai (byte/s, byte); 0 = không giới hạn
}

type pair struct {
	cli, srv *Conn
	mux      *Mux
	cliSock  *LossyConn
	srvAddr  *net.UDPAddr
}

func udpLoopback(t testing.TB) *net.UDPConn {
	t.Helper()
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// withCC chọn thuật toán congestion control cho các Conn tạo trong test này, trả lại mặc định khi xong.
func withCC(t testing.TB, name string) {
	t.Helper()
	if err := SetCongestionControl(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetCongestionControl("cubic") })
}

// newPair: 1 server (Mux) + 1 client (Dial) nối qua loopback, giống hệt cách server/client thật dùng transport.
func newPair(t testing.TB, nc netCond) *pair {
	t.Helper()
	var key [32]byte
	rand.Read(key[:])

	su := udpLoopback(t)
	ss := &LossyConn{UDPConn: su, Loss: nc.loss, Delay: nc.delay, Jitter: nc.jitter, Rate: nc.rate, QueueBytes: nc.queue}
	mux := NewMux(ss)
	srv, err := mux.Accept(77, key)
	if err != nil {
		t.Fatal(err)
	}

	cs := &LossyConn{UDPConn: udpLoopback(t), Loss: nc.loss, Delay: nc.delay, Jitter: nc.jitter, Rate: nc.rate, QueueBytes: nc.queue}
	cli, err := Dial(cs, su.LocalAddr().(*net.UDPAddr), 77, key)
	if err != nil {
		t.Fatal(err)
	}

	p := &pair{cli: cli, srv: srv, mux: mux, cliSock: cs, srvAddr: su.LocalAddr().(*net.UDPAddr)}
	t.Cleanup(func() {
		cli.Close()
		mux.Close()
	})
	return p
}

// recvAll gom dữ liệu theo stream cho tới khi đủ `streams` stream kết thúc (Fin), hoặc hết giờ.
func recvAll(t testing.TB, c *Conn, streams int, timeout time.Duration) map[uint32][]byte {
	t.Helper()
	got := map[uint32][]byte{}
	fins := 0
	deadline := time.After(timeout)
	for fins < streams {
		select {
		case d := <-c.Recv():
			got[d.StreamID] = append(got[d.StreamID], d.Data...)
			if d.Fin {
				fins++
			}
		case <-c.Done():
			t.Fatalf("kết nối đóng giữa chừng (%d/%d stream)", fins, streams)
		case <-deadline:
			t.Fatalf("hết giờ: %d/%d stream xong, stats %+v", fins, streams, c.Stats())
		}
	}
	return got
}

// Yêu cầu Phase 2: 2 Conn qua loopback, loss 20% mỗi chiều, gửi 5 MB -> nhận đủ, đúng thứ tự, đúng từng byte.
// Đồng thời gửi 50 tin urgent để kiểm tra nhiều stream chạy xen kẽ.
// Test này kiểm tra ĐỘ TIN CẬY nên dùng cửa sổ cố định; CUBIC ở loss 20% ngẫu nhiên rất chậm (xem TestTransferCubicLoss10).
func TestTransfer5MBLoss20(t *testing.T) {
	withCC(t, "fixed")
	testTransfer(t, 5<<20, netCond{loss: 0.2})
}

// Cùng kiểm tra đó với CUBIC (mặc định), loss 10%: chậm hơn vì CUBIC coi mỗi lần mất là nghẽn và giảm cwnd.
func TestTransferCubicLoss10(t *testing.T) {
	withCC(t, "cubic")
	testTransfer(t, 1<<20, netCond{loss: 0.1})
}

func testTransfer(t *testing.T, size int, nc netCond) {
	p := newPair(t, nc)

	data := make([]byte, size)
	rand.Read(data)

	fs := p.cli.OpenStream(false) // mở trước khi chạy goroutine để biết chắc id của stream file
	fileID := fs.(*SendStream).id
	go func() {
		fs.Write(data)
		fs.Close()
	}()
	go func() {
		for i := 0; i < 50; i++ {
			p.cli.SendMessage([]byte(fmt.Sprintf("msg-%d", i)))
		}
	}()

	start := time.Now()
	got := recvAll(t, p.srv, 51, 60*time.Second)

	if !bytes.Equal(got[fileID], data) {
		t.Fatalf("file nhận sai: %d/%d byte", len(got[fileID]), len(data))
	}
	msgs := map[string]bool{}
	for id, b := range got {
		if id != fileID {
			msgs[string(b)] = true
		}
	}
	if len(msgs) != 50 {
		t.Fatalf("nhận %d/50 tin khác nhau", len(msgs))
	}

	st := p.cli.Stats()
	t.Logf("%d KB, loss %.0f%%: %v, gửi %d gói, mất %d (%.1f%%), RTT %v, cwnd %d",
		size>>10, nc.loss*100, time.Since(start).Round(time.Millisecond), st.SentPkts, st.LostPkts,
		100*float64(st.LostPkts)/float64(st.SentPkts), st.RTT, st.Cwnd)
}

// Chiều server -> client (stream id chẵn) cũng hoạt động.
func TestServerToClientMessages(t *testing.T) {
	p := newPair(t, netCond{loss: 0.2})

	// server chỉ gửi được sau khi biết địa chỉ UDP của client (PING đầu tiên)
	deadline := time.Now().Add(5 * time.Second)
	for p.srv.Stats().State != "connected" {
		if time.Now().After(deadline) {
			t.Fatal("server không học được địa chỉ client")
		}
		time.Sleep(5 * time.Millisecond)
	}

	for i := 0; i < 100; i++ {
		if err := p.srv.SendMessage([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	got := recvAll(t, p.cli, 100, 30*time.Second)
	seen := map[byte]bool{}
	for id, b := range got {
		if id%2 != 0 {
			t.Fatalf("stream id của server phải chẵn, got %d", id)
		}
		seen[b[0]] = true
	}
	if len(seen) != 100 {
		t.Fatalf("nhận %d/100 tin", len(seen))
	}
}

// recordConn ghi lại mọi gói gửi đi để kiểm tra nội dung trên dây.
type recordConn struct {
	*net.UDPConn
	mu   sync.Mutex
	pkts [][]byte
}

func (r *recordConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	r.mu.Lock()
	r.pkts = append(r.pkts, append([]byte(nil), b...))
	r.mu.Unlock()
	return r.UDPConn.WriteToUDP(b, addr)
}

// Step 8: mọi gói UDP đều mã hóa. Gửi chuỗi đánh dấu rồi tìm trong mọi byte đã lên dây: không được thấy.
func TestWireIsEncrypted(t *testing.T) {
	var key [32]byte
	rand.Read(key[:])
	su := udpLoopback(t)
	mux := NewMux(su)
	defer mux.Close()
	srv, _ := mux.Accept(5, key)

	rec := &recordConn{UDPConn: udpLoopback(t)}
	cli, err := Dial(rec, su.LocalAddr().(*net.UDPAddr), 5, key)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	marker := []byte("MAT_KHAU_BI_MAT_123456")
	cli.SendMessage(append(bytes.Repeat([]byte("-"), 100), marker...))
	recvAll(t, srv, 1, 5*time.Second)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.pkts) == 0 {
		t.Fatal("không ghi được gói nào")
	}
	for i, pkt := range rec.pkts {
		if bytes.Contains(pkt, marker) {
			t.Fatalf("gói %d chứa plaintext", i)
		}
		if binary.BigEndian.Uint16(pkt) != protocol.Magic || len(pkt) > protocol.MaxPacket {
			t.Fatalf("gói %d sai định dạng: % x", i, pkt[:min(len(pkt), 12)])
		}
	}
}

// Close() gửi CLOSE -> đầu kia biết ngay, không phải đợi 10s idle timeout.
func TestCloseNotifiesPeer(t *testing.T) {
	p := newPair(t, netCond{})
	p.cli.SendMessage([]byte("hi"))
	recvAll(t, p.srv, 1, 5*time.Second)

	p.cli.Close()
	select {
	case <-p.srv.Done():
	case <-time.After(time.Second):
		t.Fatal("server không nhận được CLOSE")
	}
	if err := p.cli.SendMessage([]byte("x")); err == nil {
		t.Fatal("gửi sau Close phải báo lỗi")
	}
}

// Mux.Close() đóng mọi Conn và báo cho client.
func TestMuxCloseClosesConns(t *testing.T) {
	p := newPair(t, netCond{})
	p.cli.SendMessage([]byte("hi"))
	recvAll(t, p.srv, 1, 5*time.Second)

	p.mux.Close()
	for name, c := range map[string]*Conn{"server": p.srv, "client": p.cli} {
		select {
		case <-c.Done():
		case <-time.After(time.Second):
			t.Fatalf("%s conn vẫn sống sau Mux.Close()", name)
		}
	}
}

// Step 7: client chết đột ngột (không gửi CLOSE) -> server phát hiện sau ~idleTimeout.
func TestIdleTimeoutDetectsDeadPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("mất ~10s")
	}
	p := newPair(t, netCond{})
	p.cli.SendMessage([]byte("hi"))
	recvAll(t, p.srv, 1, 5*time.Second)

	p.cliSock.UDPConn.Close() // rút dây: socket chết, không kịp gửi CLOSE
	start := time.Now()
	select {
	case <-p.srv.Done():
		if el := time.Since(start); el < idleTimeout-time.Second {
			t.Fatalf("đóng quá sớm: %v", el)
		}
	case <-time.After(idleTimeout + 3*time.Second):
		t.Fatal("server không phát hiện client chết")
	}
}

// ---------- Unit test trên state của Conn (không chạy run(), gọi thẳng hàm như run() gọi) ----------

func bareConn() *Conn {
	return &Conn{unacked: map[uint32]*sentPacket{}, cc: newCubic(), peerWindow: recvBufMax}
}

func (c *Conn) fakeSent(pn uint32, at time.Time, off uint32) {
	c.unacked[pn] = &sentPacket{sentAt: at, size: 1000, frames: []*protocol.StreamFrame{{StreamID: 1, Offset: off}}}
	c.bytesInFlight += 1000
}

func TestRTOBackoff(t *testing.T) {
	c := bareConn()
	t0 := time.Unix(1000, 0)

	c.fakeSent(0, t0, 0)
	c.detectLoss(t0.Add(initialRTO - time.Millisecond))
	if len(c.unacked) != 1 {
		t.Fatal("chưa hết RTO đã coi là mất")
	}
	c.detectLoss(t0.Add(initialRTO + time.Millisecond))
	if len(c.unacked) != 0 || c.rtoBackoff != 1 || c.rto() != 2*initialRTO {
		t.Fatalf("sau timeout 1: unacked=%d backoff=%d rto=%v", len(c.unacked), c.rtoBackoff, c.rto())
	}

	// gói gửi lại cũng mất: phải chờ 2·RTO mới coi là mất, rồi RTO lại gấp đôi
	t1 := t0.Add(time.Second)
	c.fakeSent(1, t1, 0)
	c.detectLoss(t1.Add(2*initialRTO - time.Millisecond))
	if len(c.unacked) != 1 {
		t.Fatal("backoff không có tác dụng")
	}
	c.detectLoss(t1.Add(2*initialRTO + time.Millisecond))
	if c.rtoBackoff != 2 || c.rto() != 4*initialRTO {
		t.Fatalf("sau timeout 2: backoff=%d rto=%v", c.rtoBackoff, c.rto())
	}

	// nhiều lần nữa: chặn trần maxRTO
	for i := 0; i < 20; i++ {
		c.fakeSent(uint32(2+i), t1, 0)
		c.detectLoss(t1.Add(time.Hour))
	}
	if c.rto() != maxRTO {
		t.Fatalf("rto = %v, want trần %v", c.rto(), maxRTO)
	}

	// có ACK cho gói mới -> reset
	c.fakeSent(100, t1.Add(time.Hour), 0)
	c.onAck(&protocol.AckFrame{Window: recvBufMax, Ranges: []protocol.AckRange{{Start: 100, End: 100}}}, t1.Add(time.Hour+50*time.Millisecond))
	if c.rtoBackoff != 0 {
		t.Fatalf("có ACK mà backoff = %d", c.rtoBackoff)
	}
}

// Mất do bị "vượt mặt" (mạng vẫn sống) không được tăng backoff.
func TestLossByCountNoBackoff(t *testing.T) {
	c := bareConn()
	now := time.Unix(1000, 0)
	for pn := uint32(0); pn < 5; pn++ {
		c.fakeSent(pn, now, pn*1000)
	}
	c.onAck(&protocol.AckFrame{Window: recvBufMax, Ranges: []protocol.AckRange{{Start: 3, End: 4}}}, now.Add(time.Millisecond))
	if len(c.retransQ) != 2 { // gói 0, 1: bị gói 3, 4 vượt mặt ≥ 3 bậc. Gói 2 chưa (2+3 > 4)
		t.Fatalf("retransQ = %d frame, want 2", len(c.retransQ))
	}
	if c.rtoBackoff != 0 {
		t.Fatal("mất theo số đếm không được tăng backoff")
	}
}

// Gửi lại phải theo thứ tự packet_num (map duyệt ngẫu nhiên nên trước đây retransQ lộn xộn).
func TestRetransmitInPacketOrder(t *testing.T) {
	for trial := 0; trial < 20; trial++ { // lặp vì thứ tự duyệt map là ngẫu nhiên
		c := bareConn()
		t0 := time.Unix(1000, 0)
		for pn := uint32(0); pn < 30; pn++ {
			c.fakeSent(pn, t0, pn*1000)
		}
		c.detectLoss(t0.Add(time.Hour))
		for i, f := range c.retransQ {
			if f.Offset != uint32(i)*1000 {
				t.Fatalf("retransQ[%d].Offset = %d, want %d", i, f.Offset, i*1000)
			}
		}
	}
}

// ACK cũ tới muộn không được ghi đè cửa sổ nhận mới hơn.
func TestStaleAckDoesNotShrinkWindow(t *testing.T) {
	c := bareConn()
	now := time.Unix(1000, 0)
	c.onAck(&protocol.AckFrame{Window: 900_000, Ranges: []protocol.AckRange{{Start: 0, End: 10}}}, now)
	c.onAck(&protocol.AckFrame{Window: 100_000, Ranges: []protocol.AckRange{{Start: 0, End: 5}}}, now) // ACK cũ
	if c.peerWindow != 900_000 {
		t.Fatalf("peerWindow = %d, ACK cũ đã ghi đè", c.peerWindow)
	}
	c.onAck(&protocol.AckFrame{Window: 200_000, Ranges: []protocol.AckRange{{Start: 0, End: 10}}}, now) // window update, cùng largest
	if c.peerWindow != 200_000 {
		t.Fatalf("peerWindow = %d, window update bị bỏ qua", c.peerWindow)
	}
}

// inversions gửi n gói đánh số qua LossyConn rồi đếm số gói tới sau 1 gói có số lớn hơn nó.
func inversions(t *testing.T, l *LossyConn, n int) (inv, got int) {
	t.Helper()
	recv := udpLoopback(t)
	defer recv.Close()
	defer l.Close()

	for i := 0; i < n; i++ {
		l.WriteToUDP(binary.BigEndian.AppendUint32(nil, uint32(i)), recv.LocalAddr().(*net.UDPAddr))
	}

	buf := make([]byte, 16)
	maxSeen := -1
	for {
		recv.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		k, _, err := recv.ReadFromUDP(buf)
		if err != nil {
			return inv, got
		}
		i := int(binary.BigEndian.Uint32(buf[:k]))
		if i < maxSeen {
			inv++
		}
		maxSeen = max(maxSeen, i)
		got++
	}
}

// LossyConn với jitter giữ thứ tự FIFO như hàng đợi router thật; Reorder = true thì đảo thứ tự hàng loạt.
// Goroutine phát của LossyConn gửi đúng thứ tự tuyệt đối (đã kiểm chứng bằng hook), nhưng loopback của kernel
// (đặc biệt WSL2, nhiều CPU) đôi khi tự giao lệch vài gói, nên chỉ so sánh tương đối 2 chế độ.
func TestLossyJitterKeepsOrder(t *testing.T) {
	const n = 300
	fifo, got := inversions(t, &LossyConn{UDPConn: udpLoopback(t), Delay: 2 * time.Millisecond, Jitter: 10 * time.Millisecond}, n)
	if got < n/2 {
		t.Fatalf("chỉ nhận %d/%d gói", got, n)
	}
	reorder, _ := inversions(t, &LossyConn{UDPConn: udpLoopback(t), Delay: 2 * time.Millisecond, Jitter: 10 * time.Millisecond, Reorder: true}, n)

	t.Logf("gói bị đảo thứ tự: FIFO %d/%d, Reorder %d/%d", fifo, got, reorder, n)
	if reorder < n/2 {
		t.Fatalf("Reorder chỉ đảo %d/%d gói, mong đợi phần lớn", reorder, n)
	}
	if fifo*10 > reorder {
		t.Fatalf("FIFO đảo %d gói, không ít hơn hẳn chế độ Reorder (%d)", fifo, reorder)
	}
}

// Nút cổ chai: gửi dồn 100 gói 1000 byte vào link 100 KB/s với bộ đệm 20 KB -> chỉ khoảng 20 gói lọt, còn lại tail drop.
func TestLossyBottleneckDrops(t *testing.T) {
	recv := udpLoopback(t)
	defer recv.Close()
	l := &LossyConn{UDPConn: udpLoopback(t), Rate: 100_000, QueueBytes: 20_000}
	defer l.Close()

	for i := 0; i < 100; i++ {
		l.WriteToUDP(make([]byte, 1000), recv.LocalAddr().(*net.UDPAddr))
	}

	got := 0
	buf := make([]byte, 2000)
	start := time.Now()
	for {
		recv.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if _, _, err := recv.ReadFromUDP(buf); err != nil {
			break
		}
		got++
	}
	if got < 15 || got > 21 {
		t.Fatalf("lọt %d gói, want ≈ 20 (bộ đệm 20 KB)", got)
	}
	// 20 KB qua link 100 KB/s mất ≈ 200 ms
	if el := time.Since(start) - 300*time.Millisecond; el < 150*time.Millisecond {
		t.Fatalf("giao xong sau %v: không giới hạn băng thông", el)
	}
}
