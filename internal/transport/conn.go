package transport

import (
	"crypto/cipher"
	"fmt"
	"log"
	"net"
	"slices"
	"sync/atomic"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

const (
	tickInterval = 10 * time.Millisecond
	// idleTickInterval: nhịp đồng hồ khi không có gói nào chờ ACK. Chỉ còn việc keep-alive (2 s) / idle timeout (10 s)
	// nên không cần 10 ms; tick 10 ms cho mọi kết nối rảnh làm Go scheduler thức dậy liên tục (đo được: server với
	// 20 client không làm gì tốn 43% CPU, gần như toàn bộ là futex/findRunnable).
	idleTickInterval = 100 * time.Millisecond
	pingInterval     = 2 * time.Second  // ping every 2s when free
	idleTimeout      = 10 * time.Second // quiet 10s -> terminate
	recvBufMax       = 1 << 20          // max size for receiving window is 1MB
	maxBulkQueue     = 256              // stop receiving new frame file
	lossThreshold    = 3                // surpass by packet >= pn + 3 acked packet
	maxAckRanges     = 8
	ackFrameMax      = 1 + 4 + 1 + maxAckRanges*8 // = 70 bytes each frame
	streamFrameHdr   = 12
	closedStreamTTL  = 30 * time.Second
	maxRTO           = 5 * time.Second // trần của RTO khi backoff
	maxBackoffShift  = 6               // 200ms << 6 = 12.8s > maxRTO, đủ để chạm trần và không tràn số
	// firstProbe: client gửi lại PING đầu tiên sau khoảng này (nhân đôi mỗi lần, tối đa pingInterval) cho tới khi nghe
	// được server. Server chỉ biết địa chỉ UDP của client khi nhận được gói đầu tiên; nếu PING đầu bị mất mà phải đợi
	// tới PING keep-alive (2 s) thì client mới kết nối bị "điếc" 2 s, 4 s... (giống QUIC gửi lại gói Initial).
	firstProbe = 200 * time.Millisecond

	dirClientToServer byte = 0
	dirServerToClient byte = 1
)

type congestionController interface {
	Window() int                    // how many bytes are "in flight"/unacked
	OnAck(acked int, now time.Time) //
	OnLoss(now time.Time)
}

type fixedWindow struct{}

func (fixedWindow) Window() int {
	return 64 * 1024
}

func (fixedWindow) OnAck(acked int, now time.Time) {}
func (fixedWindow) OnLoss(now time.Time)           {}

// ccAlgo: thuật toán congestion control cho các Conn tạo ra sau này ("cubic" mặc định, hoặc "fixed").
var ccAlgo atomic.Value

// SetCongestionControl chọn thuật toán congestion control: "cubic" (mặc định) hoặc "fixed" (cửa sổ cố định 64 KB).
// "fixed" không giảm tốc khi mất gói -> nhanh hơn nhiều khi giả lập mất gói ngẫu nhiên (-loss), dùng để so sánh.
func SetCongestionControl(name string) error {
	switch name {
	case "cubic", "fixed":
		ccAlgo.Store(name)
		return nil
	}
	return fmt.Errorf("transport: congestion control %q không hỗ trợ (cubic | fixed)", name)
}

func newCongestionController() congestionController {
	if name, _ := ccAlgo.Load().(string); name == "fixed" {
		return fixedWindow{}
	}
	return newCubic()
}

type inPacket struct {
	raw  []byte
	addr *net.UDPAddr
}

type sentPacket struct {
	sentAt time.Time
	size   int
	frames []*protocol.StreamFrame // keep for later use if this packet is lost
}

type Conn struct {
	id               uint32
	sock             PacketConn
	aead             cipher.AEAD
	sendDir, recvDir byte

	//==========Communication Channel With Other Goroutines========
	in       chan inPacket              // raw packet from readLoop
	urgentCh chan *protocol.StreamFrame // Chat frame -> always received
	bulkCh   chan *protocol.StreamFrame // File frame -> blocked if bulkQ is full
	recvCh   chan Delivery              // deliver data to app
	closeReq chan struct{}              // app calls Close()
	done     chan struct{}              // close when done

	stats      atomic.Pointer[Stats] // app reads Stats() without lock
	nextStream atomic.Uint32

	// ========Only For Goroutine run()==========
	peer          *net.UDPAddr // nil until server receives the first packet from client
	nextPN        uint32
	unacked       map[uint32]*sentPacket
	retransQ      []*protocol.StreamFrame
	urgentQ       []*protocol.StreamFrame
	bulkQ         []*protocol.StreamFrame
	largestAcked  uint32
	haveAcked     bool
	recvd         ackTracker
	ackNeeded     bool
	pingNeeded    bool
	streams       map[uint32]*recvStream
	closedStreams map[uint32]time.Time
	outQ          []Delivery // wait to ship to app (run() never blocks because of slow app)
	outBytes      int        // total bytes in outQ
	pendingBytes  int        // total bytes in pending of every recvStream
	lastAdvWindow int        // receive window that last broadcast
	rtt           rttEstimator
	cc            congestionController
	bytesInFlight int
	peerWindow    int
	winAckPN      uint32 // packet_num lớn nhất của ACK đã cập nhật peerWindow
	haveWinAck    bool
	rtoBackoff    int // số lần timeout liên tiếp; RTO hiệu dụng = rto << rtoBackoff
	lastLogCwnd   int // cwnd lần ghi cclog gần nhất
	recoveryStart time.Time
	lastHeard     time.Time
	lastSent      time.Time
	heardPeer     bool // đã nhận được ít nhất 1 gói hợp lệ từ bên kia
	probes        int  // số lần client đã gửi lại PING đầu tiên khi chưa nghe được server
	isClient      bool
	sentPkts      uint64
	lostPkts      uint64
	closed        bool
}

var _ Endpoint = (*Conn)(nil)

func newConn(sock PacketConn, peer *net.UDPAddr, id uint32, key [32]byte, isServer bool) (*Conn, error) {
	aead, err := NewAEAD(key)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	c := &Conn{
		id:            id,
		sock:          sock,
		aead:          aead,
		peer:          peer,
		in:            make(chan inPacket, 256),
		urgentCh:      make(chan *protocol.StreamFrame, 64),
		bulkCh:        make(chan *protocol.StreamFrame, 64),
		recvCh:        make(chan Delivery, 64),
		closeReq:      make(chan struct{}),
		done:          make(chan struct{}),
		unacked:       map[uint32]*sentPacket{},
		recvd:         newAcKTracker(),
		streams:       map[uint32]*recvStream{},
		closedStreams: map[uint32]time.Time{},
		cc:            newCongestionController(),
		peerWindow:    recvBufMax, // pretend the other side has 1 MB until the first ack
		lastAdvWindow: recvBufMax,
		lastHeard:     now,
		lastSent:      now,
	}

	if isServer {
		c.sendDir, c.recvDir = dirServerToClient, dirClientToServer
		// nextStream = 0 -> Add(2) for 2, 4, 6, ...
	} else {
		c.sendDir, c.recvDir = dirClientToServer, dirServerToClient
		c.nextStream.Store(^uint32(0)) // = -1 -> Add(2) for 1, 3, 5, ...
		c.pingNeeded = true            // first pack for server to know client's UDP addr
		c.isClient = true
	}
	c.publishStats()
	go c.run()
	return c, nil
}

/* ========== API endpoint for app ==========*/
func (c *Conn) Recv() <-chan Delivery {
	return c.recvCh
}

func (c *Conn) Done() <-chan struct{} {
	return c.done
}

func (c *Conn) Stats() Stats {
	return *c.stats.Load()
}

func (c *Conn) ID() uint32 {
	return c.id
}

// Close sends CLOSE to the other end then stops
func (c *Conn) Close() error {
	select {
	case c.closeReq <- struct{}{}:
	case <-c.done:
	}
	<-c.done
	return nil
}

// ========== Main Loop ===========
func (c *Conn) run() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	fastTick := true

	for !c.closed {
		bulkCh := c.bulkCh
		if len(c.bulkQ) >= maxBulkQueue {
			bulkCh = nil // full -> stop receiving, app's write will be blocked
		}

		var recvCh chan Delivery
		var head Delivery

		if len(c.outQ) > 0 {
			recvCh, head = c.recvCh, c.outQ[0]
		}

		select {
		case p := <-c.in:
			c.handlePacket(p, time.Now())
			c.drainIn() // process all the pending packets, merge many acks

		case f := <-c.urgentCh:
			c.urgentQ = append(c.urgentQ, f)

		case f := <-bulkCh:
			c.bulkQ = append(c.bulkQ, f)

		case recvCh <- head:
			c.outQ[0] = Delivery{} // delete reference for GB to clean
			c.outQ = c.outQ[1:]
			c.outBytes -= len(head.Data)
			c.maybeWindowUpdate()

		case now := <-ticker.C:
			c.detectLoss(now)
			c.keepAlive(now)
			c.cleanup(now)
			c.publishStats()
			c.logCC(now, "tick")

		case <-c.closeReq:
			c.sendClose("bye")
			c.shutdown()
		}

		if !c.closed {
			c.flush(time.Now()) // send the remaining + ACK where needed
		}

		// có gói chờ ACK (cần phát hiện mất kịp thời) hoặc client còn đang dò server ⇒ tick nhanh; rảnh ⇒ tick chậm
		if busy := len(c.unacked) > 0 || (c.isClient && !c.heardPeer); busy != fastTick {
			fastTick = busy
			if busy {
				ticker.Reset(tickInterval)
			} else {
				ticker.Reset(idleTickInterval)
			}
		}
	}

	c.publishStats()
}

func (c *Conn) drainIn() {
	for i := 0; i < 32 && !c.closed; i++ {
		select {
		case p := <-c.in:
			c.handlePacket(p, time.Now())

		default:
			return
		}
	}
}

func (c *Conn) shutdown() {
	if c.closed {
		return
	}

	c.closed = true
	close(c.done) // only run() calls shutdown -> never close 2 times
}

// ========== Receiver ==========
func (c *Conn) handlePacket(p inPacket, now time.Time) {
	if c.closed {
		return
	}

	h, err := protocol.ParseHeader(p.raw)

	if err != nil || h.Magic != protocol.Magic || h.Version != protocol.Version || h.ConnID != c.id {
		return
	}

	// header (first 12 bytes) is AAD: who changes head -> Open calls err
	plain, err := c.aead.Open(nil, makeNonce(c.id, h.PacketNum, c.recvDir), p.raw[protocol.HeaderSize:], p.raw[:protocol.HeaderSize])

	if err != nil {
		return
	}

	frames, err := protocol.ParseFrames(plain)

	if err != nil {
		return
	}

	c.lastHeard = now
	c.heardPeer = true
	isNew := c.recvd.add(h.PacketNum)

	// only change addr if this is the newest packet -> bad guy sent packet from the old IP cannot steal connection
	// Client changes wifi -> 4g can continue normally
	if isNew && h.PacketNum == c.recvd.largest {
		c.peer = p.addr
	}

	for _, f := range frames {
		switch f := f.(type) {
		case *protocol.AckFrame:
			c.onAck(f, now)

		case *protocol.StreamFrame:
			c.ackNeeded = true // kể cả gói trùng cũng ACK lại (ACK trước có thể đã mất)
			if isNew {
				c.onStreamFrame(f, now)
			}

		case *protocol.PingFrame:
			c.ackNeeded = true

		case *protocol.CloseFrame:
			c.shutdown()
			return
		}
	}
}

func (c *Conn) onAck(a *protocol.AckFrame, now time.Time) {
	// ACK cũ đến muộn không được ghi đè cửa sổ mới hơn. Ranges[0].End = packet_num lớn nhất mà ACK này xác nhận.
	if len(a.Ranges) == 0 || !c.haveWinAck || a.Ranges[0].End >= c.winAckPN {
		c.peerWindow = int(a.Window)
		if len(a.Ranges) > 0 {
			c.winAckPN, c.haveWinAck = a.Ranges[0].End, true
		}
	}
	var newest uint32
	var newestPkt *sentPacket

	for pn, sp := range c.unacked {
		if !a.Contains(pn) {
			continue
		}

		delete(c.unacked, pn)
		c.bytesInFlight -= sp.size
		c.cc.OnAck(sp.size, now)

		if newestPkt == nil || pn > newest {
			newest, newestPkt = pn, sp
		}
	}

	if newestPkt != nil {
		c.rtoBackoff = 0 // có ACK mới -> mạng còn sống -> RTO về bình thường
	}

	if newestPkt != nil && (!c.haveAcked || newest > c.largestAcked) {
		c.largestAcked, c.haveAcked = newest, true
		c.rtt.update(now.Sub(newestPkt.sentAt)) // only measure RTT from the newest acked packet
	}

	c.detectLoss(now)
}

func (c *Conn) onStreamFrame(f *protocol.StreamFrame, now time.Time) {
	if _, ok := c.closedStreams[f.StreamID]; ok {
		return // finished shipping stream, this is retransmission
	}

	s := c.streams[f.StreamID]
	if s == nil {
		s = newRecvStream()
		c.streams[f.StreamID] = s
	}

	before := s.buffered
	chunks, done := s.push(f.Offset, f.Data, f.Fin)
	c.pendingBytes += s.buffered - before

	for i, d := range chunks {
		c.queueDelivery(Delivery{
			StreamID: f.StreamID,
			Data:     d,
			Fin:      done && i == len(chunks)-1,
		})
	}

	if done {
		if len(chunks) == 0 { // Fin comes in last, no data attached
			c.queueDelivery(Delivery{
				StreamID: f.StreamID,
				Fin:      true,
			})
		}

		delete(c.streams, f.StreamID)
		c.closedStreams[f.StreamID] = now
	}
}

func (c *Conn) queueDelivery(d Delivery) {
	c.outQ = append(c.outQ, d)
	c.outBytes += len(d.Data)
}

// recvWindow: remaining slots for data (flow control)
func (c *Conn) recvWindow() int {
	return max(recvBufMax-c.outBytes-c.pendingBytes, 0)
}

func (c *Conn) maybeWindowUpdate() {
	if c.lastAdvWindow < recvBufMax/4 && c.recvWindow() >= recvBufMax/2 {
		c.ackNeeded = true
	}
}

/*========== Clock =========== */
func (c *Conn) detectLoss(now time.Time) {
	rto := c.rto()
	lossDelay := c.rtt.lossDelay()
	var lost []uint32
	timedOut := false

	for pn, sp := range c.unacked {
		age := now.Sub(sp.sentAt)
		overtaken := c.haveAcked && pn < c.largestAcked // đã có gói gửi sau nó được ACK
		byCount := overtaken && pn+lossThreshold <= c.largestAcked
		byDelay := overtaken && age > lossDelay // cwnd nhỏ (< 4 gói) thì không bao giờ đủ 3 gói vượt mặt
		byTime := age > rto

		if !byCount && !byDelay && !byTime {
			continue
		}

		if !byCount && !byDelay {
			timedOut = true // mất vì hết giờ (không có ACK nào "vượt mặt") -> dấu hiệu mạng chết
		}
		lost = append(lost, pn)
	}

	if len(lost) == 0 {
		return
	}

	// map duyệt ngẫu nhiên -> sort để gửi lại theo đúng thứ tự gửi ban đầu (offset nhỏ tới trước)
	slices.Sort(lost)

	for _, pn := range lost {
		sp := c.unacked[pn]
		delete(c.unacked, pn)
		c.bytesInFlight -= sp.size
		c.lostPkts++
		c.retransQ = append(c.retransQ, sp.frames...) // only retransmit the package's lost frame

		if sp.sentAt.After(c.recoveryStart) { // each "lost batch" decreases cwnd by 1
			c.cc.OnLoss(now)
			c.recoveryStart = now
			c.logCC(now, "loss")
		}
	}

	// nhiều gói hết giờ trong cùng 1 lượt chỉ tính là 1 lần timeout
	if timedOut && c.rtoBackoff < maxBackoffShift {
		c.rtoBackoff++
	}
}

// rto: RTO theo RFC 6298, nhân đôi sau mỗi lần timeout liên tiếp (exponential backoff), tối đa maxRTO.
func (c *Conn) rto() time.Duration {
	return min(c.rtt.rto()<<c.rtoBackoff, maxRTO)
}

func (c *Conn) keepAlive(now time.Time) {
	if now.Sub(c.lastHeard) > idleTimeout {
		log.Printf("transport: conn %d timeout (silence in %v)", c.id, idleTimeout)
		c.shutdown()
		return
	}

	if now.Sub(c.lastSent) >= pingInterval {
		c.pingNeeded = true
	}

	// client chưa nghe được server: PING đầu (hoặc ACK của nó) có thể đã mất ⇒ gửi lại sớm, không đợi 2 s
	if c.isClient && !c.heardPeer && now.Sub(c.lastSent) >= min(firstProbe<<min(c.probes, 4), pingInterval) {
		c.pingNeeded = true
		c.probes++
	}
}

func (c *Conn) cleanup(now time.Time) {
	c.recvd.prune()
	for id, t := range c.closedStreams {
		if now.Sub(t) > closedStreamTTL {
			delete(c.closedStreams, id)
		}
	}
}

func (c *Conn) publishStats() {
	st := &Stats{
		RTT:      c.rtt.srtt,
		Cwnd:     c.cc.Window(),
		InFlight: c.bytesInFlight,
		LostPkts: c.lostPkts,
		SentPkts: c.sentPkts,
		State:    "connected",
	}

	if c.closed {
		st.State = "closed"
	} else if c.peer == nil {
		st.State = "waiting"
	}

	c.stats.Store(st)
}

// ========== Sender ==========
func (c *Conn) flush(now time.Time) {
	if c.peer == nil {
		return // server hasn's known client's addr
	}

	// rwnd = min(cwnd, other_rwnd) - inFlightBytes
	for min(c.cc.Window(), c.peerWindow)-c.bytesInFlight >= protocol.MaxPacket {
		frames := c.takeFrames()

		if len(frames) == 0 {
			break
		}

		c.sendPacket(frames, now)
	}

	if c.ackNeeded || c.pingNeeded { // no data, just ack/ping
		c.sendPacket(nil, now)
	}
}

// takeFrames takes frame for 1 packet, in priority order retransQ -> urgentQ ->bulkQ
func (c *Conn) takeFrames() []*protocol.StreamFrame {
	room := protocol.MaxFramePayload - ackFrameMax // save for ACK
	var out []*protocol.StreamFrame
	for _, q := range []*[]*protocol.StreamFrame{&c.retransQ, &c.urgentQ, &c.bulkQ} {
		for len(*q) > 0 {
			f := (*q)[0]

			size := streamFrameHdr + len(f.Data)
			if size > room {
				return out // packet is full
			}

			room -= size
			out = append(out, f)
			(*q)[0] = nil
			*q = (*q)[1:]
		}
	}

	return out
}

func (c *Conn) sendPacket(frames []*protocol.StreamFrame, now time.Time) {
	pn := c.nextPN
	c.nextPN++
	payload := make([]byte, 0, protocol.MaxFramePayload)

	if c.ackNeeded {
		w := c.recvWindow()
		payload = protocol.AppendAck(payload, &protocol.AckFrame{
			Window: uint32(w),
			Ranges: c.recvd.ranges(maxAckRanges),
		})
		c.ackNeeded = false
		c.lastAdvWindow = w
	}

	for _, f := range frames {
		payload = protocol.AppendStream(payload, f)
	}

	if c.pingNeeded {
		if len(frames) == 0 { // packet that has data always requires ack, no ping needed
			payload = protocol.AppendPing(payload)
		}

		c.pingNeeded = false
	}

	pkt := c.seal(pn, payload)

	if _, err := c.sock.WriteToUDP(pkt, c.peer); err != nil {
		log.Printf("transport: conn %d send packet: %v", c.id, err)
	}

	c.lastSent = now
	c.sentPkts++

	if len(frames) > 0 { // packet that has only ack/ping -> no need to track, no retransmit
		c.unacked[pn] = &sentPacket{
			sentAt: now,
			size:   len(pkt),
			frames: frames,
		}
		c.bytesInFlight += len(pkt)
	}
}

func (c *Conn) sendClose(reason string) {
	if c.peer == nil {
		return
	}

	pn := c.nextPN
	c.nextPN++
	payload := protocol.AppendClose(nil, &protocol.CloseFrame{
		Reason: reason,
	})
	c.sock.WriteToUDP(c.seal(pn, payload), c.peer) // best effort, the other side process if lost
}

func (c *Conn) seal(pn uint32, payload []byte) []byte {
	hdr := make([]byte, protocol.HeaderSize)
	(&protocol.Header{
		Magic:     protocol.Magic,
		Version:   protocol.Version,
		ConnID:    c.id,
		PacketNum: pn,
	}).Marshal(hdr)
	dst := append(make([]byte, 0, len(hdr)+len(payload)+protocol.TagSize), hdr...)
	return c.aead.Seal(dst, makeNonce(c.id, pn, c.sendDir), payload, hdr) // dst and hdr are on different heap
}
