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

// Kiến trúc của 1 Conn:
//
//	app goroutine ── SendStream.Write ──► urgentCh / bulkCh ──┐
//	app goroutine ── Close ────────────► closeReq ────────────┤
//	readLoop (udpmux.go) ── gói thô ────► in ─────────────────┼──► run()  (1 goroutine duy nhất)
//	ticker 10 ms / 100 ms ────────────────────────────────────┘      │
//	                                                                 ├──► sock.WriteToUDP (gói đã mã hóa)
//	app goroutine ◄── Recv() ◄── recvCh ◄── outQ ◄───────────────────┘
//
// Mọi trạng thái của kết nối (hàng đợi gửi, gói chờ ACK, cwnd, RTT, các stream đang nhận...) chỉ do run()
// đọc/ghi, nên không cần mutex; goroutine khác chỉ giao tiếp với run() qua channel. Riêng Stats được
// run() chép ra 1 bản (atomic.Pointer) để app đọc mà không phải hỏi run().

const (
	// tickInterval: nhịp đồng hồ khi có gói chờ ACK, để phát hiện mất gói / hết RTO kịp thời.
	tickInterval = 10 * time.Millisecond
	// idleTickInterval: nhịp đồng hồ khi không có gói nào chờ ACK. Chỉ còn việc keep-alive (2 s) / idle timeout (10 s)
	// nên không cần 10 ms; tick 10 ms cho mọi kết nối rảnh làm Go scheduler thức dậy liên tục (đo được: server với
	// 20 client không làm gì tốn 43% CPU, gần như toàn bộ là futex/findRunnable).
	idleTickInterval = 100 * time.Millisecond
	pingInterval     = 2 * time.Second  // không gửi gì trong 2 s thì gửi PING để giữ kết nối
	idleTimeout      = 10 * time.Second // không nhận được gói hợp lệ nào trong 10 s -> coi kết nối đã chết
	recvBufMax       = 1 << 20          // cửa sổ nhận tối đa 1 MB (flow control)
	// maxBulkQueue: số frame file tối đa chờ trong bulkQ. Đầy thì run() ngừng nhận từ bulkCh, nên Write của
	// app bị chặn (backpressure): đọc file nhanh hơn mạng gửi được thì không làm phình RAM.
	maxBulkQueue  = 256
	lossThreshold = 3 // gói pn bị coi là mất khi đã có gói số ≥ pn+3 được ACK (RFC 9002 kPacketThreshold)
	maxAckRanges  = 8 // số range tối đa trong 1 ACK frame
	// ackFrameMax = 6 + 8×8 = 70 byte: chỗ luôn chừa sẵn trong mỗi gói dữ liệu để ghép thêm ACK.
	ackFrameMax    = protocol.AckFrameHeader + maxAckRanges*protocol.AckRangeSize
	streamFrameHdr = protocol.StreamFrameHeader
	// closedStreamTTL: nhớ các stream đã nhận xong trong 30 s, để frame gửi lại tới muộn của chúng bị bỏ
	// thay vì tạo ra 1 stream mới "ma".
	closedStreamTTL = 30 * time.Second
	maxRTO          = 5 * time.Second // trần của RTO khi backoff
	maxBackoffShift = 6               // 200ms << 6 = 12.8s > maxRTO, đủ để chạm trần và không tràn số
	// firstProbe: client gửi lại PING đầu tiên sau khoảng này (nhân đôi mỗi lần, tối đa pingInterval) cho tới khi nghe
	// được server. Server chỉ biết địa chỉ UDP của client khi nhận được gói đầu tiên; nếu PING đầu bị mất mà phải đợi
	// tới PING keep-alive (2 s) thì client mới kết nối bị "điếc" 2 s, 4 s... (giống QUIC gửi lại gói Initial).
	firstProbe = 200 * time.Millisecond

	// byte direction trong nonce (xem makeNonce): 2 chiều dùng chung khóa nên phải khác nonce.
	dirClientToServer byte = 0
	dirServerToClient byte = 1
)

// congestionController quyết định được để bao nhiêu byte "đang bay" (đã gửi, chưa ACK) trên đường truyền.
// Có 2 bản: cubic (cubic.go) và fixedWindow (để so sánh).
type congestionController interface {
	Window() int                    // cwnd: số byte tối đa được phép đang bay
	OnAck(acked int, now time.Time) // acked byte vừa được ACK -> có thể tăng cwnd
	OnLoss(now time.Time)           // phát hiện 1 đợt mất gói -> giảm cwnd
}

// fixedWindow là congestion control "không làm gì": cwnd luôn 64 KB, không phản ứng với mất gói.
type fixedWindow struct{}

// Window trả về cửa sổ cố định 64 KB.
func (fixedWindow) Window() int {
	return 64 * 1024
}

// OnAck không làm gì: cửa sổ cố định không đổi khi có ACK.
func (fixedWindow) OnAck(acked int, now time.Time) {}

// OnLoss không làm gì: cửa sổ cố định không đổi khi mất gói.
func (fixedWindow) OnLoss(now time.Time) {}

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

// newCongestionController tạo congestion controller theo lựa chọn hiện tại của SetCongestionControl.
func newCongestionController() congestionController {
	if name, _ := ccAlgo.Load().(string); name == "fixed" {
		return fixedWindow{}
	}
	return newCubic()
}

// inPacket là 1 gói thô (chưa giải mã) readLoop chuyển cho run().
type inPacket struct {
	raw  []byte
	addr *net.UDPAddr // địa chỉ gửi, để cập nhật peer khi client đổi mạng
}

// sentPacket là 1 gói có dữ liệu đã gửi, đang chờ ACK.
type sentPacket struct {
	sentAt time.Time               // để đo RTT và phát hiện mất theo thời gian
	size   int                     // byte trên dây, để trừ bytesInFlight khi được ACK / bị mất
	frames []*protocol.StreamFrame // giữ lại để gửi lại nếu gói này bị mất (gửi lại frame, không gửi lại gói)
}

// Conn là 1 kết nối tin cậy trên UDP giữa client và server, mã hóa AES-GCM bằng k_udp.
// Phía server tạo bằng Mux.Accept, phía client bằng Dial. Cài đặt interface Endpoint.
//
// Các method public (Recv, Done, Stats, ID, Close, OpenStream, SendMessage) an toàn khi gọi từ nhiều goroutine.
type Conn struct {
	id               uint32
	sock             PacketConn
	aead             cipher.AEAD
	sendDir, recvDir byte // byte direction trong nonce khi gửi / khi nhận

	// ========== Channel giao tiếp với các goroutine khác ==========
	in       chan inPacket              // gói thô từ readLoop
	urgentCh chan *protocol.StreamFrame // frame chat, run() luôn nhận
	bulkCh   chan *protocol.StreamFrame // frame file, run() ngừng nhận khi bulkQ đầy (backpressure)
	recvCh   chan Delivery              // giao dữ liệu cho app
	closeReq chan struct{}              // app gọi Close()
	done     chan struct{}              // đóng khi kết nối kết thúc

	stats      atomic.Pointer[Stats] // app đọc Stats() mà không cần khóa
	nextStream atomic.Uint32         // stream_id vừa cấp gần nhất; OpenStream cộng 2 mỗi lần

	// ========== Chỉ goroutine run() được dùng ==========
	peer          *net.UDPAddr            // địa chỉ UDP bên kia; phía server là nil cho tới khi nhận gói đầu tiên của client
	nextPN        uint32                  // packet_num cho gói gửi tiếp theo
	unacked       map[uint32]*sentPacket  // gói có dữ liệu đã gửi, chưa ACK; key = packet_num
	retransQ      []*protocol.StreamFrame // frame của gói bị mất, chờ gửi lại; ưu tiên cao nhất
	urgentQ       []*protocol.StreamFrame // frame chat chờ gửi; ưu tiên thứ 2
	bulkQ         []*protocol.StreamFrame // frame file chờ gửi; ưu tiên thấp nhất
	largestAcked  uint32                  // packet_num lớn nhất đã được ACK
	haveAcked     bool                    // đã có gói nào được ACK chưa (largestAcked hợp lệ)
	recvd         ackTracker              // các packet_num đã nhận, để tạo ACK
	ackNeeded     bool                    // có gói cần ACK -> gửi ACK trong lần flush tới
	pingNeeded    bool                    // cần gửi PING trong lần flush tới
	streams       map[uint32]*recvStream  // các stream đang nhận dở
	closedStreams map[uint32]time.Time    // stream đã nhận xong -> thời điểm xong (xem closedStreamTTL)
	outQ          []Delivery              // chờ giao cho app; nhờ hàng đợi này run() không bao giờ bị chặn vì app đọc chậm
	outBytes      int                     // tổng byte trong outQ
	pendingBytes  int                     // tổng byte trong pending của mọi recvStream
	lastAdvWindow int                     // cửa sổ nhận đã báo cho bên kia trong ACK gần nhất
	rtt           rttEstimator
	cc            congestionController
	bytesInFlight int    // tổng byte của các gói trong unacked
	peerWindow    int    // cửa sổ nhận bên kia báo trong ACK (flow control)
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

var _ Endpoint = (*Conn)(nil) // kiểm tra lúc biên dịch: *Conn cài đặt đủ Endpoint

// newConn tạo Conn và khởi động goroutine run(). peer = nil ở phía server (chưa biết địa chỉ client).
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
		recvd:         newAckTracker(),
		streams:       map[uint32]*recvStream{},
		closedStreams: map[uint32]time.Time{},
		cc:            newCongestionController(),
		peerWindow:    recvBufMax, // chưa có ACK nào thì giả định bên kia còn trống 1 MB
		lastAdvWindow: recvBufMax,
		lastHeard:     now,
		lastSent:      now,
	}

	if isServer {
		c.sendDir, c.recvDir = dirServerToClient, dirClientToServer
		// nextStream = 0 -> Add(2) cho ra 2, 4, 6, ...
	} else {
		c.sendDir, c.recvDir = dirClientToServer, dirServerToClient
		c.nextStream.Store(^uint32(0)) // = -1 -> Add(2) cho ra 1, 3, 5, ...
		c.pingNeeded = true            // gói đầu tiên, để server biết địa chỉ UDP của client
		c.isClient = true
	}
	c.publishStats()
	go c.run()
	return c, nil
}

// ========== API cho tầng app ==========

// Recv trả về channel dữ liệu nhận được, đúng thứ tự trong từng stream.
func (c *Conn) Recv() <-chan Delivery {
	return c.recvCh
}

// Done đóng khi kết nối kết thúc (CLOSE, idle timeout hoặc Close()).
func (c *Conn) Done() <-chan struct{} {
	return c.done
}

// Stats trả về số liệu mới nhất (RTT, cwnd, số gói gửi/mất); an toàn khi gọi từ goroutine khác.
func (c *Conn) Stats() Stats {
	return *c.stats.Load()
}

// ID trả về conn_id của kết nối.
func (c *Conn) ID() uint32 {
	return c.id
}

// Close gửi CLOSE cho bên kia rồi dừng kết nối; chờ tới khi run() thoát hẳn. Gọi nhiều lần không sao.
func (c *Conn) Close() error {
	select {
	case c.closeReq <- struct{}{}:
	case <-c.done:
	}
	<-c.done
	return nil
}

// ========== Vòng lặp chính ==========

// run là goroutine duy nhất sở hữu trạng thái của Conn. Mỗi vòng lặp chờ 1 sự kiện (gói tới, app gửi
// frame, app đọc dữ liệu, tick đồng hồ, app gọi Close), xử lý nó, rồi flush: gửi những gì cwnd và
// cửa sổ nhận bên kia cho phép, kèm ACK/PING nếu cần.
func (c *Conn) run() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	fastTick := true

	for !c.closed {
		bulkCh := c.bulkCh
		if len(c.bulkQ) >= maxBulkQueue {
			bulkCh = nil // channel nil không bao giờ sẵn sàng -> ngừng nhận frame file, Write của app bị chặn
		}

		// chỉ bật nhánh giao dữ liệu khi outQ có gì (recvCh nil thì nhánh đó bị select bỏ qua)
		var recvCh chan Delivery
		var head Delivery

		if len(c.outQ) > 0 {
			recvCh, head = c.recvCh, c.outQ[0]
		}

		select {
		case p := <-c.in:
			c.handlePacket(p, time.Now())
			c.drainIn() // xử lý luôn các gói đang chờ để gom ACK của nhiều gói vào 1 gói ACK

		case f := <-c.urgentCh:
			c.urgentQ = append(c.urgentQ, f)

		case f := <-bulkCh:
			c.bulkQ = append(c.bulkQ, f)

		case recvCh <- head:
			c.outQ[0] = Delivery{} // bỏ tham chiếu để GC thu hồi được Data
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
			c.flush(time.Now()) // gửi dữ liệu đang chờ + ACK/PING nếu cần
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

// drainIn xử lý thêm tối đa 32 gói đang chờ trong c.in mà không chặn, để 1 lần flush ACK được nhiều gói.
// Giới hạn 32 để run() vẫn kịp phục vụ các sự kiện khác khi gói tới liên tục.
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

// shutdown đánh dấu kết nối đã đóng và đóng done (báo cho app và các goroutine đang chờ). Không gửi CLOSE.
func (c *Conn) shutdown() {
	if c.closed {
		return
	}

	c.closed = true
	close(c.done) // chỉ run() gọi shutdown và đã kiểm tra c.closed -> không bao giờ close 2 lần
}

// ========== Phía nhận ==========

// handlePacket xử lý 1 gói tới: kiểm tra header, giải mã + xác thực, rồi xử lý từng frame.
// Gói hỏng, giả mạo hoặc không giải mã được thì bị bỏ lặng lẽ (không trả lời, không làm đổi trạng thái).
func (c *Conn) handlePacket(p inPacket, now time.Time) {
	if c.closed {
		return
	}

	h, err := protocol.ParseHeader(p.raw)

	if err != nil || h.Magic != protocol.Magic || h.Version != protocol.Version || h.ConnID != c.id {
		return
	}

	// header (12 byte đầu) là AAD: ai sửa header (vd đổi conn_id, packet_num) thì Open báo lỗi
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

	// Cập nhật địa chỉ bên kia theo gói mới nhất (đã qua xác thực GCM): client đổi Wi-Fi -> 4G vẫn tiếp tục
	// được. Chỉ đổi khi là gói mới nhất để gói cũ tới muộn (hoặc bị kẻ xấu phát lại từ địa chỉ khác)
	// không kéo kết nối về địa chỉ sai.
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

// onAck xử lý 1 ACK frame: cập nhật cửa sổ nhận bên kia, bỏ các gói đã được xác nhận khỏi unacked
// (báo cho cc để tăng cwnd), lấy mẫu RTT từ gói mới nhất được ACK, rồi kiểm tra mất gói.
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
		// chỉ lấy mẫu RTT từ gói mới nhất vừa được ACK: gói cũ hơn có thể đã nằm chờ ACK lâu, đo sẽ bị sai lệch
		c.rtt.update(now.Sub(newestPkt.sentAt))
	}

	c.detectLoss(now)
}

// onStreamFrame đưa 1 STREAM frame vào recvStream của nó (tạo mới nếu chưa có) và xếp các đoạn đã
// liền mạch vào outQ để giao cho app. Stream nhận đủ tới FIN thì chuyển sang closedStreams.
func (c *Conn) onStreamFrame(f *protocol.StreamFrame, now time.Time) {
	if _, ok := c.closedStreams[f.StreamID]; ok {
		return // stream đã nhận xong, đây là frame gửi lại tới muộn
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
		if len(chunks) == 0 { // FIN tới sau cùng trong 1 frame rỗng -> vẫn phải báo app là stream đã hết
			c.queueDelivery(Delivery{
				StreamID: f.StreamID,
				Fin:      true,
			})
		}

		delete(c.streams, f.StreamID)
		c.closedStreams[f.StreamID] = now
	}
}

// queueDelivery xếp d vào outQ chờ giao cho app.
func (c *Conn) queueDelivery(d Delivery) {
	c.outQ = append(c.outQ, d)
	c.outBytes += len(d.Data)
}

// recvWindow trả về số byte ta còn nhận được (flow control) = 1 MB − dữ liệu chờ app đọc − dữ liệu chờ lấp chỗ trống.
// Giá trị này được gửi trong mọi ACK; bên kia không được để quá chừng này byte đang bay.
func (c *Conn) recvWindow() int {
	return max(recvBufMax-c.outBytes-c.pendingBytes, 0)
}

// maybeWindowUpdate gửi ACK chủ động khi cửa sổ nhận vừa mở lại nhiều (lần báo trước < 1/4, giờ ≥ 1/2).
// Bên gửi có thể đang dừng hẳn vì cửa sổ ta báo trước đó gần hết; không báo thì nó phải đợi tới PING
// keep-alive kế tiếp (2 s) mới biết đã có chỗ.
func (c *Conn) maybeWindowUpdate() {
	if c.lastAdvWindow < recvBufMax/4 && c.recvWindow() >= recvBufMax/2 {
		c.ackNeeded = true
	}
}

// ========== Đồng hồ ==========

// detectLoss tìm các gói trong unacked bị coi là mất, theo 1 trong 3 cách (docs/reliability.md):
//   - byCount: đã có gói số ≥ pn+3 được ACK (gói sau tới rồi mà gói này chưa tới)
//   - byDelay: đã bị gói sau "vượt mặt" và quá lossDelay (9/8 RTT) vẫn chưa được ACK
//   - byTime:  quá RTO mà vẫn chưa có ACK (không có gói nào vượt mặt, ví dụ gói cuối cùng bị mất)
//
// Frame của gói mất được đưa vào retransQ để gửi lại trong gói mới. Mỗi "đợt" mất chỉ giảm cwnd 1 lần;
// mất do hết RTO thì tăng backoff (nhân đôi RTO) vì đó là dấu hiệu mạng có thể đã chết.
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
		c.retransQ = append(c.retransQ, sp.frames...) // chỉ gửi lại các STREAM frame của gói (ACK/PING thì không)

		// chỉ gói gửi SAU lần giảm cwnd gần nhất mới làm giảm tiếp: các gói cùng 1 đợt nghẽn
		// (gửi trước khi kịp giảm) chỉ tính là 1 lần giảm, nếu không cwnd bị chia liên tục
		if sp.sentAt.After(c.recoveryStart) {
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

// keepAlive kiểm tra idle timeout (im lặng 10 s -> đóng), lên lịch PING khi rảnh 2 s, và gửi lại PING đầu
// tiên của client khi chưa nghe được server.
func (c *Conn) keepAlive(now time.Time) {
	if now.Sub(c.lastHeard) > idleTimeout {
		log.Printf("[transport] conn %d timeout (silence in %v)", c.id, idleTimeout)
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

// cleanup dọn các packet_num quá cũ và các stream đã xong quá closedStreamTTL, để bộ nhớ không tăng mãi.
func (c *Conn) cleanup(now time.Time) {
	c.recvd.prune()
	for id, t := range c.closedStreams {
		if now.Sub(t) > closedStreamTTL {
			delete(c.closedStreams, id)
		}
	}
}

// publishStats chép số liệu hiện tại ra 1 Stats mới để app đọc qua Stats() mà không cần khóa.
func (c *Conn) publishStats() {
	st := &Stats{
		RTT:      c.rtt.srtt,
		Cwnd:     c.cc.Window(),
		InFlight: c.bytesInFlight,
		LostPkts: c.lostPkts,
		SentPkts: c.sentPkts,
		State:    "connected",
	}

	if c.peer != nil {
		st.Peer = c.peer.String()
	}
	if c.closed {
		st.State = "closed"
	} else if c.peer == nil {
		st.State = "waiting"
	}

	c.stats.Store(st)
}

// ========== Phía gửi ==========

// flush gửi càng nhiều gói dữ liệu càng tốt trong giới hạn min(cwnd, cửa sổ nhận bên kia), rồi gửi thêm
// 1 gói chỉ có ACK/PING nếu vẫn còn ACK/PING chưa được ghép vào gói dữ liệu nào.
func (c *Conn) flush(now time.Time) {
	if c.peer == nil {
		return // server chưa biết địa chỉ của client
	}

	// còn được gửi = min(cwnd, cửa sổ nhận bên kia) − số byte đang bay; còn đủ chỗ cho 1 gói đầy thì gửi
	for min(c.cc.Window(), c.peerWindow)-c.bytesInFlight >= protocol.MaxPacket {
		frames := c.takeFrames()

		if len(frames) == 0 {
			break
		}

		c.sendPacket(frames, now)
	}

	if c.ackNeeded || c.pingNeeded { // ACK/PING chưa đi kèm gói dữ liệu nào -> gửi gói riêng
		c.sendPacket(nil, now)
	}
}

// takeFrames lấy frame cho 1 gói theo thứ tự ưu tiên retransQ -> urgentQ -> bulkQ, tới khi gói đầy.
// Luôn chừa ackFrameMax byte để sendPacket ghép thêm ACK. Nhờ thứ tự này tin chat không phải đợi file.
func (c *Conn) takeFrames() []*protocol.StreamFrame {
	room := protocol.MaxFramePayload - ackFrameMax // chừa chỗ cho ACK
	var out []*protocol.StreamFrame
	for _, q := range []*[]*protocol.StreamFrame{&c.retransQ, &c.urgentQ, &c.bulkQ} {
		for len(*q) > 0 {
			f := (*q)[0]

			size := streamFrameHdr + len(f.Data)
			if size > room {
				return out // gói đã đầy
			}

			room -= size
			out = append(out, f)
			(*q)[0] = nil
			*q = (*q)[1:]
		}
	}

	return out
}

// sendPacket ghép 1 gói: [ACK nếu cần][các STREAM frame][PING nếu cần], mã hóa và gửi đi.
// Gói có dữ liệu được lưu vào unacked để chờ ACK; gói chỉ có ACK/PING thì không theo dõi, không gửi lại.
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
		if len(frames) == 0 { // gói có dữ liệu thì bên kia đã phải ACK rồi, không cần thêm PING
			payload = protocol.AppendPing(payload)
		}

		c.pingNeeded = false
	}

	pkt := c.seal(pn, payload)

	if _, err := c.sock.WriteToUDP(pkt, c.peer); err != nil {
		log.Printf("[transport] conn %d send packet: %v", c.id, err)
	}

	c.lastSent = now
	c.sentPkts++

	if len(frames) > 0 { // gói chỉ có ACK/PING thì không cần theo dõi, không gửi lại
		c.unacked[pn] = &sentPacket{
			sentAt: now,
			size:   len(pkt),
			frames: frames,
		}
		c.bytesInFlight += len(pkt)
	}
}

// closeCopies: số bản CLOSE gửi liên tiếp khi đóng chủ động. Kết nối đóng ngay sau đó nên không thể chờ ACK
// để gửi lại; gửi vài bản (mỗi bản một packet_num riêng, nonce không lặp) để mất gói ngẫu nhiên khó làm mất
// hết: ở loss 10%, xác suất mất cả 3 bản là 0,1%. Bên nhận đóng ở bản đầu tiên, các bản sau bị bỏ lặng lẽ.
const closeCopies = 3

// sendClose gửi closeCopies gói chỉ chứa CLOSE. Không theo dõi, không chờ ACK; nếu mất hết thì bên kia vẫn tự
// đóng sau idle timeout.
func (c *Conn) sendClose(reason string) {
	if c.peer == nil {
		return
	}

	payload := protocol.AppendClose(nil, &protocol.CloseFrame{
		Reason: reason,
	})
	for i := 0; i < closeCopies; i++ {
		pn := c.nextPN
		c.nextPN++
		c.sock.WriteToUDP(c.seal(pn, payload), c.peer)
	}
}

// seal tạo gói hoàn chỉnh: header 12 byte (gửi rõ, làm AAD) + AES-GCM(payload) + tag 16 byte.
func (c *Conn) seal(pn uint32, payload []byte) []byte {
	hdr := make([]byte, protocol.HeaderSize)
	(&protocol.Header{
		Magic:     protocol.Magic,
		Version:   protocol.Version,
		ConnID:    c.id,
		PacketNum: pn,
	}).Marshal(hdr)
	dst := append(make([]byte, 0, len(hdr)+len(payload)+protocol.TagSize), hdr...)
	// dst là bản copy riêng của hdr: crypto/cipher yêu cầu dst không được chồng lên additionalData (hdr)
	return c.aead.Seal(dst, makeNonce(c.id, pn, c.sendDir), payload, hdr)
}
