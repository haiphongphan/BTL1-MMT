package server

import (
	"encoding/json"
	"log"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

// inStream là trạng thái của một stream đang nhận từ client (phân loại theo byte đầu tiên).
type inStream struct {
	kind  byte                      // protocol.KindMessage hoặc protocol.KindFile
	buf   []byte                    // message: gom byte tới FIN
	hdr   protocol.FileHeaderParser // file: đọc meta ở đầu stream
	relay chan []byte               // file: các mảnh chuyển cho goroutine relayFile
	drop  bool                      // stream hỏng/quá lớn/không có người nhận: bỏ phần còn lại
}

// Send xếp một tin vào hàng đợi gửi của session. Không bao giờ chặn goroutine gọi (thường là goroutine của
// người gửi), nên một người nhận chậm không làm kẹt người khác. Hàng đợi đầy (client không đọc kịp
// sendQueueSize tin) thì server ngắt kết nối của riêng client đó thay vì lặng lẽ bỏ tin; client tự kết nối lại.
func (sess *Session) Send(m protocol.AppMsg) bool {
	select {
	case sess.sendCh <- m:
		return true
	default:
		if sess.kicked.CompareAndSwap(false, true) {
			log.Printf("[Server] Hàng đợi gửi của '%s' đầy (%d tin), ngắt kết nối client quá chậm", sess.Username, sendQueueSize)
			go sess.EP.Close() // Close chờ goroutine transport thoát, không làm ở goroutine của người gửi
		}
		return false
	}
}

// writeLoop là goroutine DUY NHẤT gọi SendMessage cho session, nên thứ tự gửi = thứ tự vào hàng đợi.
// Nó đánh Seq = 1, 2, 3... riêng cho người nhận này (một client nhận tin từ nhiều người gửi khác nhau);
// client dùng SeqBuffer để giao đúng thứ tự dù tầng transport giao các stream không theo thứ tự.
func (sess *Session) writeLoop() {
	var seq uint64
	for {
		select {
		case m := <-sess.sendCh:
			seq++
			m.Seq = seq
			b, err := json.Marshal(m)
			if err != nil {
				continue
			}
			if err := sess.EP.SendMessage(append([]byte{protocol.KindMessage}, b...)); err != nil {
				return
			}
		case <-sess.EP.Done():
			return
		}
	}
}

// AttachEndpoint được gọi ngay sau khi đăng nhập thành công: gắn kết nối transport vào session pending,
// khởi động 2 goroutine của session và báo cho mọi người danh sách user mới.
func (s *Server) AttachEndpoint(connID uint32, ep transport.Endpoint) bool {
	sess := s.reg.Attach(connID, ep)
	if sess == nil {
		ep.Close()
		return false
	}
	go sess.writeLoop()
	go s.serveSession(sess)
	go s.watchPeer(sess)
	s.broadcastUserList()
	return true
}

// watchPeer chờ tới khi transport biết địa chỉ UDP thật của client (nhận được PING đầu tiên), rồi ghi log
// IP:port và phát lại danh sách user kèm địa chỉ. Tự thoát khi kết nối đóng (tối đa 10 s idle timeout).
func (s *Server) watchPeer(sess *Session) {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if p := sess.EP.Stats().Peer; p != "" {
				log.Printf("[Server] User '%s' active: TCP %s, UDP %s (ConnID %d)", sess.Username, sess.TCPAddr, p, sess.ConnID)
				s.broadcastUserList()
				return
			}
		case <-sess.EP.Done():
			return
		}
	}
}

// serveSession là vòng đọc của một session: ghép các stream client gửi lên và xử lý theo loại.
// Kết thúc (kết nối đóng hoặc panic) thì dọn session.
func (s *Server) serveSession(sess *Session) {
	streams := make(map[uint32]*inStream)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Server] Panic recovered in session '%s': %v", sess.Username, r)
		}
		for _, st := range streams { // file đang chuyển dở: báo relayFile kết thúc
			if st.relay != nil {
				close(st.relay)
			}
		}
		s.dropSession(sess)
	}()

	for {
		select {
		case d, ok := <-sess.EP.Recv():
			if !ok {
				return
			}
			st := streams[d.StreamID]
			if st == nil {
				if len(d.Data) == 0 {
					continue
				}
				st = &inStream{kind: d.Data[0]} // byte đầu cho biết loại stream
				streams[d.StreamID] = st
			}

			switch st.kind {
			case protocol.KindMessage:
				s.onMessageData(sess, st, d)
			case protocol.KindFile:
				s.onFileData(sess, st, d)
			}
			if d.Fin {
				delete(streams, d.StreamID)
			}

		case <-sess.EP.Done():
			return // CLOSE, idle timeout, hoặc bị ngắt vì quá chậm
		}
	}
}

// onMessageData gom một stream message tới FIN, giải mã JSON, sắp lại theo Seq rồi định tuyến.
func (s *Server) onMessageData(sess *Session, st *inStream, d transport.Delivery) {
	if !st.drop {
		st.buf = append(st.buf, d.Data...)
		if len(st.buf) > maxMessageSize {
			log.Printf("[Server] Tin của %s quá lớn, bỏ", sess.Username)
			st.drop, st.buf = true, nil
		}
	}
	if !d.Fin || st.drop {
		return
	}
	var msg protocol.AppMsg
	if err := json.Unmarshal(st.buf[1:], &msg); err != nil {
		log.Printf("[Server] Invalid JSON from %s: %v", sess.Username, err)
		return
	}
	// Sắp lại theo Seq của client: tin tới sớm được giữ, trả về khi các tin trước đã đủ (Seq 0 = giao ngay).
	for _, m := range sess.inSeq.Push(msg) {
		m.From = sess.Username // Ghi đè From để chống giả danh
		s.routeMessage(sess, m)
	}
}

// sendError gửi thông báo lỗi cho một client (ví dụ DM tới người không online).
func (s *Server) sendError(to *Session, text string) {
	to.Send(protocol.AppMsg{Type: "error", Text: text, TS: time.Now().UnixMilli()})
}

// routeMessage: "chat" gửi cho mọi người trừ người gửi; "dm" gửi đúng một người, không có thì báo lỗi.
func (s *Server) routeMessage(from *Session, msg protocol.AppMsg) {
	msg.TS = time.Now().UnixMilli() // giờ của server, để mọi người thấy cùng một mốc

	switch msg.Type {
	case "chat":
		for _, target := range s.reg.Snapshot() {
			if target != from {
				target.Send(msg)
			}
		}

	case "dm":
		target := s.reg.GetByName(msg.To)
		if target != nil && target.EP != nil {
			target.Send(msg)
		} else {
			s.sendError(from, "User '"+msg.To+"' hiện không online.")
		}
	}
}

// dropSession xóa session khỏi registry và báo mọi người. Idempotent: gọi nhiều lần chỉ có tác dụng một lần.
func (s *Server) dropSession(sess *Session) {
	if !s.reg.Remove(sess) {
		return
	}
	sess.EP.Close()
	log.Printf("[Server] User '%s' (ConnID %d, %s) disconnected.", sess.Username, sess.ConnID, sess.Addr())
	s.broadcastUserList()
}

// broadcastUserList gửi danh sách user online kèm địa chỉ cho mọi session đang active.
func (s *Server) broadcastUserList() {
	msg := protocol.AppMsg{
		Type:  "users",
		Users: s.reg.Names(),
		Addrs: s.reg.Addrs(),
		TS:    time.Now().UnixMilli(),
	}
	for _, sess := range s.reg.Snapshot() { // bản sao: gửi không giữ khóa
		sess.Send(msg)
	}
}

// Online trả về số user đang online (dùng cho dòng log [Runtime]).
func (s *Server) Online() int { return len(s.reg.Names()) }
