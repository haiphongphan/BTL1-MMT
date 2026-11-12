package server

import (
	"encoding/json"
	"log"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

type inStream struct {
	kind  byte
	buf   []byte
	hdr   protocol.FileHeaderParser
	relay chan []byte
	drop  bool
}

// Send xếp một tin vào hàng đợi gửi của session, không bao giờ chặn goroutine gọi:
// người nhận quá chậm làm đầy hàng đợi của chính họ thì tin mới bị bỏ (và ghi log), những người khác không bị ảnh hưởng.
func (sess *Session) Send(m protocol.AppMsg) bool {
	select {
	case sess.sendCh <- m:
		return true
	default:
		log.Printf("[Server] Hàng đợi của '%s' đầy, bỏ 1 tin %s", sess.Username, m.Type)
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

func (s *Server) AttachEndpoint(connID uint32, ep transport.Endpoint) bool {
	sess := s.reg.Attach(connID, ep)
	if sess == nil {
		ep.Close()
		return false
	}
	go sess.writeLoop()
	go s.serveSession(sess)
	s.broadcastUserList()
	return true
}

func (s *Server) serveSession(sess *Session) {
	streams := make(map[uint32]*inStream)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Server] Panic recovered in session '%s': %v", sess.Username, r)
		}
		for _, st := range streams {
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
				st = &inStream{kind: d.Data[0]}
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
			// Kết nối Transport bị ngắt hoặc Timeout
			return
		}
	}
}

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

func (s *Server) sendError(to *Session, text string) {
	to.Send(protocol.AppMsg{Type: "error", Text: text, TS: time.Now().UnixMilli()})
}

func (s *Server) routeMessage(from *Session, msg protocol.AppMsg) {
	msg.TS = time.Now().UnixMilli()

	switch msg.Type {
	case "chat": // Broadcast cho tất cả mọi người ngoại trừ người gửi
		for _, target := range s.reg.Snapshot() {
			if target != from {
				target.Send(msg)
			}
		}

	case "dm": // Gửi đích danh cho 1 user
		target := s.reg.GetByName(msg.To)
		if target != nil && target.EP != nil {
			target.Send(msg)
		} else {
			// Báo lỗi lại cho người gửi nếu receiver không online
			s.sendError(from, "User '"+msg.To+"' hiện không online.")
		}
	}
}

func (s *Server) dropSession(sess *Session) {
	if !s.reg.Remove(sess) {
		return
	}
	sess.EP.Close()
	log.Printf("[Server] User '%s' (ConnID %d) disconnected.", sess.Username, sess.ConnID)
	s.broadcastUserList()
}

func (s *Server) broadcastUserList() {
	msg := protocol.AppMsg{
		Type:  "users",
		Users: s.reg.Names(),
		TS:    time.Now().UnixMilli(),
	}
	// Snapshot danh sách để gửi không giữ lock
	for _, sess := range s.reg.Snapshot() {
		sess.Send(msg)
	}
}
