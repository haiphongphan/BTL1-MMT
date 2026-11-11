package server

import (
	"encoding/json"
	"log"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

type inStream struct {
	kind byte
	buf []byte
	hdr protocol.FileHeaderParser
	relay chan []byte
	drop bool
}

func (sess *Session) Send(b []byte) bool {
	select {
	case sess.sendCh <- b:
		return true
	
	default:
		log.Printf("[Server] Hàng đợi của '%s' đầy, bỏ 1 tin", sess.Username)
		return false
	}
}

func (sess *Session) writeLoop() {
	for {
		select {
		case b := <-sess.sendCh:
			if err := sess.EP.SendMessage(b); err != nil {
				return
			}
		case <- sess.EP.Done():
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
	msg.From = sess.Username // Ghi đè From để chống giả danh
	s.routeMessage(sess, msg)
}

func (s *Server) sendError(to *Session, text string) {
	b, err := json.Marshal(protocol.AppMsg{Type: "error", Text: text, TS: time.Now().UnixMilli()})
	if err == nil {
		to.Send(append([]byte{protocol.KindMessage}, b...))
	}
}


func (s *Server) routeMessage(from *Session, msg protocol.AppMsg) {
	msg.TS = time.Now().UnixMilli()
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}

	// Đóng gói thành message stream [0x01][JSON]
	packet := append([]byte{protocol.KindMessage}, payload...)

	switch msg.Type {
	case "chat": // Broadcast cho tất cả mọi người ngoại trừ người gửi
		for _, target := range s.reg.Snapshot() {
			if target != from {
				target.Send(packet)
			}
		}

	case "dm": // Gửi đích danh cho 1 user
		target := s.reg.GetByName(msg.To)
		if target != nil && target.EP != nil {
			target.Send(packet)
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
	users := s.reg.Names()
	msg := protocol.AppMsg{
		Type:  "users",
		Users: users,
		TS:    time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}
	packet := append([]byte{protocol.KindMessage}, payload...)

	// Snapshot danh sách để gửi không giữ lock
	for _, sess := range s.reg.Snapshot() {
		if sess.EP != nil {
			sess.Send(packet)
		}
	}
}