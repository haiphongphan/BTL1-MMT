package server

import (
	"encoding/json"
	"log"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

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
	// Bắt Panic để không làm sập Server nếu có lỗi bất ngờ ở 1 Client (Step 7)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Server] Panic recovered in session '%s': %v", sess.Username, r)
		}
		s.dropSession(sess)
	}()

	buffers := make(map[uint32][]byte)

	for {
		select {
		case d, ok := <-sess.EP.Recv():
			if !ok {
				return
			}
			// Gom mảnh stream theo StreamID
			if len(d.Data) > 0 {
				buffers[d.StreamID] = append(buffers[d.StreamID], d.Data...)
			}
			if !d.Fin {
				continue
			}

			// Khi Stream kết thúc (Fin = true), xử lý dữ liệu hoàn chỉnh
			fullData := buffers[d.StreamID]
			delete(buffers, d.StreamID)

			if len(fullData) > 0 && fullData[0] == protocol.KindMessage {
				var msg protocol.AppMsg
				if err := json.Unmarshal(fullData[1:], &msg); err != nil {
					log.Printf("[Server] Invalid JSON from %s: %v", sess.Username, err)
					continue
				}
				msg.From = sess.Username // Ghi đè From để chống giả danh
				s.routeMessage(sess, msg)
			}

		case <-sess.EP.Done():
			// Kết nối Transport bị ngắt hoặc Timeout
			return
		}
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
			errReply := protocol.AppMsg{
				Type: "error",
				Text: "User '" + msg.To + "' hiện không online.",
				TS:   time.Now().UnixMilli(),
			}
			if errBytes, err := json.Marshal(errReply); err == nil {
				from.Send(append([]byte{protocol.KindMessage}, errBytes...))
			}
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