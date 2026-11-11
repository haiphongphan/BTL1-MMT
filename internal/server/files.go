package server

import (
	"io"
	"log"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

const (
	maxMessageSize = 64 << 10 // tin chat tối đa 64 KB
	relayBuffer    = 256      // số Delivery tối đa chờ chuyển tiếp cho 1 file (~256 KB)
)

// onFileData xử lý 1 Delivery của stream file: đọc header → mở relay → chuyển tiếp từng mảnh.
func (s *Server) onFileData(sess *Session, st *inStream, d transport.Delivery) {
	switch {
	case st.drop:

	case st.relay == nil:
		meta, rest, err := st.hdr.Feed(d.Data)
		if err != nil {
			log.Printf("[Server] File header hỏng từ %s: %v", sess.Username, err)
			st.drop = true
			break
		}
		if meta == nil {
			break
		}
		meta.From = sess.Username 

		targets := s.fileTargets(sess, meta.To)
		if len(targets) == 0 {
			s.sendError(sess, "Không có ai nhận file '"+meta.Name+"'.")
			st.drop = true
			break
		}
		log.Printf("[Server] Relay file '%s' (%d B) %s → %d người", meta.Name, meta.Size, sess.Username, len(targets))
		st.relay = make(chan []byte, relayBuffer)
		go s.relayFile(*meta, targets, st.relay)
		if len(rest) > 0 {
			st.relay <- rest
		}

	default:
		if len(d.Data) > 0 {
			st.relay <- d.Data
		}
	}

	if d.Fin && st.relay != nil {
		close(st.relay)
		st.relay = nil
	}
}

func (s *Server) fileTargets(from *Session, to string) []*Session {
	var out []*Session
	for _, t := range s.reg.Snapshot() {
		if t != from && (to == "" || t.Username == to) {
			out = append(out, t)
		}
	}
	return out
}


func (s *Server) relayFile(meta protocol.FileMeta, targets []*Session, ch <-chan []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Server] Panic khi relay file '%s': %v", meta.Name, r)
		}
		for range ch {
		}
	}()

	hdr, err := protocol.EncodeFileHeader(meta)
	if err != nil {
		return
	}
	outs := make([]io.WriteCloser, len(targets))
	for i, t := range targets {
		outs[i] = t.EP.OpenStream(false)
	}
	write := func(b []byte) {
		for i, w := range outs {
			if w == nil {
				continue
			}
			if _, err := w.Write(b); err != nil {
				log.Printf("[Server] Relay tới %s lỗi: %v", targets[i].Username, err)
				outs[i] = nil 
			}
		}
	}

	write(hdr)
	for b := range ch {
		write(b)
	}
	for _, w := range outs {
		if w != nil {
			w.Close()
		}
	}
	log.Printf("[Server] Relay file '%s' xong", meta.Name)
}
