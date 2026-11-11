package server

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"
	"bytes"
	"crypto/rand"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

func newTestServer() *Server {
	return &Server{reg: NewRegistry(), users: map[string]UserAccount{}}
}

// join giả lập 1 user đã qua handshake + gắn fake transport. Trả về đầu phía client.
func join(t *testing.T, s *Server, name string, connID uint32) transport.Endpoint {
	t.Helper()
	if _, err := s.reg.AddPending(name, connID, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	cli, srv := transport.FakePair()
	if !s.AttachEndpoint(connID, srv) {
		t.Fatal("attach thất bại")
	}
	return cli
}

func send(ep transport.Endpoint, m protocol.AppMsg) {
	b, _ := json.Marshal(m)
	ep.SendMessage(append([]byte{protocol.KindMessage}, b...))
}

// next đọc tới khi gặp tin có Type = typ (bỏ qua loại khác, ví dụ "users").
func next(t *testing.T, ep transport.Endpoint, typ string) protocol.AppMsg {
	t.Helper()
	bufs := map[uint32][]byte{}
	timeout := time.After(2 * time.Second)
	for {
		select {
		case d := <-ep.Recv():
			bufs[d.StreamID] = append(bufs[d.StreamID], d.Data...)
			if !d.Fin {
				continue
			}
			data := bufs[d.StreamID]
			delete(bufs, d.StreamID)
			var m protocol.AppMsg
			if err := json.Unmarshal(data[1:], &m); err != nil {
				t.Fatal(err)
			}
			if m.Type == typ {
				return m
			}
		case <-timeout:
			t.Fatalf("không nhận được tin %q", typ)
		}
	}
}

// Step 4 + 5: broadcast đúng thứ tự, From bị ghi đè (chống giả danh)
func TestChatBroadcastInOrder(t *testing.T) {
	s := newTestServer()
	alice := join(t, s, "alice", 1)
	bob := join(t, s, "bob", 2)

	for i := 0; i < 20; i++ {
		send(alice, protocol.AppMsg{Type: "chat", From: "bob", Text: strconv.Itoa(i)})
	}
	for i := 0; i < 20; i++ {
		m := next(t, bob, "chat")
		if m.Text != strconv.Itoa(i) {
			t.Fatalf("tin thứ %d sai thứ tự: %q", i, m.Text)
		}
		if m.From != "alice" {
			t.Fatalf("From = %q, server phải ghi đè thành alice", m.From)
		}
	}
}

func TestDMOfflineReturnsError(t *testing.T) {
	s := newTestServer()
	alice := join(t, s, "alice", 1)
	send(alice, protocol.AppMsg{Type: "dm", To: "carol", Text: "hi"})
	next(t, alice, "error")
}

// Step 6: file đi qua server theo kiểu pipeline, header bị cắt nhỏ vẫn parse được, From bị ghi đè
func TestFileRelay(t *testing.T) {
	s := newTestServer()
	alice := join(t, s, "alice", 1)
	bob := join(t, s, "bob", 2)

	data := make([]byte, 300_000)
	rand.Read(data)
	hdr, _ := protocol.EncodeFileHeader(protocol.FileMeta{ID: "f1", From: "mallory", To: "bob", Name: "a.bin", Size: int64(len(data))})

	go func() {
		st := alice.OpenStream(false)
		st.Write(hdr[:3]) // cắt header làm 2 mảnh
		st.Write(hdr[3:])
		for off := 0; off < len(data); off += 1000 {
			st.Write(data[off:min(off+1000, len(data))])
		}
		st.Close()
	}()

	var p protocol.FileHeaderParser
	var meta *protocol.FileMeta
	var got []byte
	var fileStream uint32
	timeout := time.After(5 * time.Second)
	for {
		select {
		case d := <-bob.Recv():
			if fileStream == 0 && len(d.Data) > 0 && d.Data[0] == protocol.KindFile {
				fileStream = d.StreamID
			}
			if d.StreamID != fileStream {
				continue // tin "users"... bỏ qua
			}
			if meta == nil {
				m, rest, err := p.Feed(d.Data)
				if err != nil {
					t.Fatal(err)
				}
				meta = m
				got = append(got, rest...)
			} else {
				got = append(got, d.Data...)
			}
			if !d.Fin {
				continue
			}
			if meta == nil || meta.From != "alice" {
				t.Fatalf("meta = %+v, From phải là alice", meta)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("data lệch: nhận %d/%d byte", len(got), len(data))
			}
			return
		case <-timeout:
			t.Fatalf("timeout, nhận %d/%d byte", len(got), len(data))
		}
	}
}

func TestFileToOfflineUserReturnsError(t *testing.T) {
	s := newTestServer()
	alice := join(t, s, "alice", 1)
	hdr, _ := protocol.EncodeFileHeader(protocol.FileMeta{To: "carol", Name: "a.bin"})
	st := alice.OpenStream(false)
	st.Write(hdr)
	st.Close()
	next(t, alice, "error")
}


// Step 7: client chết → server xóa session và báo cho người còn lại
func TestDisconnectBroadcastsUsers(t *testing.T) {
	s := newTestServer()
	alice := join(t, s, "alice", 1)
	bob := join(t, s, "bob", 2)

	for len(next(t, alice, "users").Users) != 2 { // chờ alice thấy bob online đã
	}
	bob.Close()
	for {
		if m := next(t, alice, "users"); len(m.Users) == 1 && m.Users[0] == "alice" {
			break
		}
	}
	if s.reg.GetByName("bob") != nil {
		t.Fatal("bob vẫn còn trong registry")
	}
}

func TestDuplicateLoginRejected(t *testing.T) {
	r := NewRegistry()
	if _, err := r.AddPending("alice", 1, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.AddPending("alice", 2, [32]byte{}); err != ErrAlreadyOnline {
		t.Fatalf("err = %v, want ErrAlreadyOnline", err)
	}
}
