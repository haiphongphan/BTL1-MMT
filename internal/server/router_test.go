package server

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

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

// Step 7: client chết → server xóa session và báo cho người còn lại
func TestDisconnectBroadcastsUsers(t *testing.T) {
	s := newTestServer()
	alice := join(t, s, "alice", 1)
	bob := join(t, s, "bob", 2)

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
