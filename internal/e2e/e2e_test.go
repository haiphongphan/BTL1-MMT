// Package e2e chạy server + client thật qua loopback (TCP handshake + UDP transport tự viết)
// để kiểm tra các kịch bản hệ thống trong bảng test của báo cáo.
package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/phongphanhai06/go-collab/internal/client"
	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/server"
)

const testPass = "123456"

var testUsers = []string{"alice", "bob", "carol"}

func writeUsers(t *testing.T) string {
	t.Helper()
	users := map[string]server.UserAccount{}
	for i, u := range testUsers {
		salt := fmt.Sprintf("salt%d", i)
		h := sha256.Sum256([]byte(salt + testPass))
		users[u] = server.UserAccount{Salt: salt, Hash: hex.EncodeToString(h[:])}
	}
	b, _ := json.Marshal(users)
	p := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

type testServer struct {
	srv *server.Server
	ln  net.Listener
}

// startServer chạy server trên tcpAddr ("127.0.0.1:0" = cổng ngẫu nhiên) với UDP cổng ngẫu nhiên.
func startServer(t *testing.T, usersFile, tcpAddr string, loss float64) *testServer {
	t.Helper()
	srv, err := server.NewServer(usersFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.ListenUDP(loss); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	ts := &testServer{srv: srv, ln: ln}
	t.Cleanup(ts.stop)
	return ts
}

func (ts *testServer) addr() string { return ts.ln.Addr().String() }

func (ts *testServer) stop() {
	ts.ln.Close()
	ts.srv.Close()
}

func connect(t *testing.T, addr, user string, opt client.NetOptions) *client.Client {
	t.Helper()
	c := client.NewClient(user)
	c.DownloadDir = t.TempDir()
	if err := c.ConnectWith(addr, testPass, opt); err != nil {
		t.Fatalf("%s đăng nhập: %v", user, err)
	}
	t.Cleanup(c.Close)
	return c
}

// waitEvent đợi event loại typ thỏa pred (pred nil = bất kỳ), bắt đầu tìm từ sau *since và cập nhật *since.
func waitEvent(t *testing.T, c *client.Client, since *int64, typ string, timeout time.Duration, pred func(client.Event) bool) client.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range c.Events.Since(*since) {
			*since = ev.Seq
			if ev.Type == typ && (pred == nil || pred(ev)) {
				return ev
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s: không thấy event %q sau %v", c.User, typ, timeout)
	return client.Event{}
}

func msgOf(ev client.Event) protocol.AppMsg {
	m, _ := ev.Data.(protocol.AppMsg)
	return m
}

// waitOnline đợi tới khi c thấy đủ các user trong want.
func waitOnline(t *testing.T, c *client.Client, want ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got := map[string]bool{}
		for _, u := range c.OnlineUsers() {
			got[u] = true
		}
		ok := true
		for _, u := range want {
			ok = ok && got[u]
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var hist []string
	for _, ev := range c.Events.Since(0) {
		if ev.Type == "users" || ev.Type == "status" {
			hist = append(hist, fmt.Sprintf("%s:%v", ev.Type, ev.Data))
		}
	}
	t.Fatalf("%s: danh sách online %v, thiếu một trong %v\nlịch sử: %v", c.User, c.OnlineUsers(), want, hist)
}

// collectTexts lấy n tin loại typ đầu tiên mà c nhận được.
func collectTexts(t *testing.T, c *client.Client, typ string, n int, timeout time.Duration) []protocol.AppMsg {
	t.Helper()
	var since int64
	out := make([]protocol.AppMsg, 0, n)
	for len(out) < n {
		out = append(out, msgOf(waitEvent(t, c, &since, typ, timeout, nil)))
	}
	return out
}

// Kịch bản 1 + 2: chat chung + DM, đúng người nhận, đúng thứ tự, không mất/trùng — có và không có mất gói.
func TestChatAndDMOrder(t *testing.T) {
	cases := []struct {
		name string
		opt  client.NetOptions
		loss float64 // server
	}{
		{"no-loss", client.NetOptions{}, 0},
		{"loss10", client.NetOptions{Loss: 0.1}, 0.1},
		{"loss20", client.NetOptions{Loss: 0.2}, 0.2},
		{"loss10-jitter30ms", client.NetOptions{Loss: 0.1, Jitter: 30 * time.Millisecond}, 0.1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := startServer(t, writeUsers(t), "127.0.0.1:0", tc.loss)
			alice := connect(t, ts.addr(), "alice", tc.opt)
			bob := connect(t, ts.addr(), "bob", tc.opt)
			carol := connect(t, ts.addr(), "carol", tc.opt)
			waitOnline(t, alice, "alice", "bob", "carol")
			waitOnline(t, bob, "alice", "bob", "carol")
			waitOnline(t, carol, "alice", "bob", "carol")

			const n = 200
			for i := 0; i < n; i++ {
				if err := alice.SendChat(fmt.Sprintf("chat-%d", i)); err != nil {
					t.Fatal(err)
				}
				if err := alice.SendDM("bob", fmt.Sprintf("dm-%d", i)); err != nil {
					t.Fatal(err)
				}
			}

			for _, rc := range []*client.Client{bob, carol} {
				for i, m := range collectTexts(t, rc, "chat", n, 20*time.Second) {
					if want := fmt.Sprintf("chat-%d", i); m.Text != want || m.From != "alice" {
						t.Fatalf("%s: tin chat thứ %d = %q từ %q, muốn %q từ alice", rc.User, i, m.Text, m.From, want)
					}
				}
			}
			for i, m := range collectTexts(t, bob, "dm", n, 20*time.Second) {
				if want := fmt.Sprintf("dm-%d", i); m.Text != want {
					t.Fatalf("bob: DM thứ %d = %q, muốn %q", i, m.Text, want)
				}
			}
			// không trùng, và DM không lọt sang carol / chat không quay về alice
			time.Sleep(300 * time.Millisecond)
			for _, ev := range carol.Events.Since(0) {
				if ev.Type == "dm" {
					t.Fatalf("carol nhận DM không phải của mình: %+v", ev.Data)
				}
			}
			chats := 0
			for _, ev := range bob.Events.Since(0) {
				if ev.Type == "chat" {
					chats++
				}
			}
			if chats != n {
				t.Fatalf("bob nhận %d tin chat, muốn đúng %d (không trùng)", chats, n)
			}
			for _, ev := range alice.Events.Since(0) {
				if ev.Type == "chat" {
					t.Fatalf("alice nhận lại tin của chính mình: %+v", ev.Data)
				}
			}
		})
	}
}
