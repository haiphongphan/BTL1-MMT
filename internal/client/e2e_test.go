package client

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/server"
)

// Test hệ thống: server thật + 2 client thật qua UDP localhost, 10% loss mỗi chiều.
// Kiểm tra Step 5 (chat), 6 (file + SHA-256), 7 (rời đi → danh sách cập nhật).

func startServer(t *testing.T, dir string, tcp string, udpPort int) *server.Server {
	t.Helper()
	h := func(salt, p string) string { s := sha256.Sum256([]byte(salt + p)); return hex.EncodeToString(s[:]) }
	uf := filepath.Join(dir, "users.json")
	os.WriteFile(uf, []byte(fmt.Sprintf(`{"alice":{"salt":"a","hash":"%s"},"bob":{"salt":"b","hash":"%s"}}`,
		h("a", "pw"), h("b", "pw"))), 0o644)

	srv, err := server.NewServer(uf, udpPort)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.ListenUDP(0.1, 0); err != nil {
		t.Fatal(err)
	}
	go srv.ListenTCP(tcp)
	time.Sleep(100 * time.Millisecond)
	return srv
}

// waitEvent chờ event có Type = typ; fail ngay nếu gặp file_error.
func waitEvent(t *testing.T, c *Client, typ string, d time.Duration) Event {
	t.Helper()
	deadline := time.Now().Add(d)
	var seq int64
	for time.Now().Before(deadline) {
		for _, e := range c.Events.Since(seq) {
			seq = e.Seq
			if e.Type == "file_error" {
				t.Fatalf("file_error: %v", e.Data)
			}
			if e.Type == typ {
				return e
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("không thấy event %q sau %v", typ, d)
	return Event{}
}

func TestE2E(t *testing.T) {
	dir := t.TempDir()
	startServer(t, dir, "127.0.0.1:19100", 19101)

	alice, bob := NewClient("alice"), NewClient("bob")
	bob.DownloadDir = filepath.Join(dir, "dl")
	if err := alice.Connect("127.0.0.1:19100", "pw", 0.1); err != nil {
		t.Fatal(err)
	}
	if err := bob.Connect("127.0.0.1:19100", "pw", 0.1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	// Step 5: chat tới đúng người, đúng nội dung
	alice.SendChat("hello")
	if m := waitEvent(t, bob, "chat", 5*time.Second).Data.(protocol.AppMsg); m.From != "alice" || m.Text != "hello" {
		t.Fatalf("chat = %+v", m)
	}

	// Step 6: file 5 MB với loss, SHA-256 khớp; DM gửi trong lúc file chạy vẫn tới (không HoL)
	data := make([]byte, 5<<20)
	rand.Read(data)
	src := filepath.Join(dir, "big.bin")
	os.WriteFile(src, data, 0o644)
	start := time.Now()
	go func() {
		if err := alice.SendFile(src, "bob"); err != nil {
			t.Error(err)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	alice.SendDM("bob", "during-file")
	waitEvent(t, bob, "dm", 5*time.Second)
	t.Logf("DM tới sau %v kể từ khi bắt đầu gửi file", time.Since(start))

	done := waitEvent(t, bob, "file_done", 60*time.Second)
	elapsed := time.Since(start)
	p := done.Data.(FileProgress)
	got, _ := os.ReadFile(p.Path)
	if !bytes.Equal(got, data) {
		t.Fatal("nội dung file lệch")
	}
	t.Logf("5 MB, loss 10%% hai chiều: %v (%.1f MB/s)", elapsed, 5/elapsed.Seconds())

	// Step 7: bob rời đi → alice thấy danh sách còn 1 người.
	// Gói CLOSE có thể bị mất (10% loss) → server phải chờ idle timeout 10s, nên cho tối đa 12s.
	bob.Close()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if u := alice.OnlineUsers(); len(u) == 1 && u[0] == "alice" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if u := alice.OnlineUsers(); len(u) != 1 {
		t.Fatalf("online = %v, muốn [alice]", u)
	}
	alice.Close()
}
