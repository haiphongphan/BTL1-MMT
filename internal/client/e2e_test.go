package client

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/server"
)

// Test hệ thống: server thật + 2 client thật qua UDP localhost, 10% loss mỗi chiều.
// Kiểm tra Step 5 (chat), 6 (file + SHA-256), 7 (rời đi → danh sách cập nhật).

func startServer(t *testing.T, dir string, tcp string, udpPort int) *server.Server {
	t.Helper()
	return startServerLoss(t, dir, tcp, udpPort, 0.1)
}

// startServerLoss ghi users.json có alice, bob (mật khẩu "pw") rồi chạy server với tỉ lệ mất gói loss.
func startServerLoss(t *testing.T, dir string, tcp string, udpPort int, loss float64) *server.Server {
	t.Helper()
	h := func(salt, p string) string { s := sha256.Sum256([]byte(salt + p)); return hex.EncodeToString(s[:]) }
	uf := filepath.Join(dir, "users.json")
	os.WriteFile(uf, []byte(fmt.Sprintf(`{"alice":{"salt":"a","hash":"%s"},"bob":{"salt":"b","hash":"%s"}}`,
		h("a", "pw"), h("b", "pw"))), 0o644)

	srv, err := server.NewServer(uf, udpPort)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.ListenUDP(loss, 0); err != nil {
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

	// Thanh tiến trình bên gửi: phải có file_progress chiều "out" với số byte tăng dần trước file_done.
	var outProgress int
	for _, e := range alice.Events.Since(0) {
		if fp, ok := e.Data.(FileProgress); ok && e.Type == "file_progress" && fp.Dir == "out" && fp.Received > 0 && fp.Received < fp.Size {
			outProgress++
		}
	}
	if outProgress == 0 {
		t.Fatal("bên gửi không có sự kiện file_progress nào: thanh tiến trình bên gửi không chạy")
	}

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

// waitOnline chờ tới khi danh sách online của c có (want = true) hoặc không có (want = false) user.
func waitOnline(t *testing.T, c *Client, user string, want bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if slices.Contains(c.OnlineUsers(), user) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("sau %v: online = %v, muốn có %q = %v", d, c.OnlineUsers(), user, want)
}

// Trang đăng nhập / đăng ký dùng Login, Logout trên cùng một Client: đăng ký sai luật bị từ chối với lý do
// rõ ràng, đăng ký đúng thì online ngay, đăng xuất báo mọi người ngay và không kết nối lại ngầm, đăng nhập
// lại bằng tài khoản vừa tạo được, và tài khoản còn sau khi server khởi động lại.
func TestRegisterLoginLogout(t *testing.T) {
	dir := t.TempDir()
	const addr = "127.0.0.1:19110"
	startServerLoss(t, dir, addr, 19111, 0)

	c := NewClient("") // như Web UI: chưa biết user lúc khởi động
	for _, tc := range []struct{ user, pass, want string }{
		{"ab", "secret1", "3–20"},
		{"dave", "123", "6–64"},
		{"alice", "secret1", "đã tồn tại"},
	} {
		var ae *AuthError
		if err := c.Login(addr, tc.user, tc.pass, true); !errors.As(err, &ae) || !strings.Contains(ae.Reason, tc.want) {
			t.Fatalf("đăng ký %q: err = %v, muốn lý do chứa %q", tc.user, err, tc.want)
		}
	}
	if c.Status().LoggedIn {
		t.Fatal("đăng ký thất bại mà vẫn logged_in")
	}

	alice := NewClient("alice")
	if err := alice.Connect(addr, "pw", 0); err != nil {
		t.Fatal(err)
	}
	defer alice.Close()

	if err := c.Login(addr, "dave", "secret1", true); err != nil {
		t.Fatal(err)
	}
	if st := c.Status(); !st.LoggedIn || st.User != "dave" || st.State != "connected" {
		t.Fatalf("sau đăng ký: %+v", st)
	}
	if err := c.Login(addr, "dave", "secret1", false); !errors.Is(err, ErrLoggedIn) {
		t.Fatalf("Login lần 2 khi đang online: %v", err)
	}
	waitOnline(t, alice, "dave", true, 3*time.Second)

	start := time.Now()
	c.Logout()
	waitOnline(t, alice, "dave", false, 3*time.Second)
	t.Logf("đăng xuất: alice thấy dave rời đi sau %v", time.Since(start))

	var ae *AuthError
	if err := c.Login(addr, "dave", "sai-mat-khau", false); !errors.As(err, &ae) {
		t.Fatalf("sai mật khẩu: %v", err)
	}
	if err := c.Login(addr, "dave", "secret1", false); err != nil {
		t.Fatal(err)
	}
	c.SendChat("hi from dave")
	if m := waitEvent(t, alice, "chat", 3*time.Second).Data.(protocol.AppMsg); m.From != "dave" {
		t.Fatalf("chat = %+v", m)
	}

	// Sau Logout, listenLoop/reconnectLoop của phiên cũ không được tự đăng nhập lại (trước đây chúng thử
	// lại sau 1 s).
	c.Logout()
	time.Sleep(2500 * time.Millisecond)
	if st := c.Status(); st.LoggedIn || st.State != "disconnected" {
		t.Fatalf("sau đăng xuất 2.5 s: %+v", st)
	}
	waitOnline(t, alice, "dave", false, time.Second)

	// Tài khoản đã được ghi vào users.json: một server mới đọc file này vẫn cho dave đăng nhập.
	srv2, err := server.NewServer(filepath.Join(dir, "users.json"), 19121)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv2.ListenUDP(0, 0); err != nil {
		t.Fatal(err)
	}
	go srv2.ListenTCP("127.0.0.1:19120")
	time.Sleep(100 * time.Millisecond)
	if err := c.Login("127.0.0.1:19120", "dave", "secret1", false); err != nil {
		t.Fatalf("đăng nhập trên server mới: %v", err)
	}
	c.Close()
}
