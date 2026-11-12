package e2e

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/phongphanhai06/go-collab/internal/client"
	"github.com/phongphanhai06/go-collab/internal/protocol"
)

func longTests() bool { return os.Getenv("BTL_LONG") != "" }

func randomFile(t *testing.T, size int64) (string, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), fmt.Sprintf("data-%d.bin", size))
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(f, h), rand.Reader, size); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return p, hex.EncodeToString(h.Sum(nil))
}

func fileSHA(t *testing.T, p string) string {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil))
}

// Kịch bản 3: gửi file qua server với mất gói, SHA-256 phải khớp. In thời gian + throughput để ghi vào báo cáo.
func TestFileTransfer(t *testing.T) {
	type tc struct {
		size int64
		loss float64
	}
	cases := []tc{{1 << 10, 0}, {1 << 10, 0.1}, {10 << 20, 0}, {10 << 20, 0.1}}
	if longTests() {
		cases = append(cases, tc{200 << 20, 0}, tc{200 << 20, 0.1})
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%dKB-loss%.0f%%", c.size>>10, c.loss*100), func(t *testing.T) {
			ts := startServer(t, writeUsers(t), "127.0.0.1:0", c.loss)
			alice := connect(t, ts.addr(), "alice", client.NetOptions{Loss: c.loss})
			bob := connect(t, ts.addr(), "bob", client.NetOptions{Loss: c.loss})
			waitOnline(t, alice, "bob")

			src, sum := randomFile(t, c.size)
			start := time.Now()
			if err := alice.SendFile(src, "bob"); err != nil {
				t.Fatal(err)
			}
			var since int64
			var done client.FileProgress
			deadline := time.Now().Add(10 * time.Minute)
			for done.Path == "" && time.Now().Before(deadline) {
				for _, ev := range bob.Events.Since(since) {
					since = ev.Seq
					switch ev.Type {
					case "file_error":
						t.Fatalf("bob: %v", ev.Data)
					case "file_done":
						done = ev.Data.(client.FileProgress)
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
			el := time.Since(start)
			if done.Path == "" {
				t.Fatal("hết giờ, bob chưa nhận xong")
			}
			if got := fileSHA(t, done.Path); got != sum {
				t.Fatalf("SHA-256 không khớp: %s != %s", got, sum)
			}
			st := alice.Status()
			t.Logf("RESULT file=%dKB loss=%.0f%% time=%v throughput=%.2fMB/s sha=OK (alice: sent=%d lost=%d)",
				c.size>>10, c.loss*100, el.Round(time.Millisecond), float64(c.size)/(1<<20)/el.Seconds(), st.SentPkts, st.LostPkts)
		})
	}
}

// Kịch bản 4: chat trong lúc đang gửi file lớn phải tới nhanh (không Head-of-Line blocking).
func TestChatLatencyDuringFile(t *testing.T) {
	size := int64(50 << 20)
	if longTests() {
		size = 200 << 20
	}
	ts := startServer(t, writeUsers(t), "127.0.0.1:0", 0)
	alice := connect(t, ts.addr(), "alice", client.NetOptions{})
	bob := connect(t, ts.addr(), "bob", client.NetOptions{})
	waitOnline(t, alice, "bob")

	src, _ := randomFile(t, size)
	fileDone := make(chan error, 1)
	go func() { fileDone <- alice.SendFile(src, "bob") }()

	// đợi file thực sự bắt đầu chảy tới bob
	var since int64
	waitEvent(t, bob, &since, "file_progress", 30*time.Second, nil)

	var lat []time.Duration
	finished := false
	for i := 0; !finished || len(lat) < 10; i++ {
		text := fmt.Sprintf("ping-%d", i)
		sent := time.Now()
		if err := alice.SendDM("bob", text); err != nil {
			t.Fatal(err)
		}
		waitEvent(t, bob, &since, "dm", 10*time.Second, func(ev client.Event) bool { return msgOf(ev).Text == text })
		lat = append(lat, time.Since(sent))
		for _, ev := range bob.Events.Since(0) {
			if ev.Type == "file_done" {
				finished = true
			}
		}
		if len(lat) >= 200 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := <-fileDone; err != nil {
		t.Fatal(err)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p50, p95, mx := lat[len(lat)/2], lat[len(lat)*95/100], lat[len(lat)-1]
	t.Logf("RESULT file=%dMB chat-samples=%d p50=%v p95=%v max=%v", size>>20, len(lat), p50.Round(time.Microsecond), p95.Round(time.Microsecond), mx.Round(time.Microsecond))
	if p95 > 300*time.Millisecond && !raceEnabled {
		t.Fatalf("chat trễ p95 = %v > 300ms trong lúc gửi file", p95)
	}
}

// Kịch bản 7: sai mật khẩu, user không tồn tại, đăng nhập trùng ⇒ AUTH_FAIL.
func TestAuthFail(t *testing.T) {
	ts := startServer(t, writeUsers(t), "127.0.0.1:0", 0)
	connect(t, ts.addr(), "alice", client.NetOptions{})

	for _, c := range []struct{ user, pass, why string }{
		{"bob", "sai", "sai mật khẩu"},
		{"mallory", testPass, "user không tồn tại"},
		{"alice", testPass, "đăng nhập trùng"},
	} {
		cl := client.NewClient(c.user)
		err := cl.ConnectWith(ts.addr(), c.pass, client.NetOptions{})
		if err == nil || !strings.Contains(err.Error(), "auth failed") {
			t.Fatalf("%s: muốn AUTH_FAIL, got %v", c.why, err)
		}
		t.Logf("RESULT %s → %v", c.why, err)
	}
}

// Kịch bản 8: gói rác vào cổng UDP (ngẫu nhiên, quá ngắn, header giả với conn_id thật) ⇒ server bỏ qua, vẫn chạy.
func TestGarbageUDP(t *testing.T) {
	ts := startServer(t, writeUsers(t), "127.0.0.1:0", 0)
	alice := connect(t, ts.addr(), "alice", client.NetOptions{})
	bob := connect(t, ts.addr(), "bob", client.NetOptions{})
	waitOnline(t, alice, "bob")

	sock, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", ts.srv.UDPPort()))
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	for i := 0; i < 5000; i++ {
		n := 1 + i%1500
		b := make([]byte, n)
		rand.Read(b)
		if i%3 == 0 && n >= protocol.HeaderSize { // header đúng định dạng + conn_id của user thật, payload rác
			h := protocol.Header{Magic: protocol.Magic, Version: protocol.Version, ConnID: uint32(1001 + i%2), PacketNum: uint32(i)}
			h.Marshal(b)
		}
		sock.Write(b)
	}
	sock.Write([]byte("hello\n"))
	time.Sleep(200 * time.Millisecond)

	var since int64
	if err := alice.SendDM("bob", "vẫn sống"); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, bob, &since, "dm", 5*time.Second, func(ev client.Event) bool { return msgOf(ev).Text == "vẫn sống" })
	if alice.Status().State != "connected" || bob.Status().State != "connected" {
		t.Fatalf("client bị ngắt sau gói rác: %s / %s", alice.Status().State, bob.Status().State)
	}
	t.Log("RESULT 5000 gói rác: server không crash, alice→bob vẫn chat được")
}

// Kịch bản 6: tắt server rồi bật lại trên cùng cổng ⇒ client tự kết nối lại và chat tiếp được.
func TestServerRestartReconnect(t *testing.T) {
	users := writeUsers(t)
	ts := startServer(t, users, "127.0.0.1:0", 0)
	addr := ts.addr()
	alice := connect(t, addr, "alice", client.NetOptions{})
	bob := connect(t, addr, "bob", client.NetOptions{})
	waitOnline(t, alice, "bob")
	// chắc chắn cả 2 kết nối UDP đã thông (server chỉ biết địa chỉ UDP của client sau gói đầu tiên;
	// tắt server trước lúc đó thì CLOSE không tới được client ⇒ client chỉ biết sau 10 s idle timeout)
	var s0, s1 int64
	alice.SendChat("a")
	waitEvent(t, bob, &s0, "chat", 5*time.Second, nil)
	bob.SendChat("b")
	waitEvent(t, alice, &s1, "chat", 5*time.Second, nil)

	t.Cleanup(func() {
		if t.Failed() {
			for _, ev := range bob.Events.Since(0) {
				t.Logf("DEBUG bob %s %v", ev.Type, ev.Data)
			}
		}
	})
	down := time.Now()
	ts.stop()
	waitState(t, alice, "reconnecting", 15*time.Second)
	time.Sleep(500 * time.Millisecond)
	ts2 := startServer(t, users, addr, 0)
	_ = ts2
	waitState(t, alice, "connected", 30*time.Second)
	waitState(t, bob, "connected", 30*time.Second)
	waitOnline(t, alice, "alice", "bob")
	t.Logf("RESULT server tắt rồi bật lại: client tự kết nối lại sau %v", time.Since(down).Round(100*time.Millisecond))

	if err := alice.SendChat("sau khi reconnect"); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, bob, &s0, "chat", 5*time.Second, func(ev client.Event) bool { return msgOf(ev).Text == "sau khi reconnect" })
}

func waitState(t *testing.T, c *client.Client, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Status().State == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: trạng thái %q, muốn %q", c.User, c.Status().State, want)
}

// Kịch bản 5: client bị dừng (Ctrl+C = thoát có CLOSE; kill -9 = biến mất không báo) ⇒ server báo leave, không crash.
// Chạy client thật bằng binary để có thể kill tiến trình.
func TestClientKilled(t *testing.T) {
	if testing.Short() {
		t.Skip("cần build binary, > 10s")
	}
	if runtime.GOOS == "windows" {
		t.Skip("Windows không gửi được SIGINT cho tiến trình khác")
	}
	bin := filepath.Join(t.TempDir(), "client")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/client").CombinedOutput(); err != nil {
		t.Fatalf("build client: %v\n%s", err, out)
	}

	ts := startServer(t, writeUsers(t), "127.0.0.1:0", 0)
	carol := connect(t, ts.addr(), "carol", client.NetOptions{})

	for _, sig := range []struct {
		name string
		sig  os.Signal
		max  time.Duration
	}{
		{"Ctrl+C (SIGINT)", os.Interrupt, 2 * time.Second},
		{"kill -9 (SIGKILL)", syscall.SIGKILL, 12 * time.Second},
	} {
		cmd := exec.Command(bin, "-server", ts.addr(), "-user", "bob", "-pass", testPass, "-web", "")
		stdin, _ := cmd.StdinPipe() // giữ stdin mở để CLI không tự thoát
		pr, pw := io.Pipe()
		cmd.Stdout, cmd.Stderr = pw, pw
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		loggedIn := make(chan struct{})
		go func() { // đọc output; báo khi client in dòng đăng nhập thành công
			sc := bufio.NewScanner(pr)
			once := false
			for sc.Scan() {
				out.WriteString(sc.Text() + "\n")
				if !once && strings.Contains(sc.Text(), "THÀNH CÔNG") {
					once = true
					close(loggedIn)
				}
			}
		}()
		select {
		case <-loggedIn:
		case <-time.After(10 * time.Second):
			t.Fatal("client không đăng nhập được")
		}
		waitOnline(t, carol, "bob")

		start := time.Now()
		cmd.Process.Signal(sig.sig)
		deadline := start.Add(sig.max + 3*time.Second)
		for contains(carol.OnlineUsers(), "bob") && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		took := time.Since(start)
		cmd.Wait()
		stdin.Close()
		pw.Close()
		if contains(carol.OnlineUsers(), "bob") {
			t.Fatalf("%s: server không báo bob rời đi sau %v", sig.name, took)
		}
		t.Logf("RESULT %s: server báo bob rời đi sau %v", sig.name, took.Round(10*time.Millisecond))
		if took > sig.max {
			t.Errorf("%s: mất %v, muốn ≤ %v", sig.name, took, sig.max)
		}
	}

	// server vẫn sống: đăng nhập lại được
	connect(t, ts.addr(), "bob", client.NetOptions{})
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// Kịch bản 9 (bản rút gọn chạy trong go test): nhiều client chat liên tục rồi thoát ⇒ không mất tin,
// số goroutine trở về mức ban đầu (không rò rỉ). Bản 20 client × 5 phút: scripts/load_test.sh.
func TestManyClientsNoLeak(t *testing.T) {
	const n = 20
	rounds := 10
	if longTests() {
		rounds = 100
	}
	saved := testUsers
	testUsers = nil
	for i := 1; i <= n; i++ {
		testUsers = append(testUsers, fmt.Sprintf("user%d", i))
	}
	defer func() { testUsers = saved }()

	ts := startServer(t, writeUsers(t), "127.0.0.1:0", 0)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	clients := make([]*client.Client, n)
	for i := range clients {
		clients[i] = client.NewClient(testUsers[i])
		clients[i].DownloadDir = t.TempDir()
		if err := clients[i].ConnectWith(ts.addr(), testPass, client.NetOptions{Loss: 0.05}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range clients {
		waitOnline(t, c, testUsers...)
	}
	peak := runtime.NumGoroutine()

	start := time.Now()
	for r := 0; r < rounds; r++ {
		for i, c := range clients {
			if err := c.SendChat(fmt.Sprintf("%d-%d", i, r)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	// mỗi client nhận rounds*(n-1) tin chat, tin của từng người gửi phải đúng thứ tự
	want := rounds * (n - 1)
	for _, c := range clients {
		deadline := time.Now().Add(30 * time.Second)
		var got []protocol.AppMsg
		for time.Now().Before(deadline) {
			got = got[:0]
			for _, ev := range c.Events.Since(0) {
				if ev.Type == "chat" {
					got = append(got, msgOf(ev))
				}
			}
			if len(got) >= want {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if len(got) != want {
			t.Fatalf("%s nhận %d/%d tin", c.User, len(got), want)
		}
		last := map[string]int{}
		for _, m := range got {
			var i, r int
			fmt.Sscanf(m.Text, "%d-%d", &i, &r)
			if prev, ok := last[m.From]; ok && r != prev+1 {
				t.Fatalf("%s: tin của %s sai thứ tự (%d sau %d)", c.User, m.From, r, prev)
			}
			last[m.From] = r
		}
	}
	el := time.Since(start)

	for _, c := range clients {
		c.Close()
	}
	deadline := time.Now().Add(15 * time.Second)
	after := runtime.NumGoroutine()
	for after > base+2 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		after = runtime.NumGoroutine()
	}
	t.Logf("RESULT %d client × %d vòng (%d tin, loss 5%%) trong %v; goroutine: trước=%d lúc chạy=%d sau khi thoát=%d",
		n, rounds, n*rounds, el.Round(time.Millisecond), base, peak, after)
	if after > base+2 {
		buf := make([]byte, 1<<20)
		t.Fatalf("rò rỉ goroutine: trước %d, sau %d\n%s", base, after, buf[:runtime.Stack(buf, true)])
	}
}
