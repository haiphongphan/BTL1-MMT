// Server: nhận đăng nhập qua TCP (:9000), phục vụ mọi client qua một socket UDP tin cậy (:9001),
// phát broadcast discovery lên :9002. File này chỉ đọc cờ và lắp ráp các thành phần trong internal/server.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/phongphanhai06/go-collab/internal/server"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

func main() {
	tcpAddr := flag.String("tcp", ":9000", "TCP Listen Address")
	udpPort := flag.Int("udp-port", 9001, "UDP Transport Port")
	usersFile := flag.String("users", "users.json", "Path to users.json file")
	ccLogPath := flag.String("cclog", "", "Ghi diễn biến cwnd ra file CSV (vẽ đồ thị: go run ./scripts/plotcc file.csv > cwnd.svg)")
	ccName := flag.String("cc", "cubic", "Congestion control: cubic | fixed (cửa sổ cố định 64 KB)")
	loss := flag.Float64("loss", 0, "Tỉ lệ mất gói giả lập khi gửi (0.1 = 10%)")
	jitter := flag.Duration("jitter", 0, "Trễ ngẫu nhiên giả lập khi gửi (vd 30ms)")

	flag.Parse()

	if err := transport.SetCongestionControl(*ccName); err != nil {
		log.Fatal(err)
	}
	if *ccLogPath != "" {
		closeLog, err := transport.EnableCCLog(*ccLogPath)
		if err != nil {
			log.Fatalf("Mở file cclog thất bại: %v", err)
		}
		defer closeLog()
	}

	srv, err := server.NewServer(*usersFile, *udpPort)
	if err != nil {
		log.Fatalf("Khởi tạo Server thất bại: %v", err)
	}

	if err := srv.ListenUDP(*loss, *jitter); err != nil {
		log.Fatalf("Mở UDP thất bại: %v", err)
	}

	tcpPort := 9000
	if _, p, err := net.SplitHostPort(*tcpAddr); err == nil {
		if n, err := strconv.Atoi(p); err == nil {
			tcpPort = n
		}
	}
	go srv.StartDiscovery(context.Background(), localIP(), tcpPort)
	go logRunTime(srv, 10*time.Second)

	// Ctrl+C: gửi CLOSE cho mọi client rồi mới thoát, để client chuyển sang "reconnecting" ngay
	// thay vì chờ idle timeout 10 s.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Printf("[Server] Đang tắt: đóng mọi kết nối...")
		srv.Close()
		os.Exit(0)
	}()

	if err := srv.ListenTCP(*tcpAddr); err != nil {
		log.Fatalf("Server TCP ngưng hoạt động: %v", err)
	}
}

// logRunTime in số goroutine, bộ nhớ heap và số user online định kỳ: khi test tải, các số này phải ổn
// định theo số client chứ không tăng mãi (dấu hiệu rò goroutine/bộ nhớ).
func logRunTime(srv *server.Server, every time.Duration) {
	var m runtime.MemStats
	for range time.Tick(every) {
		runtime.ReadMemStats(&m)
		log.Printf("[Runtime] goroutines=%d heap=%.1fMB online=%d", runtime.NumGoroutine(), float64(m.HeapAlloc)/1e6, srv.Online())
	}
}

// localIP trả về IP LAN của máy (IP của interface đi ra ngoài; không gửi gói nào thật).
func localIP() string {
	c, err := net.Dial("udp4", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}
