// Command server: handshake TCP + chuyển tiếp chat/DM/file qua transport UDP tin cậy.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
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
	stats := flag.Duration("stats", 0, "In số goroutine/RAM/số user online theo chu kỳ, vd 10s (0 = tắt)")
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

	if err := srv.ListenUDP(*loss); err != nil {
		log.Fatalf("Mở UDP thất bại: %v", err)
	}

	go srv.StartDiscovery(context.Background(), localIP())
	if *stats > 0 {
		go logStats(srv, *stats)
	}

	// Ctrl+C: gửi CLOSE cho mọi client để chúng biết ngay và tự kết nối lại khi server chạy lại.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		if *stats > 0 {
			printStats(srv)
		}
		log.Printf("[Server] Đang tắt...")
		srv.Close()
		os.Exit(0)
	}()

	if err := srv.ListenTCP(*tcpAddr); err != nil {
		log.Fatalf("Server TCP ngưng hoạt động: %v", err)
	}
}

func localIP() string {
	c, err := net.Dial("udp4", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}

// logStats in số goroutine, RAM heap và số user online mỗi chu kỳ (dùng khi test tải: số goroutine không được tăng mãi).
func logStats(srv *server.Server, every time.Duration) {
	for range time.Tick(every) {
		printStats(srv)
	}
}

func printStats(srv *server.Server) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("[stats] goroutines=%d heap=%.1fMB online=%d", runtime.NumGoroutine(), float64(m.HeapAlloc)/(1<<20), srv.OnlineCount())
}
