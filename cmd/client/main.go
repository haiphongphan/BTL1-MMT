// Command client: đăng nhập server, chat/DM/gửi file qua CLI hoặc Web UI local.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/phongphanhai06/go-collab/internal/client"
	"github.com/phongphanhai06/go-collab/internal/transport"
	"github.com/phongphanhai06/go-collab/internal/webui"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:9000", "Server TCP Address")
	username := flag.String("user", "phong", "Username")
	password := flag.String("pass", "123456", "Password")
	ccLogPath := flag.String("cclog", "", "Ghi diễn biến cwnd ra file CSV (vẽ đồ thị: go run ./scripts/plotcc file.csv > cwnd.svg)")
	ccName := flag.String("cc", "cubic", "Congestion control: cubic | fixed (cửa sổ cố định 64 KB)")
	loss := flag.Float64("loss", 0, "Tỉ lệ mất gói giả lập khi gửi (0.1 = 10%)")
	delay := flag.Duration("delay", 0, "Độ trễ giả lập thêm vào mỗi gói gửi đi (vd 50ms)")
	jitter := flag.Duration("jitter", 0, "Độ trễ ngẫu nhiên thêm vào mỗi gói gửi đi, trong [0, jitter) (vd 20ms)")
	web := flag.String("web", "127.0.0.1:8081", "Địa chỉ Web UI local (rỗng = tắt)")
	bot := flag.Bool("bot", false, "Chế độ bot (test tải): tự gửi chat mỗi 1–3 giây, không đọc bàn phím")
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

	c := client.NewClient(*username)

	// Ctrl+C / kill: đóng kết nối đàng hoàng (gửi CLOSE) để server báo "leave" ngay, không phải đợi 10s timeout.
	// Đăng ký TRƯỚC khi kết nối: server đã thấy client online ngay trong lúc handshake.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		c.Close()
		os.Exit(0)
	}()

	opt := client.NetOptions{Loss: *loss, Delay: *delay, Jitter: *jitter}
	if err := c.ConnectWith(*serverAddr, *password, opt); err != nil {
		log.Fatalf("Đăng nhập thất bại: %v", err)
	}
	fmt.Println("✅ Đăng nhập THÀNH CÔNG!")

	if *web != "" {
		ui := webui.New(c)
		go func() {
			if err := ui.ListenAndServe(*web); err != nil {
				log.Printf("Web UI lỗi: %v", err)
			}
		}()
		fmt.Printf("🌐 Web UI: http://%s\n", *web)
	}

	if *bot {
		runBot(c)
		return
	}

	// Loop in Event ra console
	go func() {
		var lastSeq int64
		for {
			events := c.Events.Since(lastSeq)
			for _, ev := range events {
				lastSeq = ev.Seq
				fmt.Printf("\n[EVENT %s] %+v\n> ", ev.Type, ev.Data)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Đọc CLI input
	fmt.Println("Lệnh: /msg <text> | /dm <user> <text> | /file <path> [user] | /users | /quit  (gõ thẳng = /msg)")
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		var err error
		switch {
		case line == "":
		case line == "/quit":
			c.Close()
			return
		case line == "/users":
			fmt.Println("Online:", c.OnlineUsers())
		case strings.HasPrefix(line, "/dm "):
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 3 {
				fmt.Println("Cú pháp: /dm <user> <text>")
				break
			}
			err = c.SendDM(parts[1], parts[2])
		case strings.HasPrefix(line, "/msg "):
			err = c.SendChat(strings.TrimPrefix(line, "/msg "))
		case strings.HasPrefix(line, "/file "):
			parts := strings.Fields(line)
			to := ""
			if len(parts) >= 3 {
				to = parts[2]
			}
			path := parts[1]
			go func() {
				if err := c.SendFile(path, to); err != nil {
					fmt.Println("\nGửi file lỗi:", err)
				} else {
					fmt.Println("\nĐã gửi xong", path)
				}
			}()
		default:
			err = c.SendChat(line)
		}
		if err != nil {
			fmt.Println("Lỗi:", err)
		}
		fmt.Print("> ")
	}
	if *web != "" {
		fmt.Println("CLI đóng, Web UI vẫn chạy. Ctrl+C để thoát.")
		select {}
	}
	c.Close()
}

// runBot gửi chat mỗi 1–3 giây cho tới khi bị dừng (Ctrl+C / kill). Mỗi 10 giây in 1 dòng thống kê.
func runBot(c *client.Client) {
	var sent, recv int
	var lastSeq int64
	nextSend := time.Now()
	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-report.C:
			st := c.Status()
			log.Printf("[bot %s] state=%s sent=%d recv=%d online=%d rtt=%.1fms loss=%.1f%%",
				c.User, st.State, sent, recv, len(st.Online), st.RTTms, st.LossPct)
		case now := <-tick.C:
			for _, ev := range c.Events.Since(lastSeq) {
				lastSeq = ev.Seq
				if ev.Type == "chat" {
					recv++
				}
			}
			if now.Before(nextSend) {
				continue
			}
			nextSend = now.Add(time.Second + rand.N(2*time.Second))
			if err := c.SendChat(fmt.Sprintf("bot %s #%d", c.User, sent+1)); err == nil {
				sent++
			}
		}
	}
}
