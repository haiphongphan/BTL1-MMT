// Client: đăng nhập vào server, mở Web UI trên 127.0.0.1 và CLI trong terminal. File này chỉ đọc cờ và lắp
// ráp internal/client (mạng) + internal/webui (giao diện).
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

// runBot gửi chat tự động mỗi 1–3 s, dùng cho test tải (scripts/run_clients.sh).
func runBot(c *client.Client) {
	for i := 1; ; i++ {
		time.Sleep(time.Second + time.Duration(rand.IntN(2000))*time.Millisecond)
		if err := c.SendChat(fmt.Sprintf("bot %s #%d", c.User, i)); err != nil {
			log.Printf("bot: %v", err)
		}
	}
}

func main() {
	serverAddr := flag.String("server", "127.0.0.1:9000", "Địa chỉ TCP của server, hoặc \"auto\" để tự tìm server trong LAN (broadcast cổng 9002)")
	username := flag.String("user", "phong", "Username")
	password := flag.String("pass", "123456", "Password")
	ccLogPath := flag.String("cclog", "", "Ghi diễn biến cwnd ra file CSV (vẽ đồ thị: go run ./scripts/plotcc file.csv > cwnd.svg)")
	ccName := flag.String("cc", "cubic", "Congestion control: cubic | fixed (cửa sổ cố định 64 KB)")
	loss := flag.Float64("loss", 0, "Tỉ lệ mất gói giả lập khi gửi (0.1 = 10%)")
	web := flag.String("web", "127.0.0.1:8081", "Địa chỉ Web UI local (rỗng = tắt)")
	jitter := flag.Duration("jitter", 0, "Trễ ngẫu nhiên giả lập khi gửi (vd 30ms)")
	bot := flag.Bool("bot", false, "Chế độ bot: tự gửi chat mỗi 1–3s (test tải)")

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

	if *serverAddr == "auto" {
		fmt.Println("🔎 Đang tìm server trong LAN (UDP :9002)...")
		addr, err := client.Discover(5 * time.Second)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("🔎 Tìm thấy server:", addr)
		*serverAddr = addr
	}

	c := client.NewClient(*username)
	c.Jitter = *jitter

	if err := c.Connect(*serverAddr, *password, *loss); err != nil {
		log.Fatalf("Đăng nhập thất bại: %v", err)
	}
	fmt.Println("✅ Đăng nhập THÀNH CÔNG!")
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		fmt.Println("\nĐang thoát...")
		c.Close()
		os.Exit(0)
	}()
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
		go runBot(c)
	}

	// In event ra console (Web UI đọc cùng EventLog này qua /api/events)
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

	// Đọc lệnh CLI
	fmt.Println("Lệnh: /msg <text> | /dm <user> <text> | /file <đường-dẫn> [user] | /users | /quit  (gõ thẳng = /msg)")
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
			parts := strings.Fields(line) // /file <đường-dẫn> [user]; không có user = gửi mọi người
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
	// Hết stdin (chạy nền với < /dev/null): còn Web UI hoặc bot thì giữ process sống, không thì thoát.
	if *web != "" || *bot {
		fmt.Println("CLI đóng, Web UI/bot vẫn chạy. Ctrl+C để thoát.")
		select {}
	}
	c.Close()
}
