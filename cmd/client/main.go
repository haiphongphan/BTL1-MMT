package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/phongphanhai06/go-collab/internal/client"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:9000", "Server TCP Address")
	username := flag.String("user", "phong", "Username")
	password := flag.String("pass", "123456", "Password")
	flag.Parse()

	c := client.NewClient(*username)
	_, _, err := c.ConnectTCPAndAuth(*serverAddr, *password)
	if err != nil {
		log.Fatalf("Đăng nhập thất bại: %v", err)
	}
	fmt.Println("✅ Đăng nhập THÀNH CÔNG!")
	// TODO(ghép nối): tạo transport.Conn từ keyx.ConnID + kUDP, gửi PING đầu tiên, rồi c.AttachEndpoint(conn)


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
	fmt.Println("Lệnh: /msg <text> | /dm <user> <text> | /users | /quit  (gõ thẳng = /msg)")
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		var err error
		switch {
		case line == "":
		case line == "/quit":
			if c.EP != nil {
				c.EP.Close()
			}
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
		default:
			err = c.SendChat(line)
		}
		if err != nil {
			fmt.Println("Lỗi:", err)
		}
		fmt.Print("> ")
	}
}