package main

import (
	"flag"
	"log"
	"context"
	"net"

	"github.com/phongphanhai06/go-collab/internal/server"
)

func main() {
	tcpAddr := flag.String("tcp", ":9000", "TCP Listen Address")
	udpPort := flag.Int("udp-port", 9001, "UDP Transport Port")
	usersFile := flag.String("users", "users.json", "Path to users.json file")
	loss := flag.Float64("loss", 0, "Tỉ lệ mất gói giả lập khi gửi (0.1 = 10%)")
	flag.Parse()

	srv, err := server.NewServer(*usersFile, *udpPort)
	if err != nil {
		log.Fatalf("Khởi tạo Server thất bại: %v", err)
	}

	if err := srv.ListenUDP(*loss); err != nil {
		log.Fatalf("Mở UDP thất bại: %v", err)
	}

	go srv.StartDiscovery(context.Background(), localIP())

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