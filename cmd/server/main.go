package main

import (
	"flag"
	"log"

	"github.com/phongphanhai06/go-collab/internal/server"
)

func main() {
	tcpAddr := flag.String("tcp", ":9000", "TCP Listen Address")
	udpPort := flag.Int("udp-port", 9001, "UDP Transport Port")
	usersFile := flag.String("users", "users.json", "Path to users.json file")
	flag.Parse()

	srv, err := server.NewServer(*usersFile, *udpPort)
	if err != nil {
		log.Fatalf("Khởi tạo Server thất bại: %v", err)
	}

	if err := srv.ListenTCP(*tcpAddr); err != nil {
		log.Fatalf("Server TCP ngưng hoạt động: %v", err)
	}
}