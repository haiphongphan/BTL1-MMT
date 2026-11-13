package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

// DiscoveryPort là cổng UDP server phát broadcast để client trong LAN tự tìm thấy (client -server auto).
const DiscoveryPort = 9002

// StartDiscovery phát "COLLAB_SERVER <ip> <tcp_port> <udp_port>" tới 255.255.255.255:9002 mỗi 2 s cho tới
// khi ctx bị hủy. Client lấy IP từ địa chỉ nguồn của gói (luôn đúng interface) và cổng TCP từ nội dung.
func (s *Server) StartDiscovery(ctx context.Context, lanIP string, tcpPort int) {
	addr := &net.UDPAddr{IP: net.IPv4bcast, Port: DiscoveryPort}
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		log.Printf("[Discovery] Disabled: %v", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	msg := []byte(fmt.Sprintf("COLLAB_SERVER %s %d %d", lanIP, tcpPort, s.udpPort))
	log.Printf("[Discovery] Broadcasting %q on UDP :%d every 2s", msg, DiscoveryPort)

	for {
		conn.Write(msg) // lỗi (mạng không hỗ trợ broadcast) thì bỏ qua, thử lại lần sau
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}
