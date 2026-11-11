package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

func (s *Server) StartDiscovery(ctx context.Context, lanIP string) {
	addr := &net.UDPAddr{IP: net.IPv4bcast, Port: 9002}
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		log.Printf("[Discovery] Disabled: %v", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	msg := []byte(fmt.Sprintf("COLLAB_SERVER %s 9000 %d", lanIP, s.udpPort))
	log.Printf("[Discovery] Broadcasting on UDP :9002 every 2s...")

	for {
		select {
		case <-ticker.C:
			conn.Write(msg)
		case <-ctx.Done():
			return
		}
	}
}