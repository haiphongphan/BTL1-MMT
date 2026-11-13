package client

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// DiscoveryPort phải khớp với server.DiscoveryPort.
const DiscoveryPort = 9002

// Discover nghe broadcast "COLLAB_SERVER <ip> <tcp_port> <udp_port>" mà server phát mỗi 2 s trên cổng 9002,
// trả về địa chỉ TCP "ip:port" của server đầu tiên nghe được. IP lấy từ địa chỉ nguồn của gói (đúng
// interface mà máy này thấy được server), không tin IP trong nội dung (server có thể có nhiều interface).
// Socket bật SO_REUSEADDR/SO_REUSEPORT để nhiều client trên cùng một máy dò cùng lúc được.
func Discover(timeout time.Duration) (string, error) {
	pc, err := listenReuse(fmt.Sprintf(":%d", DiscoveryPort))
	if err != nil {
		return "", fmt.Errorf("không nghe được cổng discovery %d: %w", DiscoveryPort, err)
	}
	defer pc.Close()
	pc.SetReadDeadline(time.Now().Add(timeout))

	buf := make([]byte, 256)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return "", fmt.Errorf("không tìm thấy server trong %v (server có chạy trong cùng LAN không?): %w", timeout, err)
		}
		f := strings.Fields(string(buf[:n]))
		if len(f) != 4 || f[0] != "COLLAB_SERVER" {
			continue // gói lạ trên cổng 9002: bỏ qua
		}
		if _, err := strconv.Atoi(f[2]); err != nil {
			continue
		}
		ip := f[1]
		if ua, ok := from.(*net.UDPAddr); ok {
			ip = ua.IP.String()
		}
		return net.JoinHostPort(ip, f[2]), nil
	}
}
