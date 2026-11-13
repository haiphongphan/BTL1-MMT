//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package client

import (
	"context"
	"net"
	"syscall"
)

// listenReuse mở socket UDP với SO_REUSEADDR + SO_REUSEPORT: nhiều process cùng nghe một cổng broadcast,
// mỗi process đều nhận được bản sao của gói broadcast.
func listenReuse(addr string) (net.PacketConn, error) {
	lc := net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var serr error
		err := c.Control(func(fd uintptr) {
			if serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); serr == nil {
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
			}
		})
		if err != nil {
			return err
		}
		return serr
	}}
	return lc.ListenPacket(context.Background(), "udp4", addr)
}
