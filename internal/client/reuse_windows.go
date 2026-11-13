//go:build windows

package client

import (
	"context"
	"net"
	"syscall"
)

// listenReuse mở socket UDP với SO_REUSEADDR (trên Windows đã đủ để nhiều process cùng nhận broadcast).
func listenReuse(addr string) (net.PacketConn, error) {
	lc := net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var serr error
		err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		})
		if err != nil {
			return err
		}
		return serr
	}}
	return lc.ListenPacket(context.Background(), "udp4", addr)
}
