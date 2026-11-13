//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly || windows)

package client

import "net"

// listenReuse trên các hệ điều hành khác: không bật reuse, chỉ một client mỗi máy dò được cùng lúc.
func listenReuse(addr string) (net.PacketConn, error) { return net.ListenPacket("udp4", addr) }
