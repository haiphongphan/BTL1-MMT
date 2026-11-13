//go:build linux

package client

// SO_REUSEPORT trên Linux (gói syscall chuẩn không export hằng này cho mọi kiến trúc).
const soReusePort = 0xf
