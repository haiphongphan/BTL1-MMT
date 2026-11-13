//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package client

import "syscall"

const soReusePort = syscall.SO_REUSEPORT
