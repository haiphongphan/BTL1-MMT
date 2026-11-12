package transport

import (
	"bytes"
	"errors"
	"testing"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

func join(chunks [][]byte) string { return string(bytes.Join(chunks, nil)) }

func TestRecvStreamInOrder(t *testing.T) {
	s := newRecvStream()
	out, done := s.push(0, []byte("ab"), false)
	if join(out) != "ab" || done {
		t.Fatalf("out=%q done=%v", join(out), done)
	}
	out, done = s.push(2, []byte("cd"), true)
	if join(out) != "cd" || !done {
		t.Fatalf("out=%q done=%v", join(out), done)
	}
}

func TestRecvStreamOutOfOrder(t *testing.T) {
	s := newRecvStream()
	if out, done := s.push(4, []byte("ef"), true); len(out) != 0 || done {
		t.Fatal("đoạn tới sớm không được giao ngay")
	}
	if out, _ := s.push(2, []byte("cd"), false); len(out) != 0 {
		t.Fatal("vẫn còn lỗ ở offset 0")
	}
	if s.buffered != 4 {
		t.Fatalf("buffered = %d, want 4", s.buffered)
	}

	out, done := s.push(0, []byte("ab"), false) // lấp lỗ -> giao luôn các đoạn đang chờ
	if join(out) != "abcdef" || !done {
		t.Fatalf("out=%q done=%v", join(out), done)
	}
	if s.buffered != 0 || len(s.pending) != 0 {
		t.Fatalf("buffered=%d pending=%d sau khi giao hết", s.buffered, len(s.pending))
	}
}

func TestRecvStreamDuplicate(t *testing.T) {
	s := newRecvStream()
	s.push(0, []byte("ab"), false)
	if out, _ := s.push(0, []byte("ab"), false); len(out) != 0 {
		t.Fatal("đoạn trùng (đã giao) không được giao lại")
	}

	s.push(4, []byte("ef"), false)
	s.push(4, []byte("ef"), false) // trùng khi còn trong pending
	if s.buffered != 2 {
		t.Fatalf("buffered = %d, want 2 (không đếm trùng)", s.buffered)
	}
}

func TestRecvStreamOverlap(t *testing.T) {
	s := newRecvStream()
	s.push(0, []byte("abc"), false)
	out, _ := s.push(1, []byte("bcde"), false) // chồng lấn một phần: chỉ giao phần mới
	if join(out) != "de" {
		t.Fatalf("out = %q, want \"de\"", join(out))
	}
}

// FIN rỗng (SendStream.Close) tới trước data: stream chỉ xong khi data tới đủ.
func TestRecvStreamEmptyFinFirst(t *testing.T) {
	s := newRecvStream()
	if out, done := s.push(2, nil, true); len(out) != 0 || done {
		t.Fatal("FIN tới trước data không được báo xong")
	}
	out, done := s.push(0, []byte("ab"), false)
	if join(out) != "ab" || !done {
		t.Fatalf("out=%q done=%v", join(out), done)
	}
}

func TestSendStreamSplitsFrames(t *testing.T) {
	c := &Conn{urgentCh: make(chan *protocol.StreamFrame, 16), done: make(chan struct{})}
	s := &SendStream{c: c, id: 9, urgent: true}

	data := bytes.Repeat([]byte("x"), 2500)
	if n, err := s.Write(data); n != 2500 || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	data[0] = 'y' // app dùng lại buffer: frame đã gửi phải là bản copy
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	wantOff := []uint32{0, 1000, 2000, 2500}
	wantLen := []int{1000, 1000, 500, 0}
	for i := range wantOff {
		f := <-c.urgentCh
		if f.StreamID != 9 || f.Offset != wantOff[i] || len(f.Data) != wantLen[i] || f.Fin != (i == 3) {
			t.Fatalf("frame %d = {id %d off %d len %d fin %v}", i, f.StreamID, f.Offset, len(f.Data), f.Fin)
		}
		if i == 0 && f.Data[0] != 'x' {
			t.Fatal("frame trỏ vào buffer của app (không copy)")
		}
	}

	if _, err := s.Write([]byte("z")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write sau Close: err = %v, want ErrClosed", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal("Close 2 lần phải idempotent")
	}
}

func TestSendStreamConnClosed(t *testing.T) {
	c := &Conn{bulkCh: make(chan *protocol.StreamFrame), done: make(chan struct{})} // channel không buffer, không ai đọc
	close(c.done)
	s := &SendStream{c: c, id: 1}
	if _, err := s.Write([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed (không được block mãi)", err)
	}
}
