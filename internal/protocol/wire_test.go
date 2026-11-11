package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestWireRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := Hello{Type: "HELLO", ClientPub: []byte{1, 2, 3}}
	if err := WriteMsg(&buf, in); err != nil {
		t.Fatal(err)
	}
	var out Hello
	if err := ReadMsg(&buf, &out); err != nil {
		t.Fatal(err)
	}
	if out.Type != in.Type || !bytes.Equal(out.ClientPub, in.ClientPub) {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

// 2 message ghi liền nhau phải đọc ra đúng 2 message (kiểm tra ranh giới trên dòng byte)
func TestWireBackToBack(t *testing.T) {
	var buf bytes.Buffer
	WriteMsg(&buf, AuthResult{Type: "AUTH_OK"})
	WriteMsg(&buf, AuthResult{Type: "AUTH_FAIL", Reason: "x"})

	var a, b AuthResult
	if err := ReadMsg(&buf, &a); err != nil {
		t.Fatal(err)
	}
	if err := ReadMsg(&buf, &b); err != nil {
		t.Fatal(err)
	}
	if a.Type != "AUTH_OK" || b.Type != "AUTH_FAIL" || b.Reason != "x" {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
}

func TestWireRejectsHugeLength(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 1<<30) // client xấu khai báo 1 GB
	err := ReadMsg(bytes.NewReader(hdr[:]), &Hello{})
	if err != ErrMessageTooLarge {
		t.Fatalf("err = %v, want ErrMessageTooLarge", err)
	}
}
