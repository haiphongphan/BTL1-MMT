package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{Magic: Magic, Version: Version, Flags: 0x5A, ConnID: 0xDEADBEEF, PacketNum: 123456}
	buf := make([]byte, HeaderSize)
	h.Marshal(buf)

	// layout cố định theo docs/protocol.md mục 3.3, big-endian
	want := []byte{0xC0, 0xDE, 0x01, 0x5A, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0xE2, 0x40}
	if !bytes.Equal(buf, want) {
		t.Fatalf("bytes = % x, want % x", buf, want)
	}

	got, err := ParseHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Fatalf("got %+v, want %+v", got, h)
	}
}

func TestParseHeaderShort(t *testing.T) {
	if _, err := ParseHeader(make([]byte, HeaderSize-1)); !errors.Is(err, ErrShort) {
		t.Fatalf("err = %v, want ErrShort", err)
	}
}

func TestSizeConstants(t *testing.T) {
	if MaxFramePayload != 1172 {
		t.Fatalf("MaxFramePayload = %d, want 1172", MaxFramePayload)
	}
	// 1 STREAM đầy (12 + 1000) + 1 ACK tối đa (70) + 1 PING (1) phải vừa 1 gói
	if 12+MaxStreamData+70+1 > MaxFramePayload {
		t.Fatal("STREAM + ACK + PING không vừa 1 gói")
	}
}
