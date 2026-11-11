package protocol

import (
	"bytes"
	"testing"
)

// Header bị cắt thành từng byte một vẫn phải parse đúng
func TestFileHeaderByteByByte(t *testing.T) {
	in := FileMeta{ID: "f1", From: "alice", To: "bob", Name: "a.pdf", Size: 42, SHA256: "abc"}
	hdr, err := EncodeFileHeader(in)
	if err != nil {
		t.Fatal(err)
	}
	stream := append(hdr, []byte("DATA")...)

	var p FileHeaderParser
	for i := 0; i < len(stream); i++ {
		m, rest, err := p.Feed(stream[i : i+1])
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			continue
		}
		if *m != in {
			t.Fatalf("got %+v, want %+v", *m, in)
		}
		if i != len(hdr)-1 || len(rest) != 0 {
			t.Fatalf("header xong ở byte %d, rest=%q", i, rest)
		}
		return
	}
	t.Fatal("không parse được header")
}

func TestFileHeaderRest(t *testing.T) {
	hdr, _ := EncodeFileHeader(FileMeta{Name: "x"})
	var p FileHeaderParser
	m, rest, err := p.Feed(append(hdr, []byte("DATA")...))
	if err != nil || m == nil || !bytes.Equal(rest, []byte("DATA")) {
		t.Fatalf("m=%v rest=%q err=%v", m, rest, err)
	}
}

func TestFileHeaderRejectsHugeLen(t *testing.T) {
	var p FileHeaderParser
	if _, _, err := p.Feed([]byte{KindFile, 0xFF, 0xFF, 0xFF, 0xFF}); err != ErrBadFileHeader {
		t.Fatalf("err = %v", err)
	}
}
