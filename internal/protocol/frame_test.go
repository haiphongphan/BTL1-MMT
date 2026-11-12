package protocol

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestStreamFrameRoundTrip(t *testing.T) {
	for _, f := range []*StreamFrame{
		{StreamID: 1, Offset: 0, Data: []byte("hello")},
		{StreamID: 0xFFFFFFFF, Offset: 0xFFFFFFF0, Data: bytes.Repeat([]byte{0xAB}, MaxStreamData), Fin: true},
		{StreamID: 4, Offset: 2000, Fin: true}, // FIN rỗng
	} {
		b := AppendStream(nil, f)
		if len(b) != 12+len(f.Data) {
			t.Fatalf("len = %d, want %d", len(b), 12+len(f.Data))
		}
		got, err := ParseFrames(b)
		if err != nil {
			t.Fatal(err)
		}
		g := got[0].(*StreamFrame)
		if g.StreamID != f.StreamID || g.Offset != f.Offset || g.Fin != f.Fin || !bytes.Equal(g.Data, f.Data) {
			t.Fatalf("got %+v, want %+v", g, f)
		}
	}
}

func TestAckFrameRoundTrip(t *testing.T) {
	// ví dụ trong docs/protocol.md: đã nhận 1–5, 7–9, 12
	a := &AckFrame{Window: 48000, Ranges: []AckRange{{12, 12}, {7, 9}, {1, 5}}}
	b := AppendAck(nil, a)
	if len(b) != 6+3*8 {
		t.Fatalf("len = %d, want 30", len(b))
	}
	got, err := ParseFrames(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[0], a) {
		t.Fatalf("got %+v, want %+v", got[0], a)
	}

	for pn, want := range map[uint32]bool{1: true, 5: true, 6: false, 8: true, 10: false, 11: false, 12: true, 13: false} {
		if a.Contains(pn) != want {
			t.Errorf("Contains(%d) = %v, want %v", pn, !want, want)
		}
	}
}

func TestPingCloseRoundTrip(t *testing.T) {
	b := AppendPing(nil)
	b = AppendClose(b, &CloseFrame{Reason: "timeout"})
	got, err := ParseFrames(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d frames", len(got))
	}
	if _, ok := got[0].(*PingFrame); !ok {
		t.Fatalf("frame 0 = %T", got[0])
	}
	if c := got[1].(*CloseFrame); c.Reason != "timeout" {
		t.Fatalf("reason = %q", c.Reason)
	}
}

// Nhiều frame liền nhau trong 1 gói (ACK + STREAM + PING), tổng vẫn vừa MaxFramePayload.
func TestMultipleFramesInOnePacket(t *testing.T) {
	ack := &AckFrame{Window: 1 << 20, Ranges: make([]AckRange, 8)}
	st := &StreamFrame{StreamID: 3, Offset: 1000, Data: bytes.Repeat([]byte("x"), MaxStreamData)}

	var b []byte
	b = AppendAck(b, ack)
	b = AppendStream(b, st)
	b = AppendPing(b)
	if len(b) > MaxFramePayload {
		t.Fatalf("payload %d > MaxFramePayload %d", len(b), MaxFramePayload)
	}

	got, err := ParseFrames(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d frames, want 3", len(got))
	}
	if _, ok := got[0].(*AckFrame); !ok {
		t.Errorf("frame 0 = %T", got[0])
	}
	if s, ok := got[1].(*StreamFrame); !ok || s.Offset != 1000 || len(s.Data) != MaxStreamData {
		t.Errorf("frame 1 = %+v", got[1])
	}
	if _, ok := got[2].(*PingFrame); !ok {
		t.Errorf("frame 2 = %T", got[2])
	}
}

// Dữ liệu từ mạng không tin được: mọi cách cắt cụt đều phải trả lỗi, không được panic.
func TestParseFramesTruncated(t *testing.T) {
	full := AppendStream(nil, &StreamFrame{StreamID: 1, Data: []byte("abcdef")})
	full = AppendAck(full, &AckFrame{Ranges: []AckRange{{1, 2}, {4, 4}}})
	full = AppendClose(full, &CloseFrame{Reason: "bye"})

	boundaries := map[int]bool{0: true, 18: true, 18 + 22: true, len(full): true} // các chỗ cắt trùng ranh giới frame
	for n := 1; n < len(full); n++ {
		_, err := ParseFrames(full[:n])
		if boundaries[n] {
			if err != nil {
				t.Errorf("cut at frame boundary %d: unexpected err %v", n, err)
			}
			continue
		}
		if !errors.Is(err, ErrShort) {
			t.Errorf("cut at %d: err = %v, want ErrShort", n, err)
		}
	}
}

func TestParseFramesUnknownType(t *testing.T) {
	if _, err := ParseFrames([]byte{0x7F}); err == nil {
		t.Fatal("unknown frame type must return error")
	}
	// type lạ đứng sau 1 frame hợp lệ cũng phải báo lỗi
	if _, err := ParseFrames(append(AppendPing(nil), 0x00)); err == nil {
		t.Fatal("unknown frame type after PING must return error")
	}
}

// reason_len chỉ 1 byte: reason dài phải bị cắt ≤ 255 byte và không cắt giữa ký tự UTF-8.
func TestCloseReasonTruncatedOnRuneBoundary(t *testing.T) {
	reason := strings.Repeat("ố", 200) // mỗi ký tự 3 byte -> 600 byte
	got, err := ParseFrames(AppendClose(nil, &CloseFrame{Reason: reason}))
	if err != nil {
		t.Fatal(err)
	}
	r := got[0].(*CloseFrame).Reason
	if len(r) > MaxCloseReason {
		t.Fatalf("len = %d > %d", len(r), MaxCloseReason)
	}
	if !utf8.ValidString(r) {
		t.Fatal("reason bị cắt giữa ký tự UTF-8")
	}
	if !strings.HasPrefix(reason, r) || len(r) < MaxCloseReason-3 {
		t.Fatalf("cắt sai: len = %d", len(r))
	}
}
