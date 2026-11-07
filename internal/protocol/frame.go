package protocol

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

const (
	FrameStream byte = 0x01
	FrameAck    byte = 0x02
	FramePing   byte = 0x03
	FrameClose  byte = 0x04
)

// reason_len chỉ có 1 byte nên reason tối đa 255 byte (docs/protocol.md mục 3.4).
const MaxCloseReason = 255

type Frame interface {
	isFrame()
}

type StreamFrame struct {
	Fin      bool
	StreamID uint32
	Offset   uint32
	Data     []byte
}

type AckRange struct {
	Start, End uint32
}

type AckFrame struct {
	Window uint32
	Ranges []AckRange
}

type PingFrame struct{}

type CloseFrame struct {
	Reason string
}

func (*StreamFrame) isFrame() {}
func (*AckFrame) isFrame()    {}
func (*PingFrame) isFrame()   {}
func (*CloseFrame) isFrame()  {}

// packet có nằm trong ACK này không
func (a *AckFrame) Contains(p uint32) bool {
	for _, r := range a.Ranges {
		if p >= r.Start && p <= r.End {
			return true
		}
	}
	return false
}

func AppendStream(b []byte, f *StreamFrame) []byte {
	var flags byte
	if f.Fin {
		flags |= 1
	}
	b = append(b, FrameStream, flags)
	b = binary.BigEndian.AppendUint32(b, f.StreamID)
	b = binary.BigEndian.AppendUint32(b, f.Offset)
	b = binary.BigEndian.AppendUint16(b, uint16(len(f.Data)))
	return append(b, f.Data...)
}

func AppendAck(b []byte, a *AckFrame) []byte {
	b = append(b, FrameAck)
	b = binary.BigEndian.AppendUint32(b, a.Window)
	b = append(b, byte(len(a.Ranges)))
	for _, r := range a.Ranges {
		b = binary.BigEndian.AppendUint32(b, r.Start)
		b = binary.BigEndian.AppendUint32(b, r.End)
	}
	return b
}

func AppendPing(b []byte) []byte {
	return append(b, FramePing)
}

func AppendClose(b []byte, c *CloseFrame) []byte {
	r := c.Reason
	if len(r) > MaxCloseReason {
		// Lùi về đầu ký tự UTF-8 gần nhất để không cắt giữa ký tự nhiều byte.
		n := MaxCloseReason
		for n > 0 && !utf8.RuneStart(r[n]) {
			n--
		}
		r = r[:n]
	}
	b = append(b, FrameClose, byte(len(r)))
	return append(b, r...)
}

func ParseFrames(b []byte) ([]Frame, error) {
	var out []Frame
	for len(b) > 0 {
		switch b[0] {
		case FrameStream:
			if len(b) < 12 {

				return nil, ErrShort
			}

			n := int(binary.BigEndian.Uint16(b[10:12]))
			if len(b) < 12+n {
				return nil, ErrShort
			}

			out = append(out, &StreamFrame{
				Fin:      b[1]&1 == 1,
				StreamID: binary.BigEndian.Uint32(b[2:6]),
				Offset:   binary.BigEndian.Uint32(b[6:10]),
				Data:     b[12 : 12+n],
			})
			b = b[12+n:]

		case FrameAck:
			if len(b) < 6 {
				return nil, ErrShort
			}

			a := &AckFrame{
				Window: binary.BigEndian.Uint32(b[1:5]),
			}

			cnt := int(b[5])
			b = b[6:]
			if len(b) < cnt*8 {
				return nil, ErrShort
			}

			for i := 0; i < cnt; i++ {
				a.Ranges = append(a.Ranges, AckRange{
					Start: binary.BigEndian.Uint32(b[0:4]),
					End:   binary.BigEndian.Uint32(b[4:8]),
				})

				b = b[8:]
			}
			out = append(out, a)

		case FramePing:
			out = append(out, &PingFrame{})
			b = b[1:]

		case FrameClose:
			if len(b) < 2 {
				return nil, ErrShort
			}

			n := int(b[1])
			if len(b) < 2+n {
				return nil, ErrShort
			}

			out = append(out, &CloseFrame{string(b[2 : 2+n])})

			b = b[2+n:]

		default:
			return nil, fmt.Errorf("unexpected frame 0x%02x", b[0])
		}
	}
	return out, nil
}
