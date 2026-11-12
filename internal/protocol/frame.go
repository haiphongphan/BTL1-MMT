package protocol

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// Byte đầu tiên của mỗi frame cho biết loại frame (docs/protocol.md mục 3.4).
// Các frame xếp liền nhau trong phần plaintext của gói, không có dấu phân cách: bên nhận đọc byte loại
// để biết độ dài frame, cắt ra rồi đọc frame tiếp theo. Mọi số nguyên đều big-endian.
const (
	FrameStream byte = 0x01 // dữ liệu của 1 stream, cần ACK
	FrameAck    byte = 0x02 // xác nhận các packet_num đã nhận + cửa sổ nhận, không cần ACK lại
	FramePing   byte = 0x03 // giữ kết nối / đo RTT, cần ACK
	FrameClose  byte = 0x04 // đóng kết nối, không cần ACK
)

// Kích thước phần cố định của các frame (byte).
const (
	StreamFrameHeader = 12 // type(1) flags(1) stream_id(4) offset(4) len(2)
	AckFrameHeader    = 6  // type(1) window(4) n(1)
	AckRangeSize      = 8  // start(4) end(4)
	closeFrameHeader  = 2  // type(1) reason_len(1)
)

// MaxCloseReason: reason_len chỉ có 1 byte nên reason tối đa 255 byte (docs/protocol.md mục 3.4).
const MaxCloseReason = 255

// Frame là 1 trong 4 loại frame: *StreamFrame, *AckFrame, *PingFrame, *CloseFrame.
// Dùng type switch để phân biệt; isFrame() chỉ để package khác không tự tạo thêm loại frame.
type Frame interface {
	isFrame()
}

// StreamFrame mang 1 đoạn dữ liệu của 1 stream.
//
//	[0x01][flags 1, bit0 = FIN][stream_id 4][offset 4][len 2][data len byte]
//
// Offset là vị trí của byte đầu tiên của Data trong stream; bên nhận ghép lại theo offset nên frame tới
// lệch thứ tự hay bị gửi lại đều không sao. FIN = đây là đoạn cuối, stream dài Offset+len(Data) byte.
type StreamFrame struct {
	Fin      bool
	StreamID uint32 // client mở stream id lẻ, server mở stream id chẵn -> 2 bên không bao giờ trùng
	Offset   uint32
	Data     []byte // tối đa MaxStreamData byte
}

// AckRange là 1 đoạn packet_num liên tục [Start..End] (tính cả 2 đầu) đã nhận được.
type AckRange struct {
	Start, End uint32
}

// AckFrame xác nhận các gói đã nhận (kiểu SACK: liệt kê các đoạn, không chỉ "đã nhận liên tục tới N").
//
//	[0x02][window 4][n 1][n × (start 4, end 4)]     n ≤ 8, tối đa 70 byte
//
// Ranges xếp từ packet_num lớn nhất xuống, nên Ranges[0].End là gói mới nhất bên nhận đã thấy.
// Ví dụ đã nhận 1–5, 7–9, 12 (thiếu 6, 10, 11) → Ranges = [12..12] [7..9] [1..5].
type AckFrame struct {
	Window uint32 // số byte bên nhận còn trống (flow control): bên gửi không được để quá chừng này byte đang bay
	Ranges []AckRange
}

// PingFrame không mang dữ liệu: [0x03]. Dùng để giữ kết nối khi rảnh (keep-alive) và để báo cho server
// biết địa chỉ UDP của client ở gói đầu tiên. Bên nhận phải ACK.
type PingFrame struct{}

// CloseFrame báo đóng kết nối ngay, bên kia không phải đợi idle timeout.
//
//	[0x04][reason_len 1][reason, UTF-8]
type CloseFrame struct {
	Reason string // tối đa MaxCloseReason byte, AppendClose tự cắt nếu dài hơn
}

func (*StreamFrame) isFrame() {}
func (*AckFrame) isFrame()    {}
func (*PingFrame) isFrame()   {}
func (*CloseFrame) isFrame()  {}

// Contains cho biết packet_num p có nằm trong 1 range nào của ACK này không (tức là gói p đã tới nơi).
func (a *AckFrame) Contains(p uint32) bool {
	for _, r := range a.Ranges {
		if p >= r.Start && p <= r.End {
			return true
		}
	}
	return false
}

// AppendStream nối 1 STREAM frame vào b.
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

// AppendAck nối 1 ACK frame (cửa sổ nhận + các range) vào b.
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

// AppendPing nối 1 PING frame vào b.
func AppendPing(b []byte) []byte {
	return append(b, FramePing)
}

// AppendClose nối 1 CLOSE frame (kèm lý do) vào b.
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

// ParseFrames tách phần payload đã giải mã thành các frame; dữ liệu cụt hoặc loại frame lạ trả về lỗi.
// Data của StreamFrame trỏ thẳng vào b (không copy), b phải còn nguyên khi frame còn được dùng.
func ParseFrames(b []byte) ([]Frame, error) {
	var out []Frame
	for len(b) > 0 {
		switch b[0] {
		case FrameStream:
			if len(b) < StreamFrameHeader {
				return nil, ErrShort
			}

			n := int(binary.BigEndian.Uint16(b[10:12])) // trường len
			if len(b) < StreamFrameHeader+n {
				return nil, ErrShort
			}

			out = append(out, &StreamFrame{
				Fin:      b[1]&1 == 1,
				StreamID: binary.BigEndian.Uint32(b[2:6]),
				Offset:   binary.BigEndian.Uint32(b[6:10]),
				Data:     b[StreamFrameHeader : StreamFrameHeader+n],
			})
			b = b[StreamFrameHeader+n:]

		case FrameAck:
			if len(b) < AckFrameHeader {
				return nil, ErrShort
			}

			a := &AckFrame{
				Window: binary.BigEndian.Uint32(b[1:5]),
			}

			cnt := int(b[5]) // n: số range
			b = b[AckFrameHeader:]
			if len(b) < cnt*AckRangeSize {
				return nil, ErrShort
			}

			for i := 0; i < cnt; i++ {
				a.Ranges = append(a.Ranges, AckRange{
					Start: binary.BigEndian.Uint32(b[0:4]),
					End:   binary.BigEndian.Uint32(b[4:8]),
				})

				b = b[AckRangeSize:]
			}
			out = append(out, a)

		case FramePing:
			out = append(out, &PingFrame{})
			b = b[1:]

		case FrameClose:
			if len(b) < closeFrameHeader {
				return nil, ErrShort
			}

			n := int(b[1]) // reason_len
			if len(b) < closeFrameHeader+n {
				return nil, ErrShort
			}

			out = append(out, &CloseFrame{string(b[closeFrameHeader : closeFrameHeader+n])})

			b = b[closeFrameHeader+n:]

		default:
			return nil, fmt.Errorf("unexpected frame 0x%02x", b[0])
		}
	}
	return out, nil
}
