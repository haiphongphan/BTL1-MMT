// Package protocol định nghĩa định dạng trên dây: header gói UDP, các frame, message handshake TCP,
// nội dung stream tầng app (AppMsg, FileMeta) và SeqBuffer giữ thứ tự tin nhắn.
package protocol

import (
	"encoding/binary"
	"errors"
)

// Cấu trúc 1 gói UDP (docs/protocol.md mục 3.3):
//
//	[magic 2][version 1][flags 1][conn_id 4][packet_num 4] [ AES-GCM(frames) + tag 16 ]
//	 \_____________ header 12 byte, gửi rõ ____________/  \____ phần mã hóa ____/
//
// Header không mã hóa (bên nhận cần conn_id để tìm khóa) nhưng được dùng làm AAD của GCM:
// ai sửa 1 bit trong header thì giải mã thất bại.
const (
	Magic   uint16 = 0xC0DE // 2 byte đầu mọi gói, lọc nhanh gói rác không phải của giao thức này
	Version byte   = 1      // phiên bản giao thức; khác thì bỏ gói

	// HeaderSize = magic(2) + version(1) + flags(1) + conn_id(4) + packet_num(4) = 12.
	HeaderSize = 12

	// MaxPacket lấy từ QUIC (RFC 9000): mạng IPv6 bắt buộc truyền được gói 1280 byte, trừ header IPv6 (40)
	// và UDP (8) còn 1232, QUIC làm tròn xuống 1200 cho có dư. Giữ gói ≤ 1200 byte thì gần như không bị
	// phân mảnh IP (fragmentation) trên mọi đường truyền. Phân mảnh rất có hại: mất 1 mảnh là mất cả gói.
	MaxPacket = 1200

	// TagSize: GCM luôn gắn tag xác thực 16 byte vào cuối ciphertext (mặc định của cipher.NewGCM).
	TagSize = 16

	// MaxFramePayload = 1200 − 12 − 16 = 1172 byte: chỗ cho các frame (plaintext) trong 1 gói.
	MaxFramePayload = MaxPacket - HeaderSize - TagSize

	// MaxStreamData: số byte data tối đa của 1 STREAM frame. Tự chọn, có chủ đích: 1 STREAM frame tốn
	// 12 byte header + 1000 byte data = 1012, còn khoảng 160 byte để ghép thêm 1 ACK frame (tối đa 70 byte)
	// và PING/CLOSE vào cùng gói mà vẫn ≤ 1172. Số 1000 tròn nên dễ tính offset khi debug.
	MaxStreamData = 1000
)

// ErrShort: dữ liệu ngắn hơn độ dài mà header/frame khai báo (gói cụt hoặc hỏng).
var ErrShort = errors.New("protocol: data too short")

// Header là 12 byte đầu của mọi gói UDP, gửi không mã hóa.
type Header struct {
	Magic     uint16
	Version   byte
	Flags     byte   // chưa dùng, luôn 0 (để dành cho mở rộng)
	ConnID    uint32 // định danh kết nối, server cấp trong handshake TCP; server demux theo trường này chứ không theo IP:port
	PacketNum uint32 // số thứ tự gói, tăng dần và không bao giờ dùng lại (gửi lại thì dùng số mới); là 1 phần của nonce
}

// Marshal ghi header vào 12 byte đầu của buffer (yêu cầu len(buffer) >= HeaderSize).
// Ghi thẳng vào buffer của gói thay vì trả về slice mới để không phải cấp phát + copy cho mỗi gói gửi đi.
func (h *Header) Marshal(buffer []byte) {
	binary.BigEndian.PutUint16(buffer[0:2], h.Magic)
	buffer[2] = h.Version
	buffer[3] = h.Flags
	binary.BigEndian.PutUint32(buffer[4:8], h.ConnID)
	binary.BigEndian.PutUint32(buffer[8:12], h.PacketNum)
}

// ParseHeader đọc header 12 byte ở đầu gói UDP.
func ParseHeader(buffer []byte) (Header, error) {
	if len(buffer) < HeaderSize {
		return Header{}, ErrShort
	}

	return Header{
		Magic:     binary.BigEndian.Uint16(buffer[0:2]),
		Version:   buffer[2],
		Flags:     buffer[3],
		ConnID:    binary.BigEndian.Uint32(buffer[4:8]),
		PacketNum: binary.BigEndian.Uint32(buffer[8:12]),
	}, nil
}
