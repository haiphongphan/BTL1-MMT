package protocol

import (
	"encoding/binary"
	"errors"
)

/*
Định nghĩa gói UDP
*/
const (
	Magic   uint16 = 0xC0DE
	Version byte   = 1
	// magic(2) + version(1) + flags(1) + conn_id(4) + packet_num(4) = 12.
	HeaderSize = 12
	//Lấy từ QUIC (RFC 9000). Mạng IPv6 bắt buộc truyền được gói 1280 byte. Trừ header IPv6 (40) và UDP (8) còn 1232. QUIC lấy tròn xuống 1200 cho có dư. Giữ gói ≤ 1200 byte thì gói gần như không bị phân mảnh IP (fragmentation) trên mọi đường truyền. Phân mảnh rất có hại: mất 1 mảnh là mất cả gói.
	MaxPacket = 1200
	// Theo chuẩn AES-GCM. GCM luôn gắn thêm tag xác thực 16 byte vào cuối ciphertext (cipher.NewGCM của Go mặc định là 16).
	TagSize         = 16
	MaxFramePayload = MaxPacket - HeaderSize - TagSize
	// Tự chọn, có chủ đích. Một STREAM frame tốn 12 byte header frame + 1000 byte data = 1012. Như vậy còn khoảng 160 byte để ghép thêm 1 ACK frame (tối đa 8 range: 1 + 4 + 1 + 8×8 = 70 byte) và PING/CLOSE vào cùng gói, mà vẫn ≤ 1172. Số 1000 tròn nên cũng dễ tính offset khi debug.
	MaxStreamData = 1000
)

var ErrShort = errors.New("protocol: data too short")

type Header struct {
	Magic     uint16
	Version   byte
	Flags     byte
	ConnID    uint32
	PacketNum uint32
}

// len(buffer) >= HeaderSize
// writing straight into the packet buffer avoids an extra allocation and copy for every packet sent
func (h *Header) Marshal(buffer []byte) {
	binary.BigEndian.PutUint16(buffer[0:2], h.Magic)
	buffer[2] = h.Version
	buffer[3] = h.Flags
	binary.BigEndian.PutUint32(buffer[4:8], h.ConnID)
	binary.BigEndian.PutUint32(buffer[8:12], h.PacketNum)
}

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
