// Package transport biến UDP thành kênh tin cậy, mã hóa (AES-GCM), ghép nhiều stream độc lập
// (không Head-of-Line blocking) và tự điều tốc (CUBIC). Xem docs/reliability.md.
package transport

import (
	"io"
	"net"
	"time"
)

// Delivery là 1 đoạn byte ĐÃ ĐÚNG THỨ TỰ của 1 stream. Các Delivery của cùng 1 stream tới đúng thứ tự,
// nhưng Delivery của các stream khác nhau có thể xen kẽ nhau (stream nào đủ dữ liệu trước thì giao trước).
type Delivery struct {
	StreamID uint32
	Data     []byte
	Fin      bool // true = stream kết thúc, đây là Delivery cuối của stream (Data có thể rỗng)
}

// Stats là ảnh chụp số liệu của 1 kết nối, hiển thị trên web UI và dùng khi test.
type Stats struct {
	RTT      time.Duration // SRTT (RTT đã làm mượt)
	Cwnd     int           // cửa sổ nghẽn, byte
	InFlight int           // số byte đã gửi mà chưa được ACK
	LostPkts uint64        // số gói bị coi là mất (đã gửi lại)
	SentPkts uint64        // tổng số gói đã gửi, kể cả gói chỉ có ACK/PING
	State    string        // "waiting" (server chưa biết địa chỉ client) | "connected" | "closed"

	Peer      *net.UDPAddr // địa chỉ UDP hiện tại của bên kia (IP:port); nil khi chưa nhận gói nào
	LastHeard time.Time    // lúc nhận gói hợp lệ gần nhất (đã qua xác thực GCM); chưa nhận gói nào = lúc tạo kết nối
}

// Endpoint là những gì tầng app cần từ tầng transport. Có 2 bản cài đặt: *Conn (UDP thật) và
// FakeEndpoint (nối 2 đầu trong bộ nhớ, để test tầng app khi chưa có transport).
type Endpoint interface {
	SendMessage(b []byte) error            // gửi 1 message = 1 stream ưu tiên cao, tự đóng sau khi ghi
	OpenStream(urgent bool) io.WriteCloser // mở stream dài (vd file); Close() = gửi FIN
	Recv() <-chan Delivery                 // dữ liệu nhận được
	Done() <-chan struct{}                 // đóng khi mất kết nối / idle timeout / Close()
	Stats() Stats
	Close() error
}
