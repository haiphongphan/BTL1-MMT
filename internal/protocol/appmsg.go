package protocol

// Byte đầu tiên của mỗi stream tầng app cho biết loại stream (docs/protocol.md mục 3.5):
//
//	KindMessage: [0x01][JSON AppMsg ... hết stream]
//	KindFile:    [0x02][meta_len uint32][JSON FileMeta][nội dung file ... hết stream]
const (
	KindMessage byte = 0x01
	KindFile    byte = 0x02
)

// Các message handshake TCP, theo thứ tự (docs/protocol.md mục 3.2). Mỗi message được gửi bằng
// WriteMsg/ReadMsg: [4 byte độ dài][JSON].
//
//	client → server  Hello       khóa công khai X25519 của client
//	server → client  KeyX        khóa công khai của server + conn_id + cổng UDP
//	                             (cả 2 bên tự tính k_tcp, k_udp bằng ECDH, xem transport.DeriveKeys)
//	client → server  Auth        username/password đã mã hóa bằng k_tcp
//	server → client  AuthResult  AUTH_OK hoặc AUTH_FAIL, sau đó đóng TCP và chuyển sang UDP

// Hello là message đầu tiên client gửi: khóa công khai X25519 (32 byte).
type Hello struct {
	Type      string `json:"type"` // "HELLO"
	ClientPub []byte `json:"client_pub"`
}

// KeyX là trả lời của server cho Hello.
type KeyX struct {
	Type      string `json:"type"` // "KEYX"
	ServerPub []byte `json:"server_pub"`
	ConnID    uint32 `json:"conn_id"`  // định danh kết nối UDP sắp tạo, nằm trong header mọi gói
	UDPPort   int    `json:"udp_port"` // cổng UDP của server (mặc định 9001)
}

// Auth mang thông tin đăng nhập: Sealed = AES-GCM(k_tcp, Nonce, JSON {user, pass}).
// Mật khẩu không bao giờ đi rõ trên mạng, kể cả trên kết nối TCP.
type Auth struct {
	Type   string `json:"type"`  // "AUTH"
	Nonce  []byte `json:"nonce"` // 12 byte ngẫu nhiên
	Sealed []byte `json:"sealed"`
}

// AuthResult là kết quả đăng nhập; AUTH_OK kèm danh sách user đang online.
type AuthResult struct {
	Type   string   `json:"type"` // "AUTH_OK" hoặc "AUTH_FAIL"
	Reason string   `json:"reason,omitempty"`
	Users  []string `json:"users,omitempty"`
}

// AppMsg là nội dung 1 stream KindMessage: chat, tin riêng (dm), hoặc thông báo của server
// (danh sách user, có người vào/ra, lỗi).
type AppMsg struct {
	Type  string   `json:"type"` // chat | dm | users | join | leave | error
	From  string   `json:"from,omitempty"`
	To    string   `json:"to,omitempty"`
	Text  string   `json:"text,omitempty"`
	Users []string `json:"users,omitempty"`
	// Addrs: kèm theo tin "users", địa chỉ "ip:port" UDP mà server đang dùng để nói chuyện với từng user
	// (trước khi server nhận được gói UDP đầu tiên thì là IP lúc handshake TCP).
	Addrs map[string]string `json:"addrs,omitempty"`
	TS    int64             `json:"ts"` // UnixMilli
	// Seq: số thứ tự tăng dần của người gửi trên 1 kết nối (1, 2, 3...), người nhận dùng SeqBuffer để giao
	// đúng thứ tự. Server đánh lại Seq riêng cho từng người nhận. 0 = không cần thứ tự.
	Seq uint64 `json:"seq,omitempty"`
}

// FileMeta là phần đầu của 1 stream KindFile, mô tả file đi theo sau (xem EncodeFileHeader).
type FileMeta struct {
	ID     string `json:"id"` // id ngẫu nhiên do người gửi tạo, dùng để theo dõi tiến độ
	From   string `json:"from"`
	To     string `json:"to"` // người nhận
	Name   string `json:"name"`
	Size   int64  `json:"size"`   // byte
	SHA256 string `json:"sha256"` // hex; bên nhận tính lại để kiểm tra file nguyên vẹn
}
