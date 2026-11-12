package protocol

const (
	KindMessage byte = 0x01
	KindFile    byte = 0x02
)

type Hello struct {
	Type      string `json:"type"` //HELLO
	ClientPub []byte `json:"client_pub"`
}

type KeyX struct {
	Type      string `json:"type"` //KEYX
	ServerPub []byte `json:"server_pub"`
	ConnID    uint32 `json:"conn_id"`
	UDPPort   int    `json:"udp_port"`
}

type Auth struct {
	Type   string `json:"type"` //AUTH
	Nonce  []byte `json:"nonce"`
	Sealed []byte `json:"sealed"`
}

type AuthResult struct {
	Type   string   `json:"type"` // "AUTH_OK" hoặc "AUTH_FAIL"
	Reason string   `json:"reason,omitempty"`
	Users  []string `json:"users,omitempty"`
}

type AppMsg struct {
	Type  string   `json:"type"` // chat | dm | users | join | leave | error
	From  string   `json:"from,omitempty"`
	To    string   `json:"to,omitempty"`
	Text  string   `json:"text,omitempty"`
	Users []string `json:"users,omitempty"`
	TS    int64    `json:"ts"` // UnixMilli
	// Seq: số thứ tự tăng dần của người gửi trên 1 kết nối (1, 2, 3...), người nhận dùng SeqBuffer để giao
	// đúng thứ tự. Server đánh lại Seq riêng cho từng người nhận. 0 = không cần thứ tự.
	Seq uint64 `json:"seq,omitempty"`
}

type FileMeta struct {
	ID     string `json:"id"`
	From   string `json:"from"`
	To     string `json:"to"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
