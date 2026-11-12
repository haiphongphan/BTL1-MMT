package transport

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
)

// NewKeyPair sinh cặp khóa X25519. Gửi priv.PublicKey().Bytes() cho bên kia.
func NewKeyPair() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().GenerateKey(rand.Reader)
}

// DeriveKeys tính 2 khóa 32 byte (AES-256) từ khóa riêng của mình + public key bên kia:
//
//	shared = X25519(priv, peerPub)    (2 bên tính ra cùng 1 giá trị mà không ai gửi nó qua mạng)
//	kTCP   = SHA256(shared ‖ "tcp")   mã hóa username/password trong handshake TCP
//	kUDP   = SHA256(shared ‖ "udp")   mã hóa mọi gói UDP
//
// Tách 2 khóa để không bao giờ dùng cùng 1 khóa cho 2 kênh có cách sinh nonce khác nhau (tránh trùng nonce).
// Đây là KDF đơn giản hóa; sản phẩm thật nên dùng HKDF.
func DeriveKeys(priv *ecdh.PrivateKey, peerPub []byte) (kTCP, kUDP [32]byte, err error) {
	pub, err := ecdh.X25519().NewPublicKey(peerPub)
	if err != nil {
		return
	}
	shared, err := priv.ECDH(pub)
	if err != nil {
		return
	}
	kTCP = sha256.Sum256(append(append([]byte{}, shared...), "tcp"...))
	kUDP = sha256.Sum256(append(append([]byte{}, shared...), "udp"...))
	return
}

// NewAEAD tạo bộ mã hóa AES-256-GCM từ khóa 32 byte.
func NewAEAD(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// makeNonce tạo nonce GCM 12 byte cho 1 gói UDP:
//
//	[conn_id 4][packet_num 4][direction 1][0 0 0]
//
// GCM yêu cầu không bao giờ dùng lại 1 nonce với cùng khóa (trùng nonce là lộ được XOR của 2 bản rõ và
// giả mạo được tag). Mỗi kết nối có khóa riêng; trong 1 kết nối, packet_num tăng dần và không bao giờ dùng lại
// (gửi lại dùng số mới); còn direction (0 = client→server, 1 = server→client) tách 2 chiều, vì cả 2 bên
// đều đánh packet_num từ 0 với cùng khóa k_udp. Nonce không cần gửi kèm: bên nhận tự tính từ header.
func makeNonce(connID, pn uint32, dir byte) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint32(n[0:4], connID)
	binary.BigEndian.PutUint32(n[4:8], pn)
	n[8] = dir
	return n
}
