// internal/transport/crypto.go
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

// DeriveKeys tính 2 khóa 32 byte (AES-256) từ khóa riêng của mình + public key bên kia.
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

func NewAEAD(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func makeNonce(connID, pn uint32, dir byte) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint32(n[0:4], connID)
	binary.BigEndian.PutUint32(n[4:8], pn)
	n[8] = dir
	return n
}
