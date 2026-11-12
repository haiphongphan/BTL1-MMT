package transport

import (
	"bytes"
	"testing"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

func TestDeriveKeysBothSidesAgree(t *testing.T) {
	a, _ := NewKeyPair()
	b, _ := NewKeyPair()

	aTCP, aUDP, err := DeriveKeys(a, b.PublicKey().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	bTCP, bUDP, err := DeriveKeys(b, a.PublicKey().Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if aTCP != bTCP || aUDP != bUDP {
		t.Fatal("2 bên ECDH ra khóa khác nhau")
	}
	if aTCP == aUDP {
		t.Fatal("k_tcp và k_udp phải khác nhau")
	}

	c, _ := NewKeyPair() // người thứ 3 không ra được cùng khóa
	cTCP, _, _ := DeriveKeys(c, b.PublicKey().Bytes())
	if cTCP == aTCP {
		t.Fatal("khóa của bên thứ 3 trùng")
	}
}

func TestDeriveKeysRejectsBadPublicKey(t *testing.T) {
	a, _ := NewKeyPair()
	if _, _, err := DeriveKeys(a, []byte{1, 2, 3}); err == nil {
		t.Fatal("public key sai độ dài phải báo lỗi")
	}
}

func sealTestPacket(t *testing.T) (*Conn, []byte, []byte) {
	t.Helper()
	var key [32]byte
	key[0] = 42
	aead, err := NewAEAD(key)
	if err != nil {
		t.Fatal(err)
	}
	c := &Conn{id: 7, aead: aead, sendDir: dirClientToServer}
	payload := protocol.AppendStream(nil, &protocol.StreamFrame{StreamID: 1, Data: []byte("bí mật")})
	return c, c.seal(5, payload), payload
}

func open(c *Conn, pkt []byte, dir byte) ([]byte, error) {
	h, _ := protocol.ParseHeader(pkt)
	return c.aead.Open(nil, makeNonce(h.ConnID, h.PacketNum, dir), pkt[protocol.HeaderSize:], pkt[:protocol.HeaderSize])
}

func TestSealOpenRoundTrip(t *testing.T) {
	c, pkt, payload := sealTestPacket(t)

	if len(pkt) != protocol.HeaderSize+len(payload)+protocol.TagSize {
		t.Fatalf("len = %d", len(pkt))
	}
	if bytes.Contains(pkt, []byte("bí mật")) {
		t.Fatal("plaintext lộ trong gói")
	}

	plain, err := open(c, pkt, dirClientToServer)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain, payload) {
		t.Fatal("giải mã ra sai nội dung")
	}
}

// Header gửi rõ nhưng là AAD của GCM: sửa bất kỳ byte nào của header hay ciphertext đều làm Open thất bại.
func TestTamperedPacketRejected(t *testing.T) {
	c, pkt, _ := sealTestPacket(t)

	for i := range pkt {
		bad := append([]byte(nil), pkt...)
		bad[i] ^= 0x01 // byte 0–11: header (AAD, conn_id/pn còn nằm trong nonce); từ byte 12: ciphertext + tag
		if _, err := open(c, bad, dirClientToServer); err == nil {
			t.Fatalf("sửa byte %d mà vẫn giải mã được", i)
		}
	}
}

// Byte direction trong nonce: gói chiều client->server không mở được bằng nonce chiều ngược lại.
func TestWrongDirectionRejected(t *testing.T) {
	c, pkt, _ := sealTestPacket(t)
	if _, err := open(c, pkt, dirServerToClient); err == nil {
		t.Fatal("nonce sai hướng mà vẫn giải mã được")
	}
}

func TestNonceUniquePerDirectionAndPN(t *testing.T) {
	seen := map[string]bool{}
	for _, dir := range []byte{dirClientToServer, dirServerToClient} {
		for pn := uint32(0); pn < 100; pn++ {
			n := string(makeNonce(7, pn, dir))
			if seen[n] {
				t.Fatalf("trùng nonce: dir=%d pn=%d", dir, pn)
			}
			seen[n] = true
		}
	}
}
