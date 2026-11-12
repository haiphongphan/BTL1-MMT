package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// maxMsgSize: message handshake tối đa 1 MB. Độ dài do bên kia tự khai, không giới hạn thì kẻ xấu
// gửi độ dài 4 GB là làm server cấp phát hết RAM (OOM).
const maxMsgSize = 1 << 20

// ErrMessageTooLarge: độ dài message handshake vượt maxMsgSize.
var ErrMessageTooLarge = errors.New("protocol: message vượt quá giới hạn 1MB")

// WriteMsg ghi 1 message handshake TCP: [4 byte độ dài big-endian][JSON].
func WriteMsg(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// ReadMsg đọc 1 message handshake TCP và giải JSON vào v; từ chối message quá lớn.
func ReadMsg(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxMsgSize {
		return ErrMessageTooLarge
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
