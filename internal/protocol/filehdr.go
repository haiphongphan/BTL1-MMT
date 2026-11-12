package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
)

const maxFileMeta = 64 << 10 // meta JSON tối đa 64 KB

var ErrBadFileHeader = errors.New("protocol: header file không hợp lệ")

// EncodeFileHeader tạo phần đầu stream file: [0x02][meta_len uint32][JSON FileMeta].
func EncodeFileHeader(m FileMeta) ([]byte, error) {
	meta, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	b := []byte{KindFile}
	b = binary.BigEndian.AppendUint32(b, uint32(len(meta)))
	return append(b, meta...), nil
}

type FileHeaderParser struct {
	buf []byte
}

// Feed nhận thêm dữ liệu đầu stream file. Khi đủ header thì trả về meta và phần dữ liệu file còn lại
// trong b; chưa đủ thì meta = nil.
func (p *FileHeaderParser) Feed(b []byte) (meta *FileMeta, rest []byte, err error) {
	p.buf = append(p.buf, b...)
	if len(p.buf) < 5 {
		return nil, nil, nil
	}
	if p.buf[0] != KindFile {
		return nil, nil, ErrBadFileHeader
	}
	n := int(binary.BigEndian.Uint32(p.buf[1:5]))
	if n > maxFileMeta {
		return nil, nil, ErrBadFileHeader
	}
	if len(p.buf) < 5+n {
		return nil, nil, nil
	}
	var m FileMeta
	if err := json.Unmarshal(p.buf[5:5+n], &m); err != nil {
		return nil, nil, ErrBadFileHeader
	}
	rest = p.buf[5+n:]
	p.buf = nil
	return &m, rest, nil
}
