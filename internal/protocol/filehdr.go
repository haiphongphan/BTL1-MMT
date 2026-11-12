package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
)

const maxFileMeta = 64 << 10 // meta JSON tối đa 64 KB, chặn meta_len giả mạo làm cấp phát bộ nhớ vô hạn

// ErrBadFileHeader: stream file không bắt đầu bằng KindFile, meta_len quá lớn hoặc JSON hỏng.
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

// FileHeaderParser đọc header của stream file khi dữ liệu tới từng mẩu: header có thể bị cắt ở bất kỳ đâu
// giữa các Delivery nên phải gom lại cho tới khi đủ [0x02][meta_len][JSON]. Mỗi stream file dùng 1 parser.
type FileHeaderParser struct {
	buf []byte // phần đầu stream đã nhận nhưng chưa đủ để đọc header
}

// Feed nhận thêm dữ liệu đầu stream file. Khi đủ header thì trả về meta và phần dữ liệu file còn lại
// trong b; chưa đủ thì meta = nil.
func (p *FileHeaderParser) Feed(b []byte) (meta *FileMeta, rest []byte, err error) {
	p.buf = append(p.buf, b...)
	if len(p.buf) < 5 { // chưa đủ kind(1) + meta_len(4)
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
