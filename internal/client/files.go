package client

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

// progressEvery: tối đa 5 sự kiện tiến độ mỗi giây cho mỗi file, đủ mượt cho thanh tiến trình mà không
// làm đầy EventLog.
const progressEvery = 200 * time.Millisecond

// FileProgress là dữ liệu của các sự kiện file_start / file_progress / file_done (cả chiều gửi và nhận).
type FileProgress struct {
	ID       string `json:"id"`
	Dir      string `json:"dir"` // "out" = mình gửi, "in" = mình nhận
	From     string `json:"from"`
	Name     string `json:"name"`
	Path     string `json:"path,omitempty"`
	Received int64  `json:"received"`
	Size     int64  `json:"size"`
}

// SendFile gửi file cho một người (to rỗng = mọi người). Đọc file 2 lượt: lượt 1 tính SHA-256 vì meta (có
// hash) phải đi trước dữ liệu trong cùng stream; lượt 2 gửi, transport tự cắt ≤ 1000 byte/frame. Dùng stream
// ưu tiên thấp (bulk) nên chat vẫn đi trước. Hàm chặn tới khi gửi xong; tiến độ báo qua EventLog.
func (c *Client) SendFile(path, to string) error {
	ep := c.endpoint()
	if ep == nil {
		return ErrNotConnected
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	size, err := io.Copy(h, f) // lượt 1: tính hash (meta phải đi TRƯỚC data)
	if err != nil {
		return err
	}
	if size > 1<<32-1 {
		return errors.New("file quá lớn (offset stream là uint32, tối đa 4 GB)")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	meta := protocol.FileMeta{
		ID:     newID(),
		From:   c.User,
		To:     to,
		Name:   filepath.Base(path),
		Size:   size,
		SHA256: hex.EncodeToString(h.Sum(nil)),
	}
	hdr, err := protocol.EncodeFileHeader(meta)
	if err != nil {
		return err
	}

	pr := &progressReader{r: f, ev: c.Events, p: FileProgress{ID: meta.ID, Dir: "out", From: c.User, Name: meta.Name, Size: size}}
	c.Events.Add("file_start", pr.p)

	st := ep.OpenStream(false) // bulk: ưu tiên thấp hơn chat
	if _, err := st.Write(hdr); err != nil {
		return err
	}
	// lượt 2: gửi qua progressReader để thanh tiến trình bên gửi chạy. Write của stream bulk bị chặn khi hàng
	// đợi transport đầy, nên tốc độ đọc file = tốc độ mạng thật, và tiến độ phản ánh đúng.
	if _, err := io.Copy(st, pr); err != nil {
		c.Events.Add("file_error", fmt.Sprintf("gửi '%s' lỗi: %v", meta.Name, err))
		return err
	}
	if err := st.Close(); err != nil {
		return err
	}

	pr.p.Received = size
	c.Events.Add("file_done", pr.p)
	return nil
}

// progressReader bọc file, đếm số byte đã đọc và phát file_progress tối đa mỗi progressEvery.
type progressReader struct {
	r      io.Reader
	ev     *EventLog
	p      FileProgress
	lastEv time.Time
}

func (pr *progressReader) Read(b []byte) (int, error) {
	n, err := pr.r.Read(b)
	pr.p.Received += int64(n)
	if time.Since(pr.lastEv) >= progressEvery {
		pr.lastEv = time.Now()
		pr.ev.Add("file_progress", pr.p)
	}
	return n, err
}

// newID sinh id ngẫu nhiên cho file, để UI ghép các sự kiện tiến độ của cùng một file.
func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// fileRecv là trạng thái nhận một file: đọc meta, rồi ghi đĩa và băm SHA-256 cùng lúc khi dữ liệu tới.
type fileRecv struct {
	hdr      protocol.FileHeaderParser
	meta     protocol.FileMeta
	f        *os.File // nil khi chưa đủ header hoặc đã lỗi
	path     string
	w        io.Writer // MultiWriter(file, hasher): ghi và băm trong 1 lần
	hasher   hash.Hash
	received int64
	lastEv   time.Time
	failed   bool
}

// onFileData xử lý một đoạn của stream file: đọc meta ở đầu (có thể bị cắt giữa nhiều đoạn), tạo file
// trong DownloadDir, ghi dữ liệu, tới FIN thì kiểm tra kích thước và SHA-256.
func (c *Client) onFileData(r *fileRecv, d transport.Delivery) {
	data := d.Data

	if r.f == nil && !r.failed { // còn đang đọc header
		meta, rest, err := r.hdr.Feed(data)
		switch {
		case err != nil:
			r.failed = true
			c.Events.Add("file_error", err.Error())
		case meta != nil:
			if err := r.open(c.DownloadDir, *meta); err != nil {
				r.failed = true
				c.Events.Add("file_error", err.Error())
			} else {
				c.Events.Add("file_start", r.progress())
			}
		}
		data = rest
	}

	if r.f != nil && len(data) > 0 {
		if _, err := r.w.Write(data); err != nil {
			r.abort()
			r.failed = true
			c.Events.Add("file_error", err.Error())
			return
		}
		r.received += int64(len(data))
		if time.Since(r.lastEv) >= progressEvery {
			r.lastEv = time.Now()
			c.Events.Add("file_progress", r.progress())
		}
	}

	if d.Fin && r.f != nil {
		c.finishFile(r)
	}
}

// open tạo file đích (tên an toàn, không ghi đè file có sẵn) và chuẩn bị MultiWriter(file, sha256).
func (r *fileRecv) open(dir string, meta protocol.FileMeta) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, path, err := createUnique(dir, safeName(meta.Name))
	if err != nil {
		return err
	}
	r.meta, r.f, r.path = meta, f, path
	r.hasher = sha256.New()
	r.w = io.MultiWriter(f, r.hasher)
	r.lastEv = time.Now()
	return nil
}

// finishFile đóng file và so SHA-256: khớp thì báo file_done, lệch thì xóa file và báo file_error.
func (c *Client) finishFile(r *fileRecv) {
	r.f.Close()
	r.f = nil
	got := hex.EncodeToString(r.hasher.Sum(nil))
	if r.received != r.meta.Size || got != r.meta.SHA256 {
		os.Remove(r.path)
		c.Events.Add("file_error", fmt.Sprintf("'%s' hỏng: nhận %d/%d byte, SHA-256 không khớp", r.meta.Name, r.received, r.meta.Size))
		return
	}
	c.Events.Add("file_done", r.progress())
}

// abort: kết nối rớt giữa chừng → xóa file dở dang
func (r *fileRecv) abort() {
	if r.f != nil {
		r.f.Close()
		os.Remove(r.path)
		r.f = nil
	}
}

func (r *fileRecv) progress() FileProgress {
	return FileProgress{ID: r.meta.ID, Dir: "in", From: r.meta.From, Name: r.meta.Name, Path: r.path, Received: r.received, Size: r.meta.Size}
}

// safeName bỏ mọi phần đường dẫn trong tên file người gửi đặt (chống "../../etc/passwd").
func safeName(name string) string {
	name = filepath.Base(filepath.Clean("/" + name))
	if name == "/" || name == "." || name == ".." {
		return "file"
	}
	return name
}

// createUnique tạo file mới với O_EXCL; trùng tên thì thử "a (1).txt", "a (2).txt"...
func createUnique(dir, name string) (*os.File, string, error) {
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	for i := 0; i < 1000; i++ {
		n := name
		if i > 0 {
			n = fmt.Sprintf("%s (%d)%s", stem, i, ext)
		}
		p := filepath.Join(dir, n)
		f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			return f, p, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", errors.New("quá nhiều file trùng tên")
}
