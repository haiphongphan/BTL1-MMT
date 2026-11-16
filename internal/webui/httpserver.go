// Package webui là HTTP/1.1 server tự viết trên socket TCP thô (không dùng net/http): phục vụ trang HTML/JS
// thuần và một API JSON nhỏ để trình duyệt điều khiển process client local.
package webui

import (
	"bufio"
	"embed"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/phongphanhai06/go-collab/internal/client"
)

// static chứa index.html, app.js, style.css, nhúng vào binary lúc build.
//
//go:embed static
var static embed.FS

// maxHeaderLine giới hạn độ dài một dòng request/header để client xấu không làm tràn bộ nhớ.
const maxHeaderLine = 8 << 10

// Request là phần của HTTP request mà server này cần. Body đọc tối đa Content-Length byte.
type Request struct {
	Method, Path, RawQuery string
	Headers                map[string]string
	Body                   io.Reader
	ContentLength          int64
}

// Response luôn có Content-Length và Connection: close (mỗi kết nối một request, không keep-alive).
// File lớn (tải xuống) dùng Stream + Length thay cho Body để không phải đọc cả file vào RAM.
type Response struct {
	Status      int
	ContentType string
	Body        []byte
	Stream      io.ReadCloser // khác nil thì gửi Length byte từ Stream thay cho Body, gửi xong tự đóng
	Length      int64
	Disposition string // header Content-Disposition (vd. attachment; filename*=...), rỗng = không gửi
}

// Server là Web UI của một client.
type Server struct {
	c *client.Client
}

// New tạo Web UI cho client c.
func New(c *client.Client) *Server { return &Server{c: c} }

// ListenAndServe nghe TCP trên addr (nên là 127.0.0.1:port), mỗi kết nối một goroutine.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("[webui] listening on http://%s", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[webui] accept: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// handleConn đọc một request, định tuyến, ghi response rồi đóng kết nối. Panic trong handler chỉ làm hỏng
// request đó (trả 500), không làm sập client.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[webui] panic: %v", r)
			writeResponse(conn, Response{Status: 500, ContentType: "text/plain", Body: []byte("internal error")})
		}
	}()
	conn.SetDeadline(time.Now().Add(10 * time.Minute)) // đủ cho upload file lớn, không giữ kết nối treo mãi

	req, err := readRequest(bufio.NewReader(conn))
	if err != nil {
		writeResponse(conn, Response{Status: 400, ContentType: "text/plain", Body: []byte(err.Error())})
		return
	}
	writeResponse(conn, s.route(req))
}

// readRequest đọc request line ("POST /api/send?x=1 HTTP/1.1"), các header tới dòng trống, và gắn Body
// là LimitReader(Content-Length).
func readRequest(r *bufio.Reader) (*Request, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return nil, fmt.Errorf("request line hỏng: %q", line)
	}
	req := &Request{Method: parts[0], Headers: map[string]string{}}
	req.Path, req.RawQuery, _ = strings.Cut(parts[1], "?")

	for {
		h, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if h == "" {
			break
		}
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Headers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	if cl := req.Headers["content-length"]; cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("content-length hỏng: %q", cl)
		}
		req.ContentLength = n
	}
	req.Body = io.LimitReader(r, req.ContentLength)
	return req, nil
}

// readLine đọc một dòng kết thúc bằng CRLF (hoặc LF), bỏ ký tự xuống dòng.
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) > maxHeaderLine {
		return "", fmt.Errorf("header quá dài")
	}
	return strings.TrimRight(line, "\r\n"), nil
}

var statusText = map[int]string{
	200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 405: "Method Not Allowed",
	409: "Conflict", 500: "Internal Server Error", 502: "Bad Gateway",
}

// writeResponse ghi status line, header và body theo HTTP/1.1.
func writeResponse(w io.Writer, res Response) {
	text := statusText[res.Status]
	if text == "" {
		text = "Unknown"
	}
	length := int64(len(res.Body))
	if res.Stream != nil {
		defer res.Stream.Close()
		length = res.Length
	}
	fmt.Fprintf(w, "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n",
		res.Status, text, res.ContentType, length)
	if res.Disposition != "" {
		fmt.Fprintf(w, "Content-Disposition: %s\r\n", res.Disposition)
	}
	io.WriteString(w, "Cache-Control: no-store\r\nConnection: close\r\n\r\n")
	if res.Stream != nil {
		io.CopyN(w, res.Stream, length)
		return
	}
	w.Write(res.Body)
}
