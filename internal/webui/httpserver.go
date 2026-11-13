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
type Response struct {
	Status      int
	ContentType string
	Body        []byte
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
			writeResponse(conn, Response{500, "text/plain", []byte("internal error")})
		}
	}()
	conn.SetDeadline(time.Now().Add(10 * time.Minute)) // đủ cho upload file lớn, không giữ kết nối treo mãi

	req, err := readRequest(bufio.NewReader(conn))
	if err != nil {
		writeResponse(conn, Response{400, "text/plain", []byte(err.Error())})
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
	200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed", 500: "Internal Server Error",
}

// writeResponse ghi status line, header và body theo HTTP/1.1.
func writeResponse(w io.Writer, res Response) {
	text := statusText[res.Status]
	if text == "" {
		text = "Unknown"
	}
	fmt.Fprintf(w, "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n"+
		"Cache-Control: no-store\r\nConnection: close\r\n\r\n",
		res.Status, text, res.ContentType, len(res.Body))
	w.Write(res.Body)
}
