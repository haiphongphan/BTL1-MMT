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

//go:embed static
var static embed.FS

const maxHeaderLine = 8 << 10 
type Request struct {
	Method, Path, RawQuery string
	Headers                map[string]string
	Body                   io.Reader 
	ContentLength          int64
}

type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

type Server struct {
	c *client.Client
}

func New(c *client.Client) *Server { return &Server{c: c} }

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

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	defer func() { 
		if r := recover(); r != nil {
			log.Printf("[webui] panic: %v", r)
			writeResponse(conn, Response{500, "text/plain", []byte("internal error")})
		}
	}()
	conn.SetDeadline(time.Now().Add(10 * time.Minute)) 

	req, err := readRequest(bufio.NewReader(conn))
	if err != nil {
		writeResponse(conn, Response{400, "text/plain", []byte(err.Error())})
		return
	}
	writeResponse(conn, s.route(req))
}

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
