package webui

import (
	"encoding/json"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const maxJSONBody = 64 << 10

func (s *Server) route(req *Request) Response {
	switch {
	case req.Path == "/" || req.Path == "/index.html":
		return s.static("index.html", "text/html; charset=utf-8")
	case req.Path == "/app.js":
		return s.static("app.js", "application/javascript; charset=utf-8")
	case req.Path == "/style.css":
		return s.static("style.css", "text/css; charset=utf-8")

	case req.Path == "/api/events":
		return s.apiEvents(req)
	case req.Path == "/api/status":
		return jsonResp(200, s.c.Status())
	case req.Path == "/api/send":
		return s.apiSend(req)
	case req.Path == "/api/upload":
		return s.apiUpload(req)
	}
	return Response{404, "text/plain", []byte("not found")}
}

func (s *Server) static(name, ctype string) Response {
	b, err := static.ReadFile("static/" + name)
	if err != nil {
		return Response{404, "text/plain", []byte("missing " + name)}
	}
	return Response{200, ctype, b}
}

func jsonResp(status int, v any) Response {
	b, err := json.Marshal(v)
	if err != nil {
		return Response{500, "text/plain", []byte(err.Error())}
	}
	return Response{status, "application/json", b}
}

func jsonErr(status int, msg string) Response {
	return jsonResp(status, map[string]string{"error": msg})
}

func (s *Server) apiEvents(req *Request) Response {
	q, _ := url.ParseQuery(req.RawQuery)
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	return jsonResp(200, s.c.Events.Since(since))
}

func (s *Server) apiSend(req *Request) Response {
	if req.Method != "POST" {
		return jsonErr(405, "POST only")
	}
	if req.ContentLength > maxJSONBody {
		return jsonErr(400, "body quá lớn")
	}
	var in struct{ To, Text string }
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return jsonErr(400, "JSON hỏng: "+err.Error())
	}
	in.Text = strings.TrimSpace(in.Text)
	if in.Text == "" {
		return jsonErr(400, "text rỗng")
	}
	var err error
	if in.To == "" {
		err = s.c.SendChat(in.Text)
	} else {
		err = s.c.SendDM(in.To, in.Text)
	}
	if err != nil {
		return jsonErr(500, err.Error())
	}
	return jsonResp(200, map[string]bool{"ok": true})
}

func (s *Server) apiUpload(req *Request) Response {
	if req.Method != "POST" {
		return jsonErr(405, "POST only")
	}
	q, _ := url.ParseQuery(req.RawQuery)
	name := filepath.Base(q.Get("name"))
	if name == "" || name == "." || name == "/" {
		return jsonErr(400, "thiếu name")
	}
	if req.ContentLength == 0 {
		return jsonErr(400, "file rỗng")
	}

	dir, err := os.MkdirTemp("", "collab-upload-*")
	if err != nil {
		return jsonErr(500, err.Error())
	}
	tmp := filepath.Join(dir, name)
	f, err := os.Create(tmp)
	if err != nil {
		os.RemoveAll(dir)
		return jsonErr(500, err.Error())
	}
	n, err := io.Copy(f, req.Body)
	f.Close()
	if err != nil || n != req.ContentLength {
		os.RemoveAll(dir)
		return jsonErr(400, "upload dở dang")
	}

	to := q.Get("to")
	go func() {
		defer os.RemoveAll(dir)
		if err := s.c.SendFile(tmp, to); err != nil {
			log.Printf("[webui] SendFile %s: %v", name, err)
		}
	}()
	return jsonResp(200, map[string]any{"ok": true, "size": n})
}
