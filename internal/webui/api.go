package webui

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/phongphanhai06/go-collab/internal/client"
)

// maxJSONBody: giới hạn thân request JSON (tin chat), file đi qua /api/upload riêng.
const maxJSONBody = 64 << 10

// route ánh xạ đường dẫn tới handler:
//
//	GET  /, /app.js, /style.css      file tĩnh
//	GET  /api/events?since=N          các event có seq > N (trình duyệt poll mỗi 400 ms)
//	GET  /api/status                  đã đăng nhập chưa, trạng thái kết nối, RTT, cwnd, mất gói, user online
//	POST /api/login    {server, user, pass}   đăng nhập
//	POST /api/register {server, user, pass}   đăng ký tài khoản mới rồi đăng nhập luôn
//	POST /api/logout                  đăng xuất, Web UI quay về trang đăng nhập
//	POST /api/send   {to, text}       chat (to rỗng) hoặc DM
//	POST /api/upload?to=&name=        thân là nội dung file thô (không multipart)
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
	case req.Path == "/api/login":
		return s.apiLogin(req, false)
	case req.Path == "/api/register":
		return s.apiLogin(req, true)
	case req.Path == "/api/logout":
		if req.Method != "POST" {
			return jsonErr(405, "POST only")
		}
		s.c.Logout()
		return jsonResp(200, map[string]bool{"ok": true})
	case req.Path == "/api/send":
		return s.apiSend(req)
	case req.Path == "/api/upload":
		return s.apiUpload(req)
	}
	return Response{404, "text/plain", []byte("not found")}
}

// static trả một file nhúng trong binary.
func (s *Server) static(name, ctype string) Response {
	b, err := static.ReadFile("static/" + name)
	if err != nil {
		return Response{404, "text/plain", []byte("missing " + name)}
	}
	return Response{200, ctype, b}
}

// jsonResp mã hóa v thành JSON.
func jsonResp(status int, v any) Response {
	b, err := json.Marshal(v)
	if err != nil {
		return Response{500, "text/plain", []byte(err.Error())}
	}
	return Response{status, "application/json", b}
}

// jsonErr trả {"error": msg}.
func jsonErr(status int, msg string) Response {
	return jsonResp(status, map[string]string{"error": msg})
}

// apiEvents: GET /api/events?since=N.
func (s *Server) apiEvents(req *Request) Response {
	q, _ := url.ParseQuery(req.RawQuery)
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	return jsonResp(200, s.c.Events.Since(since))
}

// apiLogin: POST /api/login hoặc /api/register {server, user, pass}. Chặn tới khi handshake xong (tối đa
// khoảng 5 s, thêm 5 s nếu server = "auto") rồi trả {ok} hoặc {error} để trang đăng nhập hiện lỗi.
// Tên và mật khẩu được server kiểm tra; ở đây chỉ chặn ô trống.
func (s *Server) apiLogin(req *Request, register bool) Response {
	if req.Method != "POST" {
		return jsonErr(405, "POST only")
	}
	if req.ContentLength > maxJSONBody {
		return jsonErr(400, "body quá lớn")
	}
	var in struct{ Server, User, Pass string }
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		return jsonErr(400, "JSON hỏng: "+err.Error())
	}
	in.Server, in.User = strings.TrimSpace(in.Server), strings.TrimSpace(in.User)
	if in.Server == "" || in.User == "" || in.Pass == "" {
		return jsonErr(400, "Cần nhập đủ server, tên đăng nhập và mật khẩu")
	}

	err := s.c.Login(in.Server, in.User, in.Pass, register)
	var ae *client.AuthError
	switch {
	case err == nil:
		return jsonResp(200, map[string]bool{"ok": true})
	case errors.As(err, &ae):
		return jsonErr(401, ae.Reason) // sai mật khẩu, tên đã tồn tại, đang online ở máy khác...
	case errors.Is(err, client.ErrLoggedIn), errors.Is(err, client.ErrLoginBusy):
		return jsonErr(409, err.Error())
	default:
		return jsonErr(502, "Không kết nối được server: "+err.Error())
	}
}

// apiSend: POST /api/send {to, text}; to rỗng = chat chung.
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

// apiUpload: POST /api/upload?to=&name=. Thân được ghi thẳng ra file tạm (không đọc cả file vào RAM),
// rồi SendFile chạy trong goroutine riêng; trình duyệt theo dõi tiến độ qua các event file_progress.
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
