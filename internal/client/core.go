// Package client là phía ứng dụng của người dùng: đăng nhập, giữ kết nối transport, tự kết nối lại,
// gửi/nhận chat, DM, file và ghi mọi sự kiện vào EventLog để CLI và Web UI hiển thị.
package client

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

// Client là process phía người dùng. Một Client có thể đăng nhập, đăng xuất rồi đăng nhập lại bằng
// tài khoản khác (trang đăng nhập của Web UI). An toàn cho nhiều goroutine: CLI, Web UI và listenLoop
// cùng dùng một Client.
type Client struct {
	Events      *EventLog     // mọi sự kiện cho UI, có số thứ tự tăng dần
	DownloadDir string        // thư mục lưu file nhận được
	Jitter      time.Duration // giả lập trễ ngẫu nhiên chiều gửi (test)
	Loss        float64       // giả lập mất gói chiều gửi (0.1 = 10%); đặt trước khi Login

	mu       sync.Mutex // bảo vệ mọi field bên dưới
	user     string
	loggedIn bool // đã đăng nhập thành công và chưa đăng xuất (kể cả lúc đang kết nối lại)
	authing  bool // đang chạy Login, chặn Login thứ hai chạy song song
	// gen tăng mỗi lần Login hoặc Logout. listenLoop và reconnectLoop nhớ gen lúc chúng bắt đầu; gen đổi
	// nghĩa là phiên đó đã hết, chúng phải dừng thay vì kết nối lại bằng tài khoản cũ.
	gen      uint64
	loginSeq int64 // seq của EventLog lúc bắt đầu phiên đăng nhập này
	online   []string
	addrs    map[string]string // user → "ip:port" do server báo kèm tin "users"
	ep       transport.Endpoint
	closing  bool
	state    string
	addr     string // địa chỉ server, lưu lại để reconnect
	pass     string
	seq      uint64 // Seq của tin gửi đi trên kết nối hiện tại (1, 2, 3...); về 0 khi có kết nối mới
}

// AuthError là lý do server trả trong AUTH_FAIL (sai mật khẩu, tên đã tồn tại...), để UI hiện nguyên văn.
type AuthError struct{ Reason string }

func (e *AuthError) Error() string { return "auth failed: " + e.Reason }

// Các lỗi của Login/Logout.
var (
	ErrLoggedIn  = errors.New("client: đã đăng nhập, hãy đăng xuất trước")
	ErrLoginBusy = errors.New("client: đang đăng nhập, chờ chút")
	errStale     = errors.New("client: phiên đăng nhập đã kết thúc")
)

// Status là ảnh chụp trạng thái cho thanh trạng thái của Web UI (/api/status).
type Status struct {
	User     string            `json:"user"`
	LoggedIn bool              `json:"logged_in"` // false: Web UI hiện trang đăng nhập
	Since    int64             `json:"since"`     // Web UI đọc event từ seq này: không hiện tin của phiên trước
	State    string            `json:"state"`     // connected | reconnecting | disconnected
	Server   string            `json:"server"`    // địa chỉ TCP của server (trước khi đăng nhập: giá trị cờ -server)
	Online   []string          `json:"online"`
	Addrs    map[string]string `json:"addrs"`
	RTTms    float64           `json:"rtt_ms"`
	Cwnd     int               `json:"cwnd"`
	InFlight int               `json:"in_flight"`
	SentPkts uint64            `json:"sent_pkts"`
	LostPkts uint64            `json:"lost_pkts"`
	LossPct  float64           `json:"loss_pct"`
}

// NewClient tạo client chưa kết nối. username có thể rỗng nếu người dùng sẽ đăng nhập qua Web UI;
// gọi Connect (dùng username này) hoặc Login để đăng nhập.
func NewClient(username string) *Client {
	return &Client{
		user:        username,
		Events:      NewEventLog(),
		DownloadDir: "downloads",
	}
}

// Username trả về tên đang đăng nhập (hoặc tên truyền vào NewClient nếu chưa đăng nhập).
func (c *Client) Username() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.user
}

// SetServer đặt địa chỉ server mặc định, Web UI điền sẵn vào ô Server của trang đăng nhập.
func (c *Client) SetServer(addr string) {
	c.mu.Lock()
	c.addr = addr
	c.mu.Unlock()
}

// OnlineUsers trả về bản sao danh sách user đang online.
func (c *Client) OnlineUsers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.online...)
}

func (c *Client) setOnline(users []string, addrs map[string]string) {
	c.mu.Lock()
	c.online = users
	if addrs != nil {
		c.addrs = addrs
	}
	c.mu.Unlock()
}

func (c *Client) endpoint() transport.Endpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ep
}

func (c *Client) setState(st string) {
	c.mu.Lock()
	c.state = st
	c.mu.Unlock()
	c.Events.Add("status", st)
}

// Status gom trạng thái kết nối và số liệu của transport (RTT, cwnd, in-flight, tỉ lệ mất gói).
func (c *Client) Status() Status {
	c.mu.Lock()
	st := Status{User: c.user, LoggedIn: c.loggedIn, Since: c.loginSeq, State: c.state, Server: c.addr,
		Online: append([]string(nil), c.online...), Addrs: make(map[string]string, len(c.addrs))}
	for k, v := range c.addrs {
		st.Addrs[k] = v
	}
	ep := c.ep
	c.mu.Unlock()
	if st.State == "" {
		st.State = "disconnected"
	}
	if ep != nil {
		s := ep.Stats()
		st.RTTms = float64(s.RTT.Microseconds()) / 1000
		st.Cwnd, st.InFlight, st.SentPkts, st.LostPkts = s.Cwnd, s.InFlight, s.SentPkts, s.LostPkts
		if s.SentPkts > 0 {
			st.LossPct = 100 * float64(s.LostPkts) / float64(s.SentPkts)
		}
	}
	return st
}

// Connect đăng nhập bằng username đã truyền vào NewClient (dùng cho cờ -user và trong test).
// loss > 0 thì giả lập mất gói chiều gửi.
func (c *Client) Connect(serverTCPAddr, password string, loss float64) error {
	c.Loss = loss
	return c.Login(serverTCPAddr, c.Username(), password, false)
}

// Login đăng nhập (register = false) hoặc đăng ký tài khoản mới rồi đăng nhập luôn (register = true):
// TCP handshake rồi mở kênh UDP. serverTCPAddr = "auto" thì tự tìm server trong LAN trước. Địa chỉ và
// mật khẩu được giữ trong RAM để tự kết nối lại khi mất mạng. Lỗi từ server có kiểu *AuthError.
func (c *Client) Login(serverTCPAddr, user, password string, register bool) error {
	c.mu.Lock()
	switch {
	case c.authing:
		c.mu.Unlock()
		return ErrLoginBusy
	case c.loggedIn:
		c.mu.Unlock()
		return ErrLoggedIn
	}
	c.authing = true
	c.gen++
	gen := c.gen
	c.closing = false
	c.mu.Unlock()

	err := c.login(gen, serverTCPAddr, user, password, register)

	c.mu.Lock()
	c.authing = false
	if err == nil {
		c.loggedIn = true
	}
	c.mu.Unlock()
	return err
}

// login là phần việc của Login sau khi đã giữ chỗ (authing = true).
func (c *Client) login(gen uint64, addr, user, password string, register bool) error {
	if addr == "auto" {
		c.Events.Add("status", "đang tìm server trong LAN (UDP :9002)...")
		found, err := Discover(5 * time.Second)
		if err != nil {
			return err
		}
		c.Events.Add("status", "tìm thấy server "+found)
		addr = found
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("địa chỉ server phải có dạng ip:port: %w", err)
	}

	c.mu.Lock()
	c.user, c.addr, c.pass = user, addr, password
	c.online, c.addrs = nil, nil
	c.loginSeq = c.Events.Last() // Web UI chỉ hiện event từ đây trở đi
	c.mu.Unlock()
	return c.dial(gen, register)
}

// Logout đăng xuất: đóng kết nối (gửi CLOSE để server báo mọi người ngay), dừng kết nối lại và quên mật
// khẩu. Sau đó có thể Login lại, kể cả bằng tài khoản khác.
func (c *Client) Logout() {
	c.mu.Lock()
	wasIn := c.loggedIn
	c.closing = true
	c.loggedIn = false
	c.gen++ // reconnectLoop / listenLoop của phiên cũ thấy gen đổi và tự dừng
	ep := c.ep
	c.ep = nil
	c.pass = ""
	c.online, c.addrs = nil, nil
	c.mu.Unlock()
	if ep != nil {
		ep.Close()
	}
	if wasIn {
		c.setState("disconnected")
	}
}

// dial: handshake TCP lấy conn_id và k_udp, rồi Dial transport tới cổng UDP server báo trong KEYX.
// register = true chỉ dùng ở lần đầu (đăng ký); kết nối lại luôn là AUTH.
func (c *Client) dial(gen uint64, register bool) error {
	c.mu.Lock()
	addr, user, pass := c.addr, c.user, c.pass
	c.mu.Unlock()
	loss := c.Loss

	keyx, kUDP, err := c.handshake(addr, user, pass, register)
	if err != nil {
		return err
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	srvUDP, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(keyx.UDPPort)))
	if err != nil {
		return err
	}

	sock, err := net.ListenUDP("udp", nil)
	if err != nil {
		return err
	}
	var pc transport.PacketConn = sock
	if loss > 0 || c.Jitter > 0 {
		pc = &transport.LossyConn{UDPConn: sock, Loss: loss, Jitter: c.Jitter}
	}
	conn, err := transport.Dial(pc, srvUDP, keyx.ConnID, kUDP) // Dial tự gửi PING đầu tiên
	if err != nil {
		sock.Close()
		return err
	}
	if !c.attach(gen, conn) { // người dùng đã đăng xuất trong lúc đang kết nối
		conn.Close()
		return errStale
	}
	c.setState("connected")
	return nil
}

// Close thoát hẳn (Ctrl+C, /quit): giống Logout.
func (c *Client) Close() { c.Logout() }

// handshake chạy phía client của handshake: HELLO → KEYX → AUTH hoặc REGISTER (mật khẩu mã hóa bằng
// k_tcp) → AUTH_OK/AUTH_FAIL. Trả về KEYX (conn_id, cổng UDP) và k_udp để mở kênh UDP.
func (c *Client) handshake(serverTCPAddr, user, password string, register bool) (*protocol.KeyX, [32]byte, error) {
	conn, err := net.DialTimeout("tcp", serverTCPAddr, 5*time.Second)
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer conn.Close()

	priv, _ := transport.NewKeyPair()
	hello := protocol.Hello{Type: "HELLO", ClientPub: priv.PublicKey().Bytes()}
	if err := protocol.WriteMsg(conn, hello); err != nil {
		return nil, [32]byte{}, err
	}

	var keyx protocol.KeyX
	if err := protocol.ReadMsg(conn, &keyx); err != nil {
		return nil, [32]byte{}, err
	}

	kTCP, kUDP, err := transport.DeriveKeys(priv, keyx.ServerPub)
	if err != nil {
		return nil, [32]byte{}, err
	}

	aead, _ := transport.NewAEAD(kTCP)
	credJSON, _ := json.Marshal(map[string]string{"user": user, "pass": password})
	nonce := make([]byte, 12)
	rand.Read(nonce)

	typ := "AUTH"
	if register {
		typ = "REGISTER"
	}
	auth := protocol.Auth{Type: typ, Nonce: nonce, Sealed: aead.Seal(nil, nonce, credJSON, nil)}
	if err := protocol.WriteMsg(conn, auth); err != nil {
		return nil, [32]byte{}, err
	}

	var res protocol.AuthResult
	if err := protocol.ReadMsg(conn, &res); err != nil {
		return nil, [32]byte{}, err
	}
	if res.Type != "AUTH_OK" {
		return nil, [32]byte{}, &AuthError{Reason: res.Reason}
	}

	c.setOnline(res.Users, nil)
	c.Events.Add("users", res.Users)
	return &keyx, kUDP, nil
}

// attach dùng kết nối transport mới và khởi động vòng nhận của nó, nếu phiên gen vẫn còn hiệu lực.
func (c *Client) attach(gen uint64, ep transport.Endpoint) bool {
	c.mu.Lock()
	if c.gen != gen || c.closing {
		c.mu.Unlock()
		return false
	}
	c.ep = ep
	c.seq = 0 // kết nối mới: đánh số lại từ 1, server cũng có SeqBuffer mới cho kết nối này
	c.mu.Unlock()
	go c.listenLoop(gen, ep)
	return true
}

// inStream là trạng thái một stream đang nhận từ server.
type inStream struct {
	kind byte
	buf  []byte    // message: gom tới FIN
	file *fileRecv // file: ghi thẳng xuống đĩa
}

// listenLoop là goroutine nhận của một kết nối: ghép các đoạn theo stream, phân loại theo byte đầu
// (message hay file). Khi kết nối đóng mà người dùng không chủ động thoát thì chuyển sang kết nối lại.
func (c *Client) listenLoop(gen uint64, ep transport.Endpoint) {
	streams := make(map[uint32]*inStream)
	var inSeq protocol.SeqBuffer // sắp lại tin server gửi theo Seq; mỗi kết nối một bộ đệm mới
	defer func() {
		for _, st := range streams { // file nhận dở → xóa file chưa hoàn chỉnh
			if st.file != nil {
				st.file.abort()
			}
		}
	}()

	for {
		select {
		case d, ok := <-ep.Recv():
			if !ok {
				return
			}
			st := streams[d.StreamID]
			if st == nil {
				if len(d.Data) == 0 {
					continue
				}
				st = &inStream{kind: d.Data[0]}
				if st.kind == protocol.KindFile {
					st.file = &fileRecv{}
				}
				streams[d.StreamID] = st
			}

			switch st.kind {
			case protocol.KindMessage:
				st.buf = append(st.buf, d.Data...)
				if d.Fin {
					c.handleMessage(&inSeq, st.buf)
				}
			case protocol.KindFile:
				c.onFileData(st.file, d)
			}
			if d.Fin {
				delete(streams, d.StreamID)
			}

		case <-ep.Done():
			// Logout đã gỡ ep và tự đặt trạng thái; chỉ kết nối lại khi đây vẫn là kết nối của phiên hiện tại.
			c.mu.Lock()
			current := c.ep == ep && c.gen == gen && !c.closing
			if current {
				c.ep = nil
			}
			c.mu.Unlock()
			if !current {
				return
			}
			c.setState("reconnecting")
			go c.reconnectLoop(gen)
			return
		}
	}
}

// handleMessage giải mã 1 stream message rồi đưa qua SeqBuffer: mỗi tin là 1 stream riêng (không HoL),
// nên khi mất gói tin #2 có thể tới trước tin #1; SeqBuffer giữ #2 lại cho tới khi #1 tới.
func (c *Client) handleMessage(inSeq *protocol.SeqBuffer, data []byte) {
	var msg protocol.AppMsg
	if err := json.Unmarshal(data[1:], &msg); err != nil {
		return
	}
	for _, m := range inSeq.Push(msg) {
		if m.Type == "users" {
			c.setOnline(m.Users, m.Addrs)
		}
		c.Events.Add(m.Type, m)
	}
}

// reconnectLoop thử handshake lại với backoff 1, 2, 4... tối đa 30 s. Trong ~10 s đầu server có thể còn giữ
// session cũ (chưa hết idle timeout) và trả AUTH_FAIL "đang online": cứ thử tiếp là vào được.
// Dừng ngay khi người dùng đăng xuất (gen đổi).
func (c *Client) reconnectLoop(gen uint64) {
	delay := time.Second
	for {
		time.Sleep(delay)
		if !c.current(gen) {
			return
		}
		err := c.dial(gen, false)
		if err == nil || errors.Is(err, errStale) {
			return
		}
		c.Events.Add("status", fmt.Sprintf("kết nối lại thất bại: %v", err))
		delay = min(delay*2, 30*time.Second)
	}
}

// current: phiên gen còn hiệu lực (chưa đăng xuất, chưa đăng nhập phiên mới).
func (c *Client) current(gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen == gen && !c.closing
}

// ErrNotConnected: đang mất kết nối (đang kết nối lại), chưa gửi được.
var ErrNotConnected = errors.New("client: chưa có kết nối UDP")

// send gửi một AppMsg thành một stream ưu tiên cao (không bị file chặn), kèm Seq để bên nhận sắp đúng thứ tự.

func (c *Client) send(m protocol.AppMsg) error {
	// Lấy endpoint và cấp Seq trong cùng một lần giữ khóa, để Seq luôn thuộc đúng kết nối đang dùng.
	c.mu.Lock()
	ep := c.ep
	if ep != nil {
		c.seq++
		m.Seq = c.seq
	}
	c.mu.Unlock()
	if ep == nil {
		return ErrNotConnected
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return ep.SendMessage(append([]byte{protocol.KindMessage}, b...))
}

// SendChat gửi tin cho mọi người đang online.
func (c *Client) SendChat(text string) error {
	return c.send(protocol.AppMsg{Type: "chat", Text: text})
}

// SendDM gửi tin riêng cho một người.
func (c *Client) SendDM(to, text string) error {
	return c.send(protocol.AppMsg{Type: "dm", To: to, Text: text})
}
