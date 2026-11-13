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

// Client là một người dùng đã (hoặc đang) đăng nhập. An toàn cho nhiều goroutine: CLI, Web UI và
// listenLoop cùng dùng một Client.
type Client struct {
	User        string
	Events      *EventLog     // mọi sự kiện cho UI, có số thứ tự tăng dần
	DownloadDir string        // thư mục lưu file nhận được
	Jitter      time.Duration // giả lập trễ ngẫu nhiên chiều gửi (test)

	mu      sync.Mutex // bảo vệ mọi field bên dưới
	online  []string
	addrs   map[string]string // user → "ip:port" do server báo kèm tin "users"
	ep      transport.Endpoint
	closing bool
	state   string
	addr    string // lưu lại để reconnect
	pass    string
	loss    float64
	seq     uint64 // Seq của tin gửi đi trên kết nối hiện tại (1, 2, 3...); về 0 khi có kết nối mới
}

// Status là ảnh chụp trạng thái cho thanh trạng thái của Web UI (/api/status).
type Status struct {
	User     string            `json:"user"`
	State    string            `json:"state"`  // connected | reconnecting | disconnected
	Server   string            `json:"server"` // địa chỉ TCP của server
	Online   []string          `json:"online"`
	Addrs    map[string]string `json:"addrs"`
	RTTms    float64           `json:"rtt_ms"`
	Cwnd     int               `json:"cwnd"`
	InFlight int               `json:"in_flight"`
	SentPkts uint64            `json:"sent_pkts"`
	LostPkts uint64            `json:"lost_pkts"`
	LossPct  float64           `json:"loss_pct"`
}

// NewClient tạo client chưa kết nối; gọi Connect để đăng nhập.
func NewClient(username string) *Client {
	return &Client{
		User:        username,
		Events:      NewEventLog(),
		DownloadDir: "downloads",
	}
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
	st := Status{User: c.User, State: c.state, Server: c.addr, Online: append([]string(nil), c.online...),
		Addrs: make(map[string]string, len(c.addrs))}
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

func (c *Client) isClosing() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closing
}

// Connect đăng nhập lần đầu (TCP handshake rồi mở kênh UDP). Địa chỉ và mật khẩu được giữ lại để tự
// kết nối lại khi mất mạng. loss > 0 thì giả lập mất gói chiều gửi.
func (c *Client) Connect(serverTCPAddr, password string, loss float64) error {
	c.mu.Lock()
	c.addr, c.pass, c.loss = serverTCPAddr, password, loss
	c.mu.Unlock()
	return c.dial()
}

// dial: handshake TCP lấy conn_id và k_udp, rồi Dial transport tới cổng UDP server báo trong KEYX.
func (c *Client) dial() error {
	c.mu.Lock()
	addr, pass, loss := c.addr, c.pass, c.loss
	c.mu.Unlock()

	keyx, kUDP, err := c.ConnectTCPAndAuth(addr, pass)
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
	c.AttachEndpoint(conn)
	c.setState("connected")
	return nil
}

// Close thoát hẳn: gửi CLOSE cho server (server báo mọi người ngay) và không kết nối lại nữa.
func (c *Client) Close() {
	c.mu.Lock()
	c.closing = true
	ep := c.ep
	c.mu.Unlock()
	if ep != nil {
		ep.Close()
	}
}

// ConnectTCPAndAuth chạy phía client của handshake: HELLO → KEYX → AUTH (mật khẩu mã hóa bằng k_tcp)
// → AUTH_OK/AUTH_FAIL. Trả về KEYX (conn_id, cổng UDP) và k_udp để mở kênh UDP.
func (c *Client) ConnectTCPAndAuth(serverTCPAddr, password string) (*protocol.KeyX, [32]byte, error) {
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
	credJSON, _ := json.Marshal(map[string]string{"user": c.User, "pass": password})
	nonce := make([]byte, 12)
	rand.Read(nonce)

	auth := protocol.Auth{Type: "AUTH", Nonce: nonce, Sealed: aead.Seal(nil, nonce, credJSON, nil)}
	if err := protocol.WriteMsg(conn, auth); err != nil {
		return nil, [32]byte{}, err
	}

	var res protocol.AuthResult
	if err := protocol.ReadMsg(conn, &res); err != nil {
		return nil, [32]byte{}, err
	}
	if res.Type == "AUTH_FAIL" {
		return nil, [32]byte{}, fmt.Errorf("auth failed: %s", res.Reason)
	}

	c.setOnline(res.Users, nil)
	c.Events.Add("users", res.Users)
	return &keyx, kUDP, nil
}

// AttachEndpoint dùng kết nối transport mới và khởi động vòng nhận của nó.
func (c *Client) AttachEndpoint(ep transport.Endpoint) {
	c.mu.Lock()
	c.ep = ep
	c.seq = 0 // kết nối mới: đánh số lại từ 1, server cũng có SeqBuffer mới cho kết nối này
	c.mu.Unlock()
	go c.listenLoop(ep)
}

// inStream là trạng thái một stream đang nhận từ server.
type inStream struct {
	kind byte
	buf  []byte    // message: gom tới FIN
	file *fileRecv // file: ghi thẳng xuống đĩa
}

// listenLoop là goroutine nhận của một kết nối: ghép các đoạn theo stream, phân loại theo byte đầu
// (message hay file). Khi kết nối đóng mà người dùng không chủ động thoát thì chuyển sang kết nối lại.
func (c *Client) listenLoop(ep transport.Endpoint) {
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
			if c.isClosing() {
				c.setState("disconnected")
				return
			}
			c.setState("reconnecting")
			go c.reconnectLoop()
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
func (c *Client) reconnectLoop() {
	delay := time.Second
	for !c.isClosing() {
		time.Sleep(delay)
		if err := c.dial(); err != nil {
			c.Events.Add("status", fmt.Sprintf("kết nối lại thất bại: %v", err))
			delay = min(delay*2, 30*time.Second)
			continue
		}
		return
	}
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
