// Package client là logic phía client: đăng nhập, kết nối lại, gửi/nhận chat và file, EventLog cho UI.
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

type Client struct {
	User        string
	Events      *EventLog
	DownloadDir string

	mu      sync.Mutex // bảo vệ mọi field bên dưới
	online  []string
	ep      transport.Endpoint
	closing bool
	state   string
	addr    string // lưu lại để reconnect
	pass    string
	net     NetOptions
	outSeq  uint64 // Seq của tin gửi gần nhất trên kết nối hiện tại
}

// NetOptions giả lập mạng xấu ở chiều GỬI của client (xem transport.LossyConn).
type NetOptions struct {
	Loss   float64       // tỉ lệ mất gói, 0.1 = 10%
	Delay  time.Duration // độ trễ cố định thêm vào mỗi gói
	Jitter time.Duration // độ trễ ngẫu nhiên thêm [0, Jitter), không đảo thứ tự gói
}

type Status struct {
	User     string   `json:"user"`
	State    string   `json:"state"`
	Online   []string `json:"online"`
	RTTms    float64  `json:"rtt_ms"`
	Cwnd     int      `json:"cwnd"`
	InFlight int      `json:"in_flight"`
	SentPkts uint64   `json:"sent_pkts"`
	LostPkts uint64   `json:"lost_pkts"`
	LossPct  float64  `json:"loss_pct"`
}

// NewClient tạo client chưa kết nối; file nhận được lưu vào thư mục "downloads".
func NewClient(username string) *Client {
	return &Client{
		User:        username,
		Events:      NewEventLog(),
		DownloadDir: "downloads",
	}
}

// OnlineUsers trả về bản sao danh sách user đang online (theo tin "users" mới nhất từ server).
func (c *Client) OnlineUsers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.online...)
}

func (c *Client) setOnline(users []string) {
	c.mu.Lock()
	c.online = users
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

// Status trả về trạng thái kết nối và số liệu mạng (RTT, cwnd, tỉ lệ mất gói) cho Web UI.
func (c *Client) Status() Status {
	c.mu.Lock()
	st := Status{User: c.User, State: c.state, Online: append([]string(nil), c.online...)}
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

// Connect đăng nhập (TCP) rồi mở kết nối UDP; loss > 0 để giả lập mất gói.
func (c *Client) Connect(serverTCPAddr, password string, loss float64) error {
	return c.ConnectWith(serverTCPAddr, password, NetOptions{Loss: loss})
}

// ConnectWith giống Connect nhưng nhận đủ tham số giả lập mạng.
func (c *Client) ConnectWith(serverTCPAddr, password string, opt NetOptions) error {
	c.mu.Lock()
	c.addr, c.pass, c.net = serverTCPAddr, password, opt
	c.mu.Unlock()
	return c.dial()
}

func (c *Client) dial() error {
	c.mu.Lock()
	addr, pass, opt := c.addr, c.pass, c.net
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
	if opt.Loss > 0 || opt.Delay > 0 || opt.Jitter > 0 {
		pc = &transport.LossyConn{UDPConn: sock, Loss: opt.Loss, Delay: opt.Delay, Jitter: opt.Jitter}
	}
	conn, err := transport.Dial(pc, srvUDP, keyx.ConnID, kUDP) // Dial tự gửi PING đầu tiên
	if err != nil {
		sock.Close()
		return err
	}
	if c.isClosing() { // Close() được gọi trong lúc đang kết nối (vd Ctrl+C khi đang reconnect)
		conn.Close()
		return errors.New("client: đã đóng")
	}
	c.AttachEndpoint(conn)
	c.setState("connected")
	return nil
}

// Close đóng kết nối (gửi CLOSE cho server) và dừng việc tự kết nối lại.
func (c *Client) Close() {
	c.mu.Lock()
	c.closing = true
	ep := c.ep
	c.mu.Unlock()
	if ep != nil {
		ep.Close()
	}
}

// ConnectTCPAndAuth chạy handshake TCP: HELLO → KEYX → AUTH (mật khẩu mã hóa bằng k_tcp) → AUTH_OK.
// Trả về KEYX (conn_id, cổng UDP) và khóa k_udp cho transport.
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

	c.setOnline(res.Users)
	c.Events.Add("users", res.Users)
	return &keyx, kUDP, nil
}

// AttachEndpoint gắn kết nối transport mới (kể cả khi kết nối lại) và chạy goroutine nhận dữ liệu.
func (c *Client) AttachEndpoint(ep transport.Endpoint) {
	c.mu.Lock()
	c.ep = ep
	c.outSeq = 0 // kết nối mới ⇒ Seq bắt đầu lại từ 1 (server cũng tạo SeqBuffer mới)
	c.mu.Unlock()
	go c.listenLoop(ep)
}

type inStream struct {
	kind byte
	buf  []byte    // message: gom tới FIN
	file *fileRecv // file: ghi thẳng xuống đĩa
}

func (c *Client) listenLoop(ep transport.Endpoint) {
	streams := make(map[uint32]*inStream)
	var order protocol.SeqBuffer // tin từ server giao theo đúng Seq
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
					c.handleMessage(st.buf, &order)
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

func (c *Client) handleMessage(data []byte, order *protocol.SeqBuffer) {
	var msg protocol.AppMsg
	if err := json.Unmarshal(data[1:], &msg); err != nil {
		return
	}
	for _, m := range order.Push(msg) {
		if m.Type == "users" {
			c.setOnline(m.Users)
		}
		c.Events.Add(m.Type, m)
	}
}

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

var ErrNotConnected = errors.New("client: chưa có kết nối UDP")

func (c *Client) send(m protocol.AppMsg) error {
	c.mu.Lock() // lấy ep và Seq cùng lúc: Seq phải thuộc đúng kết nối sẽ gửi nó
	ep := c.ep
	c.outSeq++
	m.Seq = c.outSeq
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

// SendChat gửi tin chat chung cho mọi người đang online.
func (c *Client) SendChat(text string) error {
	return c.send(protocol.AppMsg{Type: "chat", Text: text})
}

// SendDM gửi tin nhắn riêng cho user to; server báo lỗi nếu người nhận không online.
func (c *Client) SendDM(to, text string) error {
	return c.send(protocol.AppMsg{Type: "dm", To: to, Text: text})
}
