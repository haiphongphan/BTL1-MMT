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
	loss    float64
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

func NewClient(username string) *Client {
	return &Client{
		User:        username,
		Events:      NewEventLog(),
		DownloadDir: "downloads",
	}
}

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

func (c *Client) Connect(serverTCPAddr, password string, loss float64) error {
	c.mu.Lock()
	c.addr, c.pass, c.loss = serverTCPAddr, password, loss
	c.mu.Unlock()
	return c.dial()
}

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
	if loss > 0 {
		pc = &transport.LossyConn{UDPConn: sock, Loss: loss}
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

func (c *Client) Close() {
	c.mu.Lock()
	c.closing = true
	ep := c.ep
	c.mu.Unlock()
	if ep != nil {
		ep.Close()
	}
}

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

func (c *Client) AttachEndpoint(ep transport.Endpoint) {
	c.mu.Lock()
	c.ep = ep
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
					c.handleMessage(st.buf)
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

func (c *Client) handleMessage(data []byte) {
	var msg protocol.AppMsg
	if err := json.Unmarshal(data[1:], &msg); err != nil {
		return
	}
	if msg.Type == "users" {
		c.setOnline(msg.Users)
	}
	c.Events.Add(msg.Type, msg)
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
	ep := c.endpoint()
	if ep == nil {
		return ErrNotConnected
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return ep.SendMessage(append([]byte{protocol.KindMessage}, b...))
}

func (c *Client) SendChat(text string) error {
	return c.send(protocol.AppMsg{Type: "chat", Text: text})
}

func (c *Client) SendDM(to, text string) error {
	return c.send(protocol.AppMsg{Type: "dm", To: to, Text: text})
}
