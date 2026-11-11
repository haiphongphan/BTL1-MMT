package client

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"time"
	"sync"
	"errors"

	"github.com/phongphanhai06/go-collab/internal/protocol"
	"github.com/phongphanhai06/go-collab/internal/transport"
)

type Client struct {
	User     string
	EP       transport.Endpoint
	Events   *EventLog
	mu sync.Mutex
	online   []string
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

func NewClient(username string) *Client {
	return &Client{
		User:   username,
		Events: NewEventLog(),
	}
}

// ConnectTCPAndAuth thực hiện Handshake TCP (Step 2, 3)
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

// AttachEndpoint gắn Endpoint (Fake hoặc Real UDP) và bắt đầu nhận tin
func (c *Client) AttachEndpoint(ep transport.Endpoint) {
	c.EP = ep
	go c.listenLoop()
}

func (c *Client) listenLoop() {
	buffers := make(map[uint32][]byte)
	for {
		select {
		case d, ok := <-c.EP.Recv():
			if !ok {
				return
			}
			if len(d.Data) > 0 {
				buffers[d.StreamID] = append(buffers[d.StreamID], d.Data...)
			}
			if !d.Fin {
				continue
			}

			fullData := buffers[d.StreamID]
			delete(buffers, d.StreamID)

			if len(fullData) > 0 && fullData[0] == protocol.KindMessage {
				var msg protocol.AppMsg
				if err := json.Unmarshal(fullData[1:], &msg); err == nil {
					if msg.Type == "users" {
						c.setOnline(msg.Users)
					}
					c.Events.Add(msg.Type, msg)
				}
			}

		case <-c.EP.Done():
			c.Events.Add("status", "disconnected")
			return
		}
	}
}

var ErrNotConnected = errors.New("client: chưa có kết nối UDP")

func (c *Client) send(m protocol.AppMsg) error {
	if c.EP == nil {
		return ErrNotConnected
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return c.EP.SendMessage(append([]byte{protocol.KindMessage}, b...))
}

func (c *Client) SendChat(text string) error {
	return c.send(protocol.AppMsg{Type: "chat", Text: text})
}

func (c *Client) SendDM(to, text string) error {
	return c.send(protocol.AppMsg{Type: "dm", To: to, Text: text})
}
