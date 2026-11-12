# Đọc hiểu go-collab: code và cơ chế từ A đến Z

Tài liệu này dẫn bạn đi qua **toàn bộ code** của app nhắn tin / gửi file, theo đúng đường đi của dữ liệu.
Mỗi mục có link tới đúng dòng code (mở trong VS Code, Ctrl+Click). Đọc theo thứ tự từ trên xuống là
cách nhanh nhất để hiểu; muốn ôn thi / vấn đáp thì nhảy tới [mục 13](#13-câu-hỏi-tự-kiểm-tra-kèm-đáp-án).

> Tài liệu khác trong repo: [`protocol.md`](protocol.md) (định dạng gói/frame, tham số), [`reliability.md`](reliability.md)
> (lý do thiết kế + số đo). Tài liệu này **giải thích code**, hai file kia là **đặc tả**.

---

## Mục lục

1. [Bức tranh tổng thể](#1-bức-tranh-tổng-thể)
2. [Bản đồ thư mục](#2-bản-đồ-thư-mục)
3. [Một phiên làm việc từ đầu tới cuối](#3-một-phiên-làm-việc-từ-đầu-tới-cuối)
4. [Tầng `protocol`: định dạng trên dây](#4-tầng-protocol-định-dạng-trên-dây)
5. [Bảo mật: ECDH, AES-GCM, nonce](#5-bảo-mật-ecdh-aes-gcm-nonce)
6. [Tầng `transport`: UDP tin cậy](#6-tầng-transport-udp-tin-cậy) ← phần quan trọng nhất
7. [Server](#7-server)
8. [Client](#8-client)
9. [Web UI](#9-web-ui)
10. [Tất cả goroutine trong hệ thống](#10-tất-cả-goroutine-trong-hệ-thống)
11. [Chuyện gì xảy ra khi... (kịch bản lỗi)](#11-chuyện-gì-xảy-ra-khi-kịch-bản-lỗi)
12. [Chỗ code hiện tại lệch với tài liệu / hạn chế](#12-chỗ-code-hiện-tại-lệch-với-tài-liệu--hạn-chế)
13. [Câu hỏi tự kiểm tra (kèm đáp án)](#13-câu-hỏi-tự-kiểm-tra-kèm-đáp-án)
14. [Bảng thuật ngữ](#14-bảng-thuật-ngữ)

---

## 1. Bức tranh tổng thể

### 1.1 App làm gì

- Nhiều client đăng nhập vào 1 server.
- Chat chung (broadcast), nhắn riêng (DM), gửi file cho 1 người hoặc cho tất cả.
- Mỗi client có 1 Web UI chạy local (`http://127.0.0.1:8081`) và 1 CLI.

### 1.2 Ý tưởng cốt lõi

Đề bài yêu cầu dữ liệu đi trên **UDP**, nên nhóm tự viết lại những gì TCP/QUIC cho sẵn:

| UDP "trần" | Ta phải tự làm | Ở đâu |
|---|---|---|
| Gói có thể mất | ACK + phát hiện mất + gửi lại | `transport/ack.go`, `conn.go: detectLoss` |
| Gói có thể tới sai thứ tự | Đánh `offset` cho từng byte, ghép lại | `transport/stream.go: recvStream` |
| Gói có thể trùng | Nhớ `packet_num` đã nhận, bỏ đoạn đã giao | `ackTracker.add`, `recvStream.push` |
| Không có điều tốc | Congestion control CUBIC + flow control | `cubic.go`, `flush()` |
| Không mã hóa | ECDH trao khóa + AES-256-GCM từng gói | `crypto.go`, `seal()` |
| Không có "kết nối" | `conn_id` + keep-alive + idle timeout | `udpmux.go`, `keepAlive()` |

Thiết kế được **mượn rất nhiều từ QUIC** (RFC 9000/9002): packet number không bao giờ dùng lại, nhiều stream
độc lập trong 1 kết nối, SACK, phát hiện mất theo số đếm/thời gian, nonce ghép từ packet number.

### 1.3 Các tầng

```
 ┌──────────────────────────────────────────────────────────────┐
 │  Web UI (trình duyệt)  ── HTTP tự viết ──  internal/webui    │  giao diện
 ├──────────────────────────────────────────────────────────────┤
 │  internal/client           internal/server                   │  tầng APP: chat, dm, file,
 │  (Client, EventLog)        (Server, Registry, Session, relay)│  đăng nhập, định tuyến
 ├──────────────────────────────────────────────────────────────┤
 │  internal/transport   — interface Endpoint                   │  tầng TRANSPORT: UDP tin cậy,
 │  (Conn, Mux, Dial, SendStream, recvStream, CUBIC, RTT, ACK)  │  mã hóa, stream, CC
 ├──────────────────────────────────────────────────────────────┤
 │  internal/protocol   — header, frame, message handshake      │  định dạng byte trên dây
 ├──────────────────────────────────────────────────────────────┤
 │  net.UDPConn / net.TCPConn (thư viện chuẩn Go)               │
 └──────────────────────────────────────────────────────────────┘
```

Ranh giới quan trọng nhất là interface **`transport.Endpoint`** ([api.go:30](../internal/transport/api.go#L30)):

```go
type Endpoint interface {
    SendMessage(b []byte) error            // 1 tin = 1 stream ưu tiên cao
    OpenStream(urgent bool) io.WriteCloser // stream dài (file); Close() = FIN
    Recv() <-chan Delivery                 // dữ liệu nhận được
    Done() <-chan struct{}                 // đóng khi mất kết nối
    Stats() Stats
    Close() error
}
```

Tầng app **chỉ biết interface này**, không biết gì về gói, ACK hay mã hóa. Nhờ vậy có `FakeEndpoint`
([fake.go](../internal/transport/fake.go)) nối 2 đầu bằng channel để test tầng app mà không cần mạng.

### 1.4 Cổng mạng

| Cổng | Giao thức | Dùng cho |
|---|---|---|
| 9000 | TCP | Handshake: trao khóa + đăng nhập, xong thì đóng |
| 9001 | UDP | **Mọi dữ liệu**: chat, file, ACK, PING |
| 9002 | UDP broadcast | Server quảng bá `COLLAB_SERVER <ip> 9000 9001` mỗi 2 s |
| 8081 | TCP (HTTP) | Web UI của client, chỉ nghe `127.0.0.1` |

**Vì sao handshake dùng TCP?** Đăng nhập cần trao đổi theo thứ tự, tin cậy, chỉ vài message. Dùng TCP cho
gọn, xong thì đóng. Mọi thứ sau đó đi UDP.

---

## 2. Bản đồ thư mục

```
cmd/
  server/main.go        đọc cờ, mở UDP, bật discovery, nghe TCP
  client/main.go        đọc cờ, đăng nhập, bật Web UI, vòng lặp CLI
internal/
  protocol/             ĐỊNH DẠNG BYTE (không có logic mạng)
    packet.go           header 12 byte, các hằng số kích thước
    frame.go            4 loại frame: STREAM, ACK, PING, CLOSE + ParseFrames
    wire.go             message TCP: [4 byte độ dài][JSON]
    appmsg.go           Hello/KeyX/Auth/AuthResult, AppMsg, FileMeta
    filehdr.go          header của stream file + parser nhận từng mẩu
    seq.go              SeqBuffer (sắp xếp tin theo Seq) — hiện CHƯA được dùng, xem mục 12
  transport/            UDP TIN CẬY
    api.go              Endpoint, Delivery, Stats
    conn.go             ★ Conn: vòng lặp run(), gửi, nhận, ACK, mất gói, keep-alive
    stream.go           SendStream (cắt frame) + recvStream (ghép theo offset)
    ack.go              ackTracker: nhớ gói đã nhận, tạo SACK range
    rtt.go              ước lượng RTT, RTO
    cubic.go            congestion control CUBIC
    crypto.go           ECDH, KDF, AES-GCM, nonce
    udpmux.go           readLoop, Mux (server), Dial (client)
    lossy.go            LossyConn: giả lập mất gói / trễ / nút cổ chai
    fake.go             FakeEndpoint cho test
    cclog.go            ghi cwnd ra CSV để vẽ đồ thị
  server/
    auth.go             Server, handshake TCP, kiểm tra mật khẩu
    registry.go         bảng user online (Session)
    router.go           mỗi session: writeLoop + serveSession, định tuyến chat/dm
    files.go            chuyển tiếp (relay) file
    discovery.go        broadcast UDP 9002
  client/
    core.go             Client: đăng nhập, listenLoop, reconnect, gửi tin
    files.go            gửi file (2 lượt đọc), nhận file (ghi đĩa + kiểm SHA-256)
    event.go            EventLog: hàng sự kiện cho CLI/Web UI đọc
  webui/
    httpserver.go       HTTP/1.1 tự viết (không dùng net/http)
    api.go              /api/events, /api/status, /api/send, /api/upload
    static/             index.html, app.js, style.css (nhúng bằng go:embed)
scripts/plotcc          vẽ đồ thị cwnd từ CSV ra SVG
users.json              tài khoản: salt + sha256(salt+password)
```

**Thứ tự đọc code đề xuất:** `protocol/packet.go` → `protocol/frame.go` → `transport/api.go` →
`transport/stream.go` → `transport/conn.go` (đọc `run()` trước) → `ack.go` → `rtt.go` → `cubic.go` →
`crypto.go` → `udpmux.go` → `server/auth.go` → `server/router.go` → `server/files.go` → `client/core.go` → `client/files.go`.

---

## 3. Một phiên làm việc từ đầu tới cuối

Đây là "câu chuyện" của hệ thống. Đọc phần này trước, các mục sau sẽ đi sâu từng bước.

### Bước 0. Server khởi động — [cmd/server/main.go](../cmd/server/main.go)

1. `transport.SetCongestionControl("cubic")` — chọn thuật toán CC cho mọi Conn sau này.
2. `server.NewServer("users.json", 9001)` — đọc tài khoản, `nextConnID = 1000`.
3. `srv.ListenUDP(loss)` ([auth.go:51](../internal/server/auth.go#L51)) — mở **1 socket UDP :9001 dùng chung cho mọi client**,
   bọc bằng `LossyConn` nếu có `-loss`, rồi `transport.NewMux(sock)` → khởi động goroutine `readLoop`.
4. `go srv.StartDiscovery(...)` — broadcast mỗi 2 s.
5. `srv.ListenTCP(":9000")` — vòng lặp `Accept`, mỗi kết nối TCP 1 goroutine `handleHandshake`.

### Bước 1. Client đăng nhập (TCP) — [client/core.go:156](../internal/client/core.go#L156) ↔ [server/auth.go:82](../internal/server/auth.go#L82)

```
Client                                              Server (goroutine handleHandshake)
  NewKeyPair() → priv_c
  ── HELLO {client_pub} ──────────────────────────►  NewKeyPair() → priv_s
                                                     connID = nextConnID.Add(1)   (1001, 1002, ...)
                                                     kTCP, kUDP = DeriveKeys(priv_s, client_pub)
  ◄────────────── KEYX {server_pub, conn_id, 9001} ─
  kTCP, kUDP = DeriveKeys(priv_c, server_pub)
  (2 bên ra CÙNG kTCP, kUDP mà khóa chưa từng đi trên mạng)
  nonce = 12 byte ngẫu nhiên
  ── AUTH {nonce, AES-GCM(kTCP, {user,pass})} ────►  aead.Open → {user, pass}
                                                     checkPassword: sha256(salt+pass) == hash ?
                                                     reg.AddPending(user, connID, kUDP)  (trùng tên → FAIL)
                                                     mux.Accept(connID, kUDP) → Conn phía server
                                                     AttachEndpoint → chạy writeLoop + serveSession
  ◄──────────────────── AUTH_OK {users:[...]} ──────
  đóng TCP                                           đóng TCP (defer c.Close())
```

Mỗi message TCP có dạng `[4 byte độ dài big-endian][JSON]` ([wire.go](../internal/protocol/wire.go)). Độ dài
bị chặn ở 1 MB để kẻ xấu không khai "4 GB" làm server cấp phát hết RAM.

Server đặt `SetDeadline(10s)` cho TCP: client mở kết nối rồi im lặng thì goroutine không bị treo mãi.

### Bước 2. Mở kênh UDP — [client/core.go:109](../internal/client/core.go#L109)

1. Client `net.ListenUDP("udp", nil)` — **socket riêng, cổng ngẫu nhiên**.
2. `transport.Dial(sock, server:9001, connID, kUDP)` tạo `Conn` phía client (`pingNeeded = true`).
3. Tick đầu tiên (10 ms) → `flush()` → gửi **gói đầu tiên chứa PING**.
4. Server: `readLoop` đọc gói, xem `conn_id` trong header → tìm đúng `Conn` → đẩy vào `Conn.in`.
5. `Conn` phía server giải mã thành công → **lúc này mới biết địa chỉ UDP của client** (`c.peer = p.addr`).
6. Từ đây hai bên trao đổi bình thường.

> Lưu ý thứ tự: server đã gọi `AttachEndpoint` → `broadcastUserList()` **trước khi** biết địa chỉ UDP client.
> Tin "users" đó nằm chờ trong `urgentQ` của Conn (`flush()` thoát sớm khi `peer == nil`), PING tới là gửi đi ngay.
> Không mất tin nào.

Nếu PING đầu bị mất, client gửi lại sau 200, 400, 800, 1600 ms, rồi mỗi 2 s
(`firstProbe`, [conn.go:588](../internal/transport/conn.go#L588)) cho tới khi nghe được server.

### Bước 3. Gửi 1 tin chat

```
Web UI: POST /api/send {to:"", text:"hi"}
 └► webui.apiSend → client.SendChat("hi")
     └► client.send: JSON AppMsg{type:"chat",text:"hi"} , thêm byte 0x01 đầu
         └► Conn.SendMessage(b)                       [stream.go:167]
             ├► OpenStream(urgent=true) → stream_id lẻ mới (1, 3, 5...)
             ├► SendStream.Write: cắt ≤1000 byte/frame, gắn offset → urgentCh
             └► SendStream.Close: frame rỗng có FIN → urgentCh
                 └► run(): urgentQ → flush → takeFrames → sendPacket → seal → UDP
═══════════════ mạng ═══════════════
Server:
 readLoop → Conn.in → run(): handlePacket → giải mã → onStreamFrame → recvStream.push
   → outQ → recvCh → serveSession đọc Delivery
     → byte đầu 0x01 → onMessageData: gom tới FIN, parse JSON
       → msg.From = tên thật của session (chống giả danh)
       → routeMessage: "chat" → Send cho mọi session trừ người gửi
          → sess.sendCh → writeLoop → EP.SendMessage → (lại xuống transport, stream_id chẵn)
═══════════════ mạng ═══════════════
Client nhận:
 listenLoop → gom stream 0x01 tới FIN → handleMessage → Events.Add("chat", msg)
   → Web UI poll /api/events mỗi 400 ms → hiển thị
```

Người gửi **không** nhận lại tin của chính mình từ server; Web UI tự vẽ tin của mình ngay khi bấm gửi
([app.js](../internal/webui/static/app.js), dòng "Tin của mình không được server dội lại").

### Bước 4. Gửi 1 file

```
client.SendFile(path, to)                                  [client/files.go:32]
 ├ lượt 1: đọc hết file → SHA-256 + size  (meta phải đi TRƯỚC data, nên phải biết hash trước)
 ├ OpenStream(urgent=false) → stream bulk
 ├ Write([0x02][meta_len][JSON FileMeta])
 ├ lượt 2: io.Copy(stream, file)  → mỗi Write bị chặn khi bulkQ đầy (backpressure)
 └ Close() → FIN
Server: serveSession thấy byte đầu 0x02 → onFileData           [server/files.go:17]
 ├ FileHeaderParser.Feed gom cho đủ header (có thể bị cắt giữa nhiều Delivery)
 ├ meta.From = tên thật; tìm người nhận (to rỗng = tất cả trừ người gửi)
 ├ tạo channel relay (256) + goroutine relayFile
 │    └ mỗi người nhận: OpenStream(false), ghi header mới + từng mẩu data, cuối cùng Close
 └ từng Delivery data tiếp theo → đẩy vào relay; FIN → close(relay)
Client nhận: listenLoop → onFileData                           [client/files.go:121]
 ├ parser lấy header → tạo file trong downloads/ (tên an toàn, không ghi đè)
 ├ ghi qua MultiWriter(file, sha256) → vừa ghi vừa băm
 └ FIN → so size + SHA-256 với meta; sai thì xóa file, báo file_error
```

Server **không lưu file xuống đĩa**: nó chuyển tiếp từng mẩu ngay khi nhận (streaming relay).

### Bước 5. Ngắt kết nối

- **Client thoát đàng hoàng** (`/quit`): `Conn.Close()` gửi 1 gói `CLOSE "bye"` → server `handlePacket` thấy
  CLOSE → `shutdown()` → `Done()` đóng → `serveSession` thoát → `dropSession` → xóa khỏi Registry →
  broadcast danh sách user mới.
- **Client chết đột ngột** (kill -9, rút mạng): không có CLOSE. Server không nghe gì trong **10 s** →
  `keepAlive` → `shutdown()` → như trên.
- **Server chết**: client không nghe gì trong 10 s → `Done()` → `listenLoop` chuyển trạng thái `reconnecting`
  → `reconnectLoop` thử lại sau 1, 2, 4, ... tối đa 30 s, mỗi lần làm lại **toàn bộ** handshake TCP (khóa mới, conn_id mới).

---

## 4. Tầng `protocol`: định dạng trên dây

Package này chỉ biến struct ↔ byte, không có goroutine hay socket.

### 4.1 Gói UDP — [packet.go](../internal/protocol/packet.go)

```
 0      2   3   4          8          12                               ≤1200
 ┌──────┬───┬───┬──────────┬──────────┬─────────────────────────────────┐
 │magic │ver│flg│ conn_id  │packet_num│ AES-GCM( frames ) + tag 16 byte │
 │0xC0DE│ 1 │ 0 │ uint32   │ uint32   │                                 │
 └──────┴───┴───┴──────────┴──────────┴─────────────────────────────────┘
```

Các con số cần nhớ:

| Hằng | Giá trị | Vì sao |
|---|---|---|
| `MaxPacket` | 1200 | Như QUIC: IPv6 bắt buộc chở được 1280 byte, trừ header IP+UDP còn 1232, làm tròn 1200 ⇒ không bị phân mảnh IP |
| `HeaderSize` | 12 | magic 2 + ver 1 + flags 1 + conn_id 4 + pn 4 |
| `TagSize` | 16 | tag xác thực GCM |
| `MaxFramePayload` | 1172 | = 1200 − 12 − 16, chỗ cho frame (plaintext) |
| `MaxStreamData` | 1000 | data tối đa trong 1 STREAM frame, chừa chỗ ghép ACK (≤70 byte) |

- **Header gửi rõ** vì bên nhận cần `conn_id` để biết dùng khóa nào. Nhưng header là **AAD** của GCM, nên
  sửa 1 bit header là giải mã thất bại.
- `Header.Marshal` ghi thẳng vào buffer có sẵn (không cấp phát mới mỗi gói).

### 4.2 Frame — [frame.go](../internal/protocol/frame.go)

Phần plaintext của gói là **chuỗi frame nối liền**, không có dấu phân cách. Byte đầu mỗi frame là loại frame,
từ đó biết độ dài:

| Type | Frame | Bố cục | Cần ACK? |
|---|---|---|---|
| 0x01 | STREAM | `type flags(bit0=FIN) stream_id(4) offset(4) len(2) data` | Có |
| 0x02 | ACK | `type window(4) n(1) n×[start(4) end(4)]`, n ≤ 8 | **Không** |
| 0x03 | PING | `type` | Có |
| 0x04 | CLOSE | `type reason_len(1) reason` | Không |

`ParseFrames` ([frame.go:138](../internal/protocol/frame.go#L138)) là 1 vòng `for len(b) > 0 { switch b[0] }`.
Mỗi nhánh **kiểm tra độ dài trước khi cắt slice** → dữ liệu cụt trả `ErrShort`, loại lạ trả lỗi, **không bao giờ panic**.
Data của StreamFrame **trỏ thẳng vào buffer** (không copy) — an toàn vì `aead.Open(nil, ...)` tạo buffer mới mỗi gói.

`Frame` là interface có method ẩn `isFrame()`: chỉ 4 loại trong package được phép là Frame; phía dùng phân biệt bằng type switch.

`AppendClose` cắt reason ở 255 byte nhưng **lùi về đầu ký tự UTF-8** gần nhất (`utf8.RuneStart`) để không cắt đôi chữ tiếng Việt.

**Vì sao gói chỉ chứa ACK không được ACK?** Nếu có, A gửi ACK → B ACK cái ACK → A ACK lại... vô hạn.

### 4.3 Nội dung stream ở tầng app — [appmsg.go](../internal/protocol/appmsg.go), [filehdr.go](../internal/protocol/filehdr.go)

Byte đầu của **stream** (không phải của frame) cho biết loại:

```
0x01 MESSAGE : [0x01][JSON AppMsg .................. tới FIN]
0x02 FILE    : [0x02][meta_len uint32][JSON FileMeta][byte file ....... tới FIN]
```

- `AppMsg.Type`: `chat | dm | users | error` (server gửi `users` và `error`).
- `FileMeta`: `id, from, to, name, size, sha256`.
- **Meta nằm trong cùng stream với data file**: giữa các stream không có thứ tự, nếu gửi meta riêng thì data có thể tới trước meta.

`FileHeaderParser.Feed` ([filehdr.go:33](../internal/protocol/filehdr.go#L33)) gom byte cho tới khi đủ
`5 + meta_len`, rồi trả về `meta` + phần data file còn thừa (`rest`). Chặn `meta_len > 64 KB`.

### 4.4 `SeqBuffer` — [seq.go](../internal/protocol/seq.go)

Vấn đề: mỗi tin 1 stream ⇒ tin #2 có thể tới trước tin #1 khi gói của #1 bị mất. `SeqBuffer.Push` giữ tin tới
sớm trong map, khi tin còn thiếu tới thì `drain` xả liền mạch. Trùng thì bỏ, `Seq=0` giao ngay, giữ quá 4096 tin
thì `skipGap` nhảy qua chỗ trống.

**Hiện code server/client chưa dùng SeqBuffer** (đã revert) — xem [mục 12](#12-chỗ-code-hiện-tại-lệch-với-tài-liệu--hạn-chế).

---

## 5. Bảo mật: ECDH, AES-GCM, nonce

Code: [transport/crypto.go](../internal/transport/crypto.go).

### 5.1 Trao khóa ECDH X25519

```
Client: priv_c, pub_c = priv_c·G           Server: priv_s, pub_s = priv_s·G
        gửi pub_c ───────────────────────►
                  ◄─────────────────────── gửi pub_s
shared = priv_c·pub_s = priv_c·priv_s·G = priv_s·pub_c
```

Kẻ nghe lén thấy `pub_c`, `pub_s` nhưng không tính được `shared` (bài toán logarit rời rạc trên đường cong).

`DeriveKeys` ([crypto.go:25](../internal/transport/crypto.go#L25)):

```
kTCP = SHA256(shared ‖ "tcp")   // mã hóa user/pass trong handshake
kUDP = SHA256(shared ‖ "udp")   // mã hóa mọi gói UDP
```

Tách 2 khóa vì 2 kênh sinh nonce khác nhau (TCP: ngẫu nhiên; UDP: từ packet_num) → không bao giờ có chuyện
cùng khóa + cùng nonce. (Sản phẩm thật nên dùng HKDF thay vì SHA256 trơn.)

### 5.2 AES-256-GCM

GCM là **AEAD**: vừa mã hóa (bí mật) vừa sinh tag 16 byte (toàn vẹn + xác thực). Hàm:

```go
Seal(dst, nonce, plaintext, aad) → ciphertext ‖ tag
Open(dst, nonce, ciphertext‖tag, aad) → plaintext  hoặc lỗi nếu bất kỳ byte nào (kể cả aad) bị sửa
```

Ở mỗi gói ([conn.go:736 `seal`](../internal/transport/conn.go#L736)): `aad = header 12 byte`, `plaintext = các frame`.

### 5.3 Nonce không bao giờ lặp — [crypto.go:56](../internal/transport/crypto.go#L56)

```
nonce (12 byte) = conn_id(4) ‖ packet_num(4) ‖ direction(1) ‖ 0 0 0
```

- Mỗi kết nối có khóa riêng (ECDH mới mỗi lần đăng nhập).
- Trong 1 kết nối, `packet_num` tăng dần và **gửi lại cũng dùng số mới**.
- Hai chiều dùng chung `kUDP` và **cả hai đều đánh pn từ 0** → byte `direction` (0 = client→server, 1 = server→client)
  đảm bảo không trùng.
- Nonce không cần gửi kèm: bên nhận tự dựng lại từ header.

Trùng nonce trong GCM là thảm họa: lộ XOR của 2 bản rõ và giả mạo được tag. Đây là lý do thiết kế "pn không bao giờ dùng lại".

### 5.4 Mật khẩu

- `users.json` lưu `salt` + `sha256(salt + password)` ([auth.go:170](../internal/server/auth.go#L170)), không lưu mật khẩu rõ.
- Mật khẩu đi trên mạng **đã mã hóa bằng kTCP** (nonce ngẫu nhiên 12 byte).
- Server ghi đè `msg.From` / `meta.From` bằng tên của session → client không giả danh người khác được.

**Giới hạn** (nên biết khi bị hỏi): không có chứng chỉ/xác thực server ⇒ về lý thuyết bị man-in-the-middle
ở bước trao khóa; SHA256 một vòng dễ brute-force hơn bcrypt/argon2.

---

## 6. Tầng `transport`: UDP tin cậy

Đây là phần lớn nhất và khó nhất. Đọc chậm.

### 6.1 Mô hình concurrency: 1 goroutine sở hữu toàn bộ state

Sơ đồ ở đầu [conn.go](../internal/transport/conn.go#L15):

```
app goroutine ── SendStream.Write ──► urgentCh / bulkCh ──┐
app goroutine ── Close ────────────► closeReq ────────────┤
readLoop (udpmux.go) ── gói thô ────► in ─────────────────┼──► run()  (1 goroutine duy nhất)
ticker 10 ms / 100 ms ────────────────────────────────────┘      │
                                                                 ├──► sock.WriteToUDP
app goroutine ◄── Recv() ◄── recvCh ◄── outQ ◄───────────────────┘
```

Mọi field trong khối "Chỉ goroutine run() được dùng" của `Conn` ([conn.go:139](../internal/transport/conn.go#L139))
— `unacked`, hàng đợi, cwnd, RTT, các stream — **chỉ `run()` đọc/ghi**. Goroutine khác chỉ nói chuyện qua channel.
Đây là mô hình **actor**: không cần mutex, không có data race, `go test -race` sạch.

Ngoại lệ an toàn: `stats` (atomic.Pointer, run() ghi bản copy, app đọc), `nextStream` (atomic, cấp stream_id).

### 6.2 Vòng lặp `run()` — [conn.go:259](../internal/transport/conn.go#L259)

Mỗi vòng: chờ **1 sự kiện** → xử lý → `flush()`.

| Nhánh `select` | Làm gì |
|---|---|
| `p := <-c.in` | `handlePacket`, rồi `drainIn` xử lý thêm ≤32 gói đang chờ (để 1 lần flush ACK được nhiều gói) |
| `f := <-c.urgentCh` | đưa frame chat vào `urgentQ` |
| `f := <-bulkCh` | đưa frame file vào `bulkQ` — **bulkCh = nil khi bulkQ ≥ 256** |
| `recvCh <- head` | giao 1 Delivery cho app — **recvCh = nil khi outQ rỗng** |
| `<-ticker.C` | `detectLoss`, `keepAlive`, `cleanup`, `publishStats`, `logCC` |
| `<-c.closeReq` | gửi CLOSE, `shutdown()` |

**Mẹo channel nil** (rất hay bị hỏi): trong Go, gửi/nhận trên channel `nil` không bao giờ sẵn sàng, nên `select`
bỏ qua nhánh đó. Code dùng mẹo này để **bật/tắt nhánh**:

- `bulkCh = nil` khi `bulkQ` đầy ⇒ `run()` ngừng nhận frame file ⇒ `SendStream.Write` của app bị chặn ở
  `ch <- f` ⇒ **backpressure**: đọc file nhanh hơn mạng thì app tự chậm lại, RAM không phình.
- `recvCh = nil` khi không có gì để giao ⇒ không gửi `Delivery{}` rỗng.

**Tick thích ứng** (cuối vòng lặp): có gói chờ ACK (hoặc client chưa nghe được server) → tick 10 ms để phát hiện mất
kịp thời; rảnh → 100 ms. Lý do: 20 client rảnh với tick 10 ms từng tốn 43% CPU chỉ để scheduler thức dậy.

### 6.3 Phía gửi, từng bước

#### a) `SendStream.Write` — [stream.go:94](../internal/transport/stream.go#L94)

```go
for len(p) > 0 {
    n := min(len(p), 1000)
    data := copy của p[:n]          // phải copy: frame còn nằm trong hàng đợi/unacked sau khi Write trả về
    f := StreamFrame{StreamID: s.id, Offset: s.off, Data: data}
    c.enqueue(f, s.urgent)          // urgentCh hoặc bulkCh; chặn khi đầy; ErrClosed nếu conn đã đóng
    s.off += n
}
```

`Close()` gửi frame **rỗng có FIN** tại offset cuối.

`enqueue` ([stream.go:135](../internal/transport/stream.go#L135)) kiểm `done` **trước** rồi mới `select`: vì
channel có buffer, nếu chỉ có 1 `select` thì Go có thể chọn nhánh gửi thành công dù conn đã đóng.

**stream_id**: client `nextStream = 0xFFFFFFFF` rồi `Add(2)` → 1, 3, 5...; server bắt đầu 0 → 2, 4, 6...
Hai bên mở stream độc lập mà không bao giờ trùng id.

#### b) `flush` — [conn.go:628](../internal/transport/conn.go#L628)

```go
if c.peer == nil { return }    // server chưa biết địa chỉ client
for min(cwnd, peerWindow) - bytesInFlight >= 1200 {  // còn chỗ cho 1 gói đầy
    frames := takeFrames()
    if len(frames) == 0 { break }
    sendPacket(frames, now)
}
if ackNeeded || pingNeeded { sendPacket(nil, now) }   // ACK/PING chưa đi nhờ gói data nào
```

Hai giới hạn:
- `cwnd` — **congestion control**: đừng làm nghẽn **mạng**.
- `peerWindow` — **flow control**: đừng làm tràn **bên nhận**.

#### c) `takeFrames` — [conn.go:651](../internal/transport/conn.go#L651): hàng đợi ưu tiên

Lấy frame theo thứ tự **`retransQ` → `urgentQ` → `bulkQ`** cho tới khi gói đầy. Luôn chừa 70 byte cho ACK.
`room = 1172 − 70 = 1102`. Một frame file đầy tốn 12 + 1000 = 1012 ⇒ mỗi gói chở **1 frame file đầy**, nhưng có thể
chở **nhiều frame chat nhỏ**.

Nhờ thứ tự này, **tin chat chen lên trước hàng trăm frame file đang xếp hàng** — chống Head-of-Line blocking
phía gửi. Số đo: chat khi đang gửi file trễ trung bình 31 ms; nếu chat chung hàng với file thì 763 ms.

Gửi lại đứng đầu vì byte bị mất đang làm bên nhận "kẹt" chờ lấp chỗ trống.

#### d) `sendPacket` — [conn.go:675](../internal/transport/conn.go#L675)

```
pn = nextPN++                                  // KHÔNG BAO GIỜ dùng lại
payload = [ACK nếu ackNeeded][STREAM...][PING nếu pingNeeded và không có STREAM]
pkt = seal(pn, payload)                        // header + GCM
WriteToUDP(pkt, peer)
nếu có STREAM: unacked[pn] = {sentAt, size, frames}; bytesInFlight += size
```

- ACK **đi nhờ** (piggyback) gói data nếu có.
- Gói có data đã buộc bên kia ACK rồi, nên không thêm PING.
- Gói chỉ có ACK/PING **không lưu vào unacked**, không gửi lại.
- `unacked` lưu **frame**, không lưu gói: khi mất, frame được đóng vào **gói mới với pn mới**.

### 6.4 Phía nhận, từng bước

#### a) `readLoop` — [udpmux.go:14](../internal/transport/udpmux.go#L14)

```
ReadFromUDP → ParseHeader → magic/version đúng? → lookup(conn_id) → copy gói → c.in <- (non-blocking)
```

- Chỉ kiểm header, **không giải mã** (giải mã trong `run()` của từng Conn → song song giữa các client).
- `c.in` đầy (256) → **bỏ gói** như router thật; bên gửi sẽ phát hiện mất và gửi lại. readLoop không bao giờ bị
  1 client chậm làm kẹt cả server.
- Phải copy vì `buf` bị ghi đè ở lần đọc sau.

#### b) `handlePacket` — [conn.go:353](../internal/transport/conn.go#L353)

1. Kiểm header (magic, version, **conn_id == c.id**).
2. `aead.Open(nonce từ header, ciphertext, aad = header)` — sai ⇒ **bỏ lặng lẽ**, không đổi state.
3. `ParseFrames`.
4. `lastHeard = now`, `heardPeer = true`.
5. `isNew := recvd.add(pn)` — false nếu gói trùng.
6. **Connection migration**: nếu là gói **mới nhất** thì `peer = p.addr`. Client đổi Wi-Fi → 4G (đổi IP:port)
   vẫn giữ kết nối, vì server phân kênh theo `conn_id` chứ không theo địa chỉ. Chỉ đổi theo gói mới nhất (và đã qua
   GCM) để gói cũ / gói bị phát lại không kéo kết nối về địa chỉ sai.
7. Từng frame:
   - `ACK` → `onAck`
   - `STREAM` → `ackNeeded = true` (**kể cả gói trùng cũng ACK lại**, vì ACK trước có thể đã mất), chỉ xử lý data nếu `isNew`
   - `PING` → `ackNeeded = true`
   - `CLOSE` → `shutdown()`

#### c) `onStreamFrame` + `recvStream.push` — [conn.go:451](../internal/transport/conn.go#L451), [stream.go:42](../internal/transport/stream.go#L42)

Mỗi stream có `recvStream` riêng: `next` (offset tiếp theo cần giao), `pending` (đoạn tới sớm), `finAt`.

`push(off, data, fin)` với `end = off + len(data)`:

| Trường hợp | Ý nghĩa | Xử lý |
|---|---|---|
| `end <= next` | đã giao hết rồi (bản gửi lại) | bỏ |
| `off > next` | tới sớm, phía trước còn lỗ | cất `pending[off]` |
| `off <= next < end` | nối tiếp được (có thể chồng 1 phần) | cắt `data[next-off:]`, giao, rồi xả các đoạn `pending` liền mạch |

Ví dụ: frame offset 0, 1000, 2000 tới theo thứ tự 2000, 0, 1000:

```
nhận off=2000 → off > next(0)  → pending{2000}
nhận off=0    → giao [0,1000), next=1000; pending[1000]? không → dừng
nhận off=1000 → giao [1000,2000), next=2000; pending[2000]? có → giao, next=3000
```

`done = finAt >= 0 && next == finAt` → stream xong → xóa khỏi `streams`, ghi vào `closedStreams` 30 s
(để frame gửi lại tới muộn không tạo ra stream "ma").

Các đoạn sẵn sàng đi vào `outQ` → nhánh `recvCh <- head` của `run()` giao cho app. **`run()` không bao giờ bị
chặn vì app đọc chậm** — dữ liệu dồn trong `outQ`, và cửa sổ nhận co lại để bên kia gửi chậm đi.

**Đây là chỗ chống HoL phía nhận**: gói của stream file bị mất chỉ làm *stream file* kẹt trong `pending`;
stream chat có `recvStream` khác nên vẫn được giao ngay.

### 6.5 ACK (SACK) — [ack.go](../internal/transport/ack.go)

`ackTracker` nhớ tập `seen` các pn đã nhận và `largest`.

`ranges(8)` quét **lùi từ largest**, gom các đoạn liên tục:

```
đã nhận: 1 2 3 4 5 _ 7 8 9 _ _ 12
ranges : [12..12] [7..9] [1..5]      → bên gửi biết chính xác thiếu 6, 10, 11
```

Khác TCP ACK tích lũy ("đã nhận liên tục tới N"): SACK cho bên gửi biết **từng lỗ**, nên chỉ gửi lại đúng gói mất.

- Chỉ quét 4096 gói gần nhất (`ackHistory`); `prune` xóa pn cũ khi map > 8192 phần tử.
- ACK frame cũng mang `window` = cửa sổ nhận hiện tại (flow control).

**`onAck`** ([conn.go:411](../internal/transport/conn.go#L411)) phía gửi:

1. Cập nhật `peerWindow` — nhưng **chỉ khi ACK này mới hơn ACK đã cập nhật lần trước** (`Ranges[0].End >= winAckPN`):
   ACK cũ tới muộn không được ghi đè cửa sổ mới.
2. Với mỗi pn trong `unacked` mà ACK chứa: xóa, trừ `bytesInFlight`, `cc.OnAck(size)`.
3. Có gói mới được ACK ⇒ `rtoBackoff = 0` (mạng còn sống).
4. Lấy **1 mẫu RTT từ gói mới nhất vừa được ACK** (gói cũ hơn có thể đã chờ lâu, đo sẽ lệch).
5. `detectLoss`.

**Vì sao đo RTT không bao giờ mập mờ?** TCP gửi lại dùng cùng số thứ tự, nên khi nhận ACK không biết ACK cho bản gốc
hay bản gửi lại (cần thuật toán Karn). Ở đây gửi lại = pn mới, nên mỗi ACK ứng với đúng 1 lần gửi.

### 6.6 Phát hiện mất gói — [conn.go:517 `detectLoss`](../internal/transport/conn.go#L517)

Gọi sau mỗi ACK và mỗi tick. Với mỗi gói `pn` trong `unacked`:

```go
overtaken := haveAcked && pn < largestAcked          // gói gửi SAU nó đã được ACK
byCount   := overtaken && pn+3 <= largestAcked       // 3 gói vượt mặt (giống 3 dup-ACK của TCP)
byDelay   := overtaken && age > 9/8·max(SRTT, latest) // bị vượt mặt và đã quá lâu
byTime    := age > RTO                               // không ai vượt mặt, chỉ là quá lâu
```

| Cách | Khi nào cần | Nguồn |
|---|---|---|
| byCount | trường hợp phổ biến, cửa sổ lớn | RFC 9002 kPacketThreshold = 3 |
| byDelay | cwnd chỉ 2–3 gói → không bao giờ đủ 3 gói vượt mặt | RFC 9002 kTimeThreshold = 9/8 |
| byTime (RTO) | gói **cuối cùng** bị mất, không có gói nào sau nó | RFC 6298 / PTO RFC 9002 |

Gói mất: xóa khỏi `unacked`, `lostPkts++`, **frame của nó vào `retransQ`** (sort theo pn để gửi lại theo thứ tự cũ).

**Recovery period:** chỉ gọi `cc.OnLoss` nếu gói mất được gửi **sau** lần giảm cwnd gần nhất (`sentAt > recoveryStart`).
Một đợt nghẽn làm mất 20 gói liền → chỉ giảm cwnd 1 lần, không phải 20 lần.

**RTO backoff:** nếu mất vì **hết giờ** (không có gói nào vượt mặt) thì `rtoBackoff++` → RTO nhân đôi (tối đa 5 s).
Đó là dấu hiệu mạng có thể đã chết, nên đừng bắn gói dồn dập. Mất do count/delay thì mạng vẫn sống → không backoff.

### 6.7 RTT và RTO — [rtt.go](../internal/transport/rtt.go)

```
mẫu đầu: SRTT = R,  RTTVAR = R/2
sau đó : RTTVAR = 3/4·RTTVAR + 1/4·|SRTT − R|      (dùng SRTT CŨ)
         SRTT   = 7/8·SRTT   + 1/8·R
RTO    = SRTT + max(4·RTTVAR, 1 ms) + 10 ms         (chưa có mẫu: 500 ms)
RTO thực tế = min(RTO << rtoBackoff, 5 s)
```

Ví dụ trên loopback: SRTT ≈ 0,4 ms, RTTVAR ≈ 0,1 ms → RTO ≈ 0,4 + 1 + 10 = **11,4 ms**.

**Vì sao không dùng sàn 200 ms như TCP Linux?** TCP cần sàn lớn vì bên nhận được **trì hoãn ACK** 200–500 ms.
Bên nhận của ta ACK ngay trong lần flush kế tiếp. Đo được: sàn 200 ms làm throughput loss 10% giảm ~6 lần.
Ngược lại, **bỏ hẳn** khoảng đệm (`1 ms` + `10 ms`) thì trên đường truyền đều RTTVAR → 0, RTO ≈ RTT, ACK hơi chậm là
bị coi mất oan (CUBIC tụt từ 2,01 xuống 0,87 MB/s).

### 6.8 Congestion control CUBIC — [cubic.go](../internal/transport/cubic.go)

Interface ([conn.go:63](../internal/transport/conn.go#L63)): `Window()`, `OnAck(bytes, now)`, `OnLoss(now)`.
Có 2 bản: `cubic` (mặc định) và `fixedWindow` (luôn 64 KB, `-cc fixed`).

| Giai đoạn | Công thức |
|---|---|
| Khởi đầu | cwnd = 10 × 1200 = 12 000 byte, ssthresh = ∞ |
| Slow start (cwnd < ssthresh) | mỗi byte được ACK → cwnd += 1 byte ⇒ **gấp đôi mỗi RTT** |
| Mất gói (`OnLoss`) | wMax = cwnd; cwnd = max(0,7·cwnd, 2 gói); ssthresh = cwnd; epoch = 0 |
| Sau mất gói | W_cubic(t) = C·(t−K)³ + wMax, K = ∛(wMax·(1−β)/C), C = 0,4, β = 0,7 (tính theo **số gói**) |
| Reno-friendly | W_est += 0,53 gói mỗi RTT; nếu W_cubic < W_est thì dùng W_est |
| Giới hạn | mỗi RTT tăng tối đa 1,5× ; trần 1 MB (= cửa sổ nhận) |

Hình dạng đường bậc 3: **tăng nhanh** khi còn xa wMax → **chậm dần, đi ngang** quanh wMax (mức từng gây nghẽn) →
**tăng nhanh lại** để dò xem mạng có rộng hơn không.

Ví dụ số (khớp đồ thị trong `reliability.md`): mất gói ở cwnd ≈ 147 KB ⇒ wMax ≈ 122,5 gói ⇒
K = ∛(122,5 × 0,3 / 0,4) ≈ ∛91,9 ≈ **4,5 s** — đúng chu kỳ ~4 s trên đồ thị.

Ở `OnAck`, `cwnd += (target − cwnd) · acked / cwnd`: sau khi ACK đủ 1 cửa sổ (≈1 RTT) thì cwnd ≈ target.

**Giới hạn:** CUBIC coi mọi mất gói là nghẽn. Với `-loss 0.1` (mất ngẫu nhiên 10%) cwnd nằm sát đáy (Mathis:
throughput ≈ MSS/RTT · 1,22/√p). Vì vậy có `-cc fixed` để demo độ tin cậy dưới mất gói nặng.

### 6.9 Flow control

- Bên nhận: `recvWindow = 1 MB − outBytes − pendingBytes` ([conn.go:495](../internal/transport/conn.go#L495)) —
  trừ dữ liệu đang chờ app đọc và dữ liệu đang chờ lấp lỗ. Gửi trong mọi ACK.
- Bên gửi: `peerWindow` lấy từ ACK, dùng trong điều kiện `flush`.
- **Window update** ([conn.go:502](../internal/transport/conn.go#L502)): nếu lần trước báo < 256 KB mà giờ ≥ 512 KB
  (app vừa đọc nhiều), chủ động gửi ACK. Không thì bên gửi đang dừng hẳn phải đợi tới PING 2 s mới biết có chỗ.

**Chuỗi backpressure đầu-cuối** khi gửi file qua server (đáng hiểu kỹ):

```
client B (nhận) ghi đĩa chậm
 → Conn của B phía client: outQ dồn → recvWindow nhỏ → ACK báo window nhỏ
 → Conn server→B: peerWindow nhỏ → flush ngừng → bulkQ đầy (256) → bulkCh = nil
 → relayFile: w.Write bị chặn → channel relay (256) đầy
 → serveSession của A: st.relay <- data bị chặn → ngừng đọc Recv()
 → Conn A phía server: outQ dồn → recvWindow nhỏ
 → client A: peerWindow nhỏ → bulkQ đầy → io.Copy(stream, file) bị chặn
⇒ A tự động gửi chậm bằng tốc độ B nhận, không chỗ nào phình RAM vô hạn.
```

### 6.10 Keep-alive, timeout, PING đầu — [conn.go:576](../internal/transport/conn.go#L576)

| Điều kiện | Hành động |
|---|---|
| Không nhận gói hợp lệ nào trong **10 s** | `shutdown()` — kết nối chết |
| Không gửi gì trong **2 s** | `pingNeeded = true` → bên kia phải ACK → cả 2 thấy nhau còn sống |
| Client chưa nghe được server | gửi lại PING sau 200, 400, 800, 1600 ms, rồi 2 s |

`cleanup` mỗi tick: `ackTracker.prune()` và xóa `closedStreams` quá 30 s.

### 6.11 `Mux` (server) và `Dial` (client) — [udpmux.go](../internal/transport/udpmux.go)

- **Server**: 1 socket, 1 `readLoop`, nhiều `Conn`. `Mux.conns map[conn_id]*Conn` có `RWMutex` (readLoop đọc,
  Accept ghi). `Accept` thêm Conn và chạy goroutine xóa Conn khỏi bảng khi `Done()`.
- **Client**: socket riêng, `readLoop` với lookup chỉ khớp đúng 1 conn_id. Conn đóng → đóng socket.
- `Mux.Close` chép danh sách Conn rồi **thả lock trước khi** gọi `c.Close()` — vì `Close` đợi `run()` thoát, còn
  goroutine dọn dẹp trong `Accept` cũng cần `m.mu` ⇒ giữ lock sẽ deadlock.

### 6.12 Công cụ test: `LossyConn`, `FakeEndpoint`, `cclog`

- **`LossyConn`** ([lossy.go](../internal/transport/lossy.go)) bọc `*net.UDPConn`, thay `WriteToUDP`:
  - `Loss`: bỏ ngẫu nhiên (vẫn trả "thành công", giống mạng thật).
  - `Delay/Jitter`: trễ, nhưng **FIFO** (gói sau không bao giờ tới trước gói trước, như hàng đợi router). Bản đầu hẹn
    giờ từng gói độc lập → đảo thứ tự hàng loạt → 89% gói bị coi mất oan dù loss = 0.
  - `Rate/QueueBytes`: nút cổ chai + bộ đệm, đầy thì tail-drop → mất gói **do nghẽn** (thứ CUBIC được thiết kế để phát hiện).
  - Đây là lý do `transport` dùng interface `PacketConn` thay vì `*net.UDPConn` trực tiếp.
- **`FakeEndpoint`**: 2 đầu nối bằng channel, không mất gói — để test router/server.
- **`cclog`**: ghi CSV `time_ms,conn_id,event,cwnd,ssthresh,rtt_us,...`; `scripts/plotcc` vẽ ra SVG.

---

## 7. Server

### 7.1 Cấu trúc dữ liệu

```go
type Server struct {
    reg        *Registry                  // ai đang online
    users      map[string]UserAccount     // từ users.json
    nextConnID atomic.Uint32              // 1001, 1002, ...
    udpPort    int
    mux        *transport.Mux
}

type Session struct {        // 1 user đang online
    Username string
    ConnID   uint32
    KUDP     [32]byte
    EP       transport.Endpoint   // nil khi mới AddPending, gán ở Attach
    sendCh   chan []byte          // hàng đợi tin gửi tới user này (64)
}
```

**Registry** ([registry.go](../internal/server/registry.go)): 2 map `byName`, `byConnID` + `RWMutex`.

- `AddPending`: tên đã có → `ErrAlreadyOnline` (chặn đăng nhập trùng).
- `Attach`: gán `EP` cho session.
- `Remove(sess)`: chỉ xóa nếu `byName[name] == sess` — **so con trỏ**, để session cũ đã bị thay không xóa nhầm session mới.
- `Snapshot`: chép danh sách session đã có EP → gửi tin **không giữ lock**.

### 7.2 Mỗi session có 2 goroutine — [router.go](../internal/server/router.go)

```
AttachEndpoint(connID, ep):
    sess = reg.Attach(...)
    go sess.writeLoop()      // GỬI: sendCh → EP.SendMessage
    go s.serveSession(sess)  // NHẬN: EP.Recv() → phân loại stream → định tuyến
    broadcastUserList()
```

**Vì sao tách writeLoop?** Định tuyến tin chat của A tới B gọi `B.Send(packet)` — chỉ đẩy vào `B.sendCh`
(non-blocking). Việc gọi `B.EP.SendMessage` (có thể bị chặn) nằm trong goroutine của B. Nhờ vậy **1 người nhận chậm
không làm chậm người gửi hay người nhận khác**. `writeLoop` cũng là goroutine duy nhất gửi tới B, nên thứ tự các tin tới B được giữ ở phía gửi.

`Session.Send` ([router.go:20](../internal/server/router.go#L20)): `sendCh` đầy → **bỏ tin**, log.

**`serveSession`** ([router.go:56](../internal/server/router.go#L56)) — vòng lặp nhận:

```
streams := map[stream_id]*inStream
Delivery đầu tiên của stream → kind = byte đầu (0x01 / 0x02)
  0x01 → onMessageData: gom buf tới FIN (tối đa 64 KB), parse JSON, From = sess.Username, routeMessage
  0x02 → onFileData
Fin → xóa stream khỏi map
Done() → return → defer: đóng mọi relay đang mở, recover panic, dropSession
```

**`routeMessage`** ([router.go:130](../internal/server/router.go#L130)): đặt `TS` theo giờ server, rồi

- `chat` → gửi mọi session trong Snapshot **trừ người gửi**.
- `dm` → `GetByName(to)`; không online → gửi `error` về người gửi.

**`dropSession`**: `reg.Remove` (chỉ lần đầu trả true) → `EP.Close()` → `broadcastUserList`.

### 7.3 Relay file — [server/files.go](../internal/server/files.go)

Trạng thái mỗi stream file trong `inStream`: `hdr` (parser), `relay` (channel), `drop` (bỏ hết phần còn lại).

1. Chưa có relay: `hdr.Feed`. Lỗi → `drop`. Đủ header → `meta.From = sess.Username`, `fileTargets` (to rỗng = tất cả
   trừ người gửi). Không có ai → báo lỗi, `drop`.
2. Tạo `relay = make(chan []byte, 256)`, `go relayFile(meta, targets, relay)`, đẩy `rest` vào.
3. Delivery tiếp theo → `relay <- data` (**có thể chặn** → đây là mắt xích backpressure ở mục 6.9).
4. FIN → `close(relay)`.

`relayFile`: mở 1 stream bulk tới mỗi người nhận, ghi header, `for b := range ch { ghi cho từng người }`, Close.
Người nhận nào lỗi thì `outs[i] = nil` và bỏ qua từ đó. `defer` xả hết channel (`for range ch {}`) để
`serveSession` không bị chặn mãi nếu relayFile thoát sớm.

### 7.4 Discovery — [discovery.go](../internal/server/discovery.go)

Broadcast `COLLAB_SERVER <lanIP> 9000 9001` tới `255.255.255.255:9002` mỗi 2 s. `localIP()` trong `main.go` lấy IP LAN
bằng mẹo `net.Dial("udp4", "8.8.8.8:80")` (UDP "dial" không gửi gói nào, chỉ để OS chọn interface ra ngoài).

---

## 8. Client

### 8.1 Struct `Client` — [core.go](../internal/client/core.go)

```go
type Client struct {
    User        string
    Events      *EventLog     // hàng sự kiện cho UI
    DownloadDir string        // "downloads"
    mu sync.Mutex             // bảo vệ: online, ep, closing, state, addr, pass, loss
    ...
}
```

Lưu `addr, pass, loss` để **reconnect tự động** không cần hỏi lại người dùng.

### 8.2 Kết nối và vòng nhận

- `Connect` → `dial()`: `ConnectTCPAndAuth` → `ListenUDP` → `transport.Dial` → `AttachEndpoint` (gán ep + `go listenLoop`).
- **`listenLoop`** ([core.go:215](../internal/client/core.go#L215)): giống `serveSession` của server — map stream,
  byte đầu quyết định loại. Message gom tới FIN rồi `handleMessage`; file thì `onFileData` ghi thẳng xuống đĩa.
  - `defer`: file nào đang nhận dở thì `abort()` (xóa file chưa hoàn chỉnh).
  - `Done()`: nếu user chủ động đóng → `disconnected`; nếu không → `reconnecting` + `go reconnectLoop()`.
- **`handleMessage`**: `users` → cập nhật danh sách online; mọi loại → `Events.Add(type, msg)`.
- **`reconnectLoop`**: chờ 1 s, thử `dial()`, thất bại thì nhân đôi (tối đa 30 s). Mỗi lần là handshake mới hoàn toàn.

### 8.3 Gửi file 2 lượt — [client/files.go:32](../internal/client/files.go#L32)

1. Lượt 1: `io.Copy(sha256, file)` → hash + size. Từ chối file > 4 GB (offset stream là `uint32`).
2. `Seek(0)`, tạo `FileMeta` (ID ngẫu nhiên 8 byte hex), `EncodeFileHeader`.
3. `OpenStream(false)` (bulk), `Write(hdr)`, `io.Copy(stream, file)`, `Close()`.

Event `file_start` / `file_done` cho Web UI. (`progressReader` có sẵn để báo tiến độ phía gửi nhưng `io.Copy` hiện
đọc thẳng từ `f`, nên phía gửi chỉ có start/done.)

### 8.4 Nhận file — [client/files.go:121](../internal/client/files.go#L121)

- `fileRecv` giữ parser, file đang ghi, `MultiWriter(file, hasher)`, số byte đã nhận.
- Đủ header → `open`: `MkdirAll(downloads)`, `safeName` (chống path traversal: `"../../etc/passwd"` → `"passwd"`),
  `createUnique` (`O_EXCL`, trùng tên thì `a (1).txt`, `a (2).txt`...).
- Mỗi mẩu: ghi + băm cùng lúc; event `file_progress` tối đa 5 lần/giây.
- FIN → `finishFile`: so `received == size` **và** SHA-256 khớp; sai → xóa file, `file_error`.

**Vì sao vẫn kiểm SHA-256 khi transport đã tin cậy + GCM?** Kiểm tra đầu-cuối (end-to-end argument): GCM chỉ bảo vệ
từng chặng client↔server; lỗi ở server relay hay ở đĩa vẫn bị phát hiện.

### 8.5 `EventLog` — [event.go](../internal/client/event.go)

Danh sách sự kiện có `Seq` tăng dần (giữ tối đa 2000). `Since(s)` trả các sự kiện có `Seq > s`.
CLI và Web UI đều **poll** theo kiểu "cho tôi mọi thứ sau seq X" — đơn giản, không cần WebSocket, không mất sự kiện
giữa 2 lần poll. (`Event.Seq` này **khác** `AppMsg.Seq` của giao thức.)

### 8.6 CLI — [cmd/client/main.go](../cmd/client/main.go)

`/msg <text>`, `/dm <user> <text>`, `/file <path> [user]`, `/users`, `/quit`; gõ thẳng = chat. Một goroutine
in event ra console mỗi 200 ms.

---

## 9. Web UI

- **HTTP tự viết** ([httpserver.go](../internal/webui/httpserver.go)), không dùng `net/http`: đọc request line, header
  tới dòng trống, `Content-Length` → body là `io.LimitReader`. Trả về luôn kèm `Connection: close` (mỗi request 1 kết nối).
  Có giới hạn dòng header 8 KB, deadline 10 phút, `recover` panic.
- Static file nhúng vào binary bằng `//go:embed static`.
- API ([api.go](../internal/webui/api.go)):

| Endpoint | Làm gì |
|---|---|
| `GET /api/events?since=N` | `Events.Since(N)` |
| `GET /api/status` | user, state, online, RTT, cwnd, in-flight, sent/lost, loss% |
| `POST /api/send {to,text}` | `to` rỗng → chat, có → DM |
| `POST /api/upload?name=&to=` | body là file thô → lưu file tạm → `go SendFile` → xóa thư mục tạm |

- [app.js](../internal/webui/static/app.js): poll events mỗi 400 ms, status mỗi 1 s; tab "Chung" + mỗi người DM 1 tab;
  `esc()` escape HTML chống XSS khi hiển thị tin.

---

## 10. Tất cả goroutine trong hệ thống

**Server** (N client online):

| Goroutine | Số lượng | Vai trò |
|---|---|---|
| `main` → `ListenTCP` | 1 | Accept TCP |
| `handleHandshake` | 1 / handshake đang diễn ra | trao khóa + đăng nhập, sống ≤ 10 s |
| `StartDiscovery` | 1 | broadcast 2 s |
| `readLoop` (Mux) | 1 | đọc **mọi** gói UDP, phân kênh theo conn_id |
| `Conn.run` | N | mỗi kết nối 1 actor |
| Mux dọn dẹp | N | chờ `Done()` để xóa khỏi bảng |
| `writeLoop` | N | gửi tin tới từng user |
| `serveSession` | N | nhận + định tuyến từ từng user |
| `relayFile` | 1 / file đang chuyển | chuyển tiếp file |

**Client:**

| Goroutine | Vai trò |
|---|---|
| `main` | vòng lặp đọc stdin |
| in event | in sự kiện ra console |
| Web UI accept + 1 / request | HTTP |
| `readLoop` | đọc socket UDP |
| `Conn.run` | actor transport |
| Dial dọn dẹp | đóng socket khi Conn xong |
| `listenLoop` | nhận dữ liệu tầng app |
| `SendFile` | 1 / file đang gửi |
| `reconnectLoop` | khi mất kết nối |

Đồng bộ hóa: transport dùng **channel** (actor); tầng app dùng **mutex** (`Client.mu`, `Registry.mu`, `EventLog.mu`).

---

## 11. Chuyện gì xảy ra khi... (kịch bản lỗi)

| Tình huống | Cơ chế xử lý |
|---|---|
| 1 gói data bị mất giữa chừng | 3 gói sau được ACK → `byCount` → frame vào `retransQ` → gửi lại trong gói pn mới; cwnd ×0,7 |
| Gói **cuối cùng** bị mất | không ai vượt mặt → hết RTO → `byTime` → gửi lại, `rtoBackoff++` |
| ACK bị mất | gói vẫn trong `unacked` → coi là mất → gửi lại → bên nhận thấy offset đã giao → bỏ data nhưng **vẫn ACK** |
| Gói tới 2 lần | `ackTracker.add` trả false → không xử lý data, vẫn ACK |
| Gói tới sai thứ tự | `recvStream.pending` giữ, lấp lỗ thì xả |
| Gói bị sửa / giả mạo | `aead.Open` lỗi → bỏ lặng lẽ |
| Gói rác / conn_id lạ | `readLoop` bỏ ngay ở bước header / lookup |
| Đang gửi file 100 MB thì chat | chat vào `urgentQ`, chen trước `bulkQ`; ở bên nhận là stream riêng → không chờ file |
| App nhận đọc chậm | `outQ` dồn → `recvWindow` giảm → bên gửi chậm lại (flow control) |
| Mạng nghẽn (bộ đệm router tràn) | mất gói → CUBIC giảm cwnd → hồi phục theo đường bậc 3 |
| Client đổi IP:port (Wi-Fi → 4G) | server phân kênh theo conn_id; gói mới nhất hợp lệ cập nhật `peer` |
| PING đầu tiên của client bị mất | probe 200/400/800 ms... |
| Client kill -9 | server idle timeout 10 s → `dropSession` → mọi người nhận danh sách user mới |
| Server kill -9 | client idle timeout 10 s → `reconnecting` → thử lại 1, 2, 4... 30 s |
| Đăng nhập 2 nơi cùng tên | `AddPending` → `AUTH_FAIL "Tài khoản đang online ở máy khác"` |
| Client gửi `from: "admin"` | server ghi đè `From` = tên session |
| Tên file `../../x` | `safeName` → `x` |
| File hỏng trên đường | SHA-256 không khớp → xóa file, báo lỗi |
| Kết nối rớt khi đang nhận file | `listenLoop` defer → `abort()` xóa file dở |
| Kẻ xấu khai độ dài message TCP 4 GB | `ReadMsg` từ chối > 1 MB |
| Kẻ xấu mở TCP rồi im lặng | `SetDeadline(10s)` |

---

## 12. Chỗ code hiện tại lệch với tài liệu / hạn chế

Đọc kỹ phần này trước khi demo/vấn đáp, vì `protocol.md` mô tả vài thứ **không còn trong code** (sau commit
`e0f3dda` revert phần của B):

1. **`SeqBuffer` chưa được dùng.** `protocol.md` mục 3.5 nói client/server đánh `AppMsg.Seq` và bên nhận dùng
   `SeqBuffer`. Hiện tại `client.send` và `routeMessage` **không gán Seq**, `listenLoop`/`serveSession` **không qua SeqBuffer**.
   ⇒ Khi mất gói, 2 tin chat liên tiếp **có thể hiện sai thứ tự**. (`SeqBuffer` có code + test đầy đủ trong `protocol/seq.go`.)
2. **Server có bỏ tin.** `protocol.md` nói hàng đợi 4096 tin và "không bỏ tin, đầy thì ngắt client". Code hiện tại:
   `sendCh` buffer **64**, đầy thì `Session.Send` **bỏ tin** và chỉ log.
3. `pendingTimeout` (15 s) trong `auth.go` khai báo nhưng chưa dùng; `RemoveIfPending`, `GetByConnID` cũng chưa có ai gọi.
4. Discovery: server phát, nhưng client **chưa có code nghe** cổng 9002 (vẫn phải truyền `-server`). Chuỗi broadcast
   ghi cứng cổng TCP `9000` dù `-tcp` đổi được.
5. `relayFile` ghi tuần tự cho từng người nhận: 1 người nhận chậm làm chậm tất cả người nhận cùng file. Và trong
   lúc `serveSession` của A bị chặn ở `relay <-`, **chat của A cũng phải chờ** (chỉ ảnh hưởng chính người gửi file).
6. `packet_num` là `uint32`, không xử lý tràn số (≈4,3 tỉ gói/kết nối, ~5 TB dữ liệu) — tràn sẽ trùng nonce. Thực tế không chạm tới.
7. Không xác thực server (không chứng chỉ) → có thể bị MITM khi trao khóa; KDF dùng SHA256 thay vì HKDF; hash mật khẩu 1 vòng.
8. `bytesInFlight` tính byte **cả gói** (có header, tag) còn `peerWindow` là byte **data** → flow control xấp xỉ, hơi bảo thủ.
9. `progressReader` khai báo trong `SendFile` nhưng chưa được nối vào `io.Copy` → phía gửi không có `file_progress`.

---

## 13. Câu hỏi tự kiểm tra (kèm đáp án)

<details><summary><b>1. Vì sao packet_num không bao giờ dùng lại, kể cả khi gửi lại?</b></summary>

(a) Nonce GCM chứa packet_num → dùng lại là trùng nonce, phá vỡ bảo mật. (b) Đo RTT không mập mờ: mỗi ACK ứng với
đúng 1 lần gửi, không cần thuật toán Karn. (c) Phát hiện mất theo số đếm chính xác. Dữ liệu được định vị bằng
`offset` của stream, không phải packet_num.
</details>

<details><summary><b>2. packet_num và offset khác nhau thế nào?</b></summary>

packet_num đánh số **gói** (tầng truyền), dùng cho ACK, RTT, nonce, tăng mãi. offset đánh số **byte trong 1 stream**,
cố định cho mỗi byte, dùng để ghép lại. Frame của gói #41 bị mất được gửi lại trong gói #57 nhưng offset giữ nguyên.
</details>

<details><summary><b>3. Head-of-Line blocking là gì, app chống ở đâu?</b></summary>

Trong TCP, 1 byte mất làm mọi byte sau phải chờ, kể cả của tin khác. App chống ở 2 tầng: **nhận** — mỗi tin/file 1 stream
có `recvStream` riêng, mất gói file không chặn chat; **gửi** — `takeFrames` lấy `retransQ → urgentQ → bulkQ`, chat chen trước file.
</details>

<details><summary><b>4. Ba cách phát hiện mất gói và vì sao cần đủ ba?</b></summary>

byCount (≥3 gói sau đã ACK) — nhanh khi cửa sổ lớn. byDelay (bị vượt mặt + quá 9/8 RTT) — khi cwnd nhỏ, không đủ 3 gói
vượt mặt. byTime (quá RTO) — khi gói cuối bị mất, không có gói nào sau để vượt mặt.
</details>

<details><summary><b>5. Congestion control khác flow control?</b></summary>

Congestion control (cwnd, CUBIC) bảo vệ **mạng** khỏi nghẽn, suy ra từ mất gói/RTT. Flow control (cửa sổ nhận,
`peerWindow`) bảo vệ **bên nhận** khỏi bị tràn bộ nhớ, bên nhận báo trực tiếp trong ACK. Gửi được = min(cwnd, peerWindow) − inFlight.
</details>

<details><summary><b>6. Tại sao gói chỉ có ACK không cần ACK, và không vào unacked?</b></summary>

ACK cho ACK gây vòng lặp vô hạn. ACK mất cũng không sao: ACK sau chứa SACK range bao trùm thông tin cũ.
</details>

<details><summary><b>7. Vì sao Conn không cần mutex?</b></summary>

Mọi state chỉ do goroutine `run()` truy cập (mô hình actor); các goroutine khác giao tiếp qua channel. Stats được
publish qua `atomic.Pointer`.
</details>

<details><summary><b>8. Backpressure khi gửi file hoạt động thế nào?</b></summary>

`bulkQ` đầy 256 frame → `run()` đặt `bulkCh = nil` → ngừng nhận → `SendStream.Write` chặn ở `ch <- f` → `io.Copy` chậm lại.
Qua server: chuỗi window nhận nhỏ → relay chặn → serveSession chặn → window của người gửi nhỏ (mục 6.9).
</details>

<details><summary><b>9. Server biết địa chỉ UDP client bằng cách nào? Đổi mạng thì sao?</b></summary>

Từ gói đầu tiên (PING) client gửi, mang conn_id đã nhận qua TCP. Server phân kênh theo conn_id, nên client đổi IP:port
thì gói mới nhất (đã qua GCM) cập nhật `peer` — kết nối tiếp tục.
</details>

<details><summary><b>10. Vì sao meta file nằm chung stream với data?</b></summary>

Giữa các stream không có thứ tự; nếu gửi meta bằng tin riêng thì data có thể tới trước meta.
</details>

<details><summary><b>11. Vì sao không dùng RTO tối thiểu 200 ms như TCP?</b></summary>

TCP cần sàn lớn do delayed ACK (200–500 ms). Bên nhận ở đây ACK ngay, nên dùng công thức PTO của QUIC:
SRTT + max(4·RTTVAR, 1 ms) + 10 ms. Đo được: sàn 200 ms làm throughput loss 10% giảm ~6 lần.
</details>

<details><summary><b>12. Recovery period để làm gì?</b></summary>

1 đợt nghẽn thường làm mất nhiều gói liên tiếp. Nếu mỗi gói mất giảm cwnd 1 lần thì cwnd sụp về đáy. Chỉ gói gửi sau
lần giảm gần nhất mới được giảm tiếp.
</details>

<details><summary><b>13. Vì sao mất ngẫu nhiên 10% thì CUBIC chậm, `fixed` lại nhanh?</b></summary>

CUBIC coi mọi mất gói là nghẽn → giảm cwnd liên tục → nằm sát đáy (công thức Mathis ~1/√p). `fixed` giữ 64 KB bất kể mất
gói. Trên mạng thật mất do nghẽn thì `fixed` sẽ làm nghẽn tệ hơn — đó là lý do cần CUBIC. (BBR giải quyết bằng cách đo băng thông.)
</details>

<details><summary><b>14. Client bị kill -9 thì server biết sau bao lâu?</b></summary>

~10 s (idle timeout). Trong 10 s đó server vẫn PING mỗi 2 s nhưng không nhận gì. Thoát bằng `/quit` thì gần như tức thì nhờ CLOSE.
</details>

<details><summary><b>15. Một gói 1200 byte chứa gì khi gửi file?</b></summary>

12 header + [ACK ≤70 nếu đang nợ] + 1 STREAM frame (12 + 1000 data) + 16 tag. `takeFrames` chừa 70 byte cho ACK nên chỉ
vừa 1 frame file đầy; frame chat nhỏ thì nhiều frame/gói.
</details>

<details><summary><b>16. Chuyện gì xảy ra nếu ai đó sửa conn_id trong header?</b></summary>

Header là AAD của GCM, và nonce dựng từ conn_id → `Open` thất bại → gói bị bỏ. Nếu sửa thành conn_id không tồn tại thì
`readLoop` bỏ ngay.
</details>

---

## 14. Bảng thuật ngữ

| Thuật ngữ | Nghĩa trong project |
|---|---|
| **Frame** | đơn vị nhỏ trong 1 gói: STREAM / ACK / PING / CLOSE |
| **Stream** | 1 dòng byte có thứ tự (1 tin nhắn hoặc 1 file), id lẻ = client mở, chẵn = server mở |
| **Delivery** | 1 đoạn byte đã đúng thứ tự của 1 stream, transport giao cho app |
| **packet_num (pn)** | số thứ tự gói, tăng mãi, không dùng lại |
| **offset** | vị trí byte trong stream |
| **FIN** | cờ báo hết stream |
| **SACK** | Selective ACK: báo các đoạn pn đã nhận, thấy được từng lỗ |
| **unacked** | gói có data đã gửi, chờ ACK |
| **bytesInFlight** | tổng byte của các gói trong unacked |
| **cwnd** | congestion window: số byte tối đa được phép đang bay |
| **ssthresh** | ngưỡng thoát slow start |
| **wMax** | cwnd tại lần mất gói gần nhất (CUBIC) |
| **SRTT / RTTVAR** | RTT đã làm mượt / độ dao động RTT |
| **RTO / PTO** | thời gian chờ ACK tối đa trước khi coi là mất |
| **peerWindow** | cửa sổ nhận bên kia báo (flow control) |
| **retransQ / urgentQ / bulkQ** | hàng đợi gửi: gửi lại / chat / file |
| **outQ** | dữ liệu chờ giao cho app |
| **Backpressure** | bên nhận/hàng đợi đầy làm bên gửi tự chậm lại |
| **HoL blocking** | dữ liệu sau phải chờ dữ liệu trước bị mất |
| **AEAD / AAD** | mã hóa có xác thực / dữ liệu được xác thực nhưng không mã hóa (ở đây: header) |
| **ECDH X25519** | trao khóa Diffie–Hellman trên đường cong Curve25519 |
| **conn_id** | định danh kết nối, server cấp, nằm trong header mọi gói |
| **Mux** | bộ phân kênh: 1 socket UDP phục vụ nhiều Conn |
| **Session** | 1 user online phía server (EP + sendCh) |
| **Relay** | server chuyển tiếp file theo dòng, không lưu đĩa |
