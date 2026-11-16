# Hướng dẫn báo cáo BTL1: go-collab

File này để chuẩn bị buổi báo cáo và vấn đáp. Đọc theo thứ tự: phần 1 để nắm câu chuyện, phần 2–4 để trình bày, phần 5 để demo, phần 6 để trả lời về xác thực và cookie, phần 7 là bộ câu hỏi luyện tập, phần 8 là số liệu cần nhớ.

Mục lục:

1. [Tóm tắt trong 1 phút](#1-tóm-tắt-trong-1-phút)
2. [Lý thuyết nền: TCP, UDP, handshake, mật mã, HTTP](#2-lý-thuyết-nền)
3. [Thiết kế giao thức của nhóm](#3-thiết-kế-giao-thức)
4. [Hiện thực: đi qua code](#4-hiện-thực-đi-qua-code)
5. [Kịch bản demo đầy đủ](#5-kịch-bản-demo-đầy-đủ)
6. [Xác thực, phiên và cookie](#6-xác-thực-phiên-và-cookie)
7. [Bộ câu hỏi luyện tập](#7-bộ-câu-hỏi-luyện-tập)
8. [Số liệu cần nhớ](#8-số-liệu-cần-nhớ)
9. [Sự cố khi demo và cách xử lý](#9-sự-cố-khi-demo-và-cách-xử-lý)

Gợi ý phân bổ thời gian nếu có khoảng 20 phút: giới thiệu 1 phút, lý thuyết 3 phút, thiết kế giao thức 5 phút, hiện thực 3 phút, demo 7 phút, còn lại để hỏi đáp. Phân công trong nhóm: A phụ trách `protocol` và `transport`, B phụ trách `server`, `client`, `webui` và test hệ thống. Thầy có thể hỏi bất kỳ ai về bất kỳ phần nào, nên cả hai cần nắm hết.

---

## 1. Tóm tắt trong 1 phút

> Nhóm em làm ứng dụng cộng tác client–server: chat nhóm, nhắn riêng, gửi file, xem ai đang online. Điểm chính là **toàn bộ dữ liệu đi qua một giao thức tin cậy do nhóm tự viết trên UDP**, không dùng TCP cho dữ liệu. Giao thức có ACK chọn lọc (SACK), tự gửi lại gói mất, nhiều stream độc lập nên không bị Head-of-Line blocking, điều khiển tắc nghẽn CUBIC, điều khiển luồng, và mã hóa AES-256-GCM. TCP chỉ dùng cho bước bắt tay: trao khóa X25519 và đăng nhập hoặc đăng ký. Mỗi client có giao diện web do một HTTP server tự viết trên socket TCP thô phục vụ, có trang đăng nhập / đăng ký. Toàn bộ viết bằng Go, chỉ dùng thư viện chuẩn, không framework.

Ba câu "vì sao" cần nói được ngay:

- **Vì sao UDP cho dữ liệu?** Để tự làm độ tin cậy và tránh Head-of-Line blocking của TCP: một gói file bị mất không làm tin chat phải chờ.
- **Vì sao TCP cho handshake?** Handshake chỉ có 4 message, cần đúng thứ tự và tin cậy, chạy một lần. Tự làm tin cậy cho 4 message là thừa.
- **Vì sao tự viết HTTP?** Đề cấm framework HTTP và WebSocket, chỉ cho dùng socket.

---

## 2. Lý thuyết nền

### 2.1 Mô hình phân lớp

| Tầng | Ví dụ | Trong project |
|---|---|---|
| Ứng dụng | HTTP, DNS | Message JSON (chat, dm, users), header file, HTTP tự viết cho Web UI |
| Giao vận | TCP, UDP | TCP cho handshake; **giao thức tin cậy tự viết nằm trên UDP** |
| Mạng | IP | IPv4, broadcast trong LAN |
| Liên kết | Ethernet, Wi-Fi | MTU 1500 byte |

Giao thức của nhóm thực chất là một "tầng giao vận thứ hai" chạy trong ứng dụng, phía trên UDP. QUIC (nền của HTTP/3) cũng làm như vậy.

### 2.2 TCP

- **Hướng kết nối**: bắt tay 3 bước SYN → SYN-ACK → ACK để thống nhất số thứ tự ban đầu (ISN). Đóng bằng FIN/ACK hai chiều, bên đóng trước chờ ở TIME_WAIT.
- **Luồng byte (byte stream)**: không giữ ranh giới message. Gửi 2 lần 100 byte, bên nhận có thể đọc 1 lần 200 byte. Vì vậy handshake của nhóm phải tự đóng khung `[4 byte độ dài][JSON]`.
- **Tin cậy**: số thứ tự theo byte, ACK tích lũy (cumulative), tùy chọn SACK, gửi lại khi hết RTO hoặc khi nhận 3 ACK trùng (fast retransmit).
- **RTT và RTO** theo RFC 6298: `SRTT = 7/8·SRTT + 1/8·mẫu`, `RTTVAR = 3/4·RTTVAR + 1/4·|SRTT − mẫu|`, `RTO = SRTT + 4·RTTVAR`, tối thiểu 1 s (Linux dùng 200 ms).
- **Điều khiển luồng (flow control)**: bên nhận báo cửa sổ `rwnd`, bên gửi không gửi vượt.
- **Điều khiển tắc nghẽn (congestion control)**: slow start (cwnd tăng gấp đôi mỗi RTT), congestion avoidance (tăng tuyến tính), mất gói thì giảm. Reno giảm một nửa; CUBIC (mặc định của Linux) giảm còn 0,7 và tăng lại theo hàm bậc 3.
- **Head-of-Line blocking**: TCP giao dữ liệu đúng thứ tự. Một gói mất làm mọi byte phía sau phải chờ, kể cả byte của tin khác không liên quan.

### 2.3 UDP

- Không kết nối, gửi từng datagram độc lập, **giữ ranh giới message**.
- Header 8 byte: cổng nguồn, cổng đích, độ dài, checksum.
- Không đảm bảo tới nơi, không đúng thứ tự, có thể trùng; không có flow control hay congestion control.
- Hỗ trợ broadcast và multicast, TCP thì không.
- Dùng cho DNS, DHCP, VoIP, game, video, QUIC.

| | TCP | UDP |
|---|---|---|
| Kết nối | Có, bắt tay 3 bước | Không |
| Đơn vị | Luồng byte | Datagram |
| Tin cậy, đúng thứ tự | Có | Không |
| Flow / congestion control | Có | Không |
| Header | 20–60 byte | 8 byte |
| Broadcast | Không | Có |
| HoL blocking | Có | Không (nhưng phải tự lo mọi thứ) |

### 2.4 Ba lớp "handshake" trong project

Đây là chỗ dễ bị hỏi lẫn. Có **ba** thứ khác nhau:

1. **Bắt tay 3 bước của TCP** (SYN, SYN-ACK, ACK): do hệ điều hành làm khi client gọi `net.Dial("tcp", ...)`. Code không thấy nó.
2. **Handshake ứng dụng của nhóm** trên kết nối TCP đó: `HELLO → KEYX → AUTH|REGISTER → AUTH_OK|AUTH_FAIL`. Trao khóa và đăng nhập.
3. **Gói UDP đầu tiên** (PING mang `conn_id`): cho server biết IP:port UDP của client. Không có bắt tay riêng cho UDP vì khóa đã có từ bước 2.

So sánh: TLS 1.3 làm bước 2 bằng ClientHello/ServerHello có `key_share` (cũng là ECDHE). QUIC gộp cả 2 và 3 vào UDP.

### 2.5 Mật mã dùng trong project

- **ECDH X25519**: mỗi bên sinh cặp khóa tạm, gửi khóa công khai cho nhau, rồi tự tính ra **cùng một bí mật chung** mà bí mật đó không đi trên mạng. Người nghe lén có cả hai khóa công khai vẫn không tính được.
- **Forward secrecy**: khóa X25519 sinh mới cho mỗi lần đăng nhập và không lưu lại. Lộ một phiên không giúp giải mã phiên khác.
- **Tách khóa**: `k_tcp = SHA256(shared ‖ "tcp")` để mã hóa AUTH, `k_udp = SHA256(shared ‖ "udp")` để mã hóa mọi gói UDP.
- **AES-256-GCM** là AEAD: vừa mã hóa (bí mật), vừa có tag 16 byte (toàn vẹn và xác thực nguồn). Có **AAD**: dữ liệu không mã hóa nhưng được bảo vệ bởi tag. Nhóm dùng header 12 byte làm AAD.
- **Nonce GCM không bao giờ được lặp** với cùng khóa. Lặp nonce thì lộ XOR hai bản rõ và giả mạo được tag.
- **Hash mật khẩu có salt**: `sha256(salt ‖ password)`. Salt ngẫu nhiên mỗi user nên hai người cùng mật khẩu vẫn khác hash, và bảng tra sẵn (rainbow table) vô dụng.

### 2.6 HTTP/1.1, cookie, phiên

- Request: dòng `GET /path HTTP/1.1`, các header, dòng trống `\r\n`, thân dài đúng `Content-Length`. Response: dòng `HTTP/1.1 200 OK`, header, dòng trống, thân.
- HTTP **không trạng thái**: mỗi request độc lập. Để "nhớ" người đã đăng nhập, web thường dùng **cookie**: server trả `Set-Cookie: sid=<chuỗi ngẫu nhiên>; HttpOnly; Secure; SameSite=Strict`, trình duyệt gửi lại `Cookie: sid=...` ở mọi request sau. Server tra `sid` trong bảng phiên.
- Thuộc tính cookie: `HttpOnly` (JS không đọc được, chống XSS lấy trộm), `Secure` (chỉ gửi qua HTTPS), `SameSite` (không gửi khi request đến từ trang khác, chống CSRF), `Path`, `Domain`, `Max-Age`/`Expires`.
- Cookie phân biệt theo **host, không theo cổng**: `127.0.0.1:8081` và `127.0.0.1:8082` dùng chung cookie.
- Cách khác: token trong header `Authorization: Bearer <token>`, ví dụ JWT.
- Tấn công liên quan: XSS (chèn script), CSRF (trang lạ khiến trình duyệt gửi request kèm cookie của bạn), đánh cắp phiên.

Project **không dùng cookie**. Lý do và cách trả lời ở [phần 6](#6-xác-thực-phiên-và-cookie).

### 2.7 MTU và broadcast

- Ethernet MTU 1500 byte. IPv6 bắt buộc mọi đường truyền chở được gói 1280 byte. Gói vượt MTU bị phân mảnh IP: mất một mảnh là mất cả gói. Nhóm giới hạn gói UDP ở **1200 byte** giống QUIC.
- Broadcast `255.255.255.255` chỉ đi trong LAN, router không chuyển tiếp. Dùng để client tự tìm server.

---

## 3. Thiết kế giao thức

Tài liệu đầy đủ: [docs/protocol.md](docs/protocol.md). Báo cáo: chương 3.

### 3.1 Cổng

| Cổng | Giao thức | Mục đích |
|---|---|---|
| 9000 | TCP | Handshake: trao khóa, đăng nhập hoặc đăng ký. Xong thì đóng |
| 9001 | UDP | Toàn bộ dữ liệu: chat, DM, file, ACK, PING |
| 9002 | UDP broadcast | Server phát `COLLAB_SERVER <ip> <tcp_port> <udp_port>` mỗi 2 s |
| 8081, 8082… | TCP (HTTP tự viết) | Web UI của từng client, chỉ nghe `127.0.0.1` |

### 3.2 Handshake

Mỗi message TCP có dạng `[4 byte độ dài big-endian][JSON]`, tối đa 1 MB.

```
Client                                              Server
  ── HELLO {client_pub} ─────────────────────────►  sinh khóa X25519, cấp conn_id
  ◄──────────── KEYX {server_pub, conn_id, udp_port}
  hai bên: shared = X25519(priv, peer_pub)
           k_tcp = SHA256(shared‖"tcp"), k_udp = SHA256(shared‖"udp")
  ── AUTH hoặc REGISTER {nonce, sealed} ─────────►  sealed = AES-GCM(k_tcp, nonce, {"user","pass"})
                                                     REGISTER: kiểm tra luật, thêm tài khoản, ghi users.json
                                                     AUTH: so hash; chặn đăng nhập trùng
                                                     tạo transport.Conn(conn_id, k_udp)
  ◄──────────── AUTH_OK {users} hoặc AUTH_FAIL {reason}
  đóng TCP
  ── UDP: gói đầu (PING) mang conn_id ───────────►  server biết IP:port UDP của client
```

Điểm cần nói:

- Mật khẩu **không bao giờ đi rõ**, kể cả trên TCP.
- Đăng ký không cần handshake thứ hai: server tạo tài khoản rồi đăng nhập luôn.
- Server tạo kết nối UDP **trước** khi trả AUTH_OK, để PING đầu tiên tới là có người nhận.
- Đăng nhập sai: user lạ và sai mật khẩu trả **cùng một lý do**, để không lộ user nào tồn tại.

### 3.3 Gói UDP

```
 0      2   3   4          8          12                              ≤ 1200
 ┌──────┬───┬───┬──────────┬──────────┬────────────────────────────────┐
 │magic │ver│flg│ conn_id  │packet_num│ AES-GCM(frames) + tag 16 byte  │
 │0xC0DE│ 1 │   │ uint32   │ uint32   │                                │
 └──────┴───┴───┴──────────┴──────────┴────────────────────────────────┘
```

- Header 12 byte gửi rõ, vì server cần `conn_id` để biết dùng khóa nào. Header là **AAD** nên sửa một bit là giải mã thất bại.
- **Nonce = conn_id(4) ‖ packet_num(4) ‖ chiều(1) ‖ 000**. `packet_num` tăng mãi, gửi lại cũng dùng số mới. Byte chiều (0 = client→server, 1 = server→client) bảo đảm hai chiều không bao giờ trùng nonce.
- Phần frame tối đa 1200 − 12 − 16 = 1172 byte.

### 3.4 Frame

| Type | Tên | Cấu trúc | Cần ACK? |
|---|---|---|---|
| 0x01 | STREAM | type, flags (bit FIN), stream_id(4), offset(4), len(2), data ≤ 1000 | Có |
| 0x02 | ACK | type, window(4), n(1), n × [start, end] với n ≤ 8 | Không |
| 0x03 | PING | type | Có (giữ kết nối) |
| 0x04 | CLOSE | type, reason_len, reason | Không |

Một gói có thể chở nhiều frame, ví dụ ACK cho chiều kia cộng vài STREAM.

### 3.5 Stream và tầng ứng dụng

- **Mỗi tin nhắn là một stream riêng**, mỗi file cũng là một stream. Stream ghép trong một kết nối UDP, mất gói ở stream này không chặn stream khác. Đây là cách giải HoL blocking.
- Byte đầu của stream cho biết loại: `0x01` là message JSON, `0x02` là file `[độ dài][FileMeta JSON][dữ liệu]`.
- Hàng đợi gửi có ưu tiên: **gửi lại > chat (urgent) > file (bulk)**. Chat không phải xếp sau file.
- **Thứ tự tin nhắn**: vì không có HoL nên tin sau có thể tới trước tin trước. Mỗi tin có `seq`; bên nhận dùng `SeqBuffer` giữ tin tới sớm cho tới khi đủ. Server đánh lại `seq` riêng cho từng người nhận.

### 3.6 Độ tin cậy (transport)

- **SACK**: ACK liệt kê tối đa 8 khoảng packet_num đã nhận.
- **Phát hiện mất theo 3 cách** (RFC 9002): theo số (đã có gói số ≥ pn+3 được ACK), theo thời gian (gói sau đã được ACK mà gói này quá 9/8 RTT chưa có ACK), và hết RTO.
- **RTO = SRTT + max(4·RTTVAR, 1 ms) + 10 ms**, nhân đôi khi timeout liên tiếp, trần 5 s. Không cần sàn 1 s như TCP vì bên nhận ACK ngay, không trì hoãn.
- Chỉ **gửi lại frame STREAM của gói mất**, đóng vào gói mới với số mới. Nhờ vậy đo RTT không bị mập mờ (vấn đề Karn của TCP).
- Bên nhận ghép dữ liệu theo **offset**, bỏ phần trùng, giao đúng thứ tự trong từng stream.
- **Flow control**: cửa sổ nhận tối đa 1 MB, báo trong mỗi ACK.
- **Congestion control**: CUBIC (C = 0,4, β = 0,7, cwnd ban đầu 10 gói), hoặc `-cc fixed` cửa sổ cố định 64 KB.
- **Giữ kết nối**: im lặng 2 s thì gửi PING; không nhận gì 10 s thì coi kết nối chết. Đóng chủ động thì gửi CLOSE 3 bản.

### 3.7 State diagram

Client: **Logged out** (trang đăng nhập) → **Discovering** (nếu server là `auto`) → **Handshaking** → **Connected** → **Reconnecting** (backoff 1, 2, 4… tối đa 30 s) → **Closed** hoặc về **Logged out** khi đăng xuất.

Session ở server: **Pending** (đã AUTH_OK, chưa thấy gói UDP) → **Active** (biết IP:port UDP) → **Removed** (CLOSE, im lặng 10 s, hoặc hàng đợi gửi đầy).

---

## 4. Hiện thực: đi qua code

### 4.1 Cấu trúc thư mục

```
cmd/server, cmd/client, cmd/mkusers   chỉ đọc cờ và lắp ráp
internal/protocol    định dạng: header 12 byte, frame, message handshake, header file, SeqBuffer
internal/transport   UDP tin cậy: conn (vòng lặp chính), stream, ack, rtt, cubic, crypto, mux, lossy (giả lập mạng)
internal/server      auth.go (handshake, đăng ký), registry.go, router.go (chat/DM), files.go (relay file), discovery.go
internal/client      core.go (Login/Logout, nhận, kết nối lại), files.go, event.go, discovery.go
internal/webui       httpserver.go (HTTP tự viết), api.go, static/ (HTML, JS, CSS)
```

Ba tầng tách biệt đúng yêu cầu đề: mạng (`transport`, `protocol`), xử lý dữ liệu (`server`, `client`), giao diện (`webui`).

### 4.2 Mô hình đồng thời

- **Transport**: mỗi kết nối có **một goroutine `run()` sở hữu toàn bộ trạng thái** (kiểu actor). Gói tới, lệnh gửi, tick đồng hồ đều đi qua channel vào goroutine này, nên không cần mutex cho trạng thái kết nối.
- **Mux** ở server: một socket UDP chung cho mọi client. Goroutine `readLoop` đọc gói, xem `conn_id` trong header, đẩy vào đúng kết nối.
- **Server**: mỗi session có goroutine đọc `serveSession`, goroutine ghi `writeLoop` với hàng đợi 4096 tin, và `watchPeer`. Client chậm không chặn người khác; hàng đợi đầy thì ngắt riêng client đó, **không bỏ tin**.
- **Client**: `listenLoop` nhận, `reconnectLoop` kết nối lại, mỗi request HTTP một goroutine.
- Kiểm chứng: 20 client chạy 60 s, server giữ **86 goroutine** không đổi, chạy `go test -race` sạch.

### 4.3 Các hàm nên mở khi trình bày code

| Muốn chỉ | Mở |
|---|---|
| Handshake phía server, nhánh AUTH / REGISTER | [internal/server/auth.go:119](internal/server/auth.go#L119) `handleHandshake` |
| Đăng ký, ghi `users.json` nguyên tử | [internal/server/auth.go:249](internal/server/auth.go#L249) `register`, `saveUsersLocked` |
| Kiểm tra mật khẩu | [internal/server/auth.go:227](internal/server/auth.go#L227) `checkPassword` |
| Handshake phía client | [internal/client/core.go:281](internal/client/core.go#L281) `handshake` |
| Login / Logout, số thế hệ `gen` | [internal/client/core.go:162](internal/client/core.go#L162) |
| Trao khóa, nonce | [internal/transport/crypto.go:25](internal/transport/crypto.go#L25), dòng 56 |
| Nhận một gói: kiểm header, giải mã, ACK | [internal/transport/conn.go:353](internal/transport/conn.go#L353) `handlePacket` |
| Phát hiện mất gói 3 cách | [internal/transport/conn.go:517](internal/transport/conn.go#L517) `detectLoss` |
| RTT, RTO | [internal/transport/rtt.go](internal/transport/rtt.go) |
| CUBIC | [internal/transport/cubic.go:59](internal/transport/cubic.go#L59) |
| Ghép stream theo offset | [internal/transport/stream.go:42](internal/transport/stream.go#L42) |
| Hàng đợi ưu tiên | [internal/transport/conn.go:654](internal/transport/conn.go#L654) `takeFrames` |
| Định tuyến chat / DM, ghi đè `From` | [internal/server/router.go:143](internal/server/router.go#L143), dòng 172 |
| Hàng đợi gửi và ngắt client chậm | [internal/server/router.go:24](internal/server/router.go#L24) |
| Relay file kiểu pipeline | [internal/server/files.go:74](internal/server/files.go#L74) |
| Gửi file 2 lượt, kiểm SHA-256 | [internal/client/files.go:38](internal/client/files.go#L38), dòng 193 |
| Sắp lại thứ tự tin | [internal/protocol/seq.go:25](internal/protocol/seq.go#L25) |
| HTTP tự viết | [internal/webui/httpserver.go:89](internal/webui/httpserver.go#L89) `readRequest` |
| API đăng nhập của Web UI | [internal/webui/api.go:94](internal/webui/api.go#L94) `apiLogin` |

### 4.4 Ba đoạn code đáng chiếu lên

**Server chọn nhánh đăng ký hay đăng nhập** (`auth.go`):

```go
switch auth.Type {
case "REGISTER":
    if err := s.register(cred.User, cred.Pass); err != nil {
        protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: err.Error()})
        return
    }
case "AUTH":
    if !s.checkPassword(cred.User, cred.Pass) {
        protocol.WriteMsg(c, protocol.AuthResult{Type: "AUTH_FAIL", Reason: "Tài khoản hoặc mật khẩu không chính xác"})
        return
    }
default:
    // từ chối loại lạ
}
sess, err := s.reg.AddPending(cred.User, connID, c.RemoteAddr().String()) // trùng tên → "đang online ở máy khác"
```

**Mọi gói UDP đều phải qua GCM** (`conn.go`, `handlePacket`):

```go
// header (12 byte đầu) là AAD: ai sửa header thì Open báo lỗi
plain, err := c.aead.Open(nil, makeNonce(c.id, h.PacketNum, c.recvDir),
    p.raw[protocol.HeaderSize:], p.raw[:protocol.HeaderSize])
if err != nil {
    return // bỏ im lặng: gói giả mạo, gói hỏng, gói của kết nối khác
}
isNew := c.recvd.add(h.PacketNum) // packet_num đã thấy → gói trùng hoặc phát lại, không giao lại dữ liệu
if isNew && h.PacketNum == c.recvd.largest {
    c.peer = p.addr // chỉ gói mới nhất đã xác thực mới được đổi địa chỉ (client đổi mạng vẫn chạy)
}
```

**Ghi `users.json` an toàn** (`auth.go`, `saveUsersLocked`): ghi ra file tạm cùng thư mục, rồi `os.Rename` đè lên file cũ. Rename là thao tác nguyên tử nên server tắt giữa chừng không để lại file hỏng. Khóa `usersMu` bảo đảm hai người đăng ký cùng lúc không ghi đè nhau.

### 4.5 Luồng code khi bấm "Đăng nhập"

1. `app.js` gửi `POST /api/login {server, user, pass}` tới client local.
2. `webui`: `handleConn` → `readRequest` (tự đọc request line, header, thân) → `route` → `apiLogin`.
3. `client.Login` → `login` (nếu server là `auto` thì `Discover` nghe cổng 9002) → `dial` → `handshake`.
4. TCP: HELLO → KEYX → `DeriveKeys` → AUTH (mã hóa bằng `k_tcp`).
5. Server `handleHandshake`: giải mã → `checkPassword` → `AddPending` → `mux.Accept` → `AttachEndpoint` (khởi động `writeLoop`, `serveSession`, `watchPeer`) → AUTH_OK.
6. Client mở socket UDP, `transport.Dial` gửi PING. Server nhận gói đầu, biết IP:port; `watchPeer` ghi log `active` và phát danh sách user cho mọi người.
7. Client `listenLoop` nhận tin `users` → `EventLog` → `app.js` poll `/api/events` thấy và vẽ danh sách.

### 4.6 Luồng một tin nhắn riêng

Trình duyệt A → `POST /api/send {to: bob}` → `SendDM` cấp `seq`, mở stream ưu tiên cao → transport đóng gói, mã hóa, gửi UDP → server `readLoop` → `handlePacket` → ghép stream → `serveSession` → `onMessageData` (SeqBuffer, **ghi đè `From` = tên đã xác thực**) → `routeMessage` → `sendCh` của bob → `writeLoop` đánh `seq` theo dãy của bob → UDP → `listenLoop` của bob → `SeqBuffer` → `EventLog` → trình duyệt bob poll thấy.

### 4.7 Luồng gửi file

1. Client đọc file **2 lượt**: lượt 1 tính SHA-256 (vì meta chứa hash phải đi trước dữ liệu), lượt 2 gửi qua stream ưu tiên thấp.
2. Server đọc header, xác định người nhận, rồi **chuyển tiếp theo kiểu pipeline**: nhận mẩu nào chuyển mẩu đó, không giữ cả file trong RAM.
3. Bên nhận ghi đĩa và băm cùng lúc. Tên file qua `safeName` (chặn `../`), trùng tên thì thêm `(1)`.
4. Kết thúc: so số byte và SHA-256. Sai thì xóa file, báo lỗi.
5. Thanh tiến trình ở cả hai đầu, tối đa 5 cập nhật mỗi giây.

---

## 5. Kịch bản demo đầy đủ

### 5.1 Chuẩn bị trước buổi báo cáo

- [ ] Tắt server và client cũ đang chạy. Build lại bản mới nhất:
  ```bash
  go build -o bin/server ./cmd/server
  go build -o bin/client ./cmd/client
  ```
- [ ] Sao lưu tài khoản, vì demo đăng ký sẽ ghi vào `users.json`:
  ```bash
  cp users.json users.json.bak      # sau demo: mv users.json.bak users.json
  ```
- [ ] Chạy test trước một lần cho chắc. Mất khoảng 30 s nên đừng chạy lúc đang demo nếu thời gian ít:
  ```bash
  go vet ./... && go test -race ./...
  ```
- [ ] Tạo file mẫu để gửi:
  ```bash
  dd if=/dev/urandom of=demo-50m.bin bs=1m count=50
  echo "Xin chào thầy" > demo.txt
  ```
- [ ] Mở Wireshark, chọn interface **Loopback: lo0** (macOS) hoặc **Adapter for loopback traffic capture** (Windows, cần Npcap). Bộ lọc: `tcp.port == 9000 || udp.port == 9001`.
- [ ] Mở sẵn 4 terminal và 3 cửa sổ trình duyệt. Tăng cỡ chữ terminal để thầy nhìn rõ.
- [ ] Tài khoản có sẵn: `alice`, `bob`, `carol` mật khẩu `123456`; `phan` mật khẩu `654321`.
- [ ] Chọn trước tên sẽ đăng ký lúc demo, ví dụ `demo01`, và kiểm tra nó chưa có trong `users.json`.

### 5.2 Các bước demo

Mỗi bước có: làm gì, nói gì, cần thấy gì.

**Bước 1. Bật server** (terminal 1)

```bash
./bin/server
```

Cần thấy:

```
[Server] UDP transport running on :9001 (loss 0%, jitter 0s)
[Server] TCP Handshake server running on :9000
[Discovery] Broadcasting "COLLAB_SERVER 192.168.x.x 9000 9001" on UDP :9002 every 2s
```

Nói: "Server mở TCP 9000 cho handshake, một socket UDP 9001 dùng chung cho mọi client, và phát broadcast 9002 để client tự tìm."

**Bước 2. Bật 3 client không kèm tài khoản** (terminal 2, 3, 4)

```bash
./bin/client -web 127.0.0.1:8081
./bin/client -web 127.0.0.1:8082
./bin/client -web 127.0.0.1:8083
```

Mở http://127.0.0.1:8081, :8082, :8083. Cả ba hiện trang đăng nhập.

Nói: "Mỗi client là một process trên máy người dùng. Giao diện là HTTP server tự viết trên socket TCP, chỉ nghe 127.0.0.1."

**Bước 3. Đăng ký tài khoản mới** (cửa sổ 8081, tab Đăng ký)

1. Gõ mật khẩu nhập lại khác nhau → hiện "Hai mật khẩu không khớp" (kiểm tra ở trình duyệt).
2. Gõ tên `alice` → "Tên đăng nhập đã tồn tại" (do **server** trả về).
3. Gõ tên `demo01`, mật khẩu `secret1` hai lần → vào thẳng phòng chat.

Cần thấy ở server: `Registered new user 'demo01' from 127.0.0.1:...` rồi `authenticated`, `active: TCP ..., UDP ...`.

Chứng minh đã lưu và không lưu mật khẩu rõ:

```bash
grep -A3 '"demo01"' users.json      # chỉ có salt và hash
```

Nói: "Đăng ký đi chung handshake với đăng nhập, chỉ khác message thứ ba là REGISTER. Mật khẩu vẫn được mã hóa bằng khóa trao đổi qua X25519. Server ghi file tạm rồi đổi tên để không bao giờ làm hỏng users.json."

**Bước 4. Đăng nhập: sai, đúng, trùng**

1. Cửa sổ 8082: `alice` với mật khẩu sai → "Tài khoản hoặc mật khẩu không chính xác".
2. Thử user không tồn tại → **cùng thông báo đó**. Giải thích: không lộ user nào tồn tại.
3. `alice` / `123456` → vào được.
4. Cửa sổ 8083: lại `alice` → "Tài khoản đang online ở máy khác". Sau đó đăng nhập `bob`.

**Bước 5. Danh sách online và IP:port**

Chỉ cột trái: mỗi tên kèm IP:port UDP. Chỉ log server: `User 'alice' active: TCP 127.0.0.1:52387, UDP 127.0.0.1:58266`.

Nói: "Server theo dõi user, IP và cổng: địa chỉ TCP lúc handshake và địa chỉ UDP thật lấy từ gói đầu tiên đã xác thực."

**Bước 6. Chat nhóm và nhắn riêng**

1. Tab Chung: gửi vài tin từ từng cửa sổ, đúng thứ tự ở mọi nơi.
2. Bấm tên `bob` để mở tab riêng, gửi DM. Chỉ bob thấy, `demo01` không thấy.
3. Gửi DM tới người vừa thoát → lỗi "hiện không online".

**Bước 7. Gửi file và chứng minh không HoL blocking**

1. Ở tab riêng với bob, bấm nút **+**, chọn `demo-50m.bin`. Thanh tiến trình chạy ở cả hai bên.
2. **Trong lúc file đang chạy**, gửi một tin chat → tin tới ngay, không chờ file.
3. File xong: chỉ đường dẫn trong `downloads/`, rồi kiểm tra:
   ```bash
   shasum -a 256 demo-50m.bin downloads/demo-50m.bin     # Windows: certutil -hashfile <file> SHA256
   ```

Nói: "Chat và file là các stream khác nhau trong cùng kết nối UDP. Chat nằm ở hàng ưu tiên cao. Đo được chat đến sau khoảng 100 ms trong lúc file 200 MB đang chạy, còn nếu chung hàng với file thì mất khoảng 1 s."

**Bước 8. Wireshark: dữ liệu đã được mã hóa**

1. Lọc `tcp.port == 9000`, chọn một kết nối, Follow TCP Stream. Thấy JSON `HELLO`, `KEYX`, rồi `{"type":"AUTH","nonce":"...","sealed":"..."}`: chỉ có base64, **không thấy mật khẩu**.
2. Lọc `udp.port == 9001`. Mỗi gói bắt đầu bằng `c0 de 01`, sau đó `conn_id` và `packet_num` đọc được, phần còn lại là byte ngẫu nhiên.

Nói: "Header gửi rõ vì server cần conn_id để chọn khóa, nhưng header là AAD của GCM nên không sửa được."

**Bước 9. Mất kết nối và phục hồi**

1. Ctrl+C client của bob → các cửa sổ khác thấy bob biến mất gần như ngay (client gửi CLOSE).
2. Chạy lại bob, đăng nhập. Rồi `kill -9 <pid>` → không có CLOSE, các cửa sổ khác thấy bob biến mất sau khoảng 10 s (idle timeout).
   ```bash
   pgrep -fl "bin/client"        # tìm pid
   ```
3. Ctrl+C server → thanh trạng thái client chuyển 🟡 reconnecting. Bật lại server → khoảng 1 s sau 🟢 connected, tự động, không phải đăng nhập lại.

**Bước 10. Mạng xấu: mất gói và jitter**

```bash
./bin/server -loss 0.1 -jitter 30ms                     # server mất 10% gói chiều gửi
./bin/client -web 127.0.0.1:8081 -loss 0.1 -jitter 30ms # client mất 10% chiều gửi
```

1. Gửi liên tục nhiều tin → nhận đủ, đúng thứ tự. Thanh trạng thái hiện RTT khoảng 40 ms và % gói mất.
2. Gửi file nhỏ → SHA-256 vẫn khớp.
3. Nếu thầy hỏi tốc độ chậm: CUBIC coi mỗi lần mất là tắc nghẽn nên giảm tốc. Chạy lại với `-cc fixed` để thấy tốc độ cao hơn nhiều.

**Bước 11. Đăng xuất và đổi tài khoản**

Bấm **Đăng xuất** ở cửa sổ `demo01` → về trang đăng nhập; người khác thấy `demo01` rời đi ngay. Đăng nhập `carol` trên cùng cửa sổ → giao diện sạch, không còn tin của phiên trước.

**Bước 12 (nếu còn thời gian). Tải 20 client**

```bash
./bin/server -loss 0.1                      # terminal 1
bash scripts/run_clients.sh 20              # terminal 2, 20 bot chat mỗi 1–3 s
```

Chỉ dòng `[Runtime] goroutines=86 heap=...MB online=20` in mỗi 10 s: số goroutine đứng yên, không rò rỉ.

**Bước 13 (tùy chọn). Tự tìm server trong LAN**

Ở trang đăng nhập, gõ Server là `auto`. Client nghe broadcast cổng 9002 và tìm ra server trong khoảng 40 ms. Nếu demo hai máy: mở firewall `9000/TCP`, `9001/UDP` trên máy server và `9002/UDP` chiều vào trên máy client.

### 5.3 Sau demo

```bash
mv users.json.bak users.json
rm -rf downloads demo-50m.bin demo.txt logs
```

---

## 6. Xác thực, phiên và cookie

Đây là phần thầy dễ hỏi sâu. Điều quan trọng nhất: **project không dùng cookie, và đó là một quyết định có lý do**. Đừng nói vòng vo, nói thẳng và giải thích.

### 6.1 Xác thực diễn ra ở đâu

| Thành phần | Xác thực bằng gì |
|---|---|
| Người dùng với server | Mật khẩu, gửi trong AUTH hoặc REGISTER đã mã hóa bằng `k_tcp` |
| Mỗi gói UDP sau đó | Tag AES-GCM với khóa `k_udp` riêng của kết nối. Ai không có khóa thì không tạo được gói hợp lệ |
| Trình duyệt với client local | Không có. Web UI chỉ nghe `127.0.0.1`, mỗi process client là đúng một người dùng |

### 6.2 "Phiên" của hệ thống là gì

- Ở **server**, phiên là một `Session` trong `Registry`, gắn với `conn_id` và khóa `k_udp` của kết nối đó.
- `conn_id` nằm rõ trong header, nhưng biết `conn_id` không giả mạo được gì: gói phải có tag GCM đúng, mà tag cần `k_udp`. Khóa này chưa từng đi trên mạng.
- So với cookie: cookie là **bearer token**, ai có chuỗi cookie là người đó. Phiên của nhóm giống phiên TLS hơn: mỗi gói đều được xác thực bằng khóa đối xứng, lấy trộm `conn_id` vô dụng.
- Phiên kết thúc khi nhận CLOSE, im lặng 10 s, hoặc người dùng đăng xuất. Đăng nhập lại là handshake mới, khóa mới.

### 6.3 Vì sao không cần cookie

- Cookie giải quyết bài toán: **server HTTP dùng chung cho nhiều người** cần biết request nào của ai.
- Ở đây HTTP server **không phải server dùng chung**. Mỗi người chạy một process client riêng trên máy mình, HTTP server của process đó chỉ phục vụ chính người đó qua `127.0.0.1`. Process client đã giữ sẵn trạng thái đăng nhập (`loggedIn`, user, kết nối UDP).
- Server trung tâm không nói HTTP với ai. Nó nói giao thức của nhóm qua UDP, nơi mỗi gói đã được xác thực bằng GCM.
- Trình duyệt chỉ là màn hình hiển thị cho process client. Hỏi `/api/status` là biết đã đăng nhập chưa (`logged_in`).

### 6.4 Điểm yếu cần thừa nhận nếu bị hỏi

- **Không kiểm tra ai gọi API local.** Bất kỳ process nào trên cùng máy đều gọi được `127.0.0.1:8081/api/send`.
- **CSRF**: một trang web độc hại mở trên cùng máy có thể gửi `POST` tới `http://127.0.0.1:8081/api/send`. Server không kiểm `Origin`, cũng không có token.
- Mật khẩu được giữ trong RAM của process client để tự kết nối lại khi rớt mạng. Đăng xuất thì xóa.
- Hash mật khẩu là SHA-256 một vòng, nhanh nên dễ brute-force nếu lộ file.
- Không xác thực server (không có chứng chỉ), nên về lý thuyết có thể bị man-in-the-middle ở bước trao khóa.
- Đăng ký chưa giới hạn số lần, và thông báo "tên đã tồn tại" cho phép dò tên.

### 6.5 Nếu thầy hỏi "muốn thêm cookie thì làm thế nào?"

Trả lời theo các bước:

1. `POST /api/login` thành công thì sinh chuỗi ngẫu nhiên 32 byte bằng `crypto/rand`, lưu trong process client.
2. Trả header: `Set-Cookie: sid=<chuỗi>; HttpOnly; SameSite=Strict; Path=/`. Không đặt `Secure` vì đây là HTTP trên localhost.
3. Mọi `/api/*` (trừ login, register) đọc header `Cookie`, so `sid` bằng so sánh thời gian hằng. Sai thì trả `401`.
4. Đăng xuất thì xóa `sid` và trả `Set-Cookie: sid=; Max-Age=0`.
5. `SameSite=Strict` chặn được CSRF từ trang lạ. `HttpOnly` chặn script đọc cookie.

Bẫy cần biết: **cookie không phân biệt cổng**. Client A ở `127.0.0.1:8081` và client B ở `127.0.0.1:8082` dùng chung cookie trên một trình duyệt. Muốn chạy nhiều client trên một máy thì tên cookie phải kèm cổng, ví dụ `sid_8081`.

Cách nhẹ hơn để chống CSRF: kiểm header `Origin` (hoặc `Host`) phải là đúng `http://127.0.0.1:8081`, và bắt buộc `Content-Type: application/json` để trình duyệt phải gửi preflight.

### 6.6 Nếu thầy hỏi "có cơ chế giống cookie ở tầng giao thức không?"

Hiện chưa có. Khi rớt mạng, client dùng lại mật khẩu trong RAM để handshake lại. Hướng cải tiến giống TLS session ticket: AUTH_OK trả kèm một **resume token** ngẫu nhiên. Lần kết nối lại, client gửi token thay vì mật khẩu, server tra token rồi cấp phiên mới. Như vậy client không cần giữ mật khẩu.

### 6.7 Các tấn công và cách hệ thống chống

| Tấn công | Kết quả |
|---|---|
| Nghe lén mật khẩu | Không được: AUTH mã hóa bằng `k_tcp` |
| Sửa nội dung hay header gói UDP | GCM báo lỗi, gói bị bỏ im lặng |
| Gửi gói giả với `conn_id` đoán được | Không có `k_udp` nên tag sai, bị bỏ |
| Phát lại gói cũ (replay) | `packet_num` đã thấy thì không giao lại dữ liệu; gói cũ không làm đổi địa chỉ peer |
| Giả danh trong tin nhắn (`from: "admin"`) | Server ghi đè `From` bằng tên đã xác thực của kết nối |
| Đăng nhập cùng tài khoản ở 2 nơi | `AddPending` kiểm tra và thêm trong cùng một lần khóa: AUTH_FAIL "đang online ở máy khác" |
| Dò user bằng đăng nhập | User lạ và sai mật khẩu cùng một thông báo |
| Gửi độ dài message TCP 4 GB | Từ chối message lớn hơn 1 MB |
| Mở TCP rồi im lặng | Deadline 10 s cho handshake |
| Tên file `../../etc/passwd` | `safeName` chỉ giữ `passwd` |
| XSS qua nội dung chat | `esc()` trong `app.js` escape HTML trước khi hiển thị |
| Man-in-the-middle khi trao khóa | **Không chống được**: chưa xác thực server |

---

## 7. Bộ câu hỏi luyện tập

Mỗi câu có gợi ý trả lời ngắn. Tập nói thành lời, đừng đọc.

### 7.1 Lý thuyết TCP / UDP

1. **TCP bắt tay 3 bước để làm gì? Sao không 2 bước?**
   Để hai bên thống nhất số thứ tự ban đầu của cả hai chiều và xác nhận cả hai chiều đều thông. Với 2 bước, server không biết client đã nhận được SYN-ACK chưa, và một SYN cũ bị trễ có thể mở kết nối ma.
2. **TIME_WAIT là gì?**
   Bên đóng trước chờ khoảng 2 lần MSL để ACK cuối kịp tới nơi và để gói cũ của kết nối này không lẫn vào kết nối mới cùng cặp cổng.
3. **Head-of-Line blocking là gì? Project giải thế nào?**
   TCP giao byte đúng thứ tự, nên một gói mất làm mọi dữ liệu phía sau phải chờ, kể cả dữ liệu khác loại. Nhóm cho mỗi tin và mỗi file là một stream riêng; mất gói ở stream này không chặn stream khác. Cộng thêm hàng ưu tiên chat trên file ở phía gửi.
4. **Flow control khác congestion control thế nào?**
   Flow control bảo vệ **bên nhận** khỏi bị tràn bộ đệm (cửa sổ nhận 1 MB, báo trong ACK). Congestion control bảo vệ **mạng** khỏi tắc nghẽn (cwnd của CUBIC).
5. **Slow start, congestion avoidance là gì?**
   Slow start: cwnd tăng gấp đôi mỗi RTT tới ngưỡng ssthresh. Congestion avoidance: tăng chậm, Reno tăng 1 gói mỗi RTT, CUBIC tăng theo hàm bậc 3 của thời gian kể từ lần giảm gần nhất.
6. **Vì sao CUBIC chậm khi mất gói ngẫu nhiên 10%?**
   CUBIC coi mọi lần mất là tắc nghẽn và giảm cwnd còn 0,7. Mất ngẫu nhiên không phải do tắc nghẽn, nên CUBIC giảm oan liên tục. Đo được 0,11 MB/s với loss 10%, trong khi cửa sổ cố định đạt khoảng 1,7 MB/s. Giải pháp thật là thuật toán dựa trên đo băng thông như BBR.
7. **UDP không tin cậy, sao lại dùng để truyền file?**
   Vì nhóm tự xây độ tin cậy ở trên: SACK, gửi lại, ghép theo offset, kiểm SHA-256. Đổi lại được kiểm soát hoàn toàn: không HoL blocking, ưu tiên chat, chọn thuật toán tắc nghẽn.
8. **Vì sao gói tối đa 1200 byte?**
   Để không bị phân mảnh IP. IPv6 bảo đảm 1280 byte, trừ header IPv6 và UDP còn khoảng 1232; QUIC chọn 1200. Phân mảnh thì mất một mảnh là mất cả gói.
9. **TCP có checksum rồi, sao còn cần SHA-256 cho file?**
   Nguyên lý end-to-end: checksum và GCM chỉ bảo vệ từng chặng client–server. Lỗi trong server relay hay lỗi ghi đĩa chỉ phát hiện được bằng kiểm tra đầu cuối.
10. **Broadcast có đi qua router không?**
    Không. `255.255.255.255` chỉ trong LAN, nên tự tìm server chỉ chạy trong cùng mạng. Khác mạng thì nhập địa chỉ.

### 7.2 Thiết kế giao thức

11. **Vì sao handshake dùng TCP mà dữ liệu dùng UDP?**
    Handshake chỉ 4 message, cần tin cậy và đúng thứ tự, chạy một lần, nên TCP vừa đủ và đơn giản. Dữ liệu chạy lâu, cần tránh HoL và tự kiểm soát, nên dùng UDP với giao thức tự viết.
12. **Sao không làm hết trên UDP như QUIC?**
    Làm được nhưng phải tự viết thêm tin cậy cho chính handshake. Với phạm vi bài tập, TCP cho handshake gọn hơn mà vẫn thể hiện đủ các cơ chế ở phần dữ liệu.
13. **`conn_id` để làm gì? Sao không phân biệt client bằng IP:port?**
    Server dùng một socket UDP chung. `conn_id` cho biết gói thuộc kết nối nào để chọn đúng khóa. Nhờ vậy nhiều client sau cùng một NAT vẫn phân biệt được, và client đổi mạng (Wi-Fi sang 4G) vẫn giữ kết nối.
14. **Vì sao gửi lại phải dùng `packet_num` mới?**
    Để nonce GCM không bao giờ lặp, và để đo RTT không mập mờ: ACK về biết chắc là của lần gửi nào (vấn đề Karn của TCP).
15. **ACK frame có cần ACK không?**
    Không, nếu không sẽ thành vòng lặp ACK vô hạn. Gói chỉ chứa ACK không được đưa vào danh sách chờ ACK.
16. **Một gói mất được phát hiện thế nào?**
    Ba cách: theo số (đã có gói số lớn hơn 3 trở lên được ACK), theo thời gian (gói sau nó đã được ACK mà nó quá 9/8 RTT chưa có), hoặc hết RTO.
17. **RTO tính ra sao? Sao không có sàn 1 s như TCP?**
    `SRTT + max(4·RTTVAR, 1 ms) + 10 ms`, nhân đôi mỗi lần timeout liên tiếp, trần 5 s. TCP cần sàn vì bên nhận được trì hoãn ACK tới 200–500 ms; bên nhận của nhóm ACK ngay nên không cần.
18. **PING đầu tiên của client mất thì sao?**
    Client chưa nghe được server thì gửi lại PING sau 200, 400, 800 ms…, không chờ 2 s.
19. **Một bên chết đột ngột thì bên kia biết khi nào?**
    Không nhận gói hợp lệ nào trong 10 s thì coi kết nối chết. Rảnh 2 s là có PING nên kết nối sống không bao giờ im 10 s.
20. **Vì sao CLOSE gửi 3 bản?**
    CLOSE không được ACK. Mất 10% thì gửi 1 bản sẽ có khoảng 1/10 lần bên kia phải chờ 10 s. Gửi 3 bản, xác suất mất cả 3 là 1/1000.
21. **Mỗi tin là một stream thì thứ tự tin giữ thế nào?**
    Tầng ứng dụng đánh `seq`. Bên nhận dùng `SeqBuffer` giữ tin tới sớm cho tới khi tin trước tới. Server đánh lại `seq` riêng cho từng người nhận vì mỗi người nhận một tập tin khác nhau.
22. **Hàng đợi gửi của server đầy thì sao?**
    Hàng đợi 4096 tin mỗi người nhận. Đầy nghĩa là client đó quá chậm hoặc treo: server ngắt riêng client đó, client tự kết nối lại. Không bỏ tin, vì bỏ tin sẽ để lại lỗ hổng `seq`.

### 7.3 Mật mã và bảo mật

23. **ECDH hoạt động thế nào, vì sao nghe lén không lấy được khóa?**
    Mỗi bên có khóa bí mật a, b và gửi khóa công khai A, B. Bí mật chung là X25519(a, B) = X25519(b, A). Biết A và B mà không có a hay b thì không tính được (bài toán logarit rời rạc trên đường cong elliptic).
24. **Vì sao cần 2 khóa `k_tcp` và `k_udp`?**
    Hai kênh sinh nonce khác nhau: TCP dùng nonce ngẫu nhiên, UDP dùng nonce từ header. Dùng chung một khóa thì có rủi ro trùng nonce giữa hai kênh.
25. **AAD là gì, sao header lại là AAD?**
    Dữ liệu không mã hóa nhưng nằm trong tag. Header phải để rõ (cần `conn_id` để chọn khóa) nhưng không được sửa: đổi `packet_num` hay `conn_id` là giải mã thất bại.
26. **Có forward secrecy không?**
    Có. Khóa X25519 sinh mới mỗi lần handshake và không lưu, nên lộ khóa một phiên không giải mã được phiên khác.
27. **Lỗ hổng lớn nhất của phần bảo mật?**
    Không xác thực server, nên có thể bị man-in-the-middle khi trao khóa. Sửa bằng cách ký `server_pub` bằng khóa dài hạn của server mà client biết trước, giống chứng chỉ TLS.
28. **Vì sao không dùng `crypto/tls` cho nhanh?**
    Mục tiêu là tự hiện thực để hiểu cơ chế, và TLS là thư viện mức cao. Nhóm chỉ dùng các khối cơ bản: X25519, AES-GCM, SHA-256.
29. **Hash mật khẩu bằng SHA-256 có ổn không?**
    Chưa tốt: SHA-256 rất nhanh nên brute-force được nếu lộ file. Có salt nên chống được bảng tra sẵn. Cải tiến: dùng PBKDF2 hoặc Argon2. Go từ bản 1.24 có `crypto/pbkdf2` trong thư viện chuẩn.

### 7.4 Hiện thực và Go

30. **"Không dùng thư viện mạng mức cao": `net.Listen`, `net.Dial` có vi phạm không?**
    Không. Gói `net` của Go là lớp mỏng trên socket của hệ điều hành (socket, bind, listen, accept, sendto, recvfrom), tương đương `socket` của Python. Nhóm không dùng `net/http`, WebSocket hay framework nào.
31. **Goroutine `run()` của mỗi kết nối để làm gì?**
    Một goroutine sở hữu toàn bộ trạng thái kết nối, mọi thứ khác gửi lệnh qua channel. Không có hai goroutine cùng sửa trạng thái nên không cần mutex, không có race.
32. **Server có bao nhiêu goroutine với 20 client?**
    86, đứng yên suốt bài test 60 s: khoảng 4 cho mỗi client cộng các goroutine nền.
33. **Làm sao biết không có race condition?**
    Chạy `go test -race ./...` và test tải 20 client. Race detector của Go phát hiện truy cập đồng thời không đồng bộ khi chạy.
34. **Server relay file có giữ cả file trong RAM không?**
    Không. Nhận mẩu nào chuyển mẩu đó qua channel; heap server không tăng theo kích thước file (đã thử 200 MB).
35. **HTTP server tự viết xử lý request thế nào?**
    Đọc dòng request, đọc header tới dòng trống, lấy `Content-Length` rồi đọc đúng chừng đó byte thân. Trả response với `Content-Length` và `Connection: close`, mỗi kết nối một request. Có giới hạn dòng header 8 KB, deadline, và `recover` để một request lỗi không làm sập client.
36. **Trình duyệt nhận tin mới bằng cách nào khi không có WebSocket?**
    Poll `/api/events?since=N` mỗi 400 ms. Mỗi sự kiện có số thứ tự tăng dần nên tải lại trang hay mở nhiều tab không mất hay trùng tin.
37. **Upload file qua web làm sao, có dùng multipart không?**
    Không multipart. JS gửi thẳng nội dung file làm thân `POST /api/upload?to=&name=`. Client ghi ra file tạm rồi gửi đi trong goroutine riêng.
38. **Đăng xuất xong vòng kết nối lại có tự đăng nhập lại không?**
    Không. Mỗi lần đăng nhập hay đăng xuất tăng một số thế hệ `gen`. Vòng nhận và vòng kết nối lại nhớ `gen` lúc bắt đầu, thấy khác thì dừng. Test `TestRegisterLoginLogout` kiểm tra điều này.

### 7.5 Câu hỏi về demo

39. **Sao tắt client bằng Ctrl+C thì người khác thấy ngay, còn `kill -9` thì 10 s?**
    Ctrl+C được bắt và client gửi CLOSE. `kill -9` giết process ngay, không gửi được gì, nên server chỉ biết khi hết idle timeout 10 s.
40. **Tắt server rồi bật lại, client làm gì?**
    Chuyển Reconnecting, thử handshake lại sau 1, 2, 4… s, tối đa 30 s. Server bật lại thì vào được ở lần thử kế tiếp, khoảng 1 s.
41. **Trên Wireshark thấy được gì?**
    TCP 9000: JSON HELLO, KEYX, AUTH; trong AUTH chỉ có base64 của `nonce` và `sealed`. UDP 9001: thấy `c0 de 01`, `conn_id`, `packet_num`, còn lại là byte ngẫu nhiên.
42. **Wireshark thấy chữ "REGISTER", vậy người nghe lén biết người đó đang đăng ký?**
    Đúng, trường `type` để rõ. Họ biết đó là đăng ký nhưng không biết tên hay mật khẩu. Muốn giấu thì đưa loại yêu cầu vào trong `sealed`.
43. **Mất gói 10% mà chat vẫn đúng thứ tự, nhờ cái gì?**
    Transport gửi lại phần mất; `SeqBuffer` ở tầng ứng dụng sắp lại thứ tự giữa các tin. Đo 50/50 tin đúng thứ tự với loss 10% và 20%.
44. **Hai người gửi file cùng lúc cho một người thì sao?**
    Hai stream khác nhau, ghép xen kẽ trong cùng kết nối, cả hai đều tới và kiểm SHA-256 riêng.
45. **Tên file nhận bị trùng thì sao?**
    Thêm `(1)`, `(2)`. Mở file bằng cờ tạo mới độc quyền nên không ghi đè file cũ.

### 7.6 Câu hỏi về xác thực và cookie

46. **Hệ thống xác thực người dùng thế nào?**
    Tên và mật khẩu trong message AUTH, mã hóa bằng `k_tcp`. Server so `sha256(salt ‖ mật khẩu)` với hash trong `users.json`. Sau đó mọi gói UDP được xác thực bằng tag GCM với khóa `k_udp` riêng của phiên.
47. **Web có dùng cookie hay session không?**
    Không dùng cookie. Web UI chỉ phục vụ một người dùng trên `127.0.0.1` của chính máy đó; process client đã giữ trạng thái đăng nhập. Phiên thật nằm giữa process client và server, được bảo vệ bằng khóa GCM chứ không bằng cookie. Chi tiết ở [phần 6.3](#63-vì-sao-không-cần-cookie).
48. **Vậy server biết request nào của ai bằng cách nào?**
    Server không nhận HTTP. Nó nhận gói UDP, tra `conn_id` để chọn khóa; giải mã được nghĩa là gói đến từ đúng người đã đăng nhập phiên đó.
49. **Người khác đánh cắp `conn_id` có giả mạo được không?**
    Không, vì cần `k_udp` để tạo tag hợp lệ. Khác với cookie: lấy được cookie là dùng được ngay.
50. **Ai đó mở `127.0.0.1:8081` trên máy bạn thì sao?**
    Họ dùng được tài khoản đang đăng nhập trên process đó. Giống ai đó ngồi vào máy bạn khi ứng dụng chat đang mở. Port chỉ nghe localhost nên máy khác không vào được.
51. **Nếu thêm cookie thì sao?**
    Trình bày theo [phần 6.5](#65-nếu-thầy-hỏi-muốn-thêm-cookie-thì-làm-thế-nào): sid ngẫu nhiên, `HttpOnly`, `SameSite=Strict`, kiểm mọi `/api`, xóa khi đăng xuất. Nhớ nói cookie không phân biệt cổng.
52. **CSRF có xảy ra không?**
    Có thể. Một trang độc hại mở trên cùng máy có thể POST tới API local. Chống bằng kiểm `Origin` hoặc cookie `SameSite=Strict` cộng token.
53. **Mật khẩu có lưu trên client không?**
    Chỉ trong RAM của process để tự kết nối lại; không ghi đĩa; đăng xuất thì xóa. Cải tiến: resume token giống TLS session ticket.
54. **Đăng ký có an toàn khi nhiều người đăng ký cùng lúc không?**
    Có khóa ghi bảo vệ bảng tài khoản và việc ghi file; ghi file tạm rồi đổi tên nên không hỏng file. Test cho 20 goroutine đăng ký đồng thời rồi đọc lại file thấy đủ 20.
55. **Sao đăng nhập thì giấu "user không tồn tại" mà đăng ký lại báo "tên đã tồn tại"?**
    Đăng ký buộc phải báo để người dùng chọn tên khác. Đó là đánh đổi mọi hệ thống đều có; cách giảm thiểu là giới hạn số lần thử.
56. **Đăng nhập ở hai máy cùng lúc được không?**
    Không. `AddPending` kiểm tra tên đã online chưa và thêm vào trong cùng một lần giữ khóa, nên hai yêu cầu đến cùng lúc cũng chỉ một cái thành công.

### 7.7 Câu hỏi "bẫy" và mở rộng

57. **Giao thức này khác QUIC chỗ nào?**
    Cùng ý tưởng: UDP, `conn_id`, `packet_num` không lặp, ACK theo khoảng, phát hiện mất theo RFC 9002, nhiều stream. Khác: handshake qua TCP thay vì trong UDP, không có chứng chỉ, ít tính năng (không 0-RTT, không giới hạn luồng theo từng stream).
58. **`packet_num` 32 bit tràn thì sao?**
    Chưa xử lý. Cần khoảng 4,3 tỉ gói (vài TB) trên một kết nối mới tràn, thực tế không chạm tới. Cách đúng là đổi khóa hoặc đóng kết nối trước khi tràn.
59. **Muốn chạy qua Internet cần gì?**
    Server có IP công khai hoặc mở cổng 9000/TCP, 9001/UDP. Discovery không dùng được (broadcast chỉ trong LAN). Nên thêm xác thực server.
60. **Nếu có thêm thời gian, cải tiến gì trước?**
    Xác thực server chống MITM; hash mật khẩu chậm (PBKDF2 hoặc Argon2); BBR thay CUBIC; chống CSRF cho Web UI; resume token để không giữ mật khẩu.

---

## 8. Số liệu cần nhớ

Đo trên macOS Apple Silicon, loopback, ngày 13/11/2026.

| Phép đo | Kết quả |
|---|---|
| File 10 MB, loss 10% hai chiều | 7,2 s (1,46 MB/s), SHA-256 khớp |
| File 200 MB, loss 10% | 133 s (1,51 MB/s), SHA-256 khớp, RAM server không tăng |
| Chat gửi khi đang truyền file 200 MB | tới sau 108 ms |
| Độ trễ chat khi đang gửi file (test HoL, p50) | 23,6 ms (rảnh: 21,4 ms); chung hàng với file: 972 ms |
| 50 chat với loss 10% / 20%, jitter 30 ms | 50/50 đúng thứ tự, RTT khoảng 42 ms |
| Ctrl+C client hoặc server | bên kia biết sau ≤ 0,06 s |
| `kill -9` client | phát hiện sau khoảng 10 s |
| Server bật lại | client vào lại sau 1,0 s |
| 20 client, 60 s | 86 goroutine cố định, heap 12–16 MB, CPU 4,4% |
| CUBIC qua nút cổ chai 2 MB/s, không mất ngẫu nhiên | 2,01 MB/s; cửa sổ cố định 64 KB chỉ 1,47 MB/s |
| CUBIC với loss 10% | 0,11 MB/s; cửa sổ cố định 1,69 MB/s |
| Tự tìm server, 3 client cùng lúc | khoảng 40 ms |
| Đăng xuất | người khác thấy sau khoảng 20 ms |

Hằng số giao thức:

| Hằng số | Giá trị |
|---|---|
| Gói UDP tối đa | 1200 byte |
| Header | 12 byte; tag GCM 16 byte |
| Dữ liệu tối đa mỗi STREAM frame | 1000 byte |
| Số khoảng tối đa trong ACK | 8 |
| PING khi rảnh / idle timeout | 2 s / 10 s |
| RTO ban đầu / trần | 500 ms / 5 s |
| Ngưỡng mất theo số | 3 gói |
| Cửa sổ nhận | 1 MB |
| CUBIC | C = 0,4; β = 0,7; cwnd đầu 10 gói |
| Hàng đợi gửi mỗi session | 4096 tin |
| Backoff kết nối lại | 1, 2, 4… tối đa 30 s |
| Deadline handshake | 10 s; message TCP tối đa 1 MB |
| Tên đăng ký / mật khẩu | 3–20 ký tự `[A-Za-z0-9_]` / 6–64 ký tự |

---

## 9. Sự cố khi demo và cách xử lý

| Hiện tượng | Nguyên nhân thường gặp | Xử lý |
|---|---|---|
| `bind: address already in use` | Server hoặc client cũ còn chạy | `lsof -i :9000` hoặc `lsof -i :8081`, tắt process đó |
| Trang web hiện "Không kết nối được server" | Server chưa chạy hoặc sai địa chỉ ở ô Server | Kiểm tra terminal server, ô Server phải là `127.0.0.1:9000` |
| "Tài khoản đang online ở máy khác" khi đăng nhập lại ngay | Server còn giữ phiên cũ chưa hết 10 s (client cũ bị `kill -9`) | Chờ 10 s rồi thử lại |
| `auto` không tìm thấy server | Firewall chặn 9002/UDP hoặc khác mạng | Nhập thẳng `ip:9000` |
| Gửi file rất chậm khi chạy `-loss` | CUBIC giảm tốc vì mất gói ngẫu nhiên | Giải thích, rồi chạy lại với `-cc fixed` |
| Wireshark không thấy gói | Chọn sai interface | macOS chọn Loopback lo0; Windows cần Npcap loopback |
| `run_clients.sh: Permission denied` | Script chưa có quyền chạy | Chạy bằng `bash scripts/run_clients.sh 20` |
| Đăng ký báo "tên đã tồn tại" với tên định dùng | Đã đăng ký lúc tập dượt | Khôi phục `users.json.bak` hoặc dùng tên khác |
| Giao diện vẫn là bản cũ | Chạy binary cũ hoặc trình duyệt giữ trang cũ | Build lại, tải lại trang (server gửi `Cache-Control: no-store`) |

Câu chốt nếu thầy hỏi điều chưa làm: nói rõ "phần này nhóm chưa làm", nêu lý do, rồi nói sẽ làm thế nào. Thầy đánh giá cao việc biết giới hạn của hệ thống hơn là trả lời vòng vo.
