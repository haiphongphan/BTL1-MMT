### 3.1 Cổng

| Cổng | Giao thức | Mục đích |
|---|---|---|
| 9000 | TCP | Handshake, trao khóa, đăng nhập (đóng sau khi xong) |
| 9001 | UDP | Toàn bộ dữ liệu: message, file, ACK, heartbeat |
| 9002 | UDP broadcast | Server quảng bá `"COLLAB_SERVER <ip> <tcp_port> <udp_port>"` mỗi 2s; client `-server auto` nghe cổng này để tự tìm server (lấy IP từ địa chỉ nguồn của gói) |
| 8080+ | TCP (HTTP tự viết) | Web UI của từng client, chỉ bind `127.0.0.1` |

### 3.2 Handshake TCP (mỗi message: `[4 byte độ dài][JSON]`)

```
Client                                                                                    Server
  |------------------------------------HELLO {client_pub}---------------------------------->|  sinh khóa X25519, conn_id
  |<-----------------------------KEYX {server_pub, conn_id, udp_port}-----------------------|
  |   CẢ HAI: shared = ECDH; k_tcp = SHA256(shared‖"tcp"); k_udp = SHA256(shared‖"udp")     |
  |                                                                                         |        
  |---------------------AUTH {nonce, sealed(AES-GCM,k_tcp, {user,pass})}------------------->|  kiểm tra users.json
  |<-------------------AUTH_OK {users:[...]}  hoặc AUTH_FAIL{reason}------------------------|  tạo transport.Conn(conn_id, k_udp)
  |   ĐÓNG TCP                                                                              |
  |-----------------------UDP: gói đầu tiên (PING) mang conn_id---------------------------->|  server ghi lại IP:port UDP của client
```

### 3.3 Gói UDP

```
 0      2   3   4          8          12                               ≤1200
 ┌──────┬───┬───┬──────────┬──────────┬─────────────────────────────────┐
 │magic │ver│flg│ conn_id  │packet_num│ AES-GCM( frames ) + tag 16 byte │
 │0xC0DE│ 1 │   │ uint32   │ uint32   │                                 │
 └──────┴───┴───┴──────────┴──────────┴─────────────────────────────────┘
   Header 12 byte: gửi rõ, nhưng là AAD của GCM nên sửa header thì giải mã fail
```

- `packet_num` **tăng đơn điệu, không bao giờ dùng lại**, kể cả khi gửi lại: dữ liệu mất được đóng vào **gói mới với số mới** (giống QUIC). Nhờ vậy đo RTT không bị mập mờ.
- Nonce GCM (12 byte) = `conn_id(4) ‖ packet_num(4) ‖ direction(1) ‖ 000`. Byte `direction` (0 = client→server, 1 = server→client) đảm bảo hai chiều **không bao giờ trùng nonce**. Trùng nonce trong GCM là lỗi bảo mật nghiêm trọng.
- Tổng gói ≤ 1200 byte để tránh IP fragmentation, nên phần frames ≤ 1200 − 12 − 16 = **1172 byte**.

### 3.4 Frame (nằm trong phần đã mã hóa, một gói có thể chứa nhiều frame)

| Type | Tên    | Cấu trúc                                                        | Cần ACK?            |
| ---- | ------ | --------------------------------------------------------------- | ------------------- |
| 0x01 | STREAM | `type(1) flags(1, bit0=FIN) stream_id(4) offset(4) len(2) data` | Có                  |
| 0x02 | ACK    | `type(1) window(4) n(1) n × [start(4) end(4)]` (n ≤ 8)          | **Không**           |
| 0x03 | PING   | `type(1)`                                                       | Có (để giữ kết nối) |
| 0x04 | CLOSE  | `type(1) reason_len(1) reason`                                  | Không               |

Tất cả số nguyên đều **big-endian**. Số phía trên khung là offset byte tính từ đầu frame.

**STREAM (0x01)**: header 12 byte + data (≤ 1000 byte)

```
 0      1       2           6           10      12                 12+len
 ┌──────┬───────┬───────────┬───────────┬───────┬────────────────────┐
 │ type │ flags │ stream_id │  offset   │  len  │        data        │
 │ 0x01 │ bit0= │  uint32   │  uint32   │uint16 │     len byte       │
 │      │  FIN  │           │           │       │                    │
 └──────┴───────┴───────────┴───────────┴───────┴────────────────────┘
   flags: 7 6 5 4 3 2 1 0
          └─ dự phòng ─┘ └ FIN = 1: đây là đoạn cuối của stream
   offset: vị trí byte đầu tiên của data trong stream (bên nhận ghép lại theo offset)
```

**ACK (0x02)**: 6 byte + n × 8 byte (n ≤ 8, tối đa 70 byte)

```
 0      1           5     6           10          14          18
 ┌──────┬───────────┬─────┬───────────┬───────────┬───────────┬───────────┬─────
 │ type │  window   │  n  │  start_0  │   end_0   │  start_1  │   end_1   │ ...
 │ 0x02 │  uint32   │ u8  │  uint32   │  uint32   │  uint32   │  uint32   │
 └──────┴───────────┴─────┴───────────┴───────────┴───────────┴───────────┴─────
                          └──── range 0 ──────────┘└──── range 1 ──────────┘
   window: số byte bên nhận còn trống (flow control)
   range [start..end]: inclusive, sắp giảm dần; end_0 = packet_num lớn nhất đã nhận
```

Ví dụ: đã nhận gói 1–5, 7–9, 12

```
 ┌──────┬────────┬───┬────┬────┬───┬───┬───┬───┐
 │ 0x02 │ 48000  │ 3 │ 12 │ 12 │ 7 │ 9 │ 1 │ 5 │   (30 byte), thiếu 6, 10, 11
 └──────┴────────┴───┴────┴────┴───┴───┴───┴───┘
```

**PING (0x03)**: 1 byte

```
 0      1
 ┌──────┐
 │ type │
 │ 0x03 │
 └──────┘
```

**CLOSE (0x04)**: 2 byte + reason (≤ 255 byte)

```
 0      1            2                  2+reason_len
 ┌──────┬────────────┬────────────────────┐
 │ type │ reason_len │       reason       │
 │ 0x04 │     u8     │  UTF-8, reason_len │
 └──────┴────────────┴────────────────────┘
```

**Nhiều frame trong một gói**: frame xếp liền nhau, không có dấu phân cách. Bên nhận đọc `type` để biết độ dài của frame, cắt frame đó ra rồi đọc tiếp tới hết phần plaintext.

```
 ┌─────────── header 12 ──────────┐┌───────────────── plaintext sau khi giải mã (≤ 1172) ──────────────────┐┌ tag 16 ┐
 │magic│ver│flg│conn_id│packet_num ││ STREAM (12 + 1000) │ ACK (6 + 8n) │ PING (1) │                        ││        │
 └────────────────────────────────┘└───────────────────────────────────────────────────────────────────────┘└────────┘
```

- **stream_id**: client dùng số lẻ (1, 3, 5...), server dùng số chẵn (2, 4, 6...), nên hai bên không bao giờ đụng nhau.
- Mỗi STREAM frame mang tối đa **1000 byte data**, để còn chỗ ghép thêm 1 ACK frame (≤ 70 byte).
- **Gói chỉ chứa ACK thì không được ACK lại**, nếu không hai bên sẽ ACK nhau vô hạn.
- **CLOSE reason**: chuỗi UTF-8, tối đa **255 byte** (vì `reason_len` chỉ có 1 byte). Bên gửi tự cắt nếu dài hơn; nên dùng câu ngắn bằng tiếng Anh (`"timeout"`, `"server shutdown"`, `"duplicate login"`) để không cắt giữa ký tự nhiều byte.

### 3.5 Nội dung một stream (tầng app)

Byte đầu tiên của stream cho biết loại stream:

```
0x01 MESSAGE : [0x01][JSON AppMsg ...hết stream]
0x02 FILE    : [0x02][meta_len uint32][JSON FileMeta][bytes file ...hết stream]
```

> Meta được đặt **ngay trong stream file** chứ không gửi bằng message riêng. Lý do: giữa các stream **không có thứ tự**, nên nếu tách riêng thì data file có thể tới trước meta.

```go
type AppMsg struct {
	Type  string   `json:"type"`            // chat | dm | users | join | leave | error | file_done
	From  string   `json:"from,omitempty"`
	To    string   `json:"to,omitempty"`
	Text  string   `json:"text,omitempty"`
	Users []string `json:"users,omitempty"`
	Addrs map[string]string `json:"addrs,omitempty"` // kèm "users": user → "ip:port" UDP server đang dùng
	TS    int64    `json:"ts"`              // UnixMilli
	Seq   uint64   `json:"seq,omitempty"`   // số thứ tự theo kết nối, xem dưới
}

type FileMeta struct {
	ID     string `json:"id"`
	From   string `json:"from"`
	To     string `json:"to"`     // "" = gửi tất cả
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex, người nhận kiểm tra
}
```

**Thứ tự tin nhắn (`Seq`).** Mỗi tin là 1 stream riêng nên tin nào nhận đủ trước thì được giao trước. Khi mất gói,
tin #2 có thể tới trước tin #1. Để giữ đúng thứ tự mà vẫn không HoL ở tầng transport:

- Mỗi bên gửi đánh `Seq` = 1, 2, 3... cho từng **kết nối** (client → server; server đánh riêng cho từng người nhận,
  trong goroutine `writeLoop` duy nhất của người nhận đó). Kết nối lại thì đếm lại từ 1.
- Bên nhận dùng `protocol.SeqBuffer`: tin tới sớm được giữ lại cho tới khi các tin trước tới đủ; tin trùng bị bỏ;
  `Seq = 0` được giao ngay. Giữ quá 4096 tin (chỉ xảy ra khi có lỗi) thì bỏ qua chỗ trống để không treo.
- Chỉ áp dụng cho stream MESSAGE. File không cần: mỗi file là 1 stream, transport đã giao đúng thứ tự theo offset.
- Server **không bỏ tin** khi hàng đợi gửi của 1 người nhận đầy (bỏ tin sẽ để lại lỗ hổng `Seq`): hàng đợi chứa
  4096 tin, đầy thì ngắt client quá chậm đó (client tự kết nối lại).

### 3.6 State diagram

**Client** (`client.Status().State`, hiện trên thanh trạng thái Web UI):

```
                 -server auto
   ┌─────────────┐  nghe broadcast   ┌──────────────┐   AUTH_OK, Dial UDP,   ┌───────────────┐   Ctrl+C / /quit   ┌────────┐
   │ DISCOVERING │ ────────────────▶ │ HANDSHAKING  │ ─────────────────────▶ │   CONNECTED   │ ─────────────────▶ │ CLOSED │
   │  UDP :9002  │  (≤ 5 s, không    │  TCP :9000   │   gửi PING đầu         │ UDP, Web UI   │    gửi CLOSE       └────────┘
   └─────────────┘   thấy → thoát)   └──────────────┘                        └───────────────┘
                                        ▲      │ AUTH_FAIL / lỗi mạng               │ Done(): nhận CLOSE hoặc
                                        │      ▼                                    │ không nghe gì 10 s
                                     ┌──────────────────────────────┐               │
                                     │ RECONNECTING                 │ ◀─────────────┘
                                     │ chờ 1, 2, 4 … tối đa 30 s    │
                                     └──────────────────────────────┘
```

Lần đăng nhập đầu thất bại thì client thoát với thông báo lỗi; chỉ khi đã từng `CONNECTED` mới vào `RECONNECTING`.

**Session phía server:**

```
  AUTH_OK                    nhận gói UDP đầu tiên              Done(): CLOSE, idle 10 s,
 ─────────▶ PENDING ────────────────────────────────▶ ACTIVE ──────────────────────────────▶ REMOVED
            (có trong Registry,                      (biết ip:port UDP,          hoặc hàng đợi gửi đầy
             tên đã bị giữ)                           phát lại danh sách user)   (client quá chậm)
               │                                                                    ▲
               └──────────── không bao giờ gửi UDP: idle timeout 10 s ──────────────┘
```

**Kết nối transport** (`transport.Stats.State`): `waiting` (server chưa biết địa chỉ client) → `connected` → `closed`.


### 3.7 Tham số mặc định

| Tham số | Giá trị | Ghi chú |
|---|---|---|
| Đồng hồ nội bộ mỗi kết nối | 10 ms khi có gói chờ ACK, 100 ms khi rảnh | Tick 10 ms cho kết nối rảnh tốn CPU vô ích (20 client rảnh: 43% CPU → vài %) |
| Heartbeat (PING khi rảnh) | 2 s | |
| PING đầu sau `Dial` | gửi lại sau 200 ms, 400 ms, 800 ms... (tối đa 2 s) cho tới khi nhận được gói đầu tiên từ server | Server chỉ biết địa chỉ UDP của client khi nhận được gói đầu tiên |
| Idle timeout (coi như chết) | 10 s | |
| Mất gói theo số đếm | gói bị "vượt mặt" bởi gói số ≥ pn+3 đã được ACK | RFC 9002 kPacketThreshold |
| Mất gói theo thời gian | gói đã bị vượt mặt và chưa được ACK sau 9/8 · max(SRTT, RTT mới nhất), tối thiểu 1 ms | RFC 9002 kTimeThreshold; cần khi cwnd < 4 gói (không bao giờ đủ 3 gói vượt mặt) |
| RTO | ban đầu 500 ms; sau đó `SRTT + max(4·RTTVAR, 1 ms) + 10 ms` | Công thức PTO của QUIC (RFC 9002 §6.2). Không dùng sàn 200 ms của TCP vì bên nhận không trì hoãn ACK |
| RTO backoff | timeout liên tiếp thì ×2, tối đa 5 s; có ACK mới thì về bình thường | Mất theo số đếm/thời gian (mạng còn sống) không tăng backoff |
| ACK | gửi ngay trong lần flush kế tiếp (không trì hoãn); gom nhiều gói nhờ xử lý tối đa 32 gói đang chờ rồi mới flush | Gói chỉ chứa ACK thì không ACK lại |
| Cửa sổ nhận | 1 MB | |
| Congestion control | CUBIC (mặc định) hoặc cửa sổ cố định 64 KB (`-cc fixed`) | |
| CUBIC | C = 0.4, β = 0.7, cwnd ban đầu 10 × 1200 byte, sàn 2 gói, trần 1 MB | RFC 9438, có vùng Reno-friendly |
