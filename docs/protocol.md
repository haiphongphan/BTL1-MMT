### 3.1 Cổng

| Cổng | Giao thức | Mục đích |
|---|---|---|
| 9000 | TCP | Handshake, trao khóa, đăng nhập (đóng sau khi xong) |
| 9001 | UDP | Toàn bộ dữ liệu: message, file, ACK, heartbeat |
| 9002 | UDP broadcast | Server quảng bá `"COLLAB_SERVER <ip> 9000 9001"` mỗi 2s |
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

- **stream_id**: client dùng số lẻ (1, 3, 5...), server dùng số chẵn (2, 4, 6...), nên hai bên không bao giờ đụng nhau.
- Mỗi STREAM frame mang tối đa **1000 byte data**, để còn chỗ ghép thêm 1 ACK frame (≤ 71 byte).
- **Gói chỉ chứa ACK thì không được ACK lại**, nếu không hai bên sẽ ACK nhau vô hạn.

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
	TS    int64    `json:"ts"`              // UnixMilli
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

### 3.6 State diagram của client (B vẽ, đưa vào báo cáo)


### 3.7 Tham số mặc định

| Tham số | Giá trị |
|---|---|
| Heartbeat (PING khi rảnh) | 2s |
| Idle timeout (coi như chết) | 10s |
| Loss: ngưỡng số gói | gói bị "vượt mặt" bởi gói số ≥ pn+3 đã được ACK |
| RTO ban đầu / tối thiểu | 500 ms / 200 ms |
| Delayed ACK | tối đa 25 ms hoặc mỗi 2 gói |
| Cửa sổ nhận | 1 MB |
| cwnd ban đầu | 10 × 1200 byte |