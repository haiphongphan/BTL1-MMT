# go-collab

Ứng dụng cộng tác client–server: **chat chung, nhắn riêng (DM), gửi file** giữa nhiều người dùng, viết bằng Go,
chỉ dùng thư viện chuẩn. Dữ liệu đi qua **giao thức tin cậy tự viết trên UDP** (có mã hóa, chống mất gói, không
Head-of-Line blocking, CUBIC); giao diện là **Web UI** chạy trên **HTTP server tự viết** bằng socket TCP + JavaScript thuần.

## 1. Cài đặt

Yêu cầu: **Go ≥ 1.22** ([tải tại go.dev/dl](https://go.dev/dl/)). Không cần thư viện ngoài.

```bash
git clone <link repo> go-collab && cd go-collab
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

(Windows: `go build -o bin/server.exe ./cmd/server`, tương tự cho client.)

## 2. Chạy

**Server** (mặc định TCP 9000 cho handshake, UDP 9001 cho dữ liệu):

```bash
./bin/server                       # = ./bin/server -tcp :9000 -udp-port 9001 -users users.json
```

**Client** (mỗi client 1 terminal, mỗi client 1 cổng Web UI khác nhau):

```bash
./bin/client -server 127.0.0.1:9000 -user phong -pass 123456 -web 127.0.0.1:8081
./bin/client -server 127.0.0.1:9000 -user hai   -pass 123456 -web 127.0.0.1:8082
```

Mở trình duyệt tới `http://127.0.0.1:8081`: danh sách người online, tab chat chung / DM, nút 📎 gửi file
(thanh tiến trình, kiểm tra SHA-256), trạng thái mạng (RTT, cwnd, tỉ lệ mất gói). File nhận được lưu trong `downloads/`.

Client cũng có giao diện dòng lệnh: gõ thẳng = chat chung, `/dm <user> <text>`, `/file <đường dẫn> [user]`,
`/users`, `/quit`. Ctrl+C thoát và báo server ngay.

**Tài khoản demo** (`users.json`, mật khẩu lưu dạng `sha256(salt + password)`):

| User | Mật khẩu |
|---|---|
| `phong`, `hai` | `123456` |
| `user1` … `user30` | `123456` (dùng cho bot / test tải) |

**Chạy nhiều máy trong LAN:** chạy server trên 1 máy, client dùng `-server <IP máy server>:9000`. Mở firewall máy
server cho **TCP 9000** và **UDP 9001** (server cũng phát broadcast quảng bá trên UDP 9002).

### Cờ dòng lệnh

| Cờ | Server | Client | Ý nghĩa |
|---|:-:|:-:|---|
| `-tcp :9000` / `-udp-port 9001` | ✓ | | cổng handshake / dữ liệu |
| `-users users.json` | ✓ | | file tài khoản |
| `-server host:9000` / `-user` / `-pass` | | ✓ | đăng nhập |
| `-web 127.0.0.1:8081` | | ✓ | địa chỉ Web UI (`-web ""` = tắt) |
| `-bot` | | ✓ | tự gửi chat mỗi 1–3 s (test tải) |
| `-stats 10s` | ✓ | | in số goroutine / RAM / số user online theo chu kỳ |
| `-loss 0.1` | ✓ | ✓ | giả lập mất 10% gói **gửi đi** |
| `-delay 50ms` / `-jitter 20ms` | | ✓ | giả lập độ trễ cố định / ngẫu nhiên (không đảo thứ tự gói) |
| `-cc cubic` / `-cc fixed` | ✓ | ✓ | congestion control (mặc định `cubic`; `fixed` = cửa sổ cố định 64 KB) |
| `-cclog cwnd.csv` | ✓ | ✓ | ghi diễn biến cwnd/RTT ra CSV để vẽ đồ thị |

## 3. Test

```bash
go vet ./...
go test -race ./...                       # unit + tích hợp + hệ thống (e2e), ~1 phút
BTL_LONG=1 go test -v -run . -timeout 60m ./internal/e2e/   # file 200 MB, HoL với 200 MB, 100 vòng chat
./scripts/load_test.sh 20 300             # kịch bản 9: 20 bot × 5 phút, theo dõi goroutine/RAM server
./scripts/run_clients.sh 20               # chạy 20 bot tới 1 server đang chạy (Ctrl+C để dừng)
```

`internal/e2e` chạy server + client thật qua loopback: chat/DM đúng người nhận, đúng thứ tự, không mất/trùng
(loss 0–20%, jitter 30 ms); file 1 KB / 10 MB (/ 200 MB) với loss 10%, SHA-256 khớp; độ trễ chat trong lúc gửi file;
sai mật khẩu / đăng nhập trùng; gói rác vào cổng UDP; tắt rồi bật server; Ctrl+C và `kill -9` client; 20 client
không rò rỉ goroutine. Kết quả đo: [`docs/test-results.md`](docs/test-results.md).

## 4. Cấu trúc

```
cmd/server, cmd/client   main: cờ dòng lệnh
internal/protocol        định dạng gói/frame, handshake JSON, AppMsg, SeqBuffer
internal/transport       transport tin cậy trên UDP: ACK/SACK, RTO, CUBIC, AES-GCM, stream, LossyConn
internal/server          handshake + auth, registry user, định tuyến chat/DM, chuyển tiếp file
internal/client          logic client: kết nối lại, gửi/nhận file, event log cho UI
internal/webui           HTTP/1.1 server tự viết + static (HTML/CSS/JS thuần)
internal/e2e             test hệ thống
scripts/                 run_clients.sh, load_test.sh, plotcc (vẽ đồ thị cwnd)
docs/                    protocol.md, reliability.md, test-results.md
```

## 5. Giao thức & transport

Dữ liệu (chat, DM, file) đi qua **một giao thức tin cậy tự viết trên UDP**; TCP chỉ dùng cho handshake (ECDH X25519 + đăng nhập).

- Gói ≤ 1200 byte: header 12 byte gửi rõ (là AAD) + frames mã hóa **AES-256-GCM**. Nonce = `conn_id ‖ packet_num ‖ chiều`, không bao giờ lặp.
- Frame: `STREAM` (stream_id, offset, FIN), `ACK` (cửa sổ nhận + tối đa 8 SACK range), `PING`, `CLOSE`.
- Tin cậy: SACK, phát hiện mất theo số đếm / thời gian / RTO (RFC 9002), RTO backoff, chỉ gửi lại frame bị mất.
- Không Head-of-Line blocking: mỗi tin/file là 1 stream ghép độc lập + hàng đợi ưu tiên chat > file.
- Tin nhắn giao đúng thứ tự gửi nhờ `Seq` ở tầng app (xem `docs/protocol.md` mục 3.5).
- Congestion control: **CUBIC** (RFC 9438) mặc định, hoặc cửa sổ cố định 64 KB.

Chi tiết: [`docs/protocol.md`](docs/protocol.md) (định dạng gói/frame, handshake, tham số) và [`docs/reliability.md`](docs/reliability.md) (thiết kế, đồ thị cwnd, số đo).

### Vẽ đồ thị cwnd

Chỉ cần Go, ra file SVG mở bằng trình duyệt:

```bash
./bin/client -user phong -pass 123456 -cclog cwnd.csv     # gửi 1 file lớn rồi thoát
go run ./scripts/plotcc cwnd.csv > cwnd.svg
```

### Test riêng tầng transport

```bash
go test -race ./internal/transport/                                    # ~15 s
BTL_LONG=1 go test -run Long -v -timeout 30m ./internal/transport/     # đo 50 MB, HoL, đồ thị cwnd (~3 phút)
BTL_LONG=1 BTL_CCLOG=cwnd.csv go test -run LongCCGraph -v ./internal/transport/ \
  && go run ./scripts/plotcc internal/transport/cwnd.csv > cwnd.svg
```
