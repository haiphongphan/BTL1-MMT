# go-collab — Ứng dụng cộng tác client–server trên giao thức tin cậy tự thiết kế (Go)

Bài tập lớn 1, môn Mạng máy tính (CO3093). Chat nhóm, nhắn tin riêng và gửi file giữa nhiều client qua một server, viết hoàn toàn bằng **thư viện chuẩn của Go**, không dùng framework.

- **Server**: handshake và đăng nhập qua TCP (`:9000`); toàn bộ dữ liệu đi qua **UDP tin cậy tự viết** (`:9001`) với SACK, RTO, nhiều stream độc lập (không Head-of-Line blocking), congestion control CUBIC, mã hóa AES-256-GCM với khóa trao đổi bằng X25519.
- **Client**: một process trên máy người dùng. Giao diện web do **HTTP server tự viết trên raw TCP** phục vụ tại `127.0.0.1:808x`, HTML/JS thuần, có trang **đăng nhập / đăng ký**; ngoài ra có CLI trong terminal.

Tài liệu: [`docs/protocol.md`](docs/protocol.md) (đặc tả giao thức), [`docs/reliability.md`](docs/reliability.md) (thiết kế tin cậy, đồ thị cwnd), [`docs/code-walkthrough.md`](docs/code-walkthrough.md) (đọc hiểu code từ A đến Z), [`report/`](report/) (báo cáo LaTeX).

## 1. Yêu cầu

- Go ≥ 1.22 ([go.dev/dl](https://go.dev/dl/)). Đã kiểm tra build trên macOS, Linux, Windows.
- Chạy nhiều máy trong LAN: mở firewall `9000/TCP`, `9001/UDP` trên máy server (và `9002/UDP` trên máy client nếu dùng `-server auto`).

## 2. Build

```bash
git clone <repo-url> && cd BTL1
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

## 3. Chạy nhanh trên một máy

```bash
# Terminal 1 — server (Ctrl+C để tắt: server gửi CLOSE cho mọi client trước khi thoát)
./bin/server

# Terminal 2, 3 — hai client, mỗi client một cổng web riêng
./bin/client -web 127.0.0.1:8081
./bin/client -web 127.0.0.1:8082
```

Mở http://127.0.0.1:8081 và http://127.0.0.1:8082. Trang đầu tiên là **đăng nhập / đăng ký**:

- Tab **Đăng nhập**: ô Server điền sẵn `127.0.0.1:9000` (giá trị cờ `-server`; gõ `auto` để tự tìm server trong LAN), nhập tài khoản ở mục 5.
- Tab **Đăng ký**: tạo tài khoản mới rồi vào luôn. Tên 3–20 ký tự gồm chữ không dấu, số, dấu `_`; mật khẩu 6–64 ký tự, nhập lại 2 lần. Server ghi tài khoản vào `users.json`.
- Sai mật khẩu, tên đã tồn tại hay tài khoản đang online ở máy khác đều hiện lý do ngay dưới form.

Muốn bỏ qua trang đăng nhập (CLI, bot, test tải) thì truyền tài khoản bằng cờ: `./bin/client -user alice -pass 123456 -web 127.0.0.1:8081`.

Sau khi đăng nhập:

- Cột trái: danh sách người đang online kèm **IP:port** mà server đang dùng để nói chuyện với họ; **bấm tên** để mở tab chat riêng.
- Khung giữa: tab **Chung** và các tab DM; nhập tin rồi Enter.
- Nút **📎**: gửi file cho người trong tab đang mở (tab Chung = gửi cho mọi người). Thanh tiến trình hiện ở cả bên gửi và bên nhận; file nhận nằm trong `downloads/` của thư mục chạy client, được kiểm tra SHA-256 trước khi báo xong.
- Thanh trên cùng: 🟢 connected / 🟡 reconnecting / 🔴 disconnected, địa chỉ server, RTT, cwnd, in-flight, % gói mất, nút **Đăng xuất** (quay về trang đăng nhập, có thể vào bằng tài khoản khác mà không cần chạy lại client).

CLI trong terminal của client: `/msg <text>`, `/dm <user> <text>`, `/file <đường-dẫn> [user]`, `/users`, `/quit`, `/login <user> <pass>`, `/register <user> <pass>`, `/logout`. Gõ thẳng một dòng = `/msg`. Ctrl+C thoát và báo server ngay.

## 4. Chạy nhiều máy trong LAN

Trên máy server: `./bin/server` (log in ra IP:port của từng client khi họ kết nối). Trên máy khác chạy `./bin/client` rồi mở http://127.0.0.1:8081; ở trang đăng nhập, ô Server nhập `auto` (tự tìm server qua broadcast UDP :9002, tối đa 5 s) hoặc `192.168.1.10:9000` (khi mạng chặn broadcast). Dùng cờ cũng được:

```bash
./bin/client -server auto -user alice -pass 123456
./bin/client -server 192.168.1.10:9000 -user alice -pass 123456
```

Mạng có firewall thì mở thêm `9002/UDP` chiều vào trên máy client để dùng `-server auto`. Web UI luôn chỉ lắng nghe trên `127.0.0.1` của máy chạy client.

## 5. Tài khoản

`users.json` lưu `sha256(salt ‖ password)` với salt ngẫu nhiên cho từng user. Tài khoản demo có sẵn, mật khẩu đều là `123456`: `alice`, `bob`, `carol`, `phong`, `hai`, và `user1` … `user20` (dùng cho test tải). Riêng `phan` có mật khẩu `654321`.

Tạo tài khoản mới: tab **Đăng ký** trên Web UI, lệnh `/register <user> <pass>` trong CLI, hoặc cờ `-register` (`./bin/client -register -user dave -pass secret1`). Server ghi ngay vào `users.json` (ghi file tạm rồi đổi tên, không làm hỏng file nếu tắt giữa chừng).

Thêm hoặc đặt lại mật khẩu từ dòng lệnh (khi server đang tắt):

```bash
go run ./cmd/mkusers -pass matkhau dave eve     # thêm/ghi đè user dave, eve
go run ./cmd/mkusers -n 30                      # tạo user1..user30, mật khẩu 123456
```

## 6. Cờ dòng lệnh

**Server**

| Cờ | Mặc định | Ý nghĩa |
|---|---|---|
| `-tcp` | `:9000` | Địa chỉ TCP handshake |
| `-udp-port` | `9001` | Cổng UDP dữ liệu |
| `-users` | `users.json` | File tài khoản |
| `-cc` | `cubic` | Congestion control: `cubic` hoặc `fixed` (cửa sổ cố định 64 KB) |
| `-cclog` | | Ghi diễn biến cwnd/RTT ra CSV |
| `-loss` | `0` | Giả lập mất gói **chiều gửi** (0.1 = 10%) |
| `-jitter` | `0` | Giả lập trễ ngẫu nhiên chiều gửi (vd `30ms`) |

**Client**

| Cờ | Mặc định | Ý nghĩa |
|---|---|---|
| `-server` | `127.0.0.1:9000` | Địa chỉ TCP của server, hoặc `auto` để tự tìm server trong LAN. Không có `-user` thì đây là giá trị điền sẵn ở trang đăng nhập |
| `-user`, `-pass` | | Tài khoản; bỏ trống thì đăng nhập / đăng ký trên Web UI. Bắt buộc khi `-web ""` hoặc `-bot` |
| `-register` | `false` | Đăng ký `-user`/`-pass` như tài khoản mới rồi đăng nhập luôn |
| `-web` | `127.0.0.1:8081` | Địa chỉ Web UI (`""` để tắt) |
| `-cc`, `-cclog`, `-loss`, `-jitter` | | Như server |
| `-bot` | `false` | Tự gửi chat mỗi 1–3 s (test tải) |

Server phát broadcast `COLLAB_SERVER <ip> <tcp_port> <udp_port>` lên `9002/UDP` mỗi 2 s; client `-server auto` nghe cổng này, lấy IP từ địa chỉ nguồn của gói. Nhiều client trên cùng một máy dò cùng lúc được.

## 7. Test

```bash
go vet ./...
go test -race ./...            # unit (protocol, transport) + tích hợp server (FakePair) + hệ thống (server và 2 client thật, 10% loss), ~30 s
```

Phép đo dài của tầng transport (50 MB qua mạng giả lập, độ trễ chat khi đang gửi file, đồ thị cwnd):

```bash
BTL_LONG=1 go test -run Long -v -timeout 30m ./internal/transport/
BTL_LONG=1 BTL_CCLOG=cwnd.csv go test -run LongCCGraph -v ./internal/transport/ \
  && go run ./scripts/plotcc internal/transport/cwnd.csv > cwnd.svg      # mở SVG bằng trình duyệt
```

Test tải: 20 client bot cùng lúc, theo dõi dòng `[Runtime] goroutines=… heap=…` server in mỗi 10 s (phải ổn định, không tăng mãi):

```bash
./bin/server -loss 0.1
bash scripts/run_clients.sh 20            # log từng client trong logs/
```

Kiểm tra toàn vẹn file đã nhận: `shasum -a 256 <file gốc> downloads/<file>` (Windows: `certutil -hashfile <file> SHA256`).

Quan sát mã hóa bằng Wireshark với bộ lọc `tcp.port == 9000 || udp.port == 9001`: message `AUTH` chỉ có chuỗi base64 của ciphertext, payload UDP là byte ngẫu nhiên.

## 8. Giao thức và transport (tóm tắt)

Dữ liệu (chat, DM, file) đi qua **một giao thức tin cậy tự viết trên UDP**; TCP chỉ dùng cho handshake (ECDH X25519 + đăng nhập hoặc đăng ký: message `AUTH` / `REGISTER` mang `{user, pass}` đã mã hóa AES-GCM).

- Gói ≤ 1200 byte: header 12 byte gửi rõ (là AAD) + frames mã hóa **AES-256-GCM**. Nonce = `conn_id ‖ packet_num ‖ chiều`, không bao giờ lặp.
- Frame: `STREAM` (stream_id, offset, FIN), `ACK` (cửa sổ nhận + tối đa 8 SACK range), `PING`, `CLOSE`.
- Tin cậy: SACK, phát hiện mất theo số đếm / thời gian / RTO (RFC 9002), RTO backoff, chỉ gửi lại frame bị mất.
- Không Head-of-Line blocking: mỗi tin/file là 1 stream ghép độc lập + hàng đợi ưu tiên chat > file.
- Congestion control: **CUBIC** (RFC 9438) mặc định, hoặc cửa sổ cố định 64 KB. Mất gói ngẫu nhiên nặng (`-loss 0.1`) làm CUBIC giảm tốc mạnh; dùng `-cc fixed` khi muốn demo độ tin cậy ở tốc độ cao.
- Tầng ứng dụng: stream bắt đầu bằng `0x01` là message JSON, `0x02` là file (`[len][FileMeta JSON][dữ liệu]`); server chuyển tiếp file theo kiểu pipeline, không giữ cả file trong RAM; người nhận ghi đĩa và băm SHA-256 cùng lúc.
- Thứ tự tin nhắn: vì mỗi tin là một stream riêng (không HoL), khi mất gói tin sau có thể tới trước tin trước. Người gửi đánh `seq` tăng dần theo kết nối, người nhận sắp lại bằng `protocol.SeqBuffer`; server đánh lại `seq` theo dãy riêng của từng người nhận trong `writeLoop` của session đó.
- Server: mỗi session có hàng đợi gửi riêng (4096 tin) và đúng một goroutine ghi, nên một client chậm không chặn người khác. Server **không bỏ tin**: hàng đợi đầy thì ngắt riêng client quá chậm đó, client tự kết nối lại. Server ghi đè `from` của mọi tin bằng username đã xác thực (chống giả danh) và theo dõi IP:port của từng user.

## 9. Cấu trúc thư mục

```
cmd/server, cmd/client, cmd/mkusers   # main: chỉ đọc cờ và lắp ráp
internal/protocol   # header 12 byte, frame, length-prefixed JSON, header file, SeqBuffer
internal/transport  # UDP tin cậy: conn, stream, ack, rtt, cubic, crypto, mux, lossy (giả lập mạng)
internal/server     # handshake/auth, registry, router (chat/DM/relay file), discovery
internal/client     # core (connect/reconnect), discovery, gửi/nhận file, event log, test hệ thống
internal/webui      # HTTP server tự viết + static/ (HTML/JS/CSS thuần, nhúng bằng embed)
docs/               # protocol.md, reliability.md, code-walkthrough.md, img/
report/             # báo cáo LaTeX (pdflatex -shell-escape, hoặc Overleaf)
scripts/            # run_clients.sh (test tải), plotcc (vẽ đồ thị cwnd)
```
