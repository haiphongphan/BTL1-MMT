# go-collab — Ứng dụng cộng tác client–server trên giao thức tin cậy tự thiết kế (Go)

Bài tập lớn 1, môn Mạng máy tính (CO3093). Chat nhóm, nhắn tin riêng và gửi file giữa nhiều client qua một server, viết hoàn toàn bằng **thư viện chuẩn của Go**, không dùng framework.

- **Server**: handshake và đăng nhập qua TCP (`:9000`); toàn bộ dữ liệu đi qua **UDP tin cậy tự viết** (`:9001`) với SACK, RTO, nhiều stream độc lập (không Head-of-Line blocking), congestion control CUBIC, mã hóa AES-256-GCM với khóa trao đổi bằng X25519.
- **Client**: một process trên máy người dùng. Giao diện web do **HTTP server tự viết trên raw TCP** phục vụ tại `127.0.0.1:808x`, HTML/JS thuần, có trang **đăng nhập / đăng ký**; ngoài ra có CLI trong terminal.

Tài liệu: [`docs/protocol.md`](docs/protocol.md) (đặc tả giao thức), [`docs/reliability.md`](docs/reliability.md) (thiết kế tin cậy, đồ thị cwnd), [`docs/code-walkthrough.md`](docs/code-walkthrough.md) (đọc hiểu code từ A đến Z), [`docs/testing.md`](docs/testing.md) (danh sách unit test), [`report/`](report/) (báo cáo LaTeX).

## 1. Cài đặt

**Bước 1. Cài Go 1.22 trở lên** từ [go.dev/dl](https://go.dev/dl/), rồi kiểm tra:

```bash
go version          # phải in ra go1.22 hoặc mới hơn
```

Đây là dependency duy nhất: dự án chỉ dùng thư viện chuẩn của Go (`go.mod` không có dòng `require` nào), không cần
`go get` hay cài thêm gói. Đã build trên Linux, macOS và Windows.

**Bước 2. Lấy mã nguồn** (hoặc giải nén file .zip đã nộp) và **đứng ở thư mục gốc của repo**. Mọi lệnh bên dưới đều
chạy từ thư mục này, vì server đọc `users.json` theo đường dẫn tương đối.

```bash
git clone https://github.com/haiphongphan/BTL1-MMT.git
cd BTL1-MMT
```

**Bước 3. Build server và client:**

```bash
# Linux / macOS
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

```powershell
# Windows (PowerShell hoặc cmd)
go build -o bin\server.exe .\cmd\server
go build -o bin\client.exe .\cmd\client
```

## 2. Chạy trên một máy (nhiều cửa sổ terminal)

Ở các lệnh dưới, Windows thay `./bin/server` bằng `.\bin\server.exe` và `./bin/client` bằng `.\bin\client.exe`.

**Bước 1. Terminal 1: chạy server.**

```bash
./bin/server
```

Kết quả mong đợi (IP trong dòng Discovery là IP LAN của máy bạn):

```
[Server] UDP transport running on :9001 (loss 0%, jitter 0s)
[Discovery] Broadcasting "COLLAB_SERVER 192.168.1.23 9000 9001" on UDP :9002 every 2s
[Server] TCP Handshake server running on :9000
```

Nếu thấy `open users.json: no such file or directory` thì bạn đang không đứng ở thư mục gốc của repo (xem bước 2 mục 1).

**Bước 2. Terminal 2 và Terminal 3: chạy hai client**, mỗi client một cổng web riêng:

```bash
./bin/client -web 127.0.0.1:8081     # Terminal 2
./bin/client -web 127.0.0.1:8082     # Terminal 3
```

Mỗi client in ra `🌐 Web UI: http://127.0.0.1:8081` (hoặc `8082`) và `[webui] listening on ...`.

**Bước 3. Mở trình duyệt** tới http://127.0.0.1:8081 và http://127.0.0.1:8082 (hai tab). Ở tab **Đăng nhập**, ô
Server đã điền sẵn `127.0.0.1:9000`:

- Tab 8081: đăng nhập `alice` / `123456`
- Tab 8082: đăng nhập `bob` / `123456`

Kết quả mong đợi: Terminal 1 (server) in ra
`[Server] User 'alice' authenticated from 127.0.0.1:...` rồi `User 'alice' active: TCP ..., UDP ...`; trên web,
cột trái hiện cả `alice` và `bob` kèm IP:port; thanh trên cùng hiện 🟢 connected.

**Bước 4. Thử các chức năng:**

- **Chat chung:** gõ vào tab **Chung** rồi Enter, tin hiện ở cả hai trình duyệt.
- **Nhắn riêng:** bấm tên người ở cột trái để mở tab DM.
- **Gửi file:** nút **📎** gửi cho người trong tab đang mở (tab Chung = gửi cho mọi người). Thanh tiến trình hiện ở cả
  hai bên; file nhận nằm trong `downloads/` của thư mục chạy client và được kiểm tra SHA-256 trước khi báo xong.
- **Trạng thái mạng:** thanh trên cùng hiện 🟢 connected / 🟡 reconnecting / 🔴 disconnected, RTT, cwnd, in-flight,
  % gói mất, và nút **Đăng xuất**.

**Bước 5. Tắt:** Ctrl+C ở từng terminal. Client báo server ngay (`CLOSE`) nên người còn lại thấy danh sách online cập
nhật tức thì; tắt server thì mọi client chuyển sang 🟡 reconnecting và tự vào lại khi server chạy lại.

**Thêm:**

- **Đăng ký tài khoản mới:** tab **Đăng ký** trên trang đăng nhập (tên 3–20 ký tự gồm chữ không dấu, số, `_`; mật khẩu
  6–64 ký tự). Sai mật khẩu, tên đã tồn tại hay tài khoản đang online ở máy khác đều hiện lý do ngay dưới form.
- **Bỏ qua trang đăng nhập** (CLI, bot, test tải): `./bin/client -user alice -pass 123456 -web 127.0.0.1:8081`.
- **CLI trong terminal của client:** `/msg <text>`, `/dm <user> <text>`, `/file <đường-dẫn> [user]`, `/users`,
  `/quit`, `/login <user> <pass>`, `/register <user> <pass>`, `/logout`. Gõ thẳng một dòng = `/msg`.
- **Giả lập mạng kém** để thấy cơ chế truyền lại hoạt động: thêm `-loss 0.1` (mất 10% gói) vào cả server và client.

## 3. Chạy nhiều máy trong LAN

Trên máy server: `./bin/server` (log in ra IP:port của từng client khi họ kết nối). Trên máy khác chạy `./bin/client` rồi mở http://127.0.0.1:8081; ở trang đăng nhập, ô Server nhập `auto` (tự tìm server qua broadcast UDP :9002, tối đa 5 s) hoặc `192.168.1.10:9000` (khi mạng chặn broadcast). Dùng cờ cũng được:

```bash
./bin/client -server auto -user alice -pass 123456
./bin/client -server 192.168.1.10:9000 -user alice -pass 123456
```

Firewall: trên máy server mở chiều vào `9000/TCP` và `9001/UDP`; trên máy client mở `9002/UDP` nếu dùng
`-server auto`. Web UI luôn chỉ lắng nghe trên `127.0.0.1` của máy chạy client.

## 4. Tài khoản

`users.json` lưu `sha256(salt ‖ password)` với salt ngẫu nhiên cho từng user. Tài khoản demo có sẵn, mật khẩu đều là `123456`: `alice`, `bob`, `carol`, `phong`, `hai`, và `user1` … `user20` (dùng cho test tải). Riêng `phan` có mật khẩu `654321`.

Tạo tài khoản mới: tab **Đăng ký** trên Web UI, lệnh `/register <user> <pass>` trong CLI, hoặc cờ `-register` (`./bin/client -register -user dave -pass secret1`). Server ghi ngay vào `users.json` (ghi file tạm rồi đổi tên, không làm hỏng file nếu tắt giữa chừng).

Thêm hoặc đặt lại mật khẩu từ dòng lệnh (khi server đang tắt):

```bash
go run ./cmd/mkusers -pass matkhau dave eve     # thêm/ghi đè user dave, eve
go run ./cmd/mkusers -n 30                      # tạo user1..user30, mật khẩu 123456
```

## 5. Cờ dòng lệnh

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

## 6. Test

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
bash scripts/run_clients.sh 20            # Linux/macOS; log từng client trong logs/
```

```powershell
# Windows (PowerShell): chạy 20 bot, tắt bằng Stop-Process -Name client
1..20 | ForEach-Object { Start-Process -WindowStyle Hidden .\bin\client.exe -ArgumentList "-user user$_ -pass 123456 -web 127.0.0.1:$(8100 + $_) -bot" }
```

Kiểm tra toàn vẹn file đã nhận (hai mã băm phải giống nhau):

```bash
sha256sum <file gốc> downloads/<file>            # Linux
shasum -a 256 <file gốc> downloads/<file>        # macOS
certutil -hashfile downloads\<file> SHA256       # Windows
```

Quan sát mã hóa bằng Wireshark với bộ lọc `tcp.port == 9000 || udp.port == 9001`: message `AUTH` chỉ có chuỗi base64 của ciphertext, payload UDP là byte ngẫu nhiên.

## 7. Xử lý sự cố

| Hiện tượng | Cách xử lý |
|---|---|
| `open users.json: no such file or directory` khi chạy server | Chạy lệnh từ thư mục gốc của repo, hoặc chỉ đường dẫn bằng `-users <path>/users.json` |
| `address already in use` | Còn một server/client cũ đang chạy: tắt nó đi, hoặc đổi cổng (`-web 127.0.0.1:8083` cho client; `-tcp :9100 -udp-port 9101` cho server, khi đó client dùng `-server 127.0.0.1:9100`) |
| Windows hỏi quyền mạng khi chạy `server.exe` lần đầu | Chọn **Allow** cho mạng Private, nếu không máy khác trong LAN không kết nối được |
| Ô Server nhập `auto` mà không tìm thấy server | Mạng chặn broadcast hoặc firewall chặn `9002/UDP`: nhập thẳng `IP-máy-server:9000` |
| Chạy server trong WSL2, máy khác trong LAN không vào được | WSL2 mặc định nằm sau NAT của Windows: khi demo nhiều máy nên chạy bản Windows (`server.exe`), hoặc bật `networkingMode=mirrored` trong `.wslconfig` |
| `go: go.mod requires go >= ...` | Cập nhật Go lên bản mới hơn (xem `go version`) |

## 8. Giao thức và transport (tóm tắt)

TCP `:9000` chỉ dùng cho handshake (trao khóa ECDH X25519, đăng nhập/đăng ký đã mã hóa). Mọi dữ liệu (chat, DM,
file) đi qua **giao thức tin cậy tự viết trên UDP `:9001`**: gói ≤ 1200 byte mã hóa AES-256-GCM, SACK và truyền lại
theo RFC 9002, nhiều stream độc lập nên không Head-of-Line blocking, congestion control CUBIC (RFC 9438). Chi tiết:
[`docs/protocol.md`](docs/protocol.md), [`docs/reliability.md`](docs/reliability.md) và báo cáo trong `report/`.

## 9. Cấu trúc thư mục

```
cmd/server, cmd/client, cmd/mkusers   # main: chỉ đọc cờ và lắp ráp
internal/protocol   # header 12 byte, frame, length-prefixed JSON, header file, SeqBuffer
internal/transport  # UDP tin cậy: conn, stream, ack, rtt, cubic, crypto, mux, lossy (giả lập mạng)
internal/server     # handshake/auth, registry, router (chat/DM/relay file), discovery
internal/client     # core (connect/reconnect), discovery, gửi/nhận file, event log, test hệ thống
internal/webui      # HTTP server tự viết + static/ (HTML/JS/CSS thuần, nhúng bằng embed)
docs/               # protocol.md, reliability.md, code-walkthrough.md, testing.md, img/
report/             # báo cáo LaTeX (pdflatex -shell-escape, hoặc Overleaf)
scripts/            # run_clients.sh (test tải), plotcc (vẽ đồ thị cwnd)
```
