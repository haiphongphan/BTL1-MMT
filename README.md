# go-collab — Distributed streaming-based client–server collaboration app

Ứng dụng chat + gửi file nhóm, viết bằng Go thuần (không thư viện ngoài), gồm:
- **Server**: handshake + xác thực qua TCP (`:9000`), truyền dữ liệu qua **UDP tin cậy tự viết** (`:9001`) với sliding window, SACK, RTO, multi-stream (không Head-of-Line blocking), mã hóa AES-256-GCM với khóa trao đổi bằng X25519.
- **Client**: process chạy trên máy người dùng, giao diện web do **HTTP server tự viết trên raw TCP** phục vụ tại `127.0.0.1:808x`, JavaScript thuần.

Đặc tả giao thức: [docs/protocol.md](docs/protocol.md).

## Yêu cầu

- Go ≥ 1.22 — https://go.dev/dl/
- macOS / Linux / Windows. Trong LAN cần mở firewall: `9000/TCP`, `9001/UDP`, `9002/UDP` (discovery).

## Build

```bash
git clone <repo-url> && cd BTL1
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
