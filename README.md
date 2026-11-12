# go-collab

<!-- Các mục cài đặt / chạy / tài khoản demo / Web UI: B viết (xem PLAN.MD, Phase 5). -->

## Giao thức & transport

Dữ liệu (chat, DM, file) đi qua **một giao thức tin cậy tự viết trên UDP**; TCP chỉ dùng cho handshake (ECDH X25519 + đăng nhập).

- Gói ≤ 1200 byte: header 12 byte gửi rõ (là AAD) + frames mã hóa **AES-256-GCM**. Nonce = `conn_id ‖ packet_num ‖ chiều`, không bao giờ lặp.
- Frame: `STREAM` (stream_id, offset, FIN), `ACK` (cửa sổ nhận + tối đa 8 SACK range), `PING`, `CLOSE`.
- Tin cậy: SACK, phát hiện mất theo số đếm / thời gian / RTO (RFC 9002), RTO backoff, chỉ gửi lại frame bị mất.
- Không Head-of-Line blocking: mỗi tin/file là 1 stream ghép độc lập + hàng đợi ưu tiên chat > file.
- Congestion control: **CUBIC** (RFC 9438) mặc định, hoặc cửa sổ cố định 64 KB.

Chi tiết: [`docs/protocol.md`](docs/protocol.md) (định dạng gói/frame, handshake, tham số) và [`docs/reliability.md`](docs/reliability.md) (thiết kế, đồ thị cwnd, số đo).

### Cờ dành cho thử nghiệm transport (server và client đều có)

| Cờ | Ý nghĩa |
|---|---|
| `-loss 0.1` | giả lập mất 10% gói **gửi đi** |
| `-cc cubic` / `-cc fixed` | thuật toán congestion control (mặc định `cubic`). Mất gói ngẫu nhiên nặng thì CUBIC giảm tốc mạnh; `fixed` để demo độ tin cậy |
| `-cclog cwnd.csv` | ghi diễn biến cwnd/RTT ra CSV |

Vẽ đồ thị cwnd (chỉ cần Go, ra file SVG mở bằng trình duyệt):

```bash
./bin/client -user alice -pass ... -cclog cwnd.csv     # gửi 1 file lớn rồi thoát
go run ./scripts/plotcc cwnd.csv > cwnd.svg
```

### Test

```bash
go test -race ./...                                                    # ~30 s
BTL_LONG=1 go test -run Long -v -timeout 30m ./internal/transport/     # đo 50 MB, HoL, đồ thị cwnd (~3 phút)
BTL_LONG=1 BTL_CCLOG=cwnd.csv go test -run LongCCGraph -v ./internal/transport/ \
  && go run ./scripts/plotcc internal/transport/cwnd.csv > cwnd.svg
```
