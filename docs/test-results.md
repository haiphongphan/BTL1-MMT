# Kết quả test

Môi trường: WSL2 (Linux 6.18, 18 CPU), Go 1.27.3, loopback. Ngày đo: 12/11/2026.

```bash
go vet ./...                                               # sạch
go test -race ./...                                        # sạch (protocol, server, transport, e2e)
BTL_LONG=1 go test -v -run . -timeout 60m ./internal/e2e/  # số liệu bên dưới (~4.5 phút)
./scripts/load_test.sh 20 300                              # kịch bản 9
```

`-loss p` giả lập mất gói ở **chiều gửi** của từng bên. Ở các kịch bản dưới, cả server và client đều đặt loss, nên
1 tin đi qua server chịu 4 lần mất có thể (client→server, ACK về, server→client, ACK về). Congestion control
mặc định CUBIC.

## Bảng 10 kịch bản

| # | Kịch bản | Kỳ vọng | Kết quả | Test |
|---|---|---|---|---|
| 1 | 3 client, chat chung + DM | Đúng người nhận, đúng thứ tự | ✅ 200 chat + 200 DM: bob và carol nhận đủ 200 chat đúng thứ tự, chỉ bob nhận DM, alice không nhận lại tin của mình | `TestChatAndDMOrder/no-loss` |
| 2 | Loss 10%, 20%, jitter 30 ms | Không mất, không trùng tin | ✅ loss 10%, loss 20%, loss 10% + jitter 30 ms: đủ 200/200 tin, đúng thứ tự, không trùng | `TestChatAndDMOrder/loss*` |
| 3 | File 1 KB / 10 MB / 200 MB, loss 10% | SHA-256 khớp | ✅ khớp ở mọi kích thước (bảng dưới) | `TestFileTransfer` |
| 4 | Gửi file 200 MB + chat cùng lúc | Chat trễ < 300 ms | ✅ p50 32 ms, **p95 174 ms**, max 395 ms (200 mẫu) | `TestChatLatencyDuringFile` |
| 5 | Ctrl+C 1 client | Server báo leave ≤ 10 s, không crash | ✅ Ctrl+C: **20 ms** (client gửi CLOSE). `kill -9`: **10 s** (idle timeout). Sau đó đăng nhập lại được | `TestClientKilled` |
| 6 | Tắt server rồi bật lại | Client tự kết nối lại | ✅ Tắt bằng Ctrl+C (server gửi CLOSE): client kết nối lại sau ≈1 s. **`kill -9` server** (không gửi CLOSE): client phát hiện sau 10.1 s (idle timeout), kết nối lại xong sau 11.1 s. Chat tiếp được trong cả 2 trường hợp | `TestServerRestartReconnect`, `TestServerCrashReconnect` |
| 7 | Sai mật khẩu / user lạ / đăng nhập trùng | `AUTH_FAIL` | ✅ cả 3 trường hợp nhận `AUTH_FAIL` kèm lý do | `TestAuthFail` |
| 8 | Gói rác vào cổng UDP | Server bỏ qua, không crash | ✅ 5000 gói (ngẫu nhiên, quá ngắn, header đúng định dạng + conn_id thật + payload rác): server vẫn chạy, client không bị ngắt, chat bình thường | `TestGarbageUDP` |
| 9 | 20 client bot | Ổn định, goroutine không tăng | ✅ xem mục dưới | `TestManyClientsNoLeak`, `scripts/load_test.sh` |
| 10 | Wireshark | Payload và mật khẩu không đọc được | ⏳ cần chụp tay. Tự động: `TestWireIsEncrypted` không tìm thấy plaintext trong gói UDP | `internal/transport` |

## Truyền file (kịch bản 3)

Alice → server → Bob, đo từ lúc bắt đầu gửi tới lúc Bob kiểm tra xong SHA-256.

| Kích thước | Loss | Thời gian | Throughput | SHA-256 |
|---|---|---|---|---|
| 1 KB | 0% | 12 ms | — | ✅ |
| 1 KB | 10% | 33 ms | — | ✅ |
| 10 MB | 0% | 0.27 s | 37.5 MB/s | ✅ |
| 10 MB | 10% | 10.7 s | 0.94 MB/s | ✅ |
| 200 MB | 0% | 5.4 s | 36.9 MB/s | ✅ |
| 200 MB | 10% | 3 phút 13 s | 1.03 MB/s | ✅ (gửi lại 18% số gói) |

Với 10% mất **ngẫu nhiên**, CUBIC coi mỗi lần mất là nghẽn nên giữ cwnd nhỏ (công thức Mathis: throughput ∝ 1/√p),
xem `docs/reliability.md` mục 5. Khi cần demo nhanh dưới mất gói nặng: chạy server và client với `-cc fixed`.

## Tải (kịch bản 9)

**`TestManyClientsNoLeak`** (trong `go test`): 20 client, mỗi client gửi 100 tin (2000 tin, loss 5%):
mỗi client nhận đủ 1900 tin, tin của từng người gửi đúng thứ tự; goroutine của cả tiến trình: trước 4,
lúc chạy 164, **sau khi tất cả thoát: 4** ⇒ không rò rỉ.

**`scripts/load_test.sh 20 300`** (tiến trình thật, 20 bot × 5 phút, mỗi bot chat mỗi 1–3 s):

| Chỉ số | Kết quả |
|---|---|
| Số goroutine của server | **86 suốt 5 phút** (30 lần đo, mỗi 10 s; = 4 goroutine/user × 20 + 6 nền) ⇒ **về 6** khi các bot thoát |
| Heap server | dao động 9.8–18 MB theo chu kỳ GC, không tăng dần |
| CPU server (trung bình cả quá trình) | **3.1%** của 1 core (trước khi sửa tick khi rảnh: 40%) |
| RSS server | 22 MB |
| Tin nhắn | bot gửi 2 836 tin, nhận 53 883 lượt (lý tưởng 2 836 × 19 = 53 884; chênh 1 do tin đang trên đường lúc lấy số) |
| Panic / client bị ngắt vì hàng đợi đầy | 0 / 0 |

## Lỗi tìm ra nhờ test hệ thống (đã sửa)

1. **Server âm thầm bỏ tin** khi hàng đợi gửi (64 tin) của người nhận đầy: alice gửi 400 tin liên tiếp ⇒ bob mất
   85 tin dù **không mất gói**. Sửa: hàng đợi 4096 tin; đầy thì ngắt client quá chậm thay vì bỏ tin.
2. **Tin đến sai thứ tự** khi mất gói (mỗi tin là 1 stream độc lập). Sửa: `Seq` + `SeqBuffer` ở tầng app.
3. **Client mới "điếc" 2–4 s** nếu PING đầu tiên bị mất (server chưa biết địa chỉ UDP của client). File 1 KB ở
   loss 10% mất 2–4 s ở 4/8 lần chạy. Sửa: gửi lại PING đầu sau 200/400/800 ms ⇒ tối đa ≈0.6 s.
4. **Ctrl+C không báo server** (thoát mà không gửi CLOSE ⇒ server đợi 10 s). Sửa: bắt tín hiệu, đóng kết nối.
5. **Server tốn 43% CPU khi 20 client không làm gì.** Profile: gần như toàn bộ là Go scheduler thức dậy
   (`futex`, `findRunnable`) vì mỗi kết nối tick 10 ms kể cả khi rảnh (40 đầu kết nối × 100 lần/s). Sửa: tick
   10 ms khi có gói chờ ACK, 100 ms khi rảnh. 20 cặp kết nối rảnh: 72% → 7% CPU; throughput và độ trễ chat
   đo lại không đổi.
6. **Danh sách người online sai khi 2 người đăng nhập cùng lúc** (thấy khi stress test kịch bản 6: 3/20 lần).
   `broadcastUserList` đọc danh sách rồi mới xếp tin; 2 broadcast chạy xen kẽ có thể xếp bản cũ sau bản mới.
   Sửa: mutex bao cả 2 bước.
