# Kiểm thử tầng giao thức và transport

Hai package `internal/protocol` (20 test) và `internal/transport` (45 test) có tổng cộng 65 test tự động, chạy toàn
bộ khoảng 15 giây:

```bash
go vet ./...
go test -race ./...
BTL_LONG=1 go test -run Long -v ./internal/transport/   # các phép đo dài (thông lượng, đồ thị cwnd, độ trễ HoL)
```

| Nhóm | Nội dung kiểm tra |
|---|---|
| Định dạng gói, frame | Encode rồi decode lại mọi loại frame và header ra đúng giá trị; đóng gói nhiều frame trong cùng một gói; dữ liệu bị cắt cụt hoặc mã `type` lạ đều trả về lỗi chứ không panic; chuỗi `reason` của frame CLOSE được cắt đúng ranh giới ký tự UTF-8 |
| Handshake, meta file | Đọc/ghi liên tiếp các khung `[len][JSON]`; từ chối khung khai báo độ dài vượt ngưỡng; `FileHeaderParser` tách đúng metadata ngay cả khi dữ liệu được truyền vào từng byte một |
| `SeqBuffer` | Sắp xếp lại bản tin đến sai thứ tự (kể cả hoán vị ngẫu nhiên); lọc bỏ bản tin trùng số `Seq`; tự nhảy qua khoảng trống khi vượt ngưỡng `MaxSeqPending` để tránh treo |
| ACK | Sinh đúng các khoảng SACK khi có gói bị khuyết hoặc gói trùng; giới hạn tối đa 8 khoảng SACK và giới hạn bộ nhớ lưu lịch sử gói nhận |
| RTT, RTO | Khởi tạo mẫu RTT đầu tiên, công thức cập nhật SRTT/RTTVAR, đảm bảo RTO không co sát về RTT khi độ trễ đều, kiểm tra ngưỡng phát hiện mất gói theo thời gian |
| Ghép stream | Giao dữ liệu đúng thứ tự khi gói đến xuôi, ngược, trùng lặp, chồng lấn một phần, hoặc khi cờ FIN tới trước dữ liệu; tự động cắt frame ≤ 1000 byte; trả lỗi khi ghi vào stream đã đóng |
| Mã hóa | Hai đầu ECDH tính ra cùng khóa (`TestDeriveKeysBothSidesAgree`), từ chối khóa công khai không hợp lệ (`TestDeriveKeysRejectsBadPublicKey`); sửa bất kỳ byte nào trên gói đều làm giải mã thất bại (`TestTamperedPacketRejected`); nonce phân biệt giữa hai chiều truyền (`TestNonceUniquePerDirectionAndPN`, `TestWrongDirectionRejected`); không lộ plaintext trên đường truyền (`TestWireIsEncrypted`) |
| CUBIC | Pha slow start tăng gấp đôi mỗi RTT; giảm còn 70% khi mất gói; quỹ đạo hàm bậc ba quanh W_max; nhánh Reno-friendly; chặn sàn và trần `cwnd` |
| Truyền qua UDP thật | Truyền 5 MB qua đường truyền loss 20% mỗi chiều khớp từng byte; truyền 1 MB qua loss 10% với CUBIC; kiểm tra truyền dữ liệu chiều từ server → client |
| Vòng đời kết nối | Gọi `Close` báo ngay cho đầu bên kia; đóng `Mux` giải phóng toàn bộ kết nối con; tự phát hiện khi đầu bên kia ngừng phản hồi sau ≈ 10 s |
| Phát hiện mất, gửi lại | RTO exponential backoff nhân đôi khi đứt mạng; mất gói phát hiện theo số đếm không làm tăng backoff; truyền lại giữ đúng thứ tự ban đầu; gói ACK cũ đến trễ không ghi đè `window` mới |
| Bộ giả lập | Chế độ jitter vẫn bảo toàn thứ tự FIFO; đường truyền bottleneck bỏ gói khi tràn hàng đợi |

Các test tích hợp và test hệ thống của tầng ứng dụng (server, client, Web UI) được mô tả trong báo cáo, mục
*Kiểm thử*.
