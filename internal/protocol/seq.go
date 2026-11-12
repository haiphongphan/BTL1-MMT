package protocol

import "sort"

// MaxSeqPending: số tin tối đa được giữ chờ lấp chỗ trống. Vượt mức này nghĩa là có lỗi (tin bị bỏ ở đâu đó),
// SeqBuffer bỏ qua chỗ trống để không treo vĩnh viễn và không tốn RAM vô hạn.
const MaxSeqPending = 4096

// SeqBuffer sắp xếp lại các AppMsg theo Seq để giao đúng thứ tự gửi.
//
// Vì sao cần: transport đặt mỗi tin vào 1 stream riêng để tránh Head-of-Line blocking giữa các tin
// (tin sau không phải chờ tin trước được gửi lại). Hệ quả là tin nào nhận đủ trước thì được giao trước,
// nên khi mất gói, thứ tự có thể bị đảo. Người gửi đánh Seq tăng dần (1, 2, 3...) cho mỗi kết nối,
// người nhận dùng SeqBuffer để giữ tin tới sớm cho tới khi các tin trước tới đủ.
//
// Seq == 0 nghĩa là tin không cần thứ tự: được giao ngay.
// Mỗi kết nối transport dùng 1 SeqBuffer mới (kết nối lại thì Seq bắt đầu lại từ 1).
// Không an toàn cho nhiều goroutine; mỗi kết nối chỉ có 1 goroutine đọc.
type SeqBuffer struct {
	next    uint64 // Seq đang chờ; 0 = chưa khởi tạo (coi như 1)
	pending map[uint64]AppMsg
}

// Push nhận 1 tin, trả về các tin đã sẵn sàng theo đúng thứ tự (có thể rỗng).
func (b *SeqBuffer) Push(m AppMsg) []AppMsg {
	if m.Seq == 0 {
		return []AppMsg{m}
	}
	if b.next == 0 {
		b.next = 1
	}
	if m.Seq < b.next {
		return nil // trùng: đã giao rồi
	}
	if m.Seq > b.next {
		if b.pending == nil {
			b.pending = make(map[uint64]AppMsg)
		}
		b.pending[m.Seq] = m
		if len(b.pending) > MaxSeqPending {
			return b.skipGap()
		}
		return nil
	}
	out := []AppMsg{m}
	b.next++
	return b.drain(out)
}

// drain giao tiếp các tin liền mạch đang chờ.
func (b *SeqBuffer) drain(out []AppMsg) []AppMsg {
	for {
		m, ok := b.pending[b.next]
		if !ok {
			return out
		}
		delete(b.pending, b.next)
		out = append(out, m)
		b.next++
	}
}

// skipGap bỏ qua chỗ trống: nhảy tới Seq nhỏ nhất đang chờ.
func (b *SeqBuffer) skipGap() []AppMsg {
	keys := make([]uint64, 0, len(b.pending))
	for k := range b.pending {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	b.next = keys[0]
	return b.drain(nil)
}

// Pending: số tin đang giữ chờ.
func (b *SeqBuffer) Pending() int { return len(b.pending) }
