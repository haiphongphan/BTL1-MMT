package transport

import "github.com/phongphanhai06/go-collab/internal/protocol"

// ackHistory: chỉ nhớ (và ACK) 4096 packet_num gần packet_num lớn nhất đã nhận.
const ackHistory = 4096

// ackTracker là phía nhận của ACK: nhớ các packet_num đã nhận để (1) lọc gói trùng và (2) tạo các
// AckRange gửi về bên gửi. Chỉ goroutine run() dùng nên không cần khóa.
type ackTracker struct {
	seen    map[uint32]struct{} // các packet_num đã nhận
	largest uint32              // packet_num lớn nhất đã nhận
	any     bool                // đã nhận được gói nào chưa (largest = 0 chưa chắc là đã nhận gói 0)
}

// newAckTracker tạo ackTracker rỗng.
func newAckTracker() ackTracker {
	return ackTracker{
		seen: map[uint32]struct{}{},
	}
}

// add đánh dấu đã nhận gói pn; trả về false nếu gói này đã nhận trước đó (gói trùng).
func (t *ackTracker) add(pn uint32) bool {
	if _, ok := t.seen[pn]; ok {
		return false
	}

	t.seen[pn] = struct{}{}
	if !t.any || pn > t.largest {
		t.largest, t.any = pn, true
	}

	return true
}

// ranges trả về tối đa maxRanges đoạn packet_num liên tục đã nhận, quét lùi từ gói lớn nhất
// (nên đoạn mới nhất đứng đầu). Ví dụ đã nhận 1–5, 7–9, 12 → [12..12] [7..9] [1..5].
// Chỉ quét trong ackHistory gói gần nhất; gói cũ hơn thì bên gửi đã coi là mất và gửi lại từ lâu.
func (t *ackTracker) ranges(maxRanges int) []protocol.AckRange {
	if !t.any {
		return nil
	}

	var out []protocol.AckRange
	pn := int64(t.largest)
	low := int64(t.largest) - ackHistory // đổi sang int64 trước khi trừ, nếu không uint32 bị tràn ngược khi largest < 4096

	for pn >= 0 && pn > low && len(out) < maxRanges {
		if _, ok := t.seen[uint32(pn)]; !ok {
			pn--
			continue
		}

		end := pn
		for pn >= 0 {
			if _, ok := t.seen[uint32(pn)]; !ok {
				break
			}
			pn--
		}
		out = append(out, protocol.AckRange{
			Start: uint32(pn + 1),
			End:   uint32(end),
		})
	}

	return out
}

// prune xóa các packet_num quá cũ để map không phình mãi; gọi định kỳ (mỗi tick).
// Sau khi xóa, nếu gói cũ đó tới lại thì bị coi là gói mới, nhưng không sao: recvStream sẽ bỏ dữ liệu trùng.
func (t *ackTracker) prune() {
	if len(t.seen) < 2*ackHistory {
		return
	}

	low := t.largest - ackHistory
	for pn := range t.seen {
		if pn < low {
			delete(t.seen, pn)
		}
	}
}
