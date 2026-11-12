package transport

import "time"

const (
	initialRTO = 500 * time.Millisecond // chưa có mẫu RTT nào
	// granularity: sàn của 4·RTTVAR và của ngưỡng mất theo thời gian (RFC 9002 kGranularity).
	// Mạng rất đều (RTT gần như không dao động) thì RTTVAR -> 0; không có sàn thì RTO ≈ SRTT và gói nào ACK về
	// chậm hơn trung bình một chút cũng bị coi là mất oan.
	granularity = time.Millisecond
	// maxAckDelay: thời gian tối đa bên nhận có thể giữ ACK. Bên nhận của ta ACK ngay trong lần flush kế tiếp,
	// đây là khoảng dự phòng cho việc goroutine run() bận / bị lập lịch trễ (bằng 1 tick).
	maxAckDelay = tickInterval
)

// rttEstimator ước lượng RTT từ các mẫu đo (thời gian từ lúc gửi gói tới lúc nhận ACK của nó) theo RFC 6298.
type rttEstimator struct {
	srtt, rttvar time.Duration
	latest       time.Duration // mẫu RTT gần nhất, dùng cho ngưỡng mất theo thời gian
}

// update thêm 1 mẫu RTT. Mẫu đầu tiên: SRTT = mẫu, RTTVAR = mẫu/2; sau đó trung bình trượt:
//
//	RTTVAR = 3/4·RTTVAR + 1/4·|SRTT − mẫu|
//	SRTT   = 7/8·SRTT   + 1/8·mẫu
func (r *rttEstimator) update(sample time.Duration) {
	r.latest = sample
	if r.srtt == 0 {
		r.srtt, r.rttvar = sample, sample/2
		return
	}

	diff := r.srtt - sample
	if diff < 0 {
		diff = -diff
	}

	r.rttvar = (3*r.rttvar + diff) / 4 // β = 1/4 (cập nhật RTTVAR trước, dùng SRTT cũ, đúng như RFC)
	r.srtt = (7*r.srtt + sample) / 8   // α = 1/8
}

// rto tính thời gian chờ ACK tối đa trước khi coi gói là mất. SRTT/RTTVAR theo RFC 6298, nhưng thay "tối thiểu 1 s" của TCP bằng công thức PTO của QUIC (RFC 9002 mục 6.2):
//
//	RTO = SRTT + max(4·RTTVAR, granularity) + maxAckDelay
//
// TCP cần sàn 1 s (Linux: 200 ms) vì bên nhận TCP được trì hoãn ACK tới 200–500 ms. Bên nhận của ta không trì hoãn,
// nên trên loopback RTO chỉ khoảng 11 ms thay vì 200 ms -> mất gói ở đuôi được gửi lại nhanh hơn rất nhiều.
func (r *rttEstimator) rto() time.Duration {
	if r.srtt == 0 {
		return initialRTO
	}

	return r.srtt + max(4*r.rttvar, granularity) + maxAckDelay
}

// lossDelay trả về ngưỡng mất theo thời gian: gói đã bị 1 gói gửi SAU nó "vượt mặt" (được ACK) mà vẫn chưa được ACK sau 9/8 RTT thì coi là mất
// (RFC 9002 mục 6.1.2). Hệ số 9/8 chừa chỗ cho dao động RTT nhỏ.
func (r *rttEstimator) lossDelay() time.Duration {
	return max(9*max(r.srtt, r.latest)/8, granularity)
}
