package transport

import (
	"math"
	"time"
)

// CUBIC congestion control, đơn giản hóa theo RFC 9438 (bản cập nhật của RFC 8312).
//
//	slow start:  cwnd += acked                      (gấp đôi mỗi RTT) cho tới lần mất gói đầu tiên
//	mất gói:     wMax = cwnd; cwnd *= β; ssthresh = cwnd
//	sau đó:      W_cubic(t) = C·(t−K)³ + wMax,  K = ∛(wMax·(1−β)/C)    (t: giây kể từ lần mất gần nhất)
//	             W_est      = ước lượng cwnd nếu là Reno (+α gói mỗi RTT)
//	             cwnd       = max(W_cubic, W_est)    (không bao giờ tệ hơn Reno: "Reno-friendly region")
//
// Đơn vị bên trong là byte; công thức bậc 3 tính theo số gói (wMax/mss) như RFC rồi nhân lại mss.
const (
	mss        = float64(1200) // = protocol.MaxPacket
	cubicC     = 0.4
	cubicBeta  = 0.7
	cubicAlpha = 3 * (1 - cubicBeta) / (1 + cubicBeta) // ≈ 0.53: Reno cùng tốc độ trung bình với β = 0.7

	cubicInitCwnd = 10 * mss            // RFC 9002: cwnd ban đầu 10 gói
	cubicMinCwnd  = 2 * mss             // sàn: luôn có ít nhất 2 gói trên đường, nếu < 1 gói thì flush() treo
	cubicMaxCwnd  = float64(recvBufMax) // trần: không bao giờ gửi quá cửa sổ nhận 1 MB, tăng thêm là vô nghĩa
)

// cubic cài đặt congestionController theo thuật toán CUBIC ở trên. Chỉ goroutine run() dùng.
type cubic struct {
	cwnd, ssthresh, wMax float64   // byte
	wEst                 float64   // byte, ước lượng Reno trong chu kỳ hiện tại
	epoch                time.Time // bắt đầu chu kỳ bậc 3 hiện tại; zero = chưa bắt đầu
}

// newCubic bắt đầu ở slow start với cwnd = 10 gói, ssthresh = ∞.
func newCubic() *cubic {
	return &cubic{cwnd: cubicInitCwnd, ssthresh: math.Inf(1)}
}

// Window trả về cwnd hiện tại (byte).
func (c *cubic) Window() int { return int(c.cwnd) }

// Ssthresh trả -1 khi còn đang slow start (ssthresh = ∞), dùng cho cclog.
func (c *cubic) Ssthresh() int {
	if math.IsInf(c.ssthresh, 1) {
		return -1
	}
	return int(c.ssthresh)
}

// wCubic: giá trị đường bậc 3 (byte) tại t giây sau lần mất gói.
func (c *cubic) wCubic(t float64) float64 {
	wMaxSeg := c.wMax / mss
	k := math.Cbrt(wMaxSeg * (1 - cubicBeta) / cubicC) // thời điểm đường cong quay lại wMax
	return (cubicC*math.Pow(t-k, 3) + wMaxSeg) * mss
}

// OnAck tăng cwnd: slow start dưới ssthresh, sau đó theo đường CUBIC / Reno-friendly (RFC 9438).
func (c *cubic) OnAck(acked int, now time.Time) {
	a := float64(acked)
	defer func() { c.cwnd = math.Min(c.cwnd, cubicMaxCwnd) }()

	if c.cwnd < c.ssthresh { // slow start: mỗi byte được ACK thì cwnd thêm 1 byte -> gấp đôi mỗi RTT
		c.cwnd += a
		return
	}

	if c.epoch.IsZero() { // ACK đầu tiên sau lần mất gói: bắt đầu chu kỳ mới
		c.epoch = now
		c.wEst = c.cwnd
	}

	// Reno-friendly: sau 1 RTT (tổng acked ≈ cwnd) thì wEst tăng α gói
	c.wEst += cubicAlpha * mss * a / c.cwnd

	cub := c.wCubic(now.Sub(c.epoch).Seconds())
	if cub < c.wEst {
		c.cwnd = math.Max(c.cwnd, c.wEst) // vùng Reno-friendly: RTT ngắn / cửa sổ nhỏ thì Reno tăng nhanh hơn
		return
	}

	// vùng CUBIC: tiến dần về đường cong; giới hạn 1.5·cwnd mỗi RTT như RFC để không tăng vọt
	target := math.Min(cub, 1.5*c.cwnd)
	if target > c.cwnd {
		c.cwnd += (target - c.cwnd) * a / c.cwnd // sau 1 RTT thì cwnd ≈ target
	}
}

// OnLoss xử lý 1 đợt mất gói: wMax = cwnd, cwnd = β·cwnd (β = 0.7), bắt đầu chu kỳ CUBIC mới.
func (c *cubic) OnLoss(now time.Time) {
	c.wMax = c.cwnd
	c.cwnd = math.Max(c.cwnd*cubicBeta, cubicMinCwnd)
	c.ssthresh = c.cwnd
	c.epoch = time.Time{} // bắt đầu chu kỳ bậc 3 mới ở ACK kế tiếp
}
