package transport

import (
	"math"
	"testing"
	"time"
)

func TestCubicSlowStartDoubles(t *testing.T) {
	c := newCubic()
	if c.Window() != int(cubicInitCwnd) {
		t.Fatalf("cwnd đầu = %d", c.Window())
	}
	now := time.Now()
	for rtt := 0; rtt < 3; rtt++ {
		before := c.Window()
		c.OnAck(before, now) // cả cửa sổ được ACK trong 1 RTT
		if c.Window() != 2*before {
			t.Fatalf("RTT %d: %d -> %d, want gấp đôi", rtt, before, c.Window())
		}
	}
	if c.Ssthresh() != -1 {
		t.Fatal("chưa mất gói thì ssthresh phải là ∞ (-1)")
	}
}

func TestCubicOnLoss(t *testing.T) {
	c := newCubic()
	c.cwnd = 100 * mss
	c.OnLoss(time.Now())
	if c.wMax != 100*mss || c.cwnd != 70*mss || c.ssthresh != 70*mss {
		t.Fatalf("wMax=%v cwnd=%v ssthresh=%v", c.wMax/mss, c.cwnd/mss, c.ssthresh/mss)
	}
}

// Hình dạng đường bậc 3: W(0) = 0.7·wMax, W(K) = wMax, nửa đầu tăng nhanh hơn nửa sau (lõm), sau K thì vượt wMax.
func TestCubicCurveShape(t *testing.T) {
	c := newCubic()
	c.wMax = 100 * mss
	k := math.Cbrt(100 * (1 - cubicBeta) / cubicC) // ≈ 4.22 s

	w := func(t float64) float64 { return c.wCubic(t) / mss }
	if math.Abs(w(0)-70) > 0.01 || math.Abs(w(k)-100) > 0.01 {
		t.Fatalf("W(0) = %.2f, W(K) = %.2f; want 70, 100", w(0), w(k))
	}
	if w(k/2)-w(0) <= w(k)-w(k/2) {
		t.Fatal("nửa đầu phải tăng nhanh hơn nửa sau")
	}
	if w(2*k) <= 100 {
		t.Fatal("sau K phải vượt wMax")
	}
}

// simulate: mỗi bước = 1 RTT, cả cửa sổ được ACK. Trả cwnd (gói) tại t0+d.
func simulate(c *cubic, t0 time.Time, d, rtt time.Duration) float64 {
	for now := t0; now.Before(t0.Add(d)); now = now.Add(rtt) {
		c.OnAck(c.Window(), now)
	}
	return c.cwnd / mss
}

// Cửa sổ lớn, RTT dài: đường bậc 3 thắng -> sau K giây cwnd ≈ wMax.
func TestCubicRegion(t *testing.T) {
	c := newCubic()
	c.cwnd = 600 * mss
	t0 := time.Unix(1000, 0)
	c.OnLoss(t0)
	k := math.Cbrt(600 * (1 - cubicBeta) / cubicC) // ≈ 7.66 s
	kDur := time.Duration(k * float64(time.Second))

	got := simulate(c, t0, kDur, 100*time.Millisecond)
	if math.Abs(got-600) > 0.05*600 {
		t.Fatalf("t=K: cwnd = %.1f gói, want ≈ wMax 600", got)
	}
	if c.wEst/mss > 500 {
		t.Fatalf("wEst = %.1f: Reno lẽ ra phải chậm hơn nhiều", c.wEst/mss)
	}
}

// Cửa sổ nhỏ, RTT ngắn: Reno tăng nhanh hơn đường bậc 3 -> cwnd đi theo wEst (+α gói mỗi RTT).
func TestCubicRenoFriendlyRegion(t *testing.T) {
	c := newCubic()
	c.cwnd = 20 * mss
	t0 := time.Unix(1000, 0)
	c.OnLoss(t0) // cwnd = 14 gói

	got := simulate(c, t0, time.Second, 10*time.Millisecond) // 100 RTT
	want := 14 + 100*cubicAlpha                              // ≈ 67 gói
	if math.Abs(got-want) > 2 {
		t.Fatalf("cwnd = %.1f gói, want ≈ %.1f (Reno-friendly)", got, want)
	}
	if got <= c.wCubic(1)/mss {
		t.Fatal("trong vùng Reno-friendly cwnd phải lớn hơn đường bậc 3")
	}
}

func TestCubicFloorAndCap(t *testing.T) {
	c := newCubic()
	for i := 0; i < 50; i++ {
		c.OnLoss(time.Now())
	}
	if c.cwnd != cubicMinCwnd {
		t.Fatalf("cwnd = %v, want sàn %v", c.cwnd, cubicMinCwnd)
	}

	c = newCubic()
	for i := 0; i < 30; i++ {
		c.OnAck(c.Window(), time.Now()) // slow start không giới hạn -> phải bị chặn trần
	}
	if c.cwnd != cubicMaxCwnd {
		t.Fatalf("cwnd = %v, want trần %v", c.cwnd, cubicMaxCwnd)
	}
}
