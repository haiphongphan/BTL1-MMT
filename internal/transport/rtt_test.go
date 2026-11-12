package transport

import (
	"testing"
	"time"
)

func TestRTTInitial(t *testing.T) {
	var r rttEstimator
	if r.rto() != initialRTO {
		t.Fatalf("rto = %v, want %v", r.rto(), initialRTO)
	}
}

func TestRTTFirstSampleAndUpdate(t *testing.T) {
	var r rttEstimator
	r.update(100 * time.Millisecond) // mẫu đầu: SRTT = R, RTTVAR = R/2
	if r.srtt != 100*time.Millisecond || r.rttvar != 50*time.Millisecond {
		t.Fatalf("srtt=%v rttvar=%v", r.srtt, r.rttvar)
	}
	if r.rto() != 300*time.Millisecond+maxAckDelay { // 100 + 4·50 + maxAckDelay
		t.Fatalf("rto = %v, want 310ms", r.rto())
	}

	r.update(200 * time.Millisecond) // RTTVAR = 3/4·50 + 1/4·|100−200| = 62.5; SRTT = 7/8·100 + 1/8·200 = 112.5
	if r.srtt != 112500*time.Microsecond || r.rttvar != 62500*time.Microsecond {
		t.Fatalf("srtt=%v rttvar=%v", r.srtt, r.rttvar)
	}
}

// Mạng rất đều: RTTVAR -> 0 nhưng RTO vẫn phải chừa khoảng đệm, nếu không sẽ timeout oan.
func TestRTOFloorWhenRTTStable(t *testing.T) {
	var r rttEstimator
	for i := 0; i < 100; i++ {
		r.update(40 * time.Millisecond)
	}
	if r.rttvar > time.Microsecond {
		t.Fatalf("rttvar = %v, want ≈ 0", r.rttvar)
	}
	if want := 40*time.Millisecond + granularity + maxAckDelay; r.rto() != want {
		t.Fatalf("rto = %v, want %v", r.rto(), want)
	}
}

func TestLossDelay(t *testing.T) {
	var r rttEstimator
	r.update(80 * time.Millisecond)
	if r.lossDelay() != 90*time.Millisecond { // 9/8 · 80
		t.Fatalf("lossDelay = %v", r.lossDelay())
	}
	var tiny rttEstimator
	tiny.update(100 * time.Microsecond)
	if tiny.lossDelay() != granularity {
		t.Fatalf("lossDelay = %v, want sàn %v", tiny.lossDelay(), granularity)
	}
}
