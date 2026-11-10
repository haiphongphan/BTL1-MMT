package transport

import "time"

const (
	initialRTO = 500 * time.Millisecond
	minRTO = 200 * time.Millisecond
)

type rttEstimator struct {
	srtt, rttvar time.Duration
}

func (r *rttEstimator) update(sample time.Duration) {
	if r.srtt == 0 {
		r.srtt, r.rttvar = sample, sample/2
		return
	}

	diff := r.srtt - sample
	if diff < 0 {
		diff = -diff
	}

	r.rttvar = (3*r.rttvar + diff) / 4 // β = 1/4
	r.srtt = (7*r.srtt + sample) / 8 // α = 1/8
}

func (r *rttEstimator) rto() time.Duration {
	if r.srtt == 0 {
		return initialRTO
	}

	return max(r.srtt + 4 * r.rttvar, minRTO)
}