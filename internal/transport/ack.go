package transport

import "github.com/phongphanhai06/go-collab/internal/protocol"

const ackHistory = 4096 // remember/ack 4096 closest packet numbers

type ackTracker struct {
	seen    map[uint32]struct{}
	largest uint32
	any     bool // receive any packets?
}

func newAcKTracker() ackTracker {
	return ackTracker{
		seen: map[uint32]struct{}{},
	}
}

// mark pn received, false if the packet is already received
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

// ranges returns maximum "maxRanges" consecutive periods, scan backward from the largest packet
// 1–5, 7–9, 12 → [12..12] [7..9] [1..5]
func (t *ackTracker) ranges(maxRanges int) []protocol.AckRange {
	if !t.any {
		return nil
	}

	var out []protocol.AckRange
	pn := int64(t.largest)
	low := int64(t.largest) - ackHistory // convert before subtracting, otherwise uint32 underflows when largest < 4096

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

// prune deletes "pn"s that are too old to prevent map's infinite growth, call regularly
// The old packet that are retransmitted is considered "new", but recvStream filters it anyway
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
