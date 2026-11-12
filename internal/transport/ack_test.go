package transport

import (
	"reflect"
	"testing"

	"github.com/phongphanhai06/go-collab/internal/protocol"
)

func TestAckTrackerRanges(t *testing.T) {
	tr := newAcKTracker()
	for _, pn := range []uint32{1, 2, 3, 4, 5, 7, 8, 9, 12} {
		if !tr.add(pn) {
			t.Fatalf("add(%d) lần đầu phải trả true", pn)
		}
	}

	want := []protocol.AckRange{{Start: 12, End: 12}, {Start: 7, End: 9}, {Start: 1, End: 5}}
	if got := tr.ranges(8); !reflect.DeepEqual(got, want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}
}

func TestAckTrackerDuplicate(t *testing.T) {
	tr := newAcKTracker()
	tr.add(5)
	if tr.add(5) {
		t.Fatal("gói trùng phải trả false")
	}
}

func TestAckTrackerEmptyAndZero(t *testing.T) {
	tr := newAcKTracker()
	if tr.ranges(8) != nil {
		t.Fatal("chưa nhận gì thì không có range")
	}
	tr.add(0) // largest < ackHistory: trước đây từng underflow uint32
	if got := tr.ranges(8); !reflect.DeepEqual(got, []protocol.AckRange{{Start: 0, End: 0}}) {
		t.Fatalf("ranges = %v", got)
	}
}

func TestAckTrackerMaxRanges(t *testing.T) {
	tr := newAcKTracker()
	for pn := uint32(0); pn <= 20; pn += 2 { // 0, 2, 4, ..., 20: 11 range rời nhau
		tr.add(pn)
	}
	got := tr.ranges(3)
	want := []protocol.AckRange{{Start: 20, End: 20}, {Start: 18, End: 18}, {Start: 16, End: 16}} // ưu tiên các gói mới nhất
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}
}

func TestAckTrackerHistoryLimit(t *testing.T) {
	tr := newAcKTracker()
	tr.add(1)
	for pn := uint32(10000); pn <= 10010; pn++ {
		tr.add(pn)
	}
	// gói 1 cách xa hơn ackHistory -> không quét tới nữa
	want := []protocol.AckRange{{Start: 10000, End: 10010}}
	if got := tr.ranges(8); !reflect.DeepEqual(got, want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}
}

func TestAckTrackerPrune(t *testing.T) {
	tr := newAcKTracker()
	for pn := uint32(0); pn < 3*ackHistory; pn++ {
		tr.add(pn)
	}
	tr.prune()
	if len(tr.seen) > ackHistory+1 {
		t.Fatalf("prune còn giữ %d pn", len(tr.seen))
	}
	if _, ok := tr.seen[tr.largest]; !ok {
		t.Fatal("prune xóa nhầm gói mới nhất")
	}
}
