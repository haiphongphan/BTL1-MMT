package protocol

import (
	"math/rand/v2"
	"testing"
)

func seqs(ms []AppMsg) []uint64 {
	out := make([]uint64, len(ms))
	for i, m := range ms {
		out[i] = m.Seq
	}
	return out
}

func TestSeqBufferReorders(t *testing.T) {
	var b SeqBuffer
	if got := b.Push(AppMsg{Seq: 2}); len(got) != 0 {
		t.Fatalf("tin 2 tới trước tin 1 phải được giữ lại, got %v", seqs(got))
	}
	if got := b.Push(AppMsg{Seq: 3}); len(got) != 0 {
		t.Fatalf("got %v", seqs(got))
	}
	got := b.Push(AppMsg{Seq: 1})
	if s := seqs(got); len(s) != 3 || s[0] != 1 || s[1] != 2 || s[2] != 3 {
		t.Fatalf("muốn [1 2 3], got %v", s)
	}
	if b.Pending() != 0 {
		t.Fatalf("pending = %d", b.Pending())
	}
}

func TestSeqBufferDuplicatesAndUnordered(t *testing.T) {
	var b SeqBuffer
	b.Push(AppMsg{Seq: 1})
	if got := b.Push(AppMsg{Seq: 1}); len(got) != 0 {
		t.Fatal("tin trùng phải bị bỏ")
	}
	if got := b.Push(AppMsg{Seq: 0, Text: "x"}); len(got) != 1 {
		t.Fatal("Seq 0 phải được giao ngay")
	}
}

func TestSeqBufferRandomPermutation(t *testing.T) {
	const n = 1000
	perm := rand.Perm(n)
	var b SeqBuffer
	var out []uint64
	for _, i := range perm {
		out = append(out, seqs(b.Push(AppMsg{Seq: uint64(i + 1)}))...)
	}
	if len(out) != n {
		t.Fatalf("giao %d/%d tin", len(out), n)
	}
	for i, s := range out {
		if s != uint64(i+1) {
			t.Fatalf("vị trí %d = %d", i, s)
		}
	}
}

// Một tin bị mất hẳn (lỗi ở đâu đó): không được treo mãi, sau MaxSeqPending tin thì bỏ qua chỗ trống.
func TestSeqBufferSkipsPermanentGap(t *testing.T) {
	var b SeqBuffer
	var out []uint64
	for i := uint64(2); i <= MaxSeqPending+2; i++ { // thiếu Seq 1
		out = append(out, seqs(b.Push(AppMsg{Seq: i}))...)
	}
	if len(out) != MaxSeqPending+1 || out[0] != 2 {
		t.Fatalf("giao %d tin, đầu tiên %v", len(out), out[:min(1, len(out))])
	}
	if b.Pending() != 0 {
		t.Fatalf("pending = %d", b.Pending())
	}
}
