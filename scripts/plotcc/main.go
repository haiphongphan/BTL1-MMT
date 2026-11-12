// plotcc vẽ đồ thị cwnd theo thời gian từ file CSV do cờ -cclog (hoặc BTL_CCLOG trong test) ghi ra.
// Chỉ dùng thư viện chuẩn, xuất SVG (mở bằng trình duyệt, chèn được vào báo cáo).
//
//	go run ./scripts/plotcc cwnd.csv > cwnd.svg
//	go run ./scripts/plotcc -conn 1001 -from 0 -to 20 cwnd.csv > cwnd.svg
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type point struct {
	t, cwnd, ssthresh float64 // giây, KB, KB (-1 = slow start)
	loss              bool
}

func main() {
	connFilter := flag.Int("conn", -1, "chỉ vẽ conn_id này (mặc định: mọi conn có dữ liệu)")
	from := flag.Float64("from", 0, "bắt đầu từ giây thứ")
	to := flag.Float64("to", math.Inf(1), "tới giây thứ")
	title := flag.String("title", "CUBIC: cwnd theo thời gian", "tiêu đề đồ thị")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("cách dùng: go run ./scripts/plotcc [-conn ID] [-from s] [-to s] file.csv > cwnd.svg")
	}

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	series, err := read(f, *connFilter, *from, *to)
	if err != nil {
		log.Fatal(err)
	}
	if len(series) == 0 {
		log.Fatal("không có dữ liệu để vẽ")
	}
	render(os.Stdout, series, *title)
}

// read: CSV header time_ms,conn_id,event,cwnd,ssthresh,rtt_us,rto_ms,inflight,sent,lost
func read(r io.Reader, connFilter int, from, to float64) (map[int][]point, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	col := map[string]int{}
	for i, h := range header {
		col[h] = i
	}
	for _, need := range []string{"time_ms", "conn_id", "event", "cwnd", "ssthresh"} {
		if _, ok := col[need]; !ok {
			return nil, fmt.Errorf("thiếu cột %q", need)
		}
	}

	out := map[int][]point{}
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ms, _ := strconv.ParseFloat(rec[col["time_ms"]], 64)
		id, _ := strconv.Atoi(rec[col["conn_id"]])
		cwnd, _ := strconv.ParseFloat(rec[col["cwnd"]], 64)
		ss, _ := strconv.ParseFloat(rec[col["ssthresh"]], 64)
		t := ms / 1000
		if (connFilter >= 0 && id != connFilter) || t < from || t > to {
			continue
		}
		if ss >= 0 {
			ss /= 1024
		}
		out[id] = append(out[id], point{t: t, cwnd: cwnd / 1024, ssthresh: ss, loss: rec[col["event"]] == "loss"})
	}

	// conn chỉ có vài dòng (vd chiều chỉ gửi ACK) thì bỏ, đồ thị đỡ rối
	for id, pts := range out {
		if len(pts) < 10 {
			delete(out, id)
		}
	}
	return out, nil
}

const (
	width, height          = 1100.0, 460.0
	left, right, top, bot  = 70.0, 20.0, 50.0, 55.0
	plotW, plotH           = width - left - right, height - top - bot
	fontFamily             = "system-ui, -apple-system, Segoe UI, sans-serif"
	gridColor, axisColor   = "#e3e3e3", "#555"
	lossColor, threshColor = "#d43d3d", "#999"
)

var palette = []string{"#2a6fdb", "#1f9e6e", "#c27c0e", "#8e44ad", "#16a2b8"}

// niceStep chọn bước chia trục "đẹp" (1, 2, 5 × 10^n) cho khoảng span với khoảng n vạch.
func niceStep(span float64, n int) float64 {
	raw := span / float64(n)
	mag := math.Pow(10, math.Floor(math.Log10(raw)))
	for _, m := range []float64{1, 2, 5, 10} {
		if m*mag >= raw {
			return m * mag
		}
	}
	return 10 * mag
}

func render(w io.Writer, series map[int][]point, title string) {
	ids := make([]int, 0, len(series))
	t0, t1, yMax := math.Inf(1), math.Inf(-1), 0.0
	for id, pts := range series {
		ids = append(ids, id)
		for _, p := range pts {
			t0, t1 = math.Min(t0, p.t), math.Max(t1, p.t)
			yMax = math.Max(yMax, p.cwnd)
		}
	}
	sort.Ints(ids)
	if t1 <= t0 {
		t1 = t0 + 1
	}
	yStep := niceStep(yMax, 6)
	yTop := math.Ceil(yMax/yStep) * yStep
	xStep := niceStep(t1-t0, 10)

	x := func(t float64) float64 { return left + (t-t0)/(t1-t0)*plotW }
	y := func(v float64) float64 { return top + plotH - v/yTop*plotH }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %.0f %.0f" font-family="%s" font-size="12">`+"\n", width, height, fontFamily)
	fmt.Fprintf(&b, `<rect width="100%%" height="100%%" fill="#fff"/>`+"\n")
	fmt.Fprintf(&b, `<text x="%.0f" y="28" font-size="16" font-weight="600" fill="#222">%s</text>`+"\n", left, esc(title))

	// lưới + nhãn trục
	for v := 0.0; v <= yTop+1e-9; v += yStep {
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="%s"/>`+"\n", left, left+plotW, y(v), y(v), gridColor)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="end" fill="%s">%s</text>`+"\n", left-8, y(v)+4, axisColor, trim(v))
	}
	for t := math.Ceil(t0/xStep) * xStep; t <= t1+1e-9; t += xStep {
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="%s"/>`+"\n", x(t), x(t), top, top+plotH, gridColor)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="%s">%s</text>`+"\n", x(t), top+plotH+18, axisColor, trim(t))
	}
	fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="%s"/>`+"\n", left, left+plotW, top+plotH, top+plotH, axisColor)
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="%s">thời gian (s)</text>`+"\n", left+plotW/2, height-12, axisColor)
	fmt.Fprintf(&b, `<text transform="translate(18 %.1f) rotate(-90)" text-anchor="middle" fill="%s">cwnd (KB)</text>`+"\n", top+plotH/2, axisColor)

	// legend
	lx := left + plotW - 10
	legend := []struct{ color, label string }{{lossColor, "mất gói (OnLoss)"}, {threshColor, "ssthresh"}}
	for i, id := range ids {
		legend = append([]struct{ color, label string }{{palette[i%len(palette)], fmt.Sprintf("cwnd conn %d", id)}}, legend...)
	}
	for i := len(legend) - 1; i >= 0; i-- {
		l := legend[i]
		tw := 7.0 * float64(len([]rune(l.label)))
		lx -= tw + 30
		fmt.Fprintf(&b, `<rect x="%.1f" y="18" width="14" height="4" fill="%s"/><text x="%.1f" y="24" fill="#333">%s</text>`+"\n", lx, l.color, lx+19, esc(l.label))
	}

	for i, id := range ids {
		pts := series[id]
		color := palette[i%len(palette)]

		var ss strings.Builder // ssthresh: nét đứt, chỉ vẽ khi đã thoát slow start
		for j, p := range pts {
			if p.ssthresh < 0 {
				continue
			}
			cmd := "L"
			if j == 0 || pts[j-1].ssthresh < 0 {
				cmd = "M"
			}
			fmt.Fprintf(&ss, "%s%.1f %.1f ", cmd, x(p.t), y(math.Min(p.ssthresh, yTop)))
		}
		if ss.Len() > 0 {
			fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="1" stroke-dasharray="4 3"/>`+"\n", ss.String(), threshColor)
		}

		var line strings.Builder
		for j, p := range pts {
			cmd := "L"
			if j == 0 {
				cmd = "M"
			}
			fmt.Fprintf(&line, "%s%.1f %.1f ", cmd, x(p.t), y(p.cwnd))
		}
		fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="1.6" stroke-linejoin="round"/>`+"\n", line.String(), color)

		for j, p := range pts { // điểm mất gói: đỉnh răng cưa, ngay TRƯỚC khi cwnd tụt
			if p.loss && j > 0 {
				fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="2.8" fill="%s"/>`+"\n", x(p.t), y(pts[j-1].cwnd), lossColor)
			}
		}
	}
	b.WriteString("</svg>\n")
	io.WriteString(w, b.String())
}

func trim(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
