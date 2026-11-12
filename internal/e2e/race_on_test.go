//go:build race

package e2e

// Race detector làm chậm 5–10 lần và tranh CPU: số đo độ trễ không còn ý nghĩa, chỉ ghi log.
const raceEnabled = true
