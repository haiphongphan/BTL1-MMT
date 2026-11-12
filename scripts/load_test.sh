#!/usr/bin/env bash
# Kịch bản test 9: N bot chat liên tục trong DURATION giây, theo dõi số goroutine / RAM của server.
# Tự chạy server riêng trên cổng khác (mặc định 9100/9101) nên không đụng server đang chạy.
#
#   ./scripts/load_test.sh            # 20 bot × 300 giây (5 phút)
#   ./scripts/load_test.sh 20 60      # 20 bot × 60 giây
#
# Kết quả: logs/load_server.log (dòng [stats] mỗi 10 giây) + bảng tóm tắt in ra cuối.
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-20}
DURATION=${2:-300}
TCP=${TCP:-9100}
UDP=${UDP:-9101}

mkdir -p bin logs
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client

./bin/server -tcp ":$TCP" -udp-port "$UDP" -stats 10s > logs/load_server.log 2>&1 &
SERVER_PID=$!
sleep 1

pids=()
for i in $(seq 1 "$N"); do
  ./bin/client -server "127.0.0.1:$TCP" -user "user$i" -pass 123456 -web "" -bot > "logs/load_client$i.log" 2>&1 &
  pids+=($!)
done
echo "Server PID $SERVER_PID, $N bot đang chạy $DURATION giây..."

stop() {
  kill -INT "${pids[@]}" 2>/dev/null || true
  wait "${pids[@]}" 2>/dev/null || true
  sleep 2 # cho server kịp dọn session và in thêm 1 dòng [stats]
  kill -INT "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap 'stop; exit 130' INT TERM

sleep "$DURATION"
# chụp số liệu lúc còn đầy đủ bot
CPU=$(ps -o %cpu= -p "$SERVER_PID" | tr -d ' ')
RSS=$(ps -o rss= -p "$SERVER_PID" | tr -d ' ')
stop

echo
echo "===== Kết quả ($N bot × ${DURATION}s) ====="
echo "Server CPU trung bình: ${CPU}%   RSS cuối: $((RSS / 1024)) MB"
echo "--- [stats] của server (goroutine không được tăng mãi) ---"
grep '\[stats\]' logs/load_server.log | sed 's/^.*\[stats\] /  /'
echo "--- Bot (dòng thống kê cuối của mỗi bot) ---"
sent=0; recv=0
for i in $(seq 1 "$N"); do
  line=$(grep '\[bot ' "logs/load_client$i.log" | tail -1 || true)
  s=$(sed -n 's/.*sent=\([0-9]*\).*/\1/p' <<<"$line"); r=$(sed -n 's/.*recv=\([0-9]*\).*/\1/p' <<<"$line")
  sent=$((sent + ${s:-0})); recv=$((recv + ${r:-0}))
done
echo "  Tổng tin bot gửi: $sent   tổng tin bot nhận: $recv   (lý tưởng: nhận ≈ gửi × $((N - 1)))"
echo "  Panic trong log server: $(grep -c -i panic logs/load_server.log || true)"
echo "  Client bị ngắt vì hàng đợi đầy: $(grep -c 'đầy' logs/load_server.log || true)"
