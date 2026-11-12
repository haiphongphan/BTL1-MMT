#!/usr/bin/env bash
# Chạy N client ở chế độ bot (tự gửi chat mỗi 1–3 giây) tới 1 server đang chạy.
#
#   ./scripts/run_clients.sh 20                      # 20 bot user1..user20 → 127.0.0.1:9000
#   ./scripts/run_clients.sh 20 192.168.1.10:9000    # server ở máy khác
#   LOSS=0.1 JITTER=20ms ./scripts/run_clients.sh 5  # bot chạy trên mạng giả lập xấu
#
# Tài khoản bot user1..user30 (mật khẩu 123456) có sẵn trong users.json.
# Log mỗi bot: logs/clientN.log. Ctrl+C để dừng tất cả.
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-10}
SERVER=${2:-127.0.0.1:9000}
LOSS=${LOSS:-0}
JITTER=${JITTER:-0s}

if (( N > 30 )); then
  echo "Tối đa 30 bot (users.json chỉ có user1..user30)" >&2
  exit 1
fi

mkdir -p bin logs
go build -o bin/client ./cmd/client

pids=()
cleanup() {
  echo
  echo "Dừng ${#pids[@]} bot..."
  kill -INT "${pids[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup INT TERM

for i in $(seq 1 "$N"); do
  ./bin/client -server "$SERVER" -user "user$i" -pass 123456 -web "" -bot \
    -loss "$LOSS" -jitter "$JITTER" > "logs/client$i.log" 2>&1 &
  pids+=($!)
done

echo "Đã chạy $N bot tới $SERVER (log: logs/clientN.log). Nhấn Ctrl+C để dừng."
wait
