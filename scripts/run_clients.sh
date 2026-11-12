#!/usr/bin/env bash
# Chạy N client ở chế độ bot để test tải (Step 4, 10).
#   ./scripts/run_clients.sh 20            # 20 bot, server 127.0.0.1:9000
#   SERVER=192.168.1.5:9000 LOSS=0.1 ./scripts/run_clients.sh 20
# Yêu cầu users.json có user1..userN (tạo bằng: go run ./cmd/mkusers -n 20).
# Theo dõi phía server: dòng log "[Runtime] goroutines=..." mỗi 10s phải ổn định, không tăng mãi.
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-10}
SERVER=${SERVER:-127.0.0.1:9000}
LOSS=${LOSS:-0}
PASS=${PASS:-123456}

mkdir -p bin logs
go build -o bin/client ./cmd/client

for i in $(seq 1 "$N"); do
  ./bin/client -server "$SERVER" -user "user$i" -pass "$PASS" -loss "$LOSS" \
    -web "127.0.0.1:$((8100 + i))" -bot < /dev/null > "logs/client$i.log" 2>&1 &
done

echo "Đã chạy $N client bot (web UI ở cổng 8101..$((8100 + N))). Log: logs/clientN.log. Ctrl+C để dừng."
trap 'kill $(jobs -p) 2>/dev/null' INT TERM
wait
