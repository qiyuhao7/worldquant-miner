#!/bin/bash
# 24/7 mining daemon launcher. Logs: autopilot/daemon.log
cd "$(dirname "$0")"
while true; do
  echo "=== launch $(date -u +%FT%TZ) ===" >> daemon.log
  python3 miner_loop.py --hours 0 --auto-submit --max-submits 3 >> daemon.log 2>&1
  code=$?
  echo "=== exit code $code $(date -u +%FT%TZ) ===" >> daemon.log
  [ "$code" -eq 10 ] && break
  sleep 120
done
