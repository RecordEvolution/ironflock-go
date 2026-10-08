#!/usr/bin/env bash
# Starts a local Crossbar router (port 18081) with config.json and the fake
# platform, for the integration tests.
#
#   CROSSBAR=crossbar PYTHON=python3 ./start.sh
#
# State (node directory, logs, PIDs) goes to $STATE_DIR (default ./.run).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-$HERE/.run}"
CROSSBAR="${CROSSBAR:-crossbar}"
PY="${PYTHON:-python3}"

up() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

mkdir -p "$STATE_DIR/node/.crossbar"
cp "$HERE/config.json" "$STATE_DIR/node/.crossbar/config.json"

if ! up 18081; then
  (cd "$STATE_DIR/node" && nohup "$CROSSBAR" start --cbdir .crossbar > "$STATE_DIR/crossbar.log" 2>&1 &)
  for i in $(seq 1 150); do up 18081 && break; sleep 0.2; done
  up 18081 || { echo "crossbar did not start:" >&2; tail -30 "$STATE_DIR/crossbar.log" >&2; exit 1; }
fi

if [ -n "${SKIP_FAKE_PLATFORM:-}" ]; then exit 0; fi
(
  cd "$HERE/.."
  nohup "$PY" -I fake_platform.py > "$STATE_DIR/fake_platform.log" 2>&1 &
  echo $! > "$STATE_DIR/fake_platform.pid"
)
for i in $(seq 1 150); do
  [ "$(grep -c "realm ready" "$STATE_DIR/fake_platform.log" 2>/dev/null || true)" -ge 2 ] && break
  sleep 0.2
done
[ "$(grep -c "realm ready" "$STATE_DIR/fake_platform.log")" -ge 2 ] || { echo "fake platform not ready:" >&2; cat "$STATE_DIR/fake_platform.log" >&2; exit 1; }
echo "crossbar ws://localhost:18081/ws-ua-usr, fake platform pid $(cat "$STATE_DIR/fake_platform.pid")"
