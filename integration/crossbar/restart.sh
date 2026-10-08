#!/usr/bin/env bash
# Restarts only the Crossbar router — a ~4 s outage the fake platform and the
# SDK under test have to ride out (IRONFLOCK_TEST_RESTART_ROUTER).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-$HERE/.run}"
CROSSBAR="${CROSSBAR:-crossbar}"
up() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

(cd "$STATE_DIR/node" && "$CROSSBAR" stop --cbdir .crossbar >/dev/null 2>&1 || true)
for i in $(seq 1 50); do up 18081 || break; sleep 0.2; done
sleep 4
SKIP_FAKE_PLATFORM=1 "$HERE/start.sh"
