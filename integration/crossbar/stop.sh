#!/usr/bin/env bash
# Stops the fake platform and the Crossbar node start.sh started.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-$HERE/.run}"
CROSSBAR="${CROSSBAR:-crossbar}"

if [ -f "$STATE_DIR/fake_platform.pid" ]; then
  pid="$(cat "$STATE_DIR/fake_platform.pid")"
  if kill -0 "$pid" 2>/dev/null && tr '\0' ' ' < "/proc/$pid/cmdline" | grep -qF fake_platform.py; then
    kill "$pid"
  fi
  rm -f "$STATE_DIR/fake_platform.pid"
fi
[ -d "$STATE_DIR/node/.crossbar" ] && (cd "$STATE_DIR/node" && "$CROSSBAR" stop --cbdir .crossbar >/dev/null 2>&1 || true)
exit 0
