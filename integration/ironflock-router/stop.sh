#!/usr/bin/env bash
# Stops what start.sh started, by its recorded PIDs (and only if each PID still
# runs the program start.sh launched).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-$HERE/.run}"

stop_one() {  # pidfile, expected command-line fragment
  local f="$STATE_DIR/$1" want="$2" pid
  [ -f "$f" ] || return 0
  pid="$(cat "$f")"
  if kill -0 "$pid" 2>/dev/null && tr '\0' ' ' < "/proc/$pid/cmdline" | grep -qF "$want"; then
    kill "$pid"
    for i in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid"
    echo "stopped $1 ($pid)"
  fi
  rm -f "$f"
}

stop_one fake_platform.pid "fake_platform.py"
stop_one router.pid "ironflock-router"
