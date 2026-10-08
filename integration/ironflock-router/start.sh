#!/usr/bin/env bash
# Starts a local ironflock-router with the production `app` / `app_reader`
# roles plus the fake platform (router variant), for the integration tests.
#
#   ROUTER_BIN=/path/to/ironflock-router \
#   ROUTER_REF_CONFIG=/path/to/ironflock-router/examples/config.yaml \
#   PYTHON=/path/to/python-with-autobahn \
#   AUTH_MODE=dynamic ./start.sh
#
# AUTH_MODE=dynamic (recommended) uses production's dynamic WAMP-CRA profile:
# the fake platform answers auth.userapp.authenticate with the per-realm role
# (the per-app credential gets `app` on realm-2-26-dev and `app_reader` on
# realm-2-77-dev). AUTH_MODE=static uses static users with one global role.
# IDENTITY_AUTHORIZER_MODE=shadow makes the identity callout advisory.
#
# Ports: 18082 /ws-ua-usr (SDKs), 18084 /ws-svc (services), 18083 ops,
# 18091 fake S3. State (generated config, logs, PIDs) goes to $STATE_DIR
# (default ./.run).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-$HERE/.run}"
export STATE_DIR
ROUTER_BIN="${ROUTER_BIN:-ironflock-router}"
PY="${PYTHON:-python3}"
AUTH_MODE="${AUTH_MODE:-dynamic}"
: "${ROUTER_REF_CONFIG:?set ROUTER_REF_CONFIG to ironflock-router/examples/config.yaml}"
export ROUTER_REF_CONFIG

case "$AUTH_MODE" in
  static)  PROFILE=userapp_static; USERAPP_AUTH=0 ;;
  dynamic) PROFILE=userapp_cra;    USERAPP_AUTH=1 ;;
  *) echo "AUTH_MODE must be static or dynamic" >&2; exit 2 ;;
esac

mkdir -p "$STATE_DIR"
alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }
if alive "$STATE_DIR/router.pid" || alive "$STATE_DIR/fake_platform.pid"; then
  echo "already running; run stop.sh first" >&2
  exit 1
fi
for port in 18082 18083 18084 18091; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is in use" >&2; exit 1
  fi
done

python3 -I "$HERE/gen_config.py" >/dev/null

USERAPP_PROFILE="$PROFILE" \
IDENTITY_AUTHORIZER_MODE="${IDENTITY_AUTHORIZER_MODE:-enforce}" \
  nohup "$ROUTER_BIN" -c "$STATE_DIR/config.yaml" > "$STATE_DIR/router.log" 2>&1 &
echo $! > "$STATE_DIR/router.pid"

for i in $(seq 1 100); do
  if curl -fsS -o /dev/null http://127.0.0.1:18083/healthz 2>/dev/null; then break; fi
  if ! alive "$STATE_DIR/router.pid"; then echo "router exited:" >&2; tail -20 "$STATE_DIR/router.log" >&2; exit 1; fi
  sleep 0.1
done
curl -fsS -o /dev/null http://127.0.0.1:18083/healthz || { echo "router not healthy" >&2; exit 1; }

(
  cd "$HERE/.."
  FAKE_PLATFORM_ROUTER="ws://127.0.0.1:18084/ws-svc" \
  FAKE_PLATFORM_S3_PORT=18091 \
  FAKE_PLATFORM_AUTH_REALM=ironflock.auth \
  FAKE_PLATFORM_USERAPP_AUTH="$USERAPP_AUTH" \
    nohup "$PY" -I fake_platform.py > "$STATE_DIR/fake_platform.log" 2>&1 &
  echo $! > "$STATE_DIR/fake_platform.pid"
)

# Ready once the own, provider and auth realm sessions have joined. (The
# router's /readyz answers before auth.authorize is registered.)
for i in $(seq 1 150); do
  n=$(grep -c "realm ready" "$STATE_DIR/fake_platform.log" 2>/dev/null || true)
  [ "${n:-0}" -ge 3 ] && break
  if ! alive "$STATE_DIR/fake_platform.pid"; then echo "fake platform exited:" >&2; tail -20 "$STATE_DIR/fake_platform.log" >&2; exit 1; fi
  sleep 0.2
done
if [ "$(grep -c "realm ready" "$STATE_DIR/fake_platform.log")" -lt 3 ]; then
  echo "fake platform not ready:" >&2; cat "$STATE_DIR/fake_platform.log" >&2; exit 1
fi

echo "router        pid $(cat "$STATE_DIR/router.pid")  ws://localhost:18082/ws-ua-usr ($PROFILE)"
echo "fake platform pid $(cat "$STATE_DIR/fake_platform.pid")  S3 :18091, AUTH_MODE=$AUTH_MODE"
