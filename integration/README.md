# Integration and conformance tests

End-to-end tests of the Go SDK against a real [Crossbar](https://crossbar.io) router and a fake IronFlock
platform, plus a cross-SDK conformance check that the Go, Python and JavaScript SDKs put the same messages on
the wire. They are behind the `integration` build tag and skip unless their environment is configured.

## Pieces

| File | Purpose |
|------|---------|
| `crossbar/config.json` | Router node on port 18081: the app realm `realm-2-26-dev`, a provider realm `realm-2-77-dev` whose device role has the platform's read-only consumer rights, WAMP-CRA users |
| `fake_platform.py` | Joins both realms as the data backends: table writes with the `transformed.*` republish, history/series/secret procedures, `sys.appaccess.*`, the file service with an S3-like presign server on port 18090. Records every payload it receives (`test.recorded`, `test.reset`, `test.clear_recorded`) |
| `py_scenario.py`, `js_scenario.mjs` | The conformance scenario with the Python and JavaScript SDKs: writes, per step, what the platform recorded and what the SDK returned |
| `scenario_test.go` | The same scenario with the Go SDK; compares the recorded wire payloads with a reference run |
| `e2e_test.go`, `py_peer.py` | Live tests: device functions called across SDKs, table subscriptions through the platform republish (bulk rows unrolled), cross-app realtime and guards, per-app credential rotation, router restart recovery, keepalive detection of a silently dead link |

## Running

```shell
python -m venv .venv && . .venv/bin/activate
pip install crossbar ironflock            # router + the Python SDK

mkdir -p /tmp/cbnode/.crossbar && cp crossbar/config.json /tmp/cbnode/.crossbar/
crossbar start --cbdir /tmp/cbnode/.crossbar &
python fake_platform.py &

export IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18081/ws-ua-usr
export IRONFLOCK_TEST_PYTHON=$(which python)

# The Python reference run (device identity as the router expects it)
DEVICE_SERIAL_NUMBER=06a0bf96-a539-4d6a-8471-ac7adc67616e SWARM_KEY=2 APP_KEY=26 ENV=DEV \
DEVICE_KEY=42 APP_NAME=interop DEVICE_NAME=interop-dev IRONFLOCK_ENV_DIR=$(mktemp -d) \
  python py_scenario.py /tmp/py_out.json

# Go: conformance against the Python run, and the live tests
IRONFLOCK_TEST_REFERENCE=/tmp/py_out.json go test -tags integration -count=1 -v ./integration/...
```

`TestRouterRestartRecovery` additionally needs `IRONFLOCK_TEST_RESTART_ROUTER`, a shell command that stops
the router and starts it again a few seconds later.

For the JavaScript reference, build ironflock-js and run
`IRONFLOCK_JS_SDK=/path/to/ironflock-js/dist/index.mjs node js_scenario.mjs /tmp/js_out.json` with the same
environment. The Python and JavaScript runs agree on every step except `report_error`, where the JavaScript SDK
does not send `user_message`.
