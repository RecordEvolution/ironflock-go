# Integration and conformance tests

End-to-end tests of the Go SDK against real routers — [Crossbar](https://crossbar.io) and
[ironflock-router](https://github.com/RecordEvolution/ironflock-router) — and a fake IronFlock platform, plus a
cross-SDK conformance check that the Go, Python and JavaScript SDKs put the same messages on the wire. They are
behind the `integration` build tag and skip unless their environment is configured.

## Pieces

| File | Purpose |
|------|---------|
| `fake_platform.py` | Joins the routers as the platform's services: the app's data backend on `realm-2-26-dev` (table writes with the `transformed.*` republish, history/series/secret procedures, `sys.appaccess.*`, the file service with an S3-like presign server) and a provider app's backend on `realm-2-77-dev`. Records every payload it receives (`test.recorded`, `test.reset`, `test.clear_recorded`). For ironflock-router it also stubs ironflock-auth: the `identity` authorizer callout and, in dynamic mode, the per-realm WAMP-CRA authenticator |
| `crossbar/` | Crossbar node config (port 18081) and `start.sh` / `stop.sh` / `restart.sh` |
| `ironflock-router/` | ironflock-router config template whose `app` / `app_reader` roles are copied verbatim from the router's reference config by `gen_config.py`, per-app test credentials, `start.sh` / `stop.sh` (ports 18082–18084, 18091) |
| `py_scenario.py`, `js_scenario.mjs` | The conformance scenario with the Python and JavaScript SDKs: writes, per step, what the platform recorded and what the SDK returned |
| `scenario_test.go` | The same scenario with the Go SDK; compares the recorded wire payloads with a reference run |
| `e2e_test.go`, `py_peer.py` | Live tests: device functions called across SDKs, table subscriptions through the platform republish (bulk rows unrolled), cross-app realtime and guards, per-app credential rotation, router restart recovery, keepalive detection of a silently dead link |

## Against Crossbar

```shell
python -m pip install crossbar ironflock      # router + the Python SDK (reference)
just test-integration                         # or the steps below
```

```shell
integration/crossbar/start.sh

# Python reference run (the device identity the router config expects)
cd integration && DEVICE_SERIAL_NUMBER=06a0bf96-a539-4d6a-8471-ac7adc67616e SWARM_KEY=2 APP_KEY=26 ENV=DEV \
  DEVICE_KEY=42 APP_NAME=interop DEVICE_NAME=interop-dev IRONFLOCK_ENV_DIR=$(mktemp -d) \
  python -I py_scenario.py /tmp/py_out.json && cd ..

IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18081/ws-ua-usr \
IRONFLOCK_TEST_REFERENCE=/tmp/py_out.json \
IRONFLOCK_TEST_PYTHON=$(which python) \
IRONFLOCK_TEST_RESTART_ROUTER=$PWD/integration/crossbar/restart.sh \
  go test -tags integration -race -count=1 -v ./integration/...

integration/crossbar/stop.sh
```

## Against ironflock-router

Build the router against the nexus fork, then start it with the production auth shape (`AUTH_MODE=dynamic`:
the per-app credential gets `app` on its own realm and `app_reader` on the provider realm, as ironflock-auth
answers in production):

```shell
ROUTER_BIN=/path/to/ironflock-router \
ROUTER_REF_CONFIG=/path/to/ironflock-router/examples/config.yaml \
PYTHON=$(which python) \
  integration/ironflock-router/start.sh

# reference run as above, with IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18082/ws-ua-usr and
# IRONFLOCK_ENV_DIR=integration/ironflock-router/perapp_env, then:
IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18082/ws-ua-usr \
IRONFLOCK_TEST_ENV_DIR=$PWD/integration/ironflock-router/perapp_env \
IRONFLOCK_TEST_REFERENCE=/tmp/py_out_router.json \
IRONFLOCK_TEST_PYTHON=$(which python) \
  go test -tags integration -race -count=1 -v ./integration/...

integration/ironflock-router/stop.sh
```

Local deviations from production: static realm/role bindings instead of fleetdb's runtime binding, one test
service role standing in for fleetdb and fleetfiles, and the stubbed ironflock-auth (its `identity` rule is
reconstructed from the router's documentation).

## Environment

| Variable | Meaning |
|----------|---------|
| `IRONFLOCK_TEST_PLATFORM_URL` | Router URL; all tests skip without it |
| `IRONFLOCK_TEST_REFERENCE` | A reference SDK run (`py_scenario.py` output) to compare wire payloads against |
| `IRONFLOCK_TEST_OUT` | Where to write the Go run of the scenario |
| `IRONFLOCK_TEST_PYTHON` | A Python with the `ironflock` package, for the cross-SDK live tests |
| `IRONFLOCK_TEST_ENV_DIR` | Credential files (`APP_AUTH_ID.txt`, `APP_AUTH_SECRET.txt`) copied into each test's `/data/env` |
| `IRONFLOCK_TEST_RESTART_ROUTER` | Shell command that restarts the router (`TestRouterRestartRecovery`) |

For the JavaScript reference, build ironflock-js and run
`IRONFLOCK_JS_SDK=/path/to/ironflock-js/dist/index.mjs node js_scenario.mjs /tmp/js_out.json` with the same
environment. Python and JavaScript agree on every step except `report_error`, where the JavaScript SDK does not
send `user_message`; the Go SDK matches Python on every step.
