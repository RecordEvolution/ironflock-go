# Integration and conformance tests

End-to-end tests of the Go SDK against [ironflock-router](https://github.com/RecordEvolution/ironflock-router), the
IronFlock platform's WAMP router, and a fake IronFlock platform, plus a cross-SDK conformance check that the Go,
Python and JavaScript SDKs put the same messages on the wire. They are behind the `integration` build tag and skip
unless their environment is configured.

The router runs from a binary or from its container image. Its client roles (`app`, `app_reader`) are copied verbatim
from the router's reference config, and app sessions authenticate the way production does: dynamic WAMP-CRA with a
role chosen per realm. autobahn-python is test tooling only: the fake platform and the Python SDK's reference run use
it.

## Pieces

| Path | Purpose |
| --- | --- |
| `fake_platform.py` | Joins the router's service listener as the platform's services: the app's data backend on `realm-2-26-dev` (table writes with the `transformed.*` republish, history/series/secret procedures, `sys.appaccess.*`, the file service with an S3-like presign server) and a provider app's backend on `realm-2-77-dev`. Records every payload it receives (`test.recorded`, `test.reset`, `test.clear_recorded`). Stubs ironflock-auth on `ironflock.auth`: the router's `identity` authorizer callout (`auth.authorize`) and its dynamic WAMP-CRA authenticator for app sessions (`auth.userapp.authenticate`) |
| `ironflock-router/start.sh`, `stop.sh`, `restart.sh` | Start the router (binary or image) and the fake platform; stop both; restart only the router (an outage of a few seconds, for `TestRouterRestartRecovery`). `lib.sh` holds their shared code |
| `ironflock-router/config.template.yaml`, `gen_config.py` | The router config. `gen_config.py` copies the production `app`, `app_reader` and `svc_auth` roles, and the fleetdb and fleetfiles rules the fake platform's role combines, byte-for-byte from the router's reference config |
| `ironflock-router/perapp_env/` | The per-app credential (`APP_AUTH_ID.txt`, `APP_AUTH_SECRET.txt`) as the device agent writes it to `/data/env` |
| `py_scenario.py`, `js_scenario.mjs` | The conformance scenario with the Python and JavaScript SDKs: writes, per step, what the platform recorded and what the SDK returned |
| `scenario_test.go` | The same scenario with the Go SDK; compares the recorded wire payloads with a reference run |
| `e2e_test.go`, `py_peer.py` | Live tests: device functions called across SDKs, table subscriptions through the platform republish (bulk rows unrolled), cross-app realtime and guards, per-app credential rotation, router restart recovery, keepalive detection of a silently dead link |

## Quick start

You need Go, a Python with the `ironflock` package (the reference SDK; it brings autobahn for the fake platform), and
either the router image and Docker or a router binary. All commands run from the repository root.

```shell
python3 -m venv ~/.venvs/ironflock && ~/.venvs/ironflock/bin/pip install ironflock==1.9.0
export PYTHON=~/.venvs/ironflock/bin/python

# Router image (pulling it needs read access to record-1283's Artifact Registry and
# `gcloud auth configure-docker europe-docker.pkg.dev`):
ROUTER_IMAGE=europe-docker.pkg.dev/record-1283/eu.gcr.io/ironflock-router:v0.8.1 just test-integration

# Or a router binary and the reference config from its source tree:
ROUTER_BIN=/path/to/ironflock-router \
ROUTER_REF_CONFIG=/path/to/ironflock-router/examples/config.yaml \
  just test-integration
```

`just test-integration` starts the harness, runs the Python reference, runs package `wamp`'s real-router tests and the
integration suite against it, and stops the harness again.

To build the image yourself, run
`docker buildx build --build-context nexus=/path/to/nexus -t ironflock-router:dev --load .` in an ironflock-router
checkout (its Dockerfile explains the `nexus` context, a checkout of RecordEvolution's nexus fork); `just build` there
builds a binary (`bin/ironflock-router`).

## Step by step

Useful when you keep the harness running while you work; `PYTHON` is exported as above. Paths that a test or script
uses from another directory are absolute (`$PWD/...`).

```shell
# 1. Router and fake platform. Image instead of binary: ROUTER_IMAGE=... in place of
#    the two ROUTER_* lines.
ROUTER_BIN=/path/to/ironflock-router \
ROUTER_REF_CONFIG=/path/to/ironflock-router/examples/config.yaml \
  integration/ironflock-router/start.sh

# 2. Python reference run, with the device identity the fake platform admits and the
#    per-app credential.
DEVICE_SERIAL_NUMBER=06a0bf96-a539-4d6a-8471-ac7adc67616e SWARM_KEY=2 APP_KEY=26 ENV=DEV \
DEVICE_KEY=42 APP_NAME=interop DEVICE_NAME=interop-dev \
IRONFLOCK_ENV_DIR=$PWD/integration/ironflock-router/perapp_env \
IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18082/ws-ua-usr \
  "$PYTHON" -I integration/py_scenario.py /tmp/py_reference.json

# 3. The Go suites: package wamp's real-router tests, then the integration suite,
#    compared with that reference.
IRONFLOCK_TEST_ROUTER_URL=ws://localhost:18082/ws-ua-usr \
  go test -race -count=1 -run TestRealRouter -v ./wamp/
IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18082/ws-ua-usr \
IRONFLOCK_TEST_ENV_DIR=$PWD/integration/ironflock-router/perapp_env \
IRONFLOCK_TEST_REFERENCE=/tmp/py_reference.json \
IRONFLOCK_TEST_PYTHON="$PYTHON" \
IRONFLOCK_TEST_RESTART_ROUTER=$PWD/integration/ironflock-router/restart.sh \
  go test -tags integration -race -count=1 -v ./integration/...

# 4. Stop the router and the fake platform.
integration/ironflock-router/stop.sh
```

The reference run must use the per-app credential, as the Go tests do. Without it the Python SDK falls back to the
legacy device credential (serial number as authid and secret), which the fake platform admits only on the device's own
app realm. The cross-app steps then fail, and the wire comparison with the Go run fails with them.

## The harness

`start.sh` generates the router config from the template and the reference config, starts the router, waits until
its `/healthz` answers, starts the fake platform and waits until its three sessions (own realm, provider realm, auth
realm) have joined. If anything fails it prints the logs and stops what it started. It refuses to start when the
instance in its state directory is still running or when a port is taken.

| Port (variable) | Default | What |
| --- | --- | --- |
| `ROUTER_PUBLIC_PORT` | 18082 | `/ws-ua-usr`, the SDKs' listener |
| `ROUTER_OPS_PORT` | 18083 | `/healthz`, `/readyz`, `/stats` |
| `ROUTER_INTERNAL_PORT` | 18084 | `/ws-svc`, the fake platform's listener |
| `FAKE_PLATFORM_S3_PORT` | 18091 | the fake platform's object store (presigned URLs point here) |

`AUTH_MODE=dynamic` (the default) is production's auth shape: the router asks `auth.userapp.authenticate`, which the
fake platform answers with the role for the realm in the HELLO. The per-app credential gets `app` on
`realm-2-26-dev` and `app_reader` on the provider realm `realm-2-77-dev`; the legacy device credential gets `app` on
the realm of an app running on the device. `AUTH_MODE=static` uses static users with one role on every realm, so
cross-app access cannot work there. `IDENTITY_AUTHORIZER_MODE=shadow` makes the `identity` authorizer callout advisory
for debugging: the router logs what it would deny (`authorizer shadow: would deny`), and the static role rules alone
decide.

**Binary mode** (`ROUTER_BIN`, plus `ROUTER_REF_CONFIG`) runs the router as a background process; its output goes to
`router.log` in the state directory. **Image mode** (`ROUTER_IMAGE`) needs Docker. It pulls the image if it is not
present and, unless `ROUTER_REF_CONFIG` is set, copies the reference config out of it
(`/etc/ironflock-router/config.yaml`, the router's `examples/config.yaml`). The container, named
`ironflock-go-router-<ROUTER_PUBLIC_PORT>`, mounts the generated config read-only and publishes its ports on
127.0.0.1 only; use `ws://127.0.0.1:...` with a client that does not fall back from `::1` to 127.0.0.1 when it
resolves `localhost`. Read its log with `docker logs` while it runs; `stop.sh` saves it to `router.log` before it
removes the container.

The state directory (`STATE_DIR`, default `integration/ironflock-router/.run`, ignored by git) holds the generated
`config.yaml`, the logs, the PIDs or the container ID, and `harness.env`, the settings `start.sh` used. `stop.sh` and
`restart.sh` act on the instance recorded there and need no other variables; give them the same `STATE_DIR` when it
is not the default, also inside `IRONFLOCK_TEST_RESTART_ROUTER`. `stop.sh` signals a recorded PID only while its
command line still matches what `start.sh` launched, and keeps the logs.

`restart.sh` stops only the router, keeps it down for `RESTART_DOWNTIME` seconds and starts it again with the same
config. It returns once the router is healthy and the fake platform, which keeps running, has rejoined all its
realms.

A second instance next to a running one needs other ports and its own state directory:

```shell
export STATE_DIR=/tmp/ironflock-harness-2
ROUTER_PUBLIC_PORT=19082 ROUTER_OPS_PORT=19083 ROUTER_INTERNAL_PORT=19084 FAKE_PLATFORM_S3_PORT=19091 \
ROUTER_IMAGE=europe-docker.pkg.dev/record-1283/eu.gcr.io/ironflock-router:v0.8.1 \
  integration/ironflock-router/start.sh
# ... tests with IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:19082/ws-ua-usr and
#     IRONFLOCK_TEST_RESTART_ROUTER=$PWD/integration/ironflock-router/restart.sh (STATE_DIR is exported)
integration/ironflock-router/stop.sh
```

`gen_config.py --check` proves that the copied roles parse to exactly the reference roles. `start.sh` runs it when
`$PYTHON` has PyYAML (`pip install pyyaml`), as CI does.

Local deviations from production: static realm/role bindings instead of fleetdb's runtime binding, one test service
role standing in for fleetdb and fleetfiles, and the stubbed ironflock-auth (its `identity` rule is reconstructed
from the router's documentation).

## Environment

### Harness scripts

| Variable | Default | Meaning |
| --- | --- | --- |
| `ROUTER_BIN` | `ironflock-router` from `PATH` | Router binary (binary mode) |
| `ROUTER_IMAGE` | | Router image (image mode, needs Docker); set this or `ROUTER_BIN` |
| `ROUTER_REF_CONFIG` | copied out of the image | The router's reference config, `examples/config.yaml`; required in binary mode |
| `PYTHON` | `python3` | Python with autobahn (`pip install ironflock`); runs the fake platform and `gen_config.py` |
| `AUTH_MODE` | `dynamic` | `dynamic` (production) or `static` |
| `IDENTITY_AUTHORIZER_MODE` | `enforce` | `enforce` or `shadow` |
| `ROUTER_PUBLIC_PORT`, `ROUTER_OPS_PORT`, `ROUTER_INTERNAL_PORT`, `FAKE_PLATFORM_S3_PORT` | 18082, 18083, 18084, 18091 | Ports, see above |
| `IRONFLOCK_LOG_LEVEL` | `info` | The router's log level (`debug`, `info`, `warn`, `error`) |
| `STATE_DIR` | `integration/ironflock-router/.run` | Generated config, logs, PIDs or container ID, settings (all three scripts) |
| `RESTART_DOWNTIME` | 3 | Seconds `restart.sh` keeps the router down |

### Go tests

| Variable | Meaning |
| --- | --- |
| `IRONFLOCK_TEST_PLATFORM_URL` | Router URL, e.g. `ws://localhost:18082/ws-ua-usr`; all tests in `integration/` skip without it |
| `IRONFLOCK_TEST_ROUTER_URL` | Router URL for package `wamp`'s real-router tests (`TestRealRouter`, no build tag); they skip without it |
| `IRONFLOCK_TEST_REFERENCE` | A reference SDK run (`py_scenario.py` output) to compare wire payloads against |
| `IRONFLOCK_TEST_OUT` | Where to write the Go run of the scenario |
| `IRONFLOCK_TEST_PYTHON` | A Python with the `ironflock` package, for the cross-SDK live tests |
| `IRONFLOCK_TEST_ENV_DIR` | Credential files (`APP_AUTH_ID.txt`, `APP_AUTH_SECRET.txt`) copied into each test's `/data/env` |
| `IRONFLOCK_TEST_RESTART_ROUTER` | Shell command that restarts the router, run with `bash -c` from `integration/` (`TestRouterRestartRecovery`) |

### Scenario drivers

`py_scenario.py`, `js_scenario.mjs`, and `py_peer.py` (which the Go tests start):

| Variable | Meaning |
| --- | --- |
| `IRONFLOCK_TEST_PLATFORM_URL` | Router URL (default `ws://localhost:18082/ws-ua-usr`) |
| `IRONFLOCK_ENV_DIR` | The SDK's `/data/env` stand-in; point it at the per-app credential |
| `DEVICE_SERIAL_NUMBER`, `SWARM_KEY`, `APP_KEY`, `ENV`, `DEVICE_KEY`, `APP_NAME`, `DEVICE_NAME` | The device identity, as in step 2 |
| `IRONFLOCK_JS_SDK` | `js_scenario.mjs` only: the SDK module to import (default: the `ironflock` package) |

### Fake platform

`start.sh` sets the first five; the others are for running `fake_platform.py` by hand.

| Variable | Default | Meaning |
| --- | --- | --- |
| `FAKE_PLATFORM_ROUTER` | `ws://127.0.0.1:18084/ws-svc` | Router URL of the data-realm sessions |
| `FAKE_PLATFORM_AUTH_ROUTER` | `FAKE_PLATFORM_ROUTER` | Router URL of the ironflock-auth stub's session |
| `FAKE_PLATFORM_AUTH_REALM` | `ironflock.auth` | Realm of the ironflock-auth stub; empty: no stub |
| `FAKE_PLATFORM_USERAPP_AUTH` | `1` | `1`: the stub also registers `auth.userapp.authenticate` (`AUTH_MODE=dynamic`) |
| `FAKE_PLATFORM_S3_PORT` | 18091 | Port of the fake object store |
| `FAKE_PLATFORM_AUTHID`, `FAKE_PLATFORM_SECRET` | `fake-backend`, `fake-backend-secret` | The backend sessions' credential |
| `FAKE_PLATFORM_AUTH_AUTHID`, `FAKE_PLATFORM_AUTH_SECRET` | `svc_auth`, `svc-auth-secret` | The ironflock-auth stub's credential |
| `FAKE_PLATFORM_OWN_REALM`, `FAKE_PLATFORM_PROVIDER_REALM` | `realm-2-26-dev`, `realm-2-77-dev` | The app's and the provider app's data realms |

## JavaScript reference

Build ironflock-js and run
`IRONFLOCK_JS_SDK=/path/to/ironflock-js/dist/index.mjs node integration/js_scenario.mjs /tmp/js_out.json` with the
environment of step 2. Python and JavaScript agree on every step except `report_error`, where the JavaScript SDK does
not send `user_message`; the Go SDK matches Python on every step.

## CI

`.github/workflows/ci.yml` has two jobs:

- **test**: `go vet`, unit tests with `-race`, cross-builds for linux/arm, linux/arm64, linux/386, windows/amd64 and
  darwin/arm64, `go mod tidy -diff`, and golangci-lint (the same checks as `just check`).
- **integration**: starts the harness in image mode, runs the Python reference (`ironflock==1.9.0`), package `wamp`'s
  real-router tests and the integration suite, including `TestRouterRestartRecovery` with `restart.sh`, then stops the
  harness and prints its logs on failure. The
  image comes from Artifact Registry through workload identity federation, so the job runs only once the repository
  variables below are set, and only for pushes and for pull requests from this repository: GitHub gives pull requests
  from forks no OIDC token. The image is the `ROUTER_IMAGE` repository variable, default
  `europe-docker.pkg.dev/record-1283/eu.gcr.io/ironflock-router:v0.8.1`.

| Repository variable | Meaning |
| --- | --- |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | Full resource name of the workload identity provider (`projects/<number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`) |
| `GCP_SERVICE_ACCOUNT` | Email of the service account the job impersonates to pull the image |
| `ROUTER_IMAGE` | Optional: the router image to test against |

### CI setup

One-time setup by a project owner, with `gcloud` and `gh`. It creates a workload identity pool with a GitHub OIDC
provider that admits only tokens from this repository, and a service account that may only read the `eu.gcr.io`
repository (location `europe`) of `record-1283`, which workflows of this repository may impersonate.

```shell
PROJECT_ID=record-1283
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
REPO=RecordEvolution/ironflock-go
REPO_ID=$(gh api "repos/$REPO" --jq .id)   # numeric and never reused, unlike the name
POOL=github-actions                        # an existing pool for GitHub Actions can be reused: skip step 2
PROVIDER=ironflock-go
SA="ironflock-go-ci@$PROJECT_ID.iam.gserviceaccount.com"

# 1. APIs that workload identity federation with service account impersonation needs.
gcloud services enable iam.googleapis.com cloudresourcemanager.googleapis.com \
  iamcredentials.googleapis.com sts.googleapis.com --project="$PROJECT_ID"

# 2. The workload identity pool.
gcloud iam workload-identity-pools create "$POOL" \
  --project="$PROJECT_ID" --location=global --display-name="GitHub Actions"

# 3. The GitHub OIDC provider; its attribute condition admits only this repository.
gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
  --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" \
  --display-name="$REPO" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository == '$REPO' && assertion.repository_id == '$REPO_ID'"

# 4. The service account, allowed to pull from the eu.gcr.io repository only.
gcloud iam service-accounts create ironflock-go-ci \
  --project="$PROJECT_ID" --display-name="ironflock-go CI: pulls ironflock-router"
gcloud artifacts repositories add-iam-policy-binding eu.gcr.io \
  --project="$PROJECT_ID" --location=europe \
  --member="serviceAccount:$SA" --role=roles/artifactregistry.reader

# 5. Workflows of this repository may impersonate it.
gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --project="$PROJECT_ID" --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL/attribute.repository/$REPO"

# 6. Repository variables for the workflow (not secrets: neither value is sensitive).
gh variable set GCP_WORKLOAD_IDENTITY_PROVIDER --repo "$REPO" \
  --body "projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL/providers/$PROVIDER"
gh variable set GCP_SERVICE_ACCOUNT --repo "$REPO" --body "$SA"
# Optional: another router image (default ...ironflock-router:v0.8.1).
gh variable set ROUTER_IMAGE --repo "$REPO" \
  --body "europe-docker.pkg.dev/record-1283/eu.gcr.io/ironflock-router:v0.8.1"
```

New pools, providers and IAM bindings can take up to five minutes to take effect. To check the setup:
`gcloud artifacts repositories get-iam-policy eu.gcr.io --location=europe --project=record-1283` lists the reader
binding, `gcloud iam service-accounts get-iam-policy "$SA"` the impersonation binding, and the next push or pull
request runs the integration job.
