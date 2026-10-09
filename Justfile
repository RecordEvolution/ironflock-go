# Development tasks for ironflock-go.

# Platforms the SDK must build for (IronFlock devices are often 32-bit ARM).
targets := "linux/arm linux/arm64 linux/386 windows/amd64 darwin/arm64"
harness := "integration/ironflock-router"

# Run unit tests with the race detector
test:
	go test -race -count=1 ./...

# Vet, lint, cross-build for the device platforms, and check that go.mod/go.sum are tidy
check:
	go vet ./...
	golangci-lint run ./...
	for target in {{targets}}; do GOOS=${target%/*} GOARCH=${target#*/} go build ./... || exit 1; done
	go mod tidy -diff

# Unit test coverage report
coverage:
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

# Start the ironflock-router harness (ROUTER_BIN and ROUTER_REF_CONFIG, or ROUTER_IMAGE; see integration/README.md)
router-up:
	{{harness}}/start.sh

# Build an ironflock-router binary from a checkout (default ref HEAD), against the nexus fork commit this SDK pins
router-build src out="integration/ironflock-router/.bin/ironflock-router" ref="HEAD":
	#!/usr/bin/env bash
	set -euo pipefail
	# The router's go.mod replaces nexus with a path on its maintainer's machine
	# (and its `just build` wants a ../../nexus checkout): build a copy of the
	# ref with the fork's module version instead, leaving the checkout untouched.
	nexus="$(go list -m -f '{{{{with .Replace}}{{{{.Path}}@{{{{.Version}}{{{{end}}' github.com/gammazero/nexus/v3)"
	out="$(mkdir -p "$(dirname "{{out}}")" && cd "$(dirname "{{out}}")" && pwd -P)/$(basename "{{out}}")"
	tmp="$(mktemp -d "${TMPDIR:-/tmp}/ironflock-router-build.XXXXXX")"
	trap 'rm -rf -- "$tmp"' EXIT
	git -C "{{src}}" archive "{{ref}}" | tar -x -C "$tmp"
	cd "$tmp"
	go mod edit -replace="github.com/gammazero/nexus/v3=$nexus"
	go mod tidy
	go build -o "$out" ./cmd/ironflock-router
	echo "built $out ($(git -C "{{src}}" describe --tags --always "{{ref}}"), nexus $nexus)"

# Offline checks of the integration harness: the fake platform against the real services' contract tables, and the scripts
test-harness:
	"${PYTHON:-python3}" -I integration/test_fake_platform.py
	integration/ironflock-router/test_scripts.sh

# Stop the ironflock-router harness
router-down:
	{{harness}}/stop.sh

# Integration tests: start the harness (ROUTER_BIN or ROUTER_IMAGE), run the Python reference and the Go suites, stop it
test-integration: test-harness
	#!/usr/bin/env bash
	set -euo pipefail
	harness="$PWD/{{harness}}"
	# Absolute, so that restart.sh (run from integration/ by the test, as
	# ironflock-router/restart.sh) finds the instance.
	mkdir -p "${STATE_DIR:=$harness/.run}"
	STATE_DIR="$(cd "$STATE_DIR" && pwd -P)"
	export STATE_DIR
	py="${PYTHON:-python3}"
	case "$py" in
		/*) ;;
		*/*) py="$PWD/$py" ;;
		*) py="$(command -v "$py")" || { echo "${PYTHON:-python3} not found: set PYTHON" >&2; exit 1; } ;;
	esac
	url="ws://localhost:${ROUTER_PUBLIC_PORT:-18082}/ws-ua-usr"
	tmp="$(mktemp -d "${TMPDIR:-/tmp}/ironflock-go-integration.XXXXXX")"
	trap 'rm -rf -- "$tmp"' EXIT
	"$harness/start.sh"
	trap '"$harness/stop.sh"; rm -rf -- "$tmp"' EXIT
	# The Python SDK's run of the conformance scenario is the wire reference.
	cp -R "$harness/perapp_env" "$tmp/env"
	env DEVICE_SERIAL_NUMBER=06a0bf96-a539-4d6a-8471-ac7adc67616e SWARM_KEY=2 APP_KEY=26 ENV=DEV \
		DEVICE_KEY=42 APP_NAME=interop DEVICE_NAME=interop-dev \
		IRONFLOCK_ENV_DIR="$tmp/env" IRONFLOCK_TEST_PLATFORM_URL="$url" \
		"$py" -I integration/py_scenario.py "$tmp/py_reference.json"
	IRONFLOCK_TEST_ROUTER_URL="$url" go test -race -count=1 -run TestRealRouter -v ./wamp/
	IRONFLOCK_TEST_PLATFORM_URL="$url" \
	IRONFLOCK_TEST_ENV_DIR="$harness/perapp_env" \
	IRONFLOCK_TEST_REFERENCE="$tmp/py_reference.json" \
	IRONFLOCK_TEST_PYTHON="$py" \
	IRONFLOCK_TEST_RESTART_ROUTER=ironflock-router/restart.sh \
		go test -tags integration -race -count=1 -v ./integration/...
