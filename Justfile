# Development tasks for ironflock-go.

# Run unit tests with the race detector
test:
	go test -race -count=1 ./...

# Vet, lint and cross-build for the platforms IronFlock devices run on
check:
	go vet ./...
	golangci-lint run ./...
	for target in linux/arm linux/arm64 linux/386 windows/amd64 darwin/arm64; do GOOS=${target%/*} GOARCH=${target#*/} go build ./... || exit 1; done

# Unit test coverage report
coverage:
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

# Start the Crossbar harness (needs: pip install crossbar ironflock)
crossbar-up:
	integration/crossbar/start.sh

# Stop the Crossbar harness
crossbar-down:
	integration/crossbar/stop.sh

# Integration tests against the Crossbar harness, with the Python SDK as wire reference
test-integration: crossbar-up
	cd integration && env DEVICE_SERIAL_NUMBER=06a0bf96-a539-4d6a-8471-ac7adc67616e SWARM_KEY=2 APP_KEY=26 ENV=DEV DEVICE_KEY=42 APP_NAME=interop DEVICE_NAME=interop-dev IRONFLOCK_ENV_DIR=$(mktemp -d) python3 -I py_scenario.py /tmp/ironflock-py-reference.json
	IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18081/ws-ua-usr \
	IRONFLOCK_TEST_REFERENCE=/tmp/ironflock-py-reference.json \
	IRONFLOCK_TEST_PYTHON=python3 \
	IRONFLOCK_TEST_RESTART_ROUTER=$PWD/integration/crossbar/restart.sh \
	go test -tags integration -race -count=1 -v ./integration/...
