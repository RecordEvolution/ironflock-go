#!/usr/bin/env bash
# Regenerates the contract tables in this directory from the real services'
# sources (dev-only; see README.md). Needs Docker, Go, Python 3, and checkouts
# of the private repositories; by default they are siblings of this
# repository's checkout.
#
#   integration/contracts/regen.sh [typia|keys|authz|userapp|all]   (default all)
#
# FLEETDB_SRC         fleetdb-service checkout with its node_modules installed
# FLEETFILES_SRC      fleetfiles-service checkout
# IRONFLOCK_AUTH_SRC  ironflock-auth checkout
# REACCOUNTING_SRC    REaccounting checkout
#
# Afterwards run `python3 -I integration/test_fake_platform.py`: every case
# where the fake platform now disagrees with the regenerated table fails.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "$HERE/../.." && pwd -P)"
SIBLINGS="$(cd "$ROOT/.." && pwd -P)"
: "${FLEETDB_SRC:=$SIBLINGS/fleetdb-service}" "${FLEETFILES_SRC:=$SIBLINGS/fleetfiles-service}"
: "${IRONFLOCK_AUTH_SRC:=$SIBLINGS/ironflock-auth}" "${REACCOUNTING_SRC:=$SIBLINGS/REaccounting}"
NODE_IMAGE=node:22
POSTGRES_IMAGE=postgres:16-alpine

die() { echo "regen.sh: $*" >&2; exit 1; }
describe() { git -C "$1" describe --tags --always 2>/dev/null || echo unknown; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/ironflock-go-contracts.XXXXXX")"
PG_CONTAINER=
cleanup() {
  if [ -n "$PG_CONTAINER" ]; then docker rm -f "$PG_CONTAINER" > /dev/null 2>&1 || true; fi
  rm -rf -- "$TMP"
}
trap cleanup EXIT

typia() {
  [ -d "$FLEETDB_SRC/node_modules/typia" ] || die "FLEETDB_SRC=$FLEETDB_SRC: no fleetdb-service checkout with node_modules"
  local w="$TMP/typia"
  mkdir -p "$w"
  cp -R "$FLEETDB_SRC/src" "$w/src"
  cp "$HERE/gen/typia_check.ts" "$w/src/zz_typia_check.ts"
  ln -s /fleetdb/node_modules "$w/node_modules"
  cat > "$w/tsconfig.json" <<'EOF'
{
  "files": ["src/zz_typia_check.ts"],
  "compilerOptions": {
    "target": "es2021", "module": "commonjs", "resolveJsonModule": true, "outDir": "./out",
    "esModuleInterop": true, "strict": true, "skipLibCheck": true, "strictNullChecks": true,
    "plugins": [{ "transform": "typia/lib/transform" }]
  }
}
EOF
  python3 -I "$HERE/gen/typia_corpus.py" "$w/corpus.json" > /dev/null
  docker run --rm -v "$FLEETDB_SRC:/fleetdb:ro" -v "$w:/work" -w /work "$NODE_IMAGE" \
    bash -c 'node /fleetdb/node_modules/typescript/bin/tsc -p tsconfig.json > /dev/null; test -f out/zz_typia_check.js && node out/zz_typia_check.js corpus.json' \
    > "$w/real.json" || die "compiling or running fleetdb's validators failed"
  python3 -I - "$w/corpus.json" "$w/real.json" "$HERE/fleetdb_typia.json" "$(describe "$FLEETDB_SRC")" <<'EOF'
import json, sys
corpus, real, out, version = sys.argv[1:5]
verdicts = {r["name"]: r for r in json.load(open(real))}
cases = [{"name": c["name"], "type": c["type"], "args": c["args"],
          "error": None if verdicts[c["name"]]["ok"] else verdicts[c["name"]]["error"]} for c in json.load(open(corpus))]
json.dump({"source": "fleetdb-service %s types/rpc.ts, the typia validators compiled from it (typia.assert<"
                     "TableQueryArgs | SeriesQueryArgs | SecretRevealArgs | SecretVerifyArgs | AppAccessResolveArgs>); "
                     "error is JSON.stringify of the TypeGuardError" % version, "cases": cases},
          open(out, "w"), indent=1, sort_keys=True)
print("fleetdb_typia.json: %d cases" % len(cases))
EOF
}

keys() {
  [ -f "$FLEETFILES_SRC/internal/naming/naming.go" ] || die "FLEETFILES_SRC=$FLEETFILES_SRC: no fleetfiles-service checkout"
  local w="$TMP/keys" xtext
  mkdir -p "$w/naming"
  cp "$FLEETFILES_SRC/internal/naming/naming.go" "$w/naming/"
  cp "$HERE/gen/keys_probe.go.tmpl" "$w/main.go"
  xtext="$(awk '$1 == "golang.org/x/text" { print $2 }' "$FLEETFILES_SRC/go.mod")"
  printf 'module namingprobe\n\ngo 1.25\n\nrequire golang.org/x/text %s\n' "$xtext" > "$w/go.mod"
  (cd "$w" && GOFLAGS=-mod=mod go mod tidy && go build -o probe .) || die "building the NormalizeKey probe failed"
  "$w/probe" "$HERE/gen/keys_corpus.json" > "$w/real.json"
  python3 -I - "$w/real.json" "$HERE/fleetfiles_keys.json" "$(describe "$FLEETFILES_SRC")" <<'EOF'
import json, sys
real, out, version = sys.argv[1:4]
cases = [{"key": k["key"], **({"normalized": k["normalized"]} if k["ok"] else {"error": k["error"]})}
         for k in json.load(open(real))]
json.dump({"source": "fleetfiles-service %s internal/naming/naming.go NormalizeKey" % version, "cases": cases},
          open(out, "w"), indent=1, sort_keys=True, ensure_ascii=False)
print("fleetfiles_keys.json: %d cases" % len(cases))
EOF
}

authz() {
  [ -f "$IRONFLOCK_AUTH_SRC/internal/authz/policy.go" ] || die "IRONFLOCK_AUTH_SRC=$IRONFLOCK_AUTH_SRC: no ironflock-auth checkout"
  local w="$TMP/authz" nexus
  mkdir -p "$w"
  git -C "$IRONFLOCK_AUTH_SRC" archive HEAD | tar -x -C "$w"
  # The fork commit this SDK pins, not a local checkout.
  nexus="$(cd "$ROOT" && go list -m -f '{{.Path}}@{{.Version}}' github.com/ironflock/nexus/v3)"
  (cd "$w" && go get "$nexus")
  cp "$HERE/gen/authz_contract_test.go.tmpl" "$w/internal/authz/zz_contract_test.go"
  python3 -I "$HERE/gen/authz_requests.py" "$w/requests.json"
  (cd "$w" && AUTHZ_IN="$w/requests.json" AUTHZ_OUT="$w/real.json" GOFLAGS=-mod=mod \
    go test -count=1 -run '^TestContract$' ./internal/authz/ > /dev/null) || die "running Policy.Decide failed"
  python3 -I - "$w/requests.json" "$w/real.json" "$HERE/ironflock_auth_authorize.json" "$(describe "$IRONFLOCK_AUTH_SRC")" <<'EOF'
import json, sys
requests, real, out, version = sys.argv[1:5]
cases = [dict(q, verdict=v) for q, v in zip(json.load(open(requests)), json.load(open(real)))]
json.dump({"source": "ironflock-auth %s internal/authz/policy.go Policy.Decide, store: device 42 of swarm 2 with "
                     "the harness serial" % version, "cases": cases},
          open(out, "w"), indent=1, sort_keys=True, ensure_ascii=False)
print("ironflock_auth_authorize.json: %d cases" % len(cases))
EOF
}

userapp() {
  local sql="$REACCOUNTING_SRC/database/function/device/f_authenticate_userapp.sql"
  [ -f "$sql" ] || die "REACCOUNTING_SRC=$REACCOUNTING_SRC: no REaccounting checkout"
  PG_CONTAINER="ironflock-go-contracts-pg-$$"
  docker run -d --name "$PG_CONTAINER" -e POSTGRES_PASSWORD=contracts "$POSTGRES_IMAGE" > /dev/null
  local ready=0
  for _ in $(seq 1 120); do
    if docker exec "$PG_CONTAINER" psql -qAt -U postgres -c 'SELECT 1' > /dev/null 2>&1; then ready=1; break; fi
    sleep 0.5
  done
  [ "$ready" = 1 ] || die "postgres did not come up"
  python3 -I "$HERE/gen/userapp.py" "$sql" "$PG_CONTAINER" "$HERE/userapp_authenticate.json"
  docker rm -f "$PG_CONTAINER" > /dev/null
  PG_CONTAINER=
  echo "userapp_authenticate.json: regenerated ($(describe "$REACCOUNTING_SRC"))"
}

case "${1:-all}" in
  typia) typia ;;
  keys) keys ;;
  authz) authz ;;
  userapp) userapp ;;
  all) typia; keys; authz; userapp ;;
  *) die "usage: regen.sh [typia|keys|authz|userapp|all]" ;;
esac
