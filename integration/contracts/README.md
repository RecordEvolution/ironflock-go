# Contract tables

What the real backend services answered for a corpus of inputs, so that `../test_fake_platform.py` can check the fake
platform's models against them without the services (standard library only; `just test-harness` and CI's harness job
run it). Each file says in `source` which version produced it.

| File | Real source | Model it checks |
| --- | --- | --- |
| `fleetdb_typia.json` | fleetdb-service's typia validators (`src/types/rpc.ts`), compiled from its source with its own `tsc` and typia transform: `TableQueryArgs`, `SeriesQueryArgs`, `SecretRevealArgs`, `SecretVerifyArgs`, `AppAccessResolveArgs`. `error` is the TypeGuardError as autobahn-js sends it | `fake_fleetdb.typia_check` |
| `fleetfiles_keys.json` | fleetfiles-service's `internal/naming` `NormalizeKey` | `fake_fleetfiles.normalize_key` |
| `ironflock_auth_authorize.json` | ironflock-auth's `internal/authz` `Policy.Decide`, its test store holding the harness's device | `fake_auth.authorize` |
| `userapp_authenticate.json` | REaccounting's `device.f_authenticate_userapp`, run in Postgres (with pgcrypto) over the harness's platform facts, per state (grants, credential epochs, the device's app credential key) | `fake_auth.PlatformDB.authenticate_userapp` |

## Regenerating

When one of the services changes, regenerate its table from the new source, run the self-test, and bring the model
(and its version pin) in line with what fails:

```shell
integration/contracts/regen.sh all        # or: typia | keys | authz | userapp
python3 -I integration/test_fake_platform.py
```

`regen.sh` needs Docker, Go and Python 3, and checkouts of the private repositories, by default next to this
repository's checkout: `FLEETDB_SRC` (fleetdb-service, with `node_modules` installed), `FLEETFILES_SRC`,
`IRONFLOCK_AUTH_SRC`, `REACCOUNTING_SRC`. It works on copies in a temporary directory and leaves the checkouts
untouched. The inputs live in `gen/`: the payload corpus (`typia_corpus.py`), the keys (`keys_corpus.json`), the
callouts (`authz_requests.py`) and the authentication states and cases (`userapp.py`); add a case there when a model
needs one. The Go and TypeScript sources in `gen/` (`*.go.tmpl`, `typia_check.ts`) are copied into the scratch
builds; they are not part of this module.
