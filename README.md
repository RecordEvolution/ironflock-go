# ironflock-go

The Go SDK of the [IronFlock](https://www.ironflock.com) IoT DevOps platform — the Go counterpart of
[ironflock-py](https://github.com/RecordEvolution/ironflock-py) and
[ironflock-js](https://github.com/RecordEvolution/ironflock-js), with the same functionality.

## About

With this library you can publish data from your apps on your IoT edge hardware to the fleet data storage of
the IronFlock platform, read it back, react to it in realtime, call functions on other devices, read other
apps' shared data, and store files. When the library is used on a device registered in IronFlock, it uses the
private messaging realm of the device's fleet automatically, so the data collection is always private to the
app user's fleet.

## Requirements

- Go 1.25 or newer

## Installation

```shell
go get github.com/RecordEvolution/ironflock-go
go mod edit -replace=github.com/gammazero/nexus/v3=github.com/RecordEvolution/nexus/v3@v3.0.0-20261001140357-5a989b085bbb
go mod tidy
```

### WAMP library

The SDK speaks WAMP through RecordEvolution's fork of [nexus](https://github.com/RecordEvolution/nexus)
(branch `v4-contrib`) — the code [ironflock-router](https://github.com/RecordEvolution/ironflock-router) is
built on. The fork keeps the upstream module path `github.com/gammazero/nexus/v3`, so this module's `go.mod`
wires it in with a `replace` pinned to a fork commit.

The `replace` line is **required** in your app's `go.mod` too. Go applies `replace` directives only in the main
module, and the SDK does not build against upstream nexus: it uses API that only the fork has. Without the line
Go picks upstream nexus v3.3.0, and the build fails inside the SDK:

```text
# github.com/RecordEvolution/ironflock-go/wamp
.../wamp/peer.go:...: m.Details undefined (type *"github.com/gammazero/nexus/v3/wamp".Unregistered has no field or method Details)
```

The second command above adds the line:

```text
replace github.com/gammazero/nexus/v3 => github.com/RecordEvolution/nexus/v3 v3.0.0-20261001140357-5a989b085bbb
```

When you upgrade the SDK, keep the pinned commit in step with the one in this module's `go.mod`.

## Usage

```go
package main

import (
	"context"
	"log"
	"time"

	ironflock "github.com/RecordEvolution/ironflock-go"
)

func main() {
	// Reads the identity and credentials the device agent injects into the app container.
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}

	// Run connects, runs main, and shuts down on return, SIGINT or SIGTERM.
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			if err := ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"temperature": 22.5}); err != nil {
				log.Print(err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

More in [examples/](examples).

Every network operation takes a `context.Context` and is safe to call from many goroutines. `Start` (and
`Run`) block until the app's realm is joined. Operations issued before that — by a goroutine started before
`Run`, say — wait for `Start` and then for the connection, within the time they wait for a connection anyway
(see [Connection reliability](#connection-reliability)); `Stop` ends that wait with `wamp.ErrStopped`.

### Arguments and keyword arguments

WAMP messages carry positional arguments and keyword arguments. Methods that send a payload (`Publish`,
`PublishToTable`, `AppendToTable`, `Call`, `CallDeviceFunction`) take positional arguments variadically; a
`Kwargs` value among them is sent as keyword arguments, like `sql.Named` in `database/sql`:

```go
ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"temperature": 22.5}) // args [{"temperature": 22.5}]
ifl.PublishToTable(ctx, "inspections", ironflock.Kwargs{"part_id": "1"})  // kwargs {"part_id": "1"}
ifl.Call(ctx, "com.example.proc", 1, 2, ironflock.Kwargs{"scale": 10})    // args [1, 2], kwargs {"scale": 10}
```

Values can be maps, slices and scalars, or structs. They are converted by the rules of `encoding/json` —
struct fields named by their `json` tags, with `omitempty`, `omitzero`, `-`, `,string` and embedded structs,
and `MarshalJSON`/`MarshalText` methods used — so the platform receives the shape Python and JavaScript apps
send. Three things differ from `encoding/json`, as in the Python SDK:

- Floats stay floats: `2.0` is not sent as the integer `2`, and NaN and ±Inf are sent as they are.
- `[]byte` is sent as binary (msgpack bin, like Python `bytes`), not as a base64 string.
- Every `time.Time`, wherever it is, becomes an RFC 3339 string in UTC (`"2026-01-02T03:04:05.123Z"`).

```go
type Reading struct {
	Temperature float64   `json:"temperature"`
	Tsp         time.Time `json:"tsp"`
}
ifl.PublishToTable(ctx, "sensordata", Reading{Temperature: 22.5, Tsp: time.Now()})
```

Received payloads use `map[string]any`, `[]any`, `string`, `int64`, `float64`, `bool`, `[]byte` and `nil`
(`uint64` for integers above `math.MaxInt64`). A number arrives as `int64` when its sender encoded it as an
integer. The data backend does so for every whole number, so in table rows a numeric column holding 22 reads
as `int64(22)` and 22.5 as `float64`; timestamp columns, `tsp` included, arrive as epoch milliseconds.
`ironflock.Decode[T]`, `ironflock.DecodeRows[T]` and `Result.Decode` convert payloads into your own types
through `encoding/json`, so a `float64` field takes either kind of number. NaN and ±Inf decode as JSON null
does: a `float64` field keeps 0, a `*float64` field stays nil.

## Options

`ironflock.New` reads the environment the IronFlock device agent injects into every app container. Options
override it:

| Option | Environment variable | Description |
|--------|----------------------|-------------|
| `WithSerialNumber(s)` | `DEVICE_SERIAL_NUMBER` | Device serial number. Required — `New` fails without it. Can be used to authenticate as another device. |
| `WithDeviceKey(k)` | `DEVICE_KEY` | Device key |
| `WithDeviceName(n)` | `DEVICE_NAME` | Device name |
| `WithAppName(n)` | `APP_NAME` | App name |
| `WithSwarmKey(k)` | `SWARM_KEY` | Swarm (fleet) key |
| `WithAppKey(k)` | `APP_KEY` | App key |
| `WithEnv(e)` | `ENV` | `PROD` (in any case) selects the production realm; anything else, unset included, the development realm |
| `WithURL(u)` | `DEVICE_ENDPOINT_URL` | Router WebSocket URL, e.g. `wss://cbw.ironflock.com/ws-ua-usr`. Without it the URL comes from `DEVICE_ENDPOINT_URL` (path replaced by `/ws-ua-usr`), then from the studio URL |
| `WithReswarmURL(u)` | `RESWARM_URL` | Studio URL used to look up the router of a known IronFlock deployment |
| `WithCredentials(id, secret)` | `APP_AUTH_ID`, `APP_AUTH_SECRET` | WAMP-CRA credential. By default the per-app credential the agent injects is used (read from `/data/env` first, so a rotated credential is picked up on the next reconnect), falling back to the device serial number |
| `WithReconnectWindow(d)` | | How long a table operation waits for the connection — after a platform restart, or before `Start` — and retries while the data backend comes back, before it fails (default 60 s; 0 turns it off). See [Connection reliability](#connection-reliability) |
| `WithLogger(l)` | | `*slog.Logger` for connection lifecycle logs (default `slog.Default()`) |

Missing `DEVICE_KEY`, `APP_NAME`, `SWARM_KEY` or `APP_KEY` are logged as a warning; the operations that need
them fail with an error wrapping `ironflock.ErrMissingConfig`. Without `SWARM_KEY` or `APP_KEY`, `Start` fails
at once: the app's realm could never be joined.

## API Reference

### Lifecycle

| Method | Description |
|--------|-------------|
| `New(opts ...Option) (*IronFlock, error)` | Creates an instance from the environment and options; does not connect |
| `Start(ctx) error` | Connects and blocks until the app's realm is joined. A realm that does not exist yet (the data backend is still being provisioned) is waited for. A `Start` that failed (`ctx` ended first, say) may be called again. On a started or starting instance it fails with `ErrAlreadyStarted`, after `Stop` with `wamp.ErrStopped` |
| `Stop(ctx) error` | Closes every consumed-app connection and the connection itself, within `ctx`. Idempotent and final: operations then fail with `wamp.ErrStopped`, those still waiting for `Start` included, and `Run` returns once `main` does |
| `Run(ctx, main func(ctx) error) error` | `Start`, run `main` (or wait), `Stop` (within 10 s). `main`'s context is cancelled when `ctx` ends, on SIGINT or SIGTERM, and by `Stop` from anywhere; once shutdown has begun, a second signal terminates the process. Returns `main`'s error, or the `Start` error. On a started instance it fails with `ErrAlreadyStarted` and leaves it running |
| `IsConnected() bool` | Whether the connection is currently established |
| `Files() *filestore.FileStore` | The app's managed object storage — see [Managed File Storage](#managed-file-storage) |
| `Connection() *wamp.Connection` | The underlying connection, for advanced use — see [Advanced usage](#advanced-usage) |
| `Stage()` | The stage of the app's realm: `StageDevelopment` (`"DEV"`) or `StageProduction` (`"PROD"`). The cross-app API writes stages in lower case (`"dev"`, `"prod"`; `Stage.Lower()`) |
| `SerialNumber()`, `DeviceKey()`, `DeviceName()`, `AppName()`, `SwarmKey()`, `AppKey()` | The resolved identity |

### `Publish(ctx, topic, args ...any) error`

Publishes an event to a topic. The publish is acknowledged: a refusal by the router is returned as an error.
The device metadata `DEVICE_SERIAL_NUMBER`, `DEVICE_KEY` and `DEVICE_NAME` is added to the keyword arguments
(your own keys win).

```go
err := ifl.Publish(ctx, "com.myapp.mytopic", map[string]any{"temperature": 20})
```

### `PublishToTable(ctx, table, args ...any) error`

Publishes a row to a fleet table: the table's write topic `<SWARM_KEY>.<APP_KEY>.<table>`. Fire-and-forget —
the acknowledgement confirms delivery to the router, not the database insert. No app or dashboard may
subscribe to that topic; read rows back with `GetHistory` or `SubscribeToTable`.

```go
err := ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"temperature": 22.5, "humidity": 60})
```

### `AppendToTable(ctx, table, args ...any) (*Result, error)`

Appends a row by calling the table's append procedure `append.<SWARM_KEY>.<APP_KEY>.<table>` and returns the
insert outcome.

### `PublishRowsToTable(ctx, table, rows any, kwargs ...Kwargs) error`

Publishes **many rows in a single message** (bulk insert) to `bulk.<SWARM_KEY>.<APP_KEY>.<table>`. The
platform inserts the batch atomically (all-or-nothing). `rows` is a non-empty slice of rows (`[]Row`,
`[]map[string]any`, or a slice of structs).

```go
err := ifl.PublishRowsToTable(ctx, "sensordata", []ironflock.Row{
	{"tsp": "2024-01-15T10:30:00.000Z", "temperature": 22.5},
	{"tsp": "2024-01-15T10:30:01.000Z", "temperature": 22.7},
})
```

### `AppendRowsToTable(ctx, table, rows any, kwargs ...Kwargs) (*Result, error)`

Appends many rows in a single call to `appendBulk.<SWARM_KEY>.<APP_KEY>.<table>` and returns the outcome, e.g.
`{"success": true, "count": 2}`. If any row is invalid the entire batch is rejected.

### `ReportError(ctx, errOrMsg any, opts ...ReportErrorOptions) (*Result, error)`

Reports an application error into the fleet's `error-logs` table, stamped `source: "app"`, a level and a
timestamp. It is queryable with `GetHistory`, streamed on `transformed.error-logs`, usable in board
templates, and does not fire the platform's system-error toast. An `error` is recorded with `%+v`, so errors
that carry a stack trace include it.

```go
ifl.ReportError(ctx, "Sensor read timed out", ironflock.ReportErrorOptions{Level: ironflock.LevelWarn})
ifl.ReportError(ctx, err)                                                  // fire-and-forget publish
res, err := ifl.ReportError(ctx, "Calibration failed", ironflock.ReportErrorOptions{
	Append:      true,                 // use the append procedure and return the insert outcome
	UserMessage: "Please recalibrate", // operator-facing text (default: the message)
})
```

### `Subscribe(ctx, topic, handler, opts ...SubscribeOptions) (*Subscription, error)`

Subscribes to a topic. The subscription is restored automatically after every reconnect.

```go
sub, err := ifl.Subscribe(ctx, "com.myapp.mytopic", func(ev *ironflock.Event) {
	log.Println(ev.Args, ev.Kwargs)
})
defer ifl.Unsubscribe(ctx, sub)
```

Each subscription delivers its events in order, one at a time, on its own goroutine, so a handler may call any
other method, including `Call`. A slow handler delays only its own subscription. Subscriptions that must not
run their handlers concurrently share a delivery group: pass the same `wamp.NewDeliveryGroup()` as
`SubscribeOptions.Group` to each, and they deliver one event at a time across all of them, in the order the
events arrive. `Unsubscribe` takes effect at once: the handler is called for no further event. Use your own
topic names — see [URIs an app may use](#uris-an-app-may-use). `SubscribeOptions{Match: "prefix"}` subscribes
to a prefix.

### `SubscribeToTable(ctx, table, handler, opts ...SubscribeOptions) (*TableSubscription, error)`

Subscribes to the stored rows of a table: the data backend's realtime feed `transformed.<table>` and its bulk
counterpart `transformed.bulk.<table>`. Each event carries one row as stored — values typed to the
data-template columns, secret columns masked — in `ev.Args[0]` (`ev.Row()`). Rows of a bulk insert arrive as
one event per row, so handler code stays the same. The handler is called one event at a time, in the order the
events arrive on both feeds: it never runs concurrently with itself. `TableSubscription.Unsubscribe` removes
both subscriptions; no handler call starts after it.

```go
ts, err := ifl.SubscribeToTable(ctx, "sensordata", func(ev *ironflock.Event) {
	row := ev.Row()
	log.Println(row["temperature"])
})
```

### `GetHistory(ctx, table, q *TableQueryParams) ([]Row, error)`

Reads rows of a table or transform (`history.transformed.<table>`). `nil` reads the 10 most recent rows.

```go
rows, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
	Limit:     500,
	TimeRange: ironflock.Between(start, end),
	FilterAnd: []ironflock.Filter{
		ironflock.Where("temperature", ">", 20),
		ironflock.Where("humidity", "<=", 80),
	},
	Columns: []string{"temperature", "humidity"},
})

// Current value(s) only: the data backend derives the latest row per entity
// (the table's maintainLatestFlagFor columns; without one, the most recent row).
current, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
	Limit:     100,
	FilterAnd: []ironflock.Filter{ironflock.Latest()},
})

// OR groups: entries of FilterAnd are AND-ed; a group combines its own entries.
live, err := ifl.GetHistory(ctx, "parts", &ironflock.TableQueryParams{
	Limit:     100,
	FilterAnd: []ironflock.Filter{ironflock.Or(ironflock.IsNull("deleted"), ironflock.Where("deleted", "=", false))},
})
```

| Field | Description |
|-------|-------------|
| `Limit` | Maximum number of rows, 1–10000 (required) |
| `Offset` | Rows to skip |
| `TimeRange` | `&TimeRange{Start, End}`; each bound a `time.Time`, an ISO 8601 string, an epoch-milliseconds number, or `nil` for an open end (`Between`, `Since`, `Until` build one from `time.Time`). Strings and numbers cannot be mixed |
| `FilterAnd` | AND-ed filters: `Where(column, operator, value)`, `IsNull`, `IsNotNull`, the `Latest()` marker, `Or(...)`/`And(...)` groups |
| `Columns` | Columns to return (`tsp`, `device_key` and `authid` are always included); `nil` for all |

A `time.Time` bound is sent as RFC 3339 in UTC. A string bound is sent exactly as given, and the data backend
reads it with JavaScript's `Date`, so the SDK accepts the extended ISO 8601 forms `Date` reads correctly: a
date (`2026`, `2026-07`, `2026-07-01`), or a date and a time of day (`2026-07-01T12:30`, `2026-07-01T12:30:15`,
`2026-07-01T12:30:15.250`; `T`, `t` or a space between them) with an optional `Z` or UTC offset (`+02:00` or
`+0200`). A time without either is read as UTC. Other strings — basic format (`20260701T123000Z`), hours
without minutes, offsets of hours only, days a month does not have (`2026-02-30`) — fail with
`ErrInvalidArgument` before anything is sent.

Operators: `=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`, `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`, `IN`, `NOT IN`,
`IS NULL`, `IS NOT NULL`. `IN`/`NOT IN` take a slice. `IS NULL` and `IS NOT NULL` take no value and are the
only way to ask about NULL. Columns your data template declares `secret: true` come back as
`ironflock.SecretPlaceholder`.

`ironflock.DecodeRows[T](rows)` converts rows into a slice of your own struct type (decode `tsp` into an
`int64`: it arrives as epoch milliseconds).

### `GetSeriesHistory(ctx, table, q SeriesQueryParams) ([]Row, error)`

Reads **down-sampled time series** of a table — numeric columns aggregated into time buckets — ideal for
charts over long ranges (`history.transformed.series.<table>`; tables only, not transforms).

```go
series, err := ifl.GetSeriesHistory(ctx, "sensordata", ironflock.SeriesQueryParams{
	Metrics:   []string{"temperature", "humidity"},
	Method:    ironflock.MethodAvg, // AVG, SUM, COUNT, MIN, MAX, FIRST, LAST
	Limit:     500,
	TimeRange: ironflock.Between(start, end), // required
	GroupBy:   []string{"device_key"},
})
```

`FilterAnd` takes predicates only here: the `Latest()` marker and groups are not supported in series queries.

### `Call(ctx, topic, args ...any) (*Result, error)`

Calls a procedure by its full WAMP URI. A `CallOptions` value among the arguments configures the call.
An app cannot register names like `com.myapp.proc`, so this reaches procedures that platform services register
on the app's realm; to call a function another device of your app registered, use `CallDeviceFunction`.

### `CallDeviceFunction(ctx, deviceKey int, topic, args ...any) (*Result, error)`

Calls a function registered by another device of this app with `RegisterDeviceFunction`. The full URI is
`<SWARM_KEY>.<deviceKey>.<APP_KEY>.<STAGE>.<topic>`, with `STAGE` `DEV` or `PROD`.

```go
res, err := ifl.CallDeviceFunction(ctx, 42, "com.myapp.add", 1, 2)
sum := res.Value() // int64(3)
```

### `RegisterDeviceFunction(ctx, topic, handler, opts ...RegisterOptions) (*Registration, error)`

Registers a function other devices of this app — and dashboard widget actions — can call, as
`<SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.<topic>`. The registration is restored after every reconnect.
`Register` is an alias.

```go
_, err := ifl.RegisterDeviceFunction(ctx, "com.myapp.add", func(ctx context.Context, inv *ironflock.Invocation) (any, error) {
	if len(inv.Args) != 2 {
		return nil, errors.New("add takes two numbers")
	}
	a, ok1 := inv.Args[0].(int64)
	b, ok2 := inv.Args[1].(int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("add takes two integers, got %T and %T", inv.Args[0], inv.Args[1])
	}
	return a + b, nil
})
```

The returned value becomes the result (`*Result` for several values or keyword results); a returned
`*WampError` is sent with its URI, any other error as `wamp.error.runtime_error`.

### `SetDeviceLocation(ctx, long, lat float64) (*Result, error)`

Asks the platform to update the device's location. **Not available at the moment:** no platform service
answers it on the app's realm yet, so it fails with `wamp.error.no_such_procedure`.

### `GetRemoteAccessURLForPort(port int, protocol, remotePortEnvironment string) (string, bool)`

The public URL under which a port declared in `port-template.yml` is reachable once its tunnel is active,
e.g. `https://<device_key>-<app_name>-8080.app.ironflock.com`. `protocol` is `http` (default when empty),
`https`, `tcp` or `udp`; tcp/udp ports need the template's `remote_port_environment` name. Values are read
live, so call it again rather than caching the result.

## Secret Columns

A data-template column declared `secret: true` is encrypted by the data backend before it is stored. No read
path hands the plaintext back by accident: `GetHistory`, `SubscribeToTable` and cross-app reads return
`ironflock.SecretPlaceholder` (`"__secret__"`) for a non-null value. The two procedures below are callable
only by your app's own containers.

### `RevealSecrets(ctx, table, q *TableQueryParams) ([]Row, error)`

Reads rows with the secret columns decrypted. `nil` reads the 10 most recent rows; `Limit` must be 1–100.
Select rows by their non-secret columns — values are encrypted with a random IV, so filtering by the secret
itself can never match.

```go
rows, err := ifl.RevealSecrets(ctx, "credentials", &ironflock.TableQueryParams{
	Limit: 1, FilterAnd: []ironflock.Filter{ironflock.Latest()},
})
```

### `VerifySecret(ctx, table, column, candidate string, q *TableQueryParams) (*SecretVerifyResult, error)`

Checks a candidate against a stored secret without reading it back; the comparison runs in constant time
inside the data backend. `nil` checks the most recent row. `Match` is true when any selected row matches;
an unexpected response never reads as a match.

```go
res, err := ifl.VerifySecret(ctx, "credentials", "api_key", receivedKey, nil)
if err == nil && res.Match { /* ... */ }
```

## Cross-App Data Access

Read another app's fleet data, in the same project and fleet. Your app must declare the provider app in its
data-template `consumes:` section (or hold the wildcard grant, `consumes: [{app: "*"}]`), and the project user
must grant access. Access is **read-only**.

```yaml
# .ironflock/data-template.yml of your app
consumes:
  - app: weather-app
```

```go
weather, err := ifl.ConnectToApp(ctx, "weather-app")
var cerr *ironflock.CrossAppAccessError
if errors.As(err, &cerr) {
	log.Fatal(cerr.Code) // e.g. NO_GRANT
}

for _, t := range weather.Tables {
	log.Println(t.Tablename)
}
rows, err := weather.GetHistory(ctx, "forecasts", &ironflock.TableQueryParams{Limit: 100})
_, err = weather.SubscribeToTable(ctx, "forecasts", func(ev *ironflock.Event) { log.Println(ev.Row()) })
```

| Method | Description |
|--------|-------------|
| `ConnectToApp(ctx, app, opts ...ConnectToAppOptions) (*ConsumedApp, error)` | Opens (or returns the cached) read-only handle. `Stage` selects `"dev"`/`"prod"` (default: this app's stage); `OnError` is called when an open handle is later denied (e.g. the grant was revoked) |
| `ListConsumableApps(ctx) ([]ConsumedAppInfo, error)` | Lists every non-private provider and its catalog without connecting (wildcard consumers) |
| `ConnectToAllApps(ctx, opts ...ConnectToAllAppsOptions) ([]*ConsumedApp, error)` | Opens every provider with a data backend for the stage (wildcard consumers). Failures go to `OnError` unless `StopOnError` is set |
| `(*ConsumedApp).GetHistory`, `GetSeriesHistory`, `SubscribeToTable` | As for your own tables (series: tables only) |
| `(*ConsumedApp).Close(ctx)` | Closes the handle; `Stop` closes all of them |

Handles are cached per app and stage, and concurrent opens share one attempt; `ctx` bounds only the caller's
wait. `ConsumedApp.Stage` is the stage in lower case (`"dev"` or `"prod"`), unlike `IronFlock.Stage()`.
`Stop` aborts opens still in flight: they fail with an error wrapping `wamp.ErrStopped`. Errors are
`*CrossAppAccessError` with a `Code`: `NO_GRANT`, `PROVIDER_NOT_INSTALLED`, `UNKNOWN_APP`, `PRIVATE_TABLE`
(not in the provider's shared catalog), `SECRET_COLUMN` (a query filters on or selects a column the provider
marks secret — including inside filter groups), `NOT_AUTHORIZED`.

## Managed File Storage

Every app data backend gets private object storage alongside its tables.

```go
files := ifl.Files()

// Store an object and get a permanent URL back in the same call.
info, err := files.Put(ctx, "part-1.jpg", jpeg, filestore.ContentType("image/jpeg"))

// That URL is safe to put in a table column — a dashboard widget can render it.
err = ifl.PublishToTable(ctx, "inspections", ironflock.Row{"part_id": "1", "photo_url": info.URL})

data, err := files.Get(ctx, "part-1.jpg")
for obj, err := range files.Iter(ctx, filestore.Prefix("2026/")) {
	if err != nil {
		break
	}
	log.Println(obj.Key, obj.Size)
}
```

| Method | Description |
|--------|-------------|
| `Put(ctx, key, data, opts...)` / `PutReader(ctx, key, r, size, opts...)` / `PutFile(ctx, key, path, opts...)` | Store bytes, a stream, or a local file. Returns `*ObjectInfo` with a permanent `URL` |
| `Get(ctx, key, opts...)` / `GetTo(ctx, key, w, opts...)` / `GetToFile(ctx, key, path, opts...)` | Read into memory, a writer, or a file |
| `List(ctx, opts...)` / `Iter(ctx, opts...)` | One page / every object (an `iter.Seq2`) under a prefix |
| `Stat(ctx, key, opts...)` / `Exists(ctx, key, opts...)` | Metadata without transferring |
| `Delete` / `Copy(ctx, key, to, opts...)` / `Move` | `Move` is copy-then-delete and **not atomic** |
| `URL(ctx, key, opts...)` / `CloudURL` | Permanent authenticated URL (`""` where the deployment has no HTTP edge) |
| `ShareURL(ctx, key, opts...)` | Expiring bearer link (default 15 min) — hand it to a person, do not store it |
| `UploadURL(ctx, key, opts...)` | Expiring presigned upload target |
| `Usage(ctx, opts...)` | Bytes stored, quota and free bytes (`-1` when unlimited); `filestore.Detail()` adds a per-namespace breakdown |
| `Catalog(ctx)` / `RefreshCatalog(ctx)` / `Namespaces(ctx)` | Namespaces and server limits (cached) |

Options: `filestore.Namespace(n)` (default `"default"`), `ContentType`, `ToNamespace`, `Version`, `TTL`,
`Size`, `Prefix`, `Limit`, `Cursor`, `Detail`.

`Files()` does no I/O and may be called before `Start`. A file call waits up to 10 s for the connection (before
`Start` as well) and is not retried: unlike table operations, file calls do not use the reconnect window, so
during a platform restart a call can fail with `NOT_AVAILABLE` until the file service has registered again.

Objects up to the server's `InlineMaxBytes` (6 MiB) travel through the router; larger ones go directly to the
object store over HTTPS through presigned URLs (`PutReader`, `PutFile`, `GetTo` and `GetToFile` stream, so a
multi-gigabyte object never has to fit in memory). The direct path needs the device to reach the object store
host; HTTP(S) proxies from the environment are honoured. `Stop` closes the idle connections of the store's HTTP
client (`FileStore.CloseIdleConnections` does that for a store you create with `filestore.New(caller, nil)`).

`GetToFile` downloads into a temporary file next to the target and renames it into place once complete, so a
failed download never leaves a partial file and keeps the previous one; the replaced file's permission bits are
kept. Because the file is replaced rather than rewritten, its directory must be writable, and other hard links
to it keep the old content. Symbolic links are followed, as Python's `open(path, "wb")` follows them: the file
they lead to is replaced (created when the last link dangles) and the links stay. A FIFO or a device is written
in place as the object streams in, and a file that is a mount point (a single file bind-mounted into the
container) is overwritten once the download is complete.

A failure of the file service or of a direct transfer is a `*filestore.Error` with a stable `Code` — branch on
it, never on `Reason` (a cancelled context, local file errors and router-level WAMP errors other than the ones
below come back as they are):
`NOT_AUTHORIZED`, `NO_SUCH_NAMESPACE`, `NO_SUCH_OBJECT`, `TOO_LARGE`, `OBJECT_TOO_LARGE`, `QUOTA_EXCEEDED`,
`CONTENT_TYPE_NOT_ALLOWED`, `NOT_SUPPORTED`, `NOT_AVAILABLE` (no file service on this deployment, or not yet
after a restart), `PRESIGN_UNREACHABLE` (object store not reachable directly — a proxy?), `CLOCK_SKEW` (device
clock too far off; check NTP), `INTERNAL`. A newer server may add codes; they pass through as-is.

## URIs an app may use

Your app joins its own data realm, `realm-<SWARM_KEY>-<APP_KEY>-<dev|prod>`, with the router role `app`. The
router checks every publish, subscribe, call and register there against that role's allow-list; the SDK's own
methods stay inside it. It matters when you pass your own URIs to `Publish`, `Subscribe`, `Call` or the
connection. A refused action fails with `wamp.error.not_authorized` (`ironflock.WampURI(err)`).

| URI on your own realm | Allowed | SDK method |
|-----------------------|---------|------------|
| Your own names, e.g. `com.myapp.status` | publish, subscribe (prefix subscriptions too), call; never register | `Publish`, `Subscribe`, `Call` |
| `<SWARM_KEY>.<APP_KEY>.<table>`, `bulk.<SWARM_KEY>.<APP_KEY>.<table>` | publish only (never subscribe) | `PublishToTable`, `ReportError`, `PublishRowsToTable` |
| `transformed.<table>`, `transformed.bulk.<table>` | subscribe only | `SubscribeToTable` |
| `databackend.errors.…`, `files.events…` | subscribe only | – (the data backend's error and file events) |
| `<SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.<name>` | register (single), call, publish | `RegisterDeviceFunction`, `CallDeviceFunction` |
| `append.…`, `appendBulk.…`, `history.transformed.…`, `secret.reveal.…`, `secret.verify.…`, `files.read.…`, `files.write.…` | call (the SDK calls them) | table, history, secret and file APIs |
| `sys.appaccess.list`, `sys.appaccess.resolve` | call | `ListConsumableApps`, `ConnectToApp`, `ConnectToAllApps` |

**Identity-checked names.** Names that start with a digit — the table write topics and the device-function
URIs — also pass the platform's identity check: only the device a function URI names may register it (or
publish on it), and only with your realm's swarm key, app key and stage; a table write topic must carry your
realm's swarm key and app key. Prefix and wildcard patterns over these names are refused. Let the SDK build
these URIs (`RegisterDeviceFunction`, `CallDeviceFunction`, `PublishToTable`).

**Reserved names.** The router refuses `sys.` apart from the two cross-app procedures above, and `wamp.` apart
from `wamp.session.get` (the JavaScript SDK's heartbeat). The other names in the table belong to the platform:
do not start your own names with a digit, `bulk.`, `transformed.`, `databackend.errors.`, `files.`, `append.`,
`appendBulk.`, `history.` or `secret.`. Every other first segment is your app's own topic space. Nobody
subscribes to raw table topics: they carry the row as sent, including the plaintext of secret columns — use
`SubscribeToTable`.

## Connection reliability

The connection reconnects on its own. When the socket drops, the SDK retries until the router is back
(1 s backoff growing to 2 s), then restores every subscription and every registered device function — you
never re-subscribe. If one cannot be restored, the rest still are, and the failed one is retried on the next
reconnect.

A router that is reachable but has no realm for the app yet (the data backend is still being provisioned) is
retried every couple of seconds, so the app comes up the moment the realm does. A realm still missing after
a minute is most likely never going to appear, so the SDK slows to one attempt every two minutes until it
does.

A socket that dies *silently* — an idle NAT or proxy that drops the connection without telling either side —
is caught by WebSocket keepalive: the SDK pings every 20 s and drops a link whose pong is missing for two
intervals, then reconnects. This covers subscribe-only apps and cross-app connections, which never write.

Table operations (`PublishToTable`, `AppendToTable`, the bulk variants, `ReportError`, the history, series
and secret reads, and a consumed app's history reads) ride out a platform restart: they wait up to the
reconnect window (default 60 s) for the connection to come back, and retry while the data backend has not
registered its procedures again yet. Other operations — `Publish`, `Subscribe`, `Call`, the device functions,
cross-app discovery and file calls — wait up to 10 s for a connection and are not retried. An operation issued
before `Start` waits for `Start` within the same time. When the time is up, the operation fails with an error
wrapping `wamp.ErrNotConnected`.

## Errors

| Error | Test with | Meaning |
|-------|-----------|---------|
| `ironflock.ErrInvalidArgument` | `errors.Is` | Invalid parameters (the Python SDK's `ValueError`), reported before anything is sent |
| `ironflock.ErrMissingConfig` | `errors.Is` | Identity the operation needs is missing: the serial number (`New`), `SWARM_KEY` or `APP_KEY` (`Start`, table operations), a device key (device functions) |
| `ironflock.ErrAlreadyStarted` | `errors.Is` | `Start` or `Run` on an instance that is started or starting |
| `wamp.ErrNotConnected` | `errors.Is` | No connection within the operation's wait (see [Connection reliability](#connection-reliability)) |
| `wamp.ErrStopped` | `errors.Is` | Stopped for good: an operation, `Start` or `ConnectToApp` after `Stop` or interrupted by it, or a consumed app after `Close` or a revoked grant |
| `wamp.ErrNotConfigured` | `errors.Is` | `ifl.Connection()` used before `Start` configured it (the `IronFlock` methods wait for `Start` instead) |
| `*ironflock.OperationError` | `errors.As` | A failed operation; the message names the operation and the cause, which it wraps |
| `*wamp.Error` (`ironflock.WampError`) | `errors.As`, `ironflock.WampURI(err)` | The router or callee refused; carries the URI and payload |
| `*ironflock.CrossAppAccessError` | `errors.As` | Cross-app access was declined or misused (`Code`) |
| `*filestore.Error` (`ironflock.FileStoreError`) | `errors.As` | A file operation failed (`Code`) |

`errors.Is` and `errors.As` see through `*OperationError`: `errors.Is(err, wamp.ErrStopped)` holds for a
`PublishToTable` after `Stop`.

## Advanced usage

`ifl.Connection()` is the underlying `*wamp.Connection`, a self-healing WAMP session with the methods
`Call`, `Publish`, `Subscribe`, `Register` and friends; it can also be used on its own (see
[examples/connection](examples/connection)). Unlike the `IronFlock` methods, its operations do not wait for
`Start`: until `Start` has configured the connection they fail with `wamp.ErrNotConfigured`. The rules in
[URIs an app may use](#uris-an-app-may-use) apply to it as well.

## Differences from the Python and JavaScript SDKs

The Go SDK follows the Python and JavaScript SDKs and puts the same messages on the wire. Where they differ,
it takes the more robust variant; Go-specific choices:

- Every operation takes a `context.Context`; durations are `time.Duration`.
- Arguments are variadic with a `Kwargs` marker instead of `*args, **kwargs` / `(args, kwargs)`.
- Operations issued before `Start` wait for it within their window (as JavaScript waits); Python fails them at
  once.
- `SubscribeToTable` returns a handle that unsubscribes both the row and the bulk subscription.
- `VerifySecret` returns `{Match, Checked}` (as JavaScript); Python returns only the boolean.
- Filter groups (`Or`/`And`) are supported (as JavaScript), and the cross-app secret-column guard also looks
  inside them.
- Liveness uses WebSocket pings (as Python); the JavaScript SDK uses a WAMP heartbeat because browsers have no
  ping API.
- There are no retained-event options (`get_retained`, `retain`): ironflock-router does not keep retained
  events. Raw WAMP options still pass through `SubscribeOptions.Extra` and `PublishOptions.Extra`.

## Development

```shell
just test               # unit tests with the race detector (go test -race -count=1 ./...)
just check              # go vet, golangci-lint, cross-builds for the device platforms, go mod tidy -diff
just coverage           # unit test coverage summary
just test-integration   # end-to-end suites against ironflock-router (ROUTER_IMAGE, or ROUTER_BIN and ROUTER_REF_CONFIG)
```

The unit tests need nothing external: they run against an in-process nexus router. The end-to-end tests run
against [ironflock-router](https://github.com/RecordEvolution/ironflock-router) and a fake IronFlock platform,
call across to the Python SDK, and compare the wire payloads with the Python SDK's run of the same scenario.
They live in [integration/](integration), whose README covers the harness, the environment variables and CI;
the [Justfile](Justfile) lists every task.

## License

MIT
