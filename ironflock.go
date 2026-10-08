// Package ironflock is the Go SDK of the IronFlock IoT DevOps platform.
//
// An app running on an IronFlock device uses it to publish data into its
// fleet's tables, read table history, subscribe to realtime rows, call and
// register device functions, read other apps' shared data, and store files.
// When the app runs on a device registered in IronFlock, the SDK reads its
// identity and credentials from the environment the device agent injects
// and joins the app's private data realm of the fleet automatically.
//
//	ifl, err := ironflock.New()
//	if err != nil { log.Fatal(err) }
//	err = ifl.Run(context.Background(), func(ctx context.Context) error {
//		for {
//			if err := ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"temperature": 22.5}); err != nil {
//				log.Print(err)
//			}
//			select {
//			case <-ctx.Done():
//				return nil
//			case <-time.After(3 * time.Second):
//			}
//		}
//	})
//
// The connection reconnects on its own and restores every subscription and
// registered device function after a reconnect. Table operations ride out a
// platform restart for up to the reconnect window (60s by default) before
// they fail.
package ironflock

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/RecordEvolution/ironflock-go/crossbar"
	"github.com/RecordEvolution/ironflock-go/filestore"
)

// Aliases of the connection-level types, so most apps need only this package.
type (
	Event             = crossbar.Event
	EventHandler      = crossbar.EventHandler
	Invocation        = crossbar.Invocation
	InvocationHandler = crossbar.InvocationHandler
	Result            = crossbar.Result
	WampError         = crossbar.WampError
	SubscribeOptions  = crossbar.SubscribeOptions
	RegisterOptions   = crossbar.RegisterOptions
	CallOptions       = crossbar.CallOptions
	Subscription      = crossbar.Subscription
	Registration      = crossbar.Registration
	Stage             = crossbar.Stage
	FileStore         = filestore.FileStore
	FileStoreError    = filestore.Error
)

// Stages.
const (
	StageDevelopment = crossbar.StageDevelopment
	StageProduction  = crossbar.StageProduction
)

// ErrorLogsTable is the per-data-backend error table ReportError writes to.
// It behaves like any other table (GetHistory, SubscribeToTable, boards).
const ErrorLogsTable = "error-logs"

// DefaultReconnectWindow is how long a table operation rides out a platform
// restart by default (see WithReconnectWindow).
const DefaultReconnectWindow = 60 * time.Second

// Option configures New.
type Option func(*config)

type config struct {
	serialNumber    string
	deviceName      *string
	deviceKey       *string
	appName         *string
	swarmKey        *int
	appKey          *int
	env             *string
	reswarmURL      string
	url             string
	authID          string
	authSecret      string
	reconnectWindow time.Duration
	logger          *slog.Logger
}

// WithSerialNumber sets the device serial number (default: the
// DEVICE_SERIAL_NUMBER environment variable). It can also be used to
// authenticate as another device.
func WithSerialNumber(serial string) Option { return func(c *config) { c.serialNumber = serial } }

// WithDeviceName overrides the DEVICE_NAME environment variable.
func WithDeviceName(name string) Option { return func(c *config) { c.deviceName = &name } }

// WithDeviceKey overrides the DEVICE_KEY environment variable.
func WithDeviceKey(key string) Option { return func(c *config) { c.deviceKey = &key } }

// WithAppName overrides the APP_NAME environment variable.
func WithAppName(name string) Option { return func(c *config) { c.appName = &name } }

// WithSwarmKey overrides the SWARM_KEY environment variable.
func WithSwarmKey(key int) Option { return func(c *config) { c.swarmKey = &key } }

// WithAppKey overrides the APP_KEY environment variable.
func WithAppKey(key int) Option { return func(c *config) { c.appKey = &key } }

// WithEnv overrides the ENV environment variable ("PROD" in any case selects
// the production realm; anything else the development realm).
func WithEnv(env string) Option { return func(c *config) { c.env = &env } }

// WithReswarmURL sets the studio URL used to look up the router (default:
// the RESWARM_URL environment variable). DEVICE_ENDPOINT_URL and WithURL
// take precedence.
func WithReswarmURL(u string) Option { return func(c *config) { c.reswarmURL = u } }

// WithURL sets the router WebSocket URL directly, e.g.
// "wss://cbw.ironflock.com/ws-ua-usr".
func WithURL(u string) Option { return func(c *config) { c.url = u } }

// WithCredentials sets the WAMP-CRA credential explicitly instead of the
// per-app credential the device agent injects (APP_AUTH_ID/APP_AUTH_SECRET)
// or the legacy (serial, serial) fallback.
func WithCredentials(authID, secret string) Option {
	return func(c *config) { c.authID, c.authSecret = authID, secret }
}

// WithReconnectWindow sets how long a table operation (AppendToTable,
// AppendRowsToTable, PublishToTable, PublishRowsToTable, ReportError,
// GetHistory, GetSeriesHistory, RevealSecrets, VerifySecret, and a consumed
// app's history reads) rides out a platform restart before it fails: it
// waits that long for the connection to come back and for the platform to
// serve the app's tables again. Default 60s; 0 turns it off (the default
// 10s connection wait, no retries).
func WithReconnectWindow(d time.Duration) Option {
	return func(c *config) { c.reconnectWindow = d }
}

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// wampConn is the connection surface the SDK uses; *crossbar.Connection
// implements it. Tests substitute a fake.
type wampConn interface {
	Configure(cfg crossbar.Config) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	IsOpen() bool
	URL() string
	Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *crossbar.CallOptions, retryWindow time.Duration) (*crossbar.Result, error)
	Publish(ctx context.Context, topic string, args []any, kwargs map[string]any, opts *crossbar.PublishOptions, waitWindow time.Duration) error
	Subscribe(ctx context.Context, topic string, handler crossbar.EventHandler, opts *crossbar.SubscribeOptions) (*crossbar.Subscription, error)
	Unsubscribe(ctx context.Context, sub *crossbar.Subscription) error
	Register(ctx context.Context, procedure string, handler crossbar.InvocationHandler, opts *crossbar.RegisterOptions) (*crossbar.Registration, error)
	Unregister(ctx context.Context, reg *crossbar.Registration) error
}

// IronFlock is a connection to the IronFlock platform for one app on one
// device. It is safe for concurrent use.
type IronFlock struct {
	log *slog.Logger

	serialNumber string
	deviceName   string
	deviceKey    string
	appName      string
	swarmKey     int
	appKey       int
	stage        Stage
	reswarmURL   string
	url          string
	authID       string
	authSecret   string

	reconnectWindow time.Duration

	conn    wampConn
	newConn func() wampConn

	mu         sync.Mutex
	configured bool
	started    bool
	stopped    bool
	files      *filestore.FileStore
	consumed   map[string]*consumedEntry
}

// New creates an IronFlock instance from the environment the device agent
// injects (DEVICE_SERIAL_NUMBER, DEVICE_KEY, DEVICE_NAME, APP_NAME,
// SWARM_KEY, APP_KEY, ENV, DEVICE_ENDPOINT_URL / RESWARM_URL,
// APP_AUTH_ID / APP_AUTH_SECRET) and opts. It fails only when no serial
// number is available; other missing variables are logged as a warning and
// fail the operations that need them.
func New(opts ...Option) (*IronFlock, error) {
	panic("TODO: implement")
}

// Connection returns the underlying connection, for advanced use.
func (f *IronFlock) Connection() *crossbar.Connection {
	c, _ := f.conn.(*crossbar.Connection)
	return c
}

// IsConnected reports whether the connection to the platform is established.
func (f *IronFlock) IsConnected() bool { return f.conn.IsOpen() }

// Stage returns the stage of the realm the app joins.
func (f *IronFlock) Stage() Stage { return f.stage }

// SerialNumber returns the device serial number.
func (f *IronFlock) SerialNumber() string { return f.serialNumber }

// DeviceKey returns the device key ("" when unknown).
func (f *IronFlock) DeviceKey() string { return f.deviceKey }

// DeviceName returns the device name ("" when unknown).
func (f *IronFlock) DeviceName() string { return f.deviceName }

// AppName returns the app name ("" when unknown).
func (f *IronFlock) AppName() string { return f.appName }

// SwarmKey returns the swarm key (0 when unknown).
func (f *IronFlock) SwarmKey() int { return f.swarmKey }

// AppKey returns the app key (0 when unknown).
func (f *IronFlock) AppKey() int { return f.appKey }

// Files returns the app's managed object storage. It is safe to use before
// Start: every call waits for the connection like the table API does.
func (f *IronFlock) Files() *filestore.FileStore {
	panic("TODO: implement")
}

// Start configures and opens the connection, and blocks until the app's
// realm is joined or ctx is done. A realm that does not exist yet (the
// data backend is still being provisioned) is waited for.
func (f *IronFlock) Start(ctx context.Context) error {
	panic("TODO: implement")
}

// Stop closes every consumed-app connection and the connection itself. It
// is idempotent.
func (f *IronFlock) Stop(ctx context.Context) error {
	panic("TODO: implement")
}

// Run starts the connection, runs main, and stops when main returns, ctx is
// done, or the process receives SIGINT or SIGTERM. With a nil main it runs
// until ctx is done or a signal arrives. The context passed to main is
// cancelled on shutdown. Run returns main's error, or the Start error.
func (f *IronFlock) Run(ctx context.Context, main func(ctx context.Context) error) error {
	panic("TODO: implement")
}

// Publish publishes an event to topic. Positional args are sent as WAMP
// args; a Kwargs value among them as WAMP kwargs. The device metadata
// (DEVICE_SERIAL_NUMBER, DEVICE_KEY, DEVICE_NAME) is added to the kwargs,
// user keys winning. The publish is acknowledged: a refusal by the router is
// returned as an error.
func (f *IronFlock) Publish(ctx context.Context, topic string, args ...any) error {
	panic("TODO: implement")
}

// PublishToTable publishes a row to a fleet table: the table's write topic
// <SWARM_KEY>.<APP_KEY>.<table>. Fire-and-forget: the acknowledgement
// confirms delivery to the router, not the database insert.
//
//	ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"temperature": 22.5})
func (f *IronFlock) PublishToTable(ctx context.Context, table string, args ...any) error {
	panic("TODO: implement")
}

// AppendToTable appends a row to a fleet table by calling its append
// procedure append.<SWARM_KEY>.<APP_KEY>.<table>, and returns the insert
// outcome.
func (f *IronFlock) AppendToTable(ctx context.Context, table string, args ...any) (*Result, error) {
	panic("TODO: implement")
}

// PublishRowsToTable publishes many rows in a single message (bulk insert)
// to bulk.<SWARM_KEY>.<APP_KEY>.<table>; the platform inserts the batch
// atomically. rows is a non-empty slice of rows (Row / map[string]any, or
// structs encoded via their json tags). kwargs are shared by the batch.
func (f *IronFlock) PublishRowsToTable(ctx context.Context, table string, rows any, kwargs ...Kwargs) error {
	panic("TODO: implement")
}

// AppendRowsToTable appends many rows in a single call (bulk insert) to
// appendBulk.<SWARM_KEY>.<APP_KEY>.<table> and returns the outcome (e.g.
// {"success": true, "count": N}). All-or-nothing: if any row is invalid,
// nothing is persisted.
func (f *IronFlock) AppendRowsToTable(ctx context.Context, table string, rows any, kwargs ...Kwargs) (*Result, error) {
	panic("TODO: implement")
}

// ErrorLevel is the severity of a reported error.
type ErrorLevel string

// Error levels.
const (
	LevelError ErrorLevel = "error"
	LevelWarn  ErrorLevel = "warn"
	LevelInfo  ErrorLevel = "info"
	LevelDebug ErrorLevel = "debug"
)

// ReportErrorOptions configures ReportError.
type ReportErrorOptions struct {
	// Level defaults to LevelError.
	Level ErrorLevel
	// Append uses the append procedure and returns the insert outcome
	// instead of a fire-and-forget publish.
	Append bool
	// Tsp overrides the timestamp (default: now, RFC 3339 UTC).
	Tsp string
	// UserMessage is the operator-facing text boards show (default: msg).
	UserMessage string
}

// ReportError writes an application error into the data backend's
// error-logs table, stamped source "app" — queryable with GetHistory and
// streamed on transformed.error-logs, without firing the platform's
// system-error toast. errOrMsg is an error (recorded with fmt's %+v, so
// errors that carry a stack trace include it) or a message string. The
// Result is nil unless opts.Append is set.
func (f *IronFlock) ReportError(ctx context.Context, errOrMsg any, opts ...ReportErrorOptions) (*Result, error) {
	panic("TODO: implement")
}

// Subscribe subscribes handler to topic. The subscription is restored after
// every reconnect.
func (f *IronFlock) Subscribe(ctx context.Context, topic string, handler EventHandler, opts ...SubscribeOptions) (*Subscription, error) {
	panic("TODO: implement")
}

// Unsubscribe removes a subscription.
func (f *IronFlock) Unsubscribe(ctx context.Context, sub *Subscription) error {
	panic("TODO: implement")
}

// TableSubscription is the pair of subscriptions behind SubscribeToTable:
// the table's realtime feed and its bulk counterpart.
type TableSubscription struct {
	Rows *Subscription
	Bulk *Subscription
	conn wampConn
}

// Unsubscribe removes both subscriptions.
func (t *TableSubscription) Unsubscribe(ctx context.Context) error {
	panic("TODO: implement")
}

// SubscribeToTable subscribes handler to the stored rows of a table: the
// data backend's realtime feed transformed.<table> and its bulk counterpart
// transformed.bulk.<table>. Each event carries one row as stored — typed to
// the data-template columns, secret columns masked — in Args[0] (see
// Event.Row); rows of a bulk insert are delivered one event per row.
func (f *IronFlock) SubscribeToTable(ctx context.Context, table string, handler EventHandler, opts ...SubscribeOptions) (*TableSubscription, error) {
	panic("TODO: implement")
}

// Call calls a remote procedure by its full WAMP URI. Positional args are
// sent as WAMP args; a Kwargs value among them as WAMP kwargs, and a
// CallOptions value configures the call.
func (f *IronFlock) Call(ctx context.Context, topic string, args ...any) (*Result, error) {
	panic("TODO: implement")
}

// CallDeviceFunction calls a function another device of this app registered
// with RegisterDeviceFunction: <SWARM_KEY>.<deviceKey>.<APP_KEY>.<STAGE>.<topic>.
// Arguments as for Call.
func (f *IronFlock) CallDeviceFunction(ctx context.Context, deviceKey int, topic string, args ...any) (*Result, error) {
	panic("TODO: implement")
}

// RegisterDeviceFunction registers handler as a function other devices of
// this app (and dashboard widget actions) can call:
// <SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.<topic>. The router accepts
// only this shape and single registrations. The registration is restored
// after every reconnect.
func (f *IronFlock) RegisterDeviceFunction(ctx context.Context, topic string, handler InvocationHandler, opts ...RegisterOptions) (*Registration, error) {
	panic("TODO: implement")
}

// Register is an alias of RegisterDeviceFunction.
func (f *IronFlock) Register(ctx context.Context, topic string, handler InvocationHandler, opts ...RegisterOptions) (*Registration, error) {
	return f.RegisterDeviceFunction(ctx, topic, handler, opts...)
}

// Unregister removes a registration.
func (f *IronFlock) Unregister(ctx context.Context, reg *Registration) error {
	panic("TODO: implement")
}

// SetDeviceLocation asks the platform to update the device's location.
//
// Not served yet: no platform service registers
// ironflock.location_service.update on the app's realm, so it currently
// fails with wamp.error.no_such_procedure. Keep locations in a table of your
// own meanwhile.
func (f *IronFlock) SetDeviceLocation(ctx context.Context, long, lat float64) (*Result, error) {
	panic("TODO: implement")
}

// GetHistory reads rows of a table or transform via
// history.transformed.<table>. A nil q reads the 10 most recent rows. Secret
// columns come back as SecretPlaceholder.
func (f *IronFlock) GetHistory(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	panic("TODO: implement")
}

// GetSeriesHistory reads down-sampled time series of a table via
// history.transformed.series.<table>.
func (f *IronFlock) GetSeriesHistory(ctx context.Context, table string, q SeriesQueryParams) ([]Row, error) {
	panic("TODO: implement")
}

// RevealSecrets reads rows of an own table with its secret columns
// decrypted, via secret.reveal.<table>. A nil q reads the 10 most recent
// rows; Limit must be 1-100. Only the app's own containers may call it.
func (f *IronFlock) RevealSecrets(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	panic("TODO: implement")
}

// VerifySecret checks candidate against the secret column of the selected
// rows without reading it back (secret.verify.<table>); the comparison runs
// in constant time inside the data backend. A nil q checks the most recent
// row; Limit must be 1-100. A response of an unexpected shape never reads
// as a match.
func (f *IronFlock) VerifySecret(ctx context.Context, table, column, candidate string, q *TableQueryParams) (*SecretVerifyResult, error) {
	panic("TODO: implement")
}

// GetRemoteAccessURLForPort returns the public URL of a port declared in
// the app's port-template.yml, once its tunnel is active. protocol is
// "http" (default when empty), "https", "tcp" or "udp"; tcp/udp ports need
// the template's remote_port_environment name. Port values are read live
// from /data/env, so call it again rather than caching the result. It
// returns false when the URL cannot be composed.
func (f *IronFlock) GetRemoteAccessURLForPort(port int, protocol, remotePortEnvironment string) (string, bool) {
	panic("TODO: implement")
}
