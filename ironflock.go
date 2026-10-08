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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

// Shutdown budgets.
const (
	// defaultRunStopTimeout bounds the Stop that Run performs on shutdown.
	defaultRunStopTimeout = 10 * time.Second
	// defaultCleanupTimeout bounds tearing down what a failed or abandoned
	// operation left behind (a consumed-app connection whose open failed, a
	// half-made table subscription).
	defaultCleanupTimeout = 5 * time.Second
)

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
	// stopDone is closed when the first Stop has finished; nil before it.
	stopDone chan struct{}

	// openCtx is the context consumed-app opens run on: detached from the
	// callers that share an open (one caller giving up must not fail it for
	// the others), cancelled by Stop.
	openCtx    context.Context
	openCancel context.CancelFunc

	runStopTimeout time.Duration
	cleanupTimeout time.Duration
}

// New creates an IronFlock instance from the environment the device agent
// injects (DEVICE_SERIAL_NUMBER, DEVICE_KEY, DEVICE_NAME, APP_NAME,
// SWARM_KEY, APP_KEY, ENV, DEVICE_ENDPOINT_URL / RESWARM_URL,
// APP_AUTH_ID / APP_AUTH_SECRET) and opts. It fails only when no serial
// number is available; other missing variables are logged as a warning and
// fail the operations that need them.
func New(opts ...Option) (*IronFlock, error) {
	return newWithConn(crossbar.NewConnection(), func() wampConn { return crossbar.NewConnection() }, opts...)
}

// newWithConn is New with the own connection and the factory of consumed-app
// connections injected.
func newWithConn(conn wampConn, newConn func() wampConn, opts ...Option) (*IronFlock, error) {
	c := config{reconnectWindow: DefaultReconnectWindow}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	log := c.logger
	if log == nil {
		log = slog.Default()
	}

	serial, err := crossbar.SerialNumber(c.serialNumber)
	if err != nil {
		return nil, missingConfigf("%v", err)
	}
	if (c.authID == "") != (c.authSecret == "") {
		return nil, invalidf("WithCredentials needs both an auth id and a secret")
	}

	f := &IronFlock{
		log:             log,
		serialNumber:    serial,
		deviceName:      stringSetting(c.deviceName, "DEVICE_NAME"),
		deviceKey:       stringSetting(c.deviceKey, "DEVICE_KEY"),
		appName:         stringSetting(c.appName, "APP_NAME"),
		swarmKey:        keySetting(log, c.swarmKey, "SWARM_KEY"),
		appKey:          keySetting(log, c.appKey, "APP_KEY"),
		stage:           crossbar.StageFromEnv(stringSetting(c.env, "ENV")),
		reswarmURL:      c.reswarmURL,
		url:             c.url,
		authID:          c.authID,
		authSecret:      c.authSecret,
		reconnectWindow: max(c.reconnectWindow, 0),
		conn:            conn,
		newConn:         newConn,
		consumed:        make(map[string]*consumedEntry),
		runStopTimeout:  defaultRunStopTimeout,
		cleanupTimeout:  defaultCleanupTimeout,
	}
	f.openCtx, f.openCancel = context.WithCancel(context.Background())

	var missing []string
	if f.deviceKey == "" {
		missing = append(missing, "DEVICE_KEY")
	}
	if f.appName == "" {
		missing = append(missing, "APP_NAME")
	}
	if f.swarmKey <= 0 {
		missing = append(missing, "SWARM_KEY")
	}
	if f.appKey <= 0 {
		missing = append(missing, "APP_KEY")
	}
	if len(missing) > 0 {
		log.Warn("Warning: The following environment variables must be present: " + strings.Join(missing, ", "))
	}
	return f, nil
}

// stringSetting returns the option value when set, else the environment
// variable name.
func stringSetting(opt *string, name string) string {
	if opt != nil {
		return *opt
	}
	return os.Getenv(name)
}

// keySetting returns the option value when set, else the environment variable
// name parsed as an integer. Unset means 0; a value that is not an integer is
// logged and also reads as 0 (unset).
func keySetting(log *slog.Logger, opt *int, name string) int {
	if opt != nil {
		return *opt
	}
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Warn(fmt.Sprintf("%s=%q is not an integer; treating it as unset", name, raw))
		return 0
	}
	return v
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.files == nil {
		f.files = filestore.New(f.conn, nil)
	}
	return f.files
}

// Start configures and opens the connection, and blocks until the app's
// realm is joined or ctx is done. A realm that does not exist yet (the
// data backend is still being provisioned) is waited for.
//
// Start fails at once, with an error wrapping ErrMissingConfig, when
// SWARM_KEY or APP_KEY is unknown: the realm could never be joined. A
// failed Start may be retried; Start on a started or stopped IronFlock is
// an error.
func (f *IronFlock) Start(ctx context.Context) error {
	// The realm realm-<SWARM_KEY>-<APP_KEY>-<stage> can never exist without
	// both keys, so waiting for it would only hang.
	if err := f.requireKeys(); err != nil {
		return err
	}

	f.mu.Lock()
	switch {
	case f.stopped:
		f.mu.Unlock()
		return fmt.Errorf("ironflock: Start after Stop: %w", crossbar.ErrStopped)
	case f.started:
		f.mu.Unlock()
		return errors.New("ironflock: Start called while already started")
	}
	f.started = true
	if !f.configured {
		url, err := f.routerURL()
		if err == nil {
			err = f.conn.Configure(crossbar.Config{
				SwarmKey:     f.swarmKey,
				AppKey:       f.appKey,
				Stage:        f.stage,
				URL:          url,
				SerialNumber: f.serialNumber,
				AuthID:       f.authID,
				AuthSecret:   f.authSecret,
				Logger:       f.log,
			})
		}
		if err != nil {
			f.started = false
			f.mu.Unlock()
			return err
		}
		f.configured = true
	}
	f.mu.Unlock()

	if err := f.conn.Start(ctx); err != nil {
		// A failed Start may be retried.
		f.mu.Lock()
		f.started = false
		f.mu.Unlock()
		return err
	}
	return nil
}

// routerURL resolves the router URL: WithURL, else DEVICE_ENDPOINT_URL, else
// the studio URL (WithReswarmURL / RESWARM_URL), else the public cloud.
func (f *IronFlock) routerURL() (string, error) {
	if f.url != "" {
		return f.url, nil
	}
	return crossbar.WebSocketURI(f.reswarmURL)
}

// Stop closes every consumed-app connection and the connection itself. It
// is idempotent.
//
// Consumed-app connections still opening are aborted. Afterwards
// operations fail, and ConnectToApp returns an error wrapping
// crossbar.ErrStopped.
func (f *IronFlock) Stop(ctx context.Context) error {
	f.mu.Lock()
	if f.stopDone != nil {
		// Stopped or stopping: wait for the first Stop to finish.
		done := f.stopDone
		f.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan struct{})
	f.stopDone = done
	f.stopped = true
	entries := make([]*consumedEntry, 0, len(f.consumed))
	for _, e := range f.consumed {
		entries = append(entries, e)
	}
	f.consumed = make(map[string]*consumedEntry)
	f.mu.Unlock()
	defer close(done)

	// Abort consumed-app opens still in flight; one that completes anyway
	// closes its own connection (see runOpen).
	f.openCancel()
	f.closeConsumed(ctx, entries)
	return f.conn.Stop(ctx)
}

// closeConsumed closes the consumed apps of entries concurrently, waiting for
// opens still in flight within ctx. Failures are logged.
func (f *IronFlock) closeConsumed(ctx context.Context, entries []*consumedEntry) {
	var wg sync.WaitGroup
	for _, e := range entries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// An attempt still in flight when ctx ends closes its own
			// connection (see runOpen); a finished one is always closed here,
			// even when ctx has ended already.
			select {
			case <-e.done:
			default:
				select {
				case <-e.done:
				case <-ctx.Done():
					return
				}
			}
			if e.app == nil {
				return // the open failed: nothing to close
			}
			if err := e.app.close(ctx); err != nil {
				f.log.Warn(fmt.Sprintf("Failed to close consumed app '%s': %v", e.app.App, err))
			}
		}()
	}
	wg.Wait()
}

// Run starts the connection, runs main, and stops when main returns, ctx is
// done, or the process receives SIGINT or SIGTERM. With a nil main it runs
// until ctx is done or a signal arrives. The context passed to main is
// cancelled on shutdown. Run returns main's error, or the Start error.
func (f *IronFlock) Run(ctx context.Context, main func(ctx context.Context) error) error {
	runCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	err := f.Start(runCtx)
	if err == nil {
		if main != nil {
			err = main(runCtx)
		} else {
			<-runCtx.Done()
		}
	}

	// Restore the default signal behavior first, so a second Ctrl-C during
	// the shutdown below terminates the process.
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), f.runStopTimeout)
	defer stopCancel()
	if serr := f.Stop(stopCtx); serr != nil {
		f.log.Warn(fmt.Sprintf("Stopping the IronFlock connection failed: %v", serr))
	}
	return err
}

// requireKeys fails unless SWARM_KEY and APP_KEY are known.
func (f *IronFlock) requireKeys() error {
	if f.swarmKey <= 0 {
		return missingConfigf("SWARM_KEY not set in environment variables!")
	}
	if f.appKey <= 0 {
		return missingConfigf("APP_KEY not set in environment variables!")
	}
	return nil
}

// withDeviceMetadata returns kwargs with the device metadata added under the
// caller's keys (caller keys win). Unknown values are sent as nil.
func (f *IronFlock) withDeviceMetadata(kwargs map[string]any) map[string]any {
	out := make(map[string]any, len(kwargs)+3)
	out["DEVICE_SERIAL_NUMBER"] = f.serialNumber
	out["DEVICE_KEY"] = nilIfEmpty(f.deviceKey)
	out["DEVICE_NAME"] = nilIfEmpty(f.deviceName)
	for k, v := range kwargs {
		out[k] = v
	}
	return out
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// cleanupContext returns a context for tearing down what an operation left
// behind: it keeps ctx's values but not its cancellation (ctx may be the
// reason for the teardown), bounded by the cleanup budget.
func (f *IronFlock) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), f.cleanupTimeout)
}
