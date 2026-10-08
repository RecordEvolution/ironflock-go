package ironflock

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RecordEvolution/ironflock-go/crossbar"
)

// fakeCall is one recorded Call.
type fakeCall struct {
	Procedure string
	Args      []any
	Kwargs    map[string]any
	Opts      *crossbar.CallOptions
	Window    time.Duration
}

// fakePublish is one recorded Publish.
type fakePublish struct {
	Topic  string
	Args   []any
	Kwargs map[string]any
	Opts   *crossbar.PublishOptions
	Window time.Duration
}

// fakeSub is one recorded Subscribe.
type fakeSub struct {
	Topic   string
	Handler crossbar.EventHandler
	Opts    *crossbar.SubscribeOptions
	Sub     *crossbar.Subscription
	Removed bool
}

// fakeReg is one recorded Register.
type fakeReg struct {
	Procedure string
	Handler   crossbar.InvocationHandler
	Opts      *crossbar.RegisterOptions
	Reg       *crossbar.Registration
	Removed   bool
}

// fakeConn is a recording wampConn. Every operation is captured with its
// arguments; the hooks script results and errors (nil hooks succeed). It is
// safe for concurrent use. Hooks are read under the lock and run outside it.
type fakeConn struct {
	mu sync.Mutex

	cfg        *crossbar.Config
	configures int
	starts     int
	stops      int
	started    bool
	stopped    bool

	calls     []fakeCall
	publishes []fakePublish
	subs      []*fakeSub
	regs      []*fakeReg

	callFn         func(c fakeCall) (*crossbar.Result, error)
	publishFn      func(p fakePublish) error
	subscribeFn    func(topic string) error
	registerFn     func(procedure string) error
	unsubscribeErr error
	configureErr   error
	startFn        func(ctx context.Context, cfg crossbar.Config) error
	stopErr        error
}

var _ wampConn = (*fakeConn)(nil)

func (c *fakeConn) Configure(cfg crossbar.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.configures++
	if c.configureErr != nil {
		return c.configureErr
	}
	c.cfg = &cfg
	return nil
}

func (c *fakeConn) Start(ctx context.Context) error {
	c.mu.Lock()
	c.starts++
	fn := c.startFn
	var cfg crossbar.Config
	if c.cfg != nil {
		cfg = *c.cfg
	}
	c.mu.Unlock()
	if fn != nil {
		if err := fn(ctx, cfg); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.started = true
	c.mu.Unlock()
	return nil
}

func (c *fakeConn) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stops++
	c.stopped = true
	return c.stopErr
}

func (c *fakeConn) IsOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started && !c.stopped
}

func (c *fakeConn) URL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg == nil {
		return ""
	}
	return c.cfg.URL
}

func (c *fakeConn) Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *crossbar.CallOptions, retryWindow time.Duration) (*crossbar.Result, error) {
	call := fakeCall{Procedure: procedure, Args: args, Kwargs: kwargs, Opts: opts, Window: retryWindow}
	c.mu.Lock()
	c.calls = append(c.calls, call)
	fn := c.callFn
	c.mu.Unlock()
	if fn != nil {
		return fn(call)
	}
	return &crossbar.Result{}, nil
}

func (c *fakeConn) Publish(ctx context.Context, topic string, args []any, kwargs map[string]any, opts *crossbar.PublishOptions, waitWindow time.Duration) error {
	pub := fakePublish{Topic: topic, Args: args, Kwargs: kwargs, Opts: opts, Window: waitWindow}
	c.mu.Lock()
	c.publishes = append(c.publishes, pub)
	fn := c.publishFn
	c.mu.Unlock()
	if fn != nil {
		return fn(pub)
	}
	return nil
}

func (c *fakeConn) Subscribe(ctx context.Context, topic string, handler crossbar.EventHandler, opts *crossbar.SubscribeOptions) (*crossbar.Subscription, error) {
	c.mu.Lock()
	fn := c.subscribeFn
	c.mu.Unlock()
	if fn != nil {
		if err := fn(topic); err != nil {
			return nil, err
		}
	}
	s := &fakeSub{Topic: topic, Handler: handler, Opts: opts, Sub: new(crossbar.Subscription)}
	c.mu.Lock()
	c.subs = append(c.subs, s)
	c.mu.Unlock()
	return s.Sub, nil
}

func (c *fakeConn) Unsubscribe(ctx context.Context, sub *crossbar.Subscription) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.subs {
		if s.Sub == sub {
			s.Removed = true
		}
	}
	return c.unsubscribeErr
}

func (c *fakeConn) Register(ctx context.Context, procedure string, handler crossbar.InvocationHandler, opts *crossbar.RegisterOptions) (*crossbar.Registration, error) {
	c.mu.Lock()
	fn := c.registerFn
	c.mu.Unlock()
	if fn != nil {
		if err := fn(procedure); err != nil {
			return nil, err
		}
	}
	r := &fakeReg{Procedure: procedure, Handler: handler, Opts: opts, Reg: new(crossbar.Registration)}
	c.mu.Lock()
	c.regs = append(c.regs, r)
	c.mu.Unlock()
	return r.Reg, nil
}

func (c *fakeConn) Unregister(ctx context.Context, reg *crossbar.Registration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.regs {
		if r.Reg == reg {
			r.Removed = true
		}
	}
	return nil
}

// config returns the Configure argument (zero when not configured).
func (c *fakeConn) config() crossbar.Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg == nil {
		return crossbar.Config{}
	}
	return *c.cfg
}

func (c *fakeConn) allCalls() []fakeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

func (c *fakeConn) callsTo(procedure string) []fakeCall {
	var out []fakeCall
	for _, call := range c.allCalls() {
		if call.Procedure == procedure {
			out = append(out, call)
		}
	}
	return out
}

func (c *fakeConn) lastCall(t *testing.T) fakeCall {
	t.Helper()
	calls := c.allCalls()
	if len(calls) == 0 {
		t.Fatal("no call was made")
	}
	return calls[len(calls)-1]
}

func (c *fakeConn) allPublishes() []fakePublish {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.publishes)
}

func (c *fakeConn) lastPublish(t *testing.T) fakePublish {
	t.Helper()
	pubs := c.allPublishes()
	if len(pubs) == 0 {
		t.Fatal("nothing was published")
	}
	return pubs[len(pubs)-1]
}

func (c *fakeConn) allSubs() []fakeSub {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]fakeSub, len(c.subs))
	for i, s := range c.subs {
		out[i] = *s
	}
	return out
}

func (c *fakeConn) allRegs() []fakeReg {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]fakeReg, len(c.regs))
	for i, r := range c.regs {
		out[i] = *r
	}
	return out
}

func (c *fakeConn) stopCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stops
}

func (c *fakeConn) configureCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.configures
}

func (c *fakeConn) startCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts
}

// fire delivers ev to every live subscription of topic, synchronously.
func (c *fakeConn) fire(topic string, ev *crossbar.Event) {
	c.mu.Lock()
	var handlers []crossbar.EventHandler
	for _, s := range c.subs {
		if s.Topic == topic && !s.Removed {
			handlers = append(handlers, s.Handler)
		}
	}
	c.mu.Unlock()
	for _, h := range handlers {
		h(ev)
	}
}

// fakeFactory creates the fake connections of consumed apps.
type fakeFactory struct {
	mu    sync.Mutex
	conns []*fakeConn
	// setup scripts every new connection; set it before the connections are
	// created.
	setup func(c *fakeConn)
}

func (ff *fakeFactory) newConn() wampConn {
	c := &fakeConn{}
	ff.mu.Lock()
	setup := ff.setup
	ff.mu.Unlock()
	if setup != nil {
		setup(c)
	}
	ff.mu.Lock()
	ff.conns = append(ff.conns, c)
	ff.mu.Unlock()
	return c
}

func (ff *fakeFactory) all() []*fakeConn {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return slices.Clone(ff.conns)
}

func (ff *fakeFactory) setSetup(setup func(c *fakeConn)) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	ff.setup = setup
}

// connFor returns the consumed connection configured for provider app key.
func (ff *fakeFactory) connFor(t *testing.T, appKey int) *fakeConn {
	t.Helper()
	for _, c := range ff.all() {
		if c.config().AppKey == appKey {
			return c
		}
	}
	t.Fatalf("no consumed connection for provider app key %d", appKey)
	return nil
}

// logBuffer collects log output; safe for concurrent writers.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestLogger() (*slog.Logger, *logBuffer) {
	b := &logBuffer{}
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})), b
}

// platformEnv lists every environment variable the root package reads.
var platformEnv = []string{
	"DEVICE_SERIAL_NUMBER", "DEVICE_NAME", "DEVICE_KEY", "APP_NAME", "SWARM_KEY", "APP_KEY", "ENV",
	"RESWARM_URL", "DEVICE_ENDPOINT_URL", "APP_AUTH_ID", "APP_AUTH_SECRET", "IRONFLOCK_ENV_DIR",
	"INSTANCE_KEY", "TUNNEL_DOMAIN", "CLOUD_TUNNEL_DOMAIN",
}

// unsetEnv unsets names for the duration of the test.
func unsetEnv(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		t.Setenv(name, "") // restores the original value after the test
		os.Unsetenv(name)
	}
}

// setIdentityEnv sets the identity the device agent injects, with every
// other platform variable unset and the env-file directory an empty temp
// directory.
func setIdentityEnv(t *testing.T) {
	t.Helper()
	unsetEnv(t, platformEnv...)
	for k, v := range map[string]string{
		"DEVICE_SERIAL_NUMBER": "test-serial-123",
		"DEVICE_NAME":          "test-device",
		"DEVICE_KEY":           "12",
		"APP_NAME":             "TestApp",
		"SWARM_KEY":            "10",
		"APP_KEY":              "20",
		"ENV":                  "DEV",
	} {
		t.Setenv(k, v)
	}
	t.Setenv("IRONFLOCK_ENV_DIR", t.TempDir())
}

// testFlock is an IronFlock wired to fakes.
type testFlock struct {
	*IronFlock
	own     *fakeConn
	factory *fakeFactory
	logs    *logBuffer
}

// newTestFlock builds an IronFlock from the current environment and opts,
// with a fake own connection and fake consumed-app connections. It is
// stopped when the test ends.
func newTestFlock(t *testing.T, opts ...Option) *testFlock {
	t.Helper()
	log, logs := newTestLogger()
	own := &fakeConn{}
	factory := &fakeFactory{}
	f, err := newWithConn(own, factory.newConn, append([]Option{WithLogger(log)}, opts...)...)
	if err != nil {
		t.Fatalf("newWithConn: %v", err)
	}
	f.cleanupTimeout = time.Second
	f.runStopTimeout = time.Second
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = f.Stop(ctx)
	})
	return &testFlock{IronFlock: f, own: own, factory: factory, logs: logs}
}

// flock is newTestFlock with the standard identity environment.
func flock(t *testing.T, opts ...Option) *testFlock {
	t.Helper()
	setIdentityEnv(t)
	return newTestFlock(t, opts...)
}

// resultOf returns a call result whose single positional value is v.
func resultOf(v any) *crossbar.Result { return &crossbar.Result{Args: []any{v}} }

// wampErr returns a router refusal.
func wampErr(uri string, args ...any) *crossbar.WampError {
	return &crossbar.WampError{URI: uri, Args: args}
}

// checkGoroutines fails the test when, at its end, more goroutines run than
// at its start (after a grace period for goroutines that are winding down).
// Register it first, so its cleanup runs after every other cleanup.
func checkGoroutines(t *testing.T) {
	t.Helper()
	// os/signal starts its delivery goroutine on the first Notify and keeps
	// it for the life of the process; start it before taking the baseline.
	signalLoopOnce.Do(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Stop(c)
	})
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			n := runtime.NumGoroutine()
			if n <= before {
				return
			}
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<16)
				buf = buf[:runtime.Stack(buf, true)]
				t.Errorf("goroutine leak: %d goroutines at the end, %d at the start\n%s", n, before, buf)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

var signalLoopOnce sync.Once

// eventually polls cond until it holds or a second passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// mustContain fails unless s contains every part.
func mustContain(t *testing.T, s string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(s, p) {
			t.Errorf("%q does not contain %q", s, p)
		}
	}
}
