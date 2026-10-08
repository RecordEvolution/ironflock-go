// Package wamp is the WAMP connection underneath the IronFlock SDK.
//
// A Connection joins one realm of the IronFlock router with WAMP-CRA
// authentication over a msgpack WebSocket, keeps the session alive across
// router restarts and network failures, and restores every subscription and
// registration after each reconnect. Most apps use it only through the
// ironflock package; it is exported for advanced use.
package wamp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/nexus/v3/client"
	nxwamp "github.com/gammazero/nexus/v3/wamp"
)

// Reconnect and wait tunables. They mirror the Python and JavaScript SDKs.
const (
	// DefaultSessionWaitTimeout is how long an operation waits for a session
	// when the caller gives no window of its own.
	DefaultSessionWaitTimeout = 10 * time.Second
	// DefaultFirstConnectTimeout bounds Start for connections with
	// FailOnAuthError set (consumed-app connections). The primary connection
	// waits for its realm without a bound.
	DefaultFirstConnectTimeout = 50 * time.Second

	// InitialRetryDelay is the delay before the first reconnect attempt.
	InitialRetryDelay = 1 * time.Second
	// RetryDelayGrowth multiplies the delay after every failed attempt.
	RetryDelayGrowth = 1.5
	// RetryDelayJitter is the relative random jitter applied to each delay.
	RetryDelayJitter = 0.1
	// BaseMaxRetryDelay caps the reconnect delay while the router is
	// reachable and retrying is expected to succeed soon (router restart,
	// concurrent-install race).
	BaseMaxRetryDelay = 2 * time.Second
	// NoSuchRealmBackoffAfter is how long a realm may be missing before
	// reconnects slow down: past it the realm is most likely never going to
	// appear (the app was deleted, or has no data backend for this stage).
	NoSuchRealmBackoffAfter = 60 * time.Second
	// NoSuchRealmMaxRetryDelay is the reconnect ceiling once
	// NoSuchRealmBackoffAfter has passed.
	NoSuchRealmMaxRetryDelay = 120 * time.Second

	// RetryFirstDelay and RetryMaxDelay bound the backoff between retries of
	// a call whose procedure is not registered yet (see Connection.Call).
	RetryFirstDelay = 500 * time.Millisecond
	RetryMaxDelay   = 5 * time.Second

	// DefaultKeepAlive is the WebSocket ping interval. A connection whose
	// pong is missing for two intervals is dropped and reconnected, so a
	// silently half-open socket (NAT/idle timeout, router restart behind a
	// load balancer) is noticed within ~40s even when the app only
	// subscribes.
	DefaultKeepAlive = 20 * time.Second
)

// Errors returned by Connection.
var (
	// ErrNotConfigured: an operation or Start was attempted before Configure.
	ErrNotConfigured = errors.New("wamp: connection is not configured — call Configure() and Start() before performing WAMP operations")
	// ErrNotConnected: no session became available within the wait window.
	ErrNotConnected = errors.New("wamp: not connected to the IronFlock router")
	// ErrStopped: the connection was stopped (Stop) or gave up for good.
	ErrStopped = errors.New("wamp: connection stopped")
)

// AuthError reports that the router refused this connection's credentials or
// role (a reason IsFatalAuthReason accepts). It is fatal only for
// connections with FailOnAuthError set; the primary connection keeps
// retrying.
type AuthError struct {
	Realm  string
	Reason string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("authentication denied for realm %s: %s", e.Realm, e.Reason)
}

// Config configures a Connection.
type Config struct {
	// SwarmKey, AppKey and Stage name the realm:
	// realm-<SwarmKey>-<AppKey>-<dev|prod>.
	SwarmKey int
	AppKey   int
	Stage    Stage
	// Realm overrides the realm name derived from SwarmKey/AppKey/Stage.
	Realm string

	// URL is the router WebSocket URL. Empty resolves it with
	// WebSocketURI("") (DEVICE_ENDPOINT_URL, RESWARM_URL, cloud default).
	URL string

	// SerialNumber is the device identity; empty reads DEVICE_SERIAL_NUMBER.
	SerialNumber string
	// AuthID and AuthSecret are the WAMP-CRA credential. When both are empty
	// the credential is resolved with AppCredentials(SerialNumber) on EVERY
	// connection attempt, so a credential the agent rotates while the app
	// runs is picked up on the next reconnect.
	AuthID     string
	AuthSecret string

	// FailOnAuthError treats an authentication/authorization denial (a reason
	// IsFatalAuthReason accepts) as fatal: reconnecting stops, OnAuthFailure
	// is called, and a pending Start returns an *AuthError. It also bounds Start
	// by FirstConnectTimeout. Used for consumed-app connections; the primary
	// connection retries through every failure.
	FailOnAuthError bool
	// OnAuthFailure is called (once, on its own goroutine) with the close
	// reason when FailOnAuthError stops the connection.
	OnAuthFailure func(reason string)

	// OnConnect is called (on its own goroutine) after every successful join,
	// once subscriptions and registrations are restored.
	OnConnect func()
	// OnDisconnect is called (on its own goroutine) when an established
	// session is lost, with the close reason if one is known.
	OnDisconnect func(reason string)

	// SessionWaitTimeout overrides DefaultSessionWaitTimeout.
	SessionWaitTimeout time.Duration
	// FirstConnectTimeout overrides DefaultFirstConnectTimeout.
	FirstConnectTimeout time.Duration
	// KeepAlive overrides DefaultKeepAlive; a negative value disables
	// WebSocket keepalive pings.
	KeepAlive time.Duration
	// TLSConfig is used for wss:// URLs; nil uses Go's defaults.
	TLSConfig *tls.Config
	// Logger receives connection lifecycle logs; nil uses slog.Default().
	Logger *slog.Logger
}

// Connection is a self-healing WAMP session on one realm.
//
// Lifecycle: Configure, then Start (blocks until the first join), then any
// number of concurrent operations, then Stop. A Connection is safe for
// concurrent use by multiple goroutines.
//
// Reconnects: one supervisor goroutine owns the connect/retry loop. Every
// involuntary close — a refused join (wamp.error.no_such_realm while the data
// backend provisions the realm), a router restart, a dropped or silently dead
// socket (keepalive), even a clean GOODBYE — schedules the next attempt with
// exponential backoff (InitialRetryDelay, growth RetryDelayGrowth, jitter
// RetryDelayJitter, capped at BaseMaxRetryDelay). After NoSuchRealmBackoffAfter
// of continuous no_such_realm refusals the cap widens to
// NoSuchRealmMaxRetryDelay until the realm appears or any other close reason
// is seen. After each join every tracked subscription and registration is
// restored; one that fails to restore stays tracked and is retried on the
// next reconnect.
//
// Contexts: every operation waits for a session within its context (and its
// wait window). A Call is cancelled at the router when its context ends. The
// router round trip of Publish, Subscribe, Register, Unsubscribe and
// Unregister is not interruptible once sent; it is bounded by the router
// response timeout (DefaultSessionWaitTimeout).
type Connection struct {
	cfg     Config
	log     *slog.Logger
	t       tunables
	dialURL string

	// mu guards the lifecycle state below. Lock order: stateMu before mu.
	mu         sync.Mutex
	configured bool
	started    bool
	stopped    bool
	runCtx     context.Context // cancelled by Stop or a fatal auth denial
	cancelRun  context.CancelFunc
	done       chan struct{} // closed when the supervisor exits
	sess       *session      // current, restored session; nil while down
	upCh       chan struct{} // closed while sess is set, replaced when it goes
	peer       *observedPeer // WebSocket of the current attempt or session
	fatal      *AuthError

	stopFlag atomic.Bool // mirrors stopped for hot paths

	// callbacks runs OnConnect, OnDisconnect and OnAuthFailure in order.
	callbacks serialExecutor

	// stateMu serializes changes of the tracked subscriptions and
	// registrations — including their WAMP round trips — with the restore
	// after each join.
	stateMu sync.Mutex
	groups  []*subGroup
	regs    []*Registration
}

// NewConnection returns an unconfigured Connection.
func NewConnection() *Connection {
	return &Connection{t: defaultTunables()}
}

// Configure sets the connection parameters. It resolves the realm, the
// serial number and the router URL, and fails when they cannot be resolved.
// It must be called before Start and must not be called after.
func (c *Connection) Configure(cfg Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return c.stoppedErrLocked()
	}
	if c.started {
		return errors.New("wamp: Configure called after Start")
	}
	if c.t.now == nil {
		c.t = defaultTunables()
	}

	if cfg.Stage == "" {
		cfg.Stage = StageDevelopment
	} else {
		cfg.Stage = StageFromEnv(string(cfg.Stage))
	}
	if cfg.Realm == "" {
		cfg.Realm = RealmName(cfg.SwarmKey, cfg.AppKey, cfg.Stage)
	}
	serial, err := SerialNumber(cfg.SerialNumber)
	if err != nil {
		return err
	}
	cfg.SerialNumber = serial
	if cfg.URL == "" {
		if cfg.URL, err = WebSocketURI(""); err != nil {
			return err
		}
	}
	dialURL, err := toDialURL(cfg.URL)
	if err != nil {
		return err
	}
	if cfg.SessionWaitTimeout <= 0 {
		cfg.SessionWaitTimeout = DefaultSessionWaitTimeout
	}
	if cfg.FirstConnectTimeout <= 0 {
		cfg.FirstConnectTimeout = DefaultFirstConnectTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	c.cfg = cfg
	c.dialURL = dialURL
	c.log = logger.With("realm", cfg.Realm)
	if (cfg.AuthID == "") != (cfg.AuthSecret == "") {
		c.log.Warn("Only one of AuthID and AuthSecret is set; using the injected app credential instead")
	}
	if c.runCtx == nil {
		c.runCtx, c.cancelRun = context.WithCancel(context.Background())
		c.upCh = make(chan struct{})
	}
	c.configured = true
	return nil
}

// Start starts the supervisor and blocks until the first successful join.
//
// The primary connection waits until ctx is done: a realm that does not exist
// yet (the concurrent-install race) is retried until it appears. With
// FailOnAuthError the wait is additionally bounded by FirstConnectTimeout,
// and a fatal auth denial returns an *AuthError immediately. When Start fails
// the supervisor is stopped. Start on a started connection is an error.
func (c *Connection) Start(ctx context.Context) error {
	c.mu.Lock()
	switch {
	case !c.configured:
		c.mu.Unlock()
		return ErrNotConfigured
	case c.stopped:
		err := c.stoppedErrLocked()
		c.mu.Unlock()
		return err
	case c.started:
		c.mu.Unlock()
		return errAlreadyStarted
	}
	c.started = true
	c.done = make(chan struct{})
	up := c.upCh
	c.mu.Unlock()

	c.log.Info("Starting connection to IronFlock app realm", "url", c.cfg.URL)
	go c.supervise()

	var timeout <-chan time.Time
	if c.cfg.FailOnAuthError {
		timer := time.NewTimer(c.cfg.FirstConnectTimeout)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-up:
		return nil
	case <-c.runCtx.Done():
		c.mu.Lock()
		fatal := c.fatal
		err := c.stoppedErrLocked()
		c.mu.Unlock()
		c.abortStart()
		if fatal != nil {
			return fatal
		}
		return err
	case <-ctx.Done():
		c.abortStart()
		return fmt.Errorf("wamp: no session on realm %s: %w", c.cfg.Realm, ctx.Err())
	case <-timeout:
		c.abortStart()
		return &connectTimeoutError{realm: c.cfg.Realm, timeout: c.cfg.FirstConnectTimeout}
	}
}

// abortStart stops the connection after a failed Start and waits, briefly,
// for the supervisor to tear down.
func (c *Connection) abortStart() {
	c.stopInternal()
	timer := time.NewTimer(c.t.startTeardown)
	defer timer.Stop()
	select {
	case <-c.done:
	case <-timer.C:
		c.forceClose()
	}
}

// stopInternal marks the connection stopped and cancels the supervisor. It
// returns the supervisor's done channel (nil if it never started).
func (c *Connection) stopInternal() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		c.stopped = true
		c.stopFlag.Store(true)
		if c.cancelRun != nil {
			c.cancelRun()
		}
	}
	return c.done
}

// Stop ends the session (WAMP GOODBYE) and the supervisor, and waits for
// them within ctx. It is idempotent. Tracked subscriptions and registrations
// are kept, so a stopped connection is not restartable by design.
//
// When ctx ends first the WebSocket is closed without waiting for the
// router's GOODBYE and ctx.Err() is returned; the supervisor exits shortly
// after. Event and procedure handlers still running are not waited for, but
// no queued event is delivered after Stop.
func (c *Connection) Stop(ctx context.Context) error {
	done := c.stopInternal()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		c.forceClose()
		return ctx.Err()
	}
}

// IsOpen reports whether a session is currently established.
func (c *Connection) IsOpen() bool {
	s := c.currentSession()
	return s != nil && s.alive()
}

// currentSession returns the current, restored session, or nil.
func (c *Connection) currentSession() *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sess
}

// Realm returns the configured realm name.
func (c *Connection) Realm() string { return c.cfg.Realm }

// URL returns the resolved router URL.
func (c *Connection) URL() string { return c.cfg.URL }

// SerialNumber returns the device serial number.
func (c *Connection) SerialNumber() string { return c.cfg.SerialNumber }

// WaitSession blocks until a session is established, at most timeout (0
// uses SessionWaitTimeout) or until ctx is done.
//
// Errors: ErrNotConfigured before Configure; ErrStopped after Stop or a fatal
// auth denial (wrapped with the *AuthError); otherwise, on timeout, an error
// wrapping ErrNotConnected whose message reads "Not connected to the
// IronFlock router (realm '<realm>', no session after <n>s). Ensure Start()
// was called and the connection is established."; ctx.Err() when ctx ends
// first.
func (c *Connection) WaitSession(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = c.defaultWait()
	}
	_, err := c.waitSession(ctx, time.Now().Add(timeout), timeout)
	return err
}

func (c *Connection) defaultWait() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.SessionWaitTimeout > 0 {
		return c.cfg.SessionWaitTimeout
	}
	return DefaultSessionWaitTimeout
}

// waitSession returns the current session, waiting for one until deadline.
// reported is the wait named in the timeout error.
func (c *Connection) waitSession(ctx context.Context, deadline time.Time, reported time.Duration) (*session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		c.mu.Lock()
		if !c.configured {
			c.mu.Unlock()
			return nil, ErrNotConfigured
		}
		if c.stopped {
			err := c.stoppedErrLocked()
			c.mu.Unlock()
			return nil, err
		}
		s, wake, stop, realm := c.sess, c.upCh, c.runCtx.Done(), c.cfg.Realm
		c.mu.Unlock()
		if s != nil {
			if s.alive() {
				return s, nil
			}
			// Gone, but the supervisor has not noticed yet.
			wake = s.down
		}
		if timer == nil {
			wait := time.Until(deadline)
			if wait <= 0 {
				return nil, &notConnectedError{realm: realm, wait: reported}
			}
			timer = time.NewTimer(wait)
		}
		select {
		case <-wake:
		case <-stop:
		case <-timer.C:
			return nil, &notConnectedError{realm: realm, wait: reported}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// sleep waits d, returning early with ctx's error or ErrStopped.
func (c *Connection) sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.runCtx.Done():
		return c.stoppedErr()
	}
}

// Call calls a remote procedure.
//
// retryWindow rides out a platform restart for procedures the platform
// serves on the app's realm: Call waits up to that long for a session, and
// retries while the router answers wamp.error.no_such_procedure (after a
// router or data-backend restart the backend re-registers its procedures a
// few seconds after the app's session is back), sleeping RetryFirstDelay,
// doubling up to RetryMaxDelay, as long as the next attempt still starts
// within the window. In both cases the call never reached a callee, so a
// retry cannot run it twice. Every other error, including a connection lost
// mid-call, is returned as it comes. retryWindow 0 waits the default session
// timeout and does not retry.
//
// Errors from the router or callee are returned as *Error.
func (c *Connection) Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *CallOptions, retryWindow time.Duration) (*Result, error) {
	if err := validateURI("procedure", procedure); err != nil {
		return nil, err
	}
	var deadline time.Time
	if retryWindow > 0 {
		deadline = time.Now().Add(retryWindow)
	}
	delay := c.t.retryFirstDelay
	for {
		var s *session
		var err error
		if retryWindow > 0 {
			s, err = c.waitSession(ctx, deadline, max(time.Until(deadline), 0).Round(time.Millisecond))
		} else {
			wait := c.defaultWait()
			s, err = c.waitSession(ctx, time.Now().Add(wait), wait)
		}
		if err != nil {
			return nil, err
		}
		res, err := s.cli.Call(ctx, procedure, callOptions(opts), wireArgs(args, kwargs), nxwamp.Dict(kwargs), nil)
		if err == nil {
			return &Result{
				Args:    normalizeList(res.Arguments),
				Kwargs:  normalizeDict(res.ArgumentsKw),
				Details: normalizeDict(res.Details),
			}, nil
		}
		err = c.callError(procedure, err)
		var werr *Error
		if retryWindow <= 0 || !errors.As(err, &werr) || werr.URI != URINoSuchProcedure ||
			time.Now().Add(delay).After(deadline) {
			return nil, err
		}
		c.log.Debug("Procedure not registered yet; retrying", "procedure", procedure, "retry_in", delay)
		if err := c.sleep(ctx, delay); err != nil {
			return nil, err
		}
		delay = min(delay*2, c.t.retryMaxDelay)
	}
}

// Publish publishes an event, waiting up to waitWindow (0: the default
// session timeout) for a session. With opts.Acknowledge it waits for the
// router to confirm, and a refusal is returned as *Error.
func (c *Connection) Publish(ctx context.Context, topic string, args []any, kwargs map[string]any, opts *PublishOptions, waitWindow time.Duration) error {
	if err := validateURI("topic", topic); err != nil {
		return err
	}
	if waitWindow <= 0 {
		waitWindow = c.defaultWait()
	}
	s, err := c.waitSession(ctx, time.Now().Add(waitWindow), waitWindow)
	if err != nil {
		return err
	}
	err = s.cli.Publish(topic, publishOptions(opts), wireArgs(args, kwargs), nxwamp.Dict(kwargs))
	return c.requestError("publish to topic '"+topic+"'", publishPrefix, err)
}

// Subscribe subscribes handler to topic and tracks the subscription so it is
// restored after every reconnect. Several handlers may subscribe to the same
// topic (and match policy): they share one WAMP subscription and each
// receives every event. A refusal is returned as *Error.
//
// The WAMP subscription takes the options of the first subscription of its
// topic. Subscribing a topic that is already subscribed with a different
// match policy is an error: nexus can hold only one subscription per topic
// string.
func (c *Connection) Subscribe(ctx context.Context, topic string, handler EventHandler, opts *SubscribeOptions) (*Subscription, error) {
	if err := validateURI("topic", topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("wamp: subscribe: nil handler")
	}
	var extra map[string]any
	if opts != nil {
		extra = opts.Extra
	}
	match, err := matchPolicy(optMatch(opts), extra)
	if err != nil {
		return nil, err
	}
	options := subscribeOptions(opts, match)

	wait := c.defaultWait()
	deadline := time.Now().Add(wait)
	for {
		s, err := c.waitSession(ctx, deadline, wait)
		if err != nil {
			return nil, err
		}
		sub, retry, err := c.subscribeOn(s, topic, match, options, handler)
		if !retry {
			return sub, err
		}
	}
}

func optMatch(opts *SubscribeOptions) string {
	if opts == nil {
		return ""
	}
	return opts.Match
}

// subscribeOn subscribes on s. retry reports that s was lost before the
// subscription could be made, so the caller should try the next session.
func (c *Connection) subscribeOn(s *session, topic, match string, options nxwamp.Dict, handler EventHandler) (sub *Subscription, retry bool, err error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.currentSession() != s {
		return nil, true, nil
	}
	g := c.findGroup(topic)
	if g != nil && g.match != match {
		return nil, false, fmt.Errorf("wamp: topic '%s' is already subscribed with match policy %q; "+
			"one connection can subscribe a topic with only one match policy", topic, matchName(g.match))
	}
	sub = &Subscription{conn: c, topic: topic, match: match, handler: handler}
	if g != nil && g.sess.Load() == s {
		sub.group = g
		g.add(sub)
		return sub, false, nil
	}
	tracked := g != nil
	if !tracked {
		g = &subGroup{topic: topic, match: match, options: options}
	}
	if err := s.cli.Subscribe(topic, g.onEvent, cloneDict(g.options)); err != nil {
		if !s.alive() && !c.stopFlag.Load() {
			return nil, true, nil
		}
		return nil, false, c.requestError("subscribe to topic '"+topic+"'", subscribePrefix(topic), err)
	}
	g.sess.Store(s)
	if !tracked {
		c.groups = append(c.groups, g)
	}
	sub.group = g
	g.add(sub)
	return sub, false, nil
}

func matchName(m string) string {
	if m == "" {
		return "exact"
	}
	return m
}

func (c *Connection) findGroup(topic string) *subGroup {
	for _, g := range c.groups {
		if g.topic == topic {
			return g
		}
	}
	return nil
}

func (c *Connection) removeGroup(g *subGroup) {
	c.groups = slices.DeleteFunc(c.groups, func(x *subGroup) bool { return x == g })
}

// Unsubscribe removes a subscription. It is untracked first — it is never
// restored again, whatever the router answers — and the WAMP subscription is
// dropped once no handler of its topic remains. Unsubscribing an
// already-removed subscription is a no-op.
func (c *Connection) Unsubscribe(ctx context.Context, sub *Subscription) error {
	if sub == nil || sub.conn != c {
		return nil
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	g := sub.group
	if g == nil || !sub.removed.CompareAndSwap(false, true) {
		return nil
	}
	sub.queue.close()
	if g.remove(sub) > 0 {
		return nil
	}
	c.removeGroup(g)
	return c.dropSubscription(g)
}

// UnsubscribeTopic removes every subscription of topic.
func (c *Connection) UnsubscribeTopic(ctx context.Context, topic string) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	g := c.findGroup(topic)
	if g == nil {
		return nil
	}
	for _, sub := range g.list() {
		sub.removed.Store(true)
		sub.queue.close()
		g.remove(sub)
	}
	c.removeGroup(g)
	return c.dropSubscription(g)
}

// dropSubscription ends the WAMP subscription of an untracked group, if it
// lives on the current session. c.stateMu must be held.
func (c *Connection) dropSubscription(g *subGroup) error {
	s := g.sess.Swap(nil)
	if s == nil || s != c.currentSession() {
		return nil // died with its session: nothing to undo
	}
	err := c.requestError("unsubscribe from topic '"+g.topic+"'", unsubscribePrefix(g.topic), s.cli.Unsubscribe(g.topic))
	if isGone(err, "wamp.error.no_such_subscription") {
		c.log.Debug("Ignoring stale subscription handle", "topic", g.topic, "error", err)
		return nil
	}
	return err
}

// Register registers handler as procedure and tracks the registration so it
// is restored after every reconnect. ForceReregister defaults to true. A
// refusal is returned as *Error.
//
// A procedure can be registered once per connection: registering it again
// before Unregister fails with wamp.error.procedure_already_exists.
func (c *Connection) Register(ctx context.Context, procedure string, handler InvocationHandler, opts *RegisterOptions) (*Registration, error) {
	if err := validateURI("procedure", procedure); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("wamp: register: nil handler")
	}
	var match string
	var extra map[string]any
	if opts != nil {
		match, extra = opts.Match, opts.Extra
	}
	policy, err := matchPolicy(match, extra)
	if err != nil {
		return nil, err
	}
	reg := &Registration{conn: c, procedure: procedure, handler: handler, options: registerOptions(opts, policy)}

	wait := c.defaultWait()
	deadline := time.Now().Add(wait)
	for {
		s, err := c.waitSession(ctx, deadline, wait)
		if err != nil {
			return nil, err
		}
		retry, err := c.registerOn(s, reg)
		if !retry {
			if err != nil {
				return nil, err
			}
			return reg, nil
		}
	}
}

func (c *Connection) registerOn(s *session, reg *Registration) (retry bool, err error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.currentSession() != s {
		return true, nil
	}
	for _, r := range c.regs {
		if r.procedure == reg.procedure {
			return false, &Error{URI: URIProcedureAlreadyExists, Args: []any{
				fmt.Sprintf("procedure '%s' is already registered on this connection", reg.procedure)}}
		}
	}
	if err := s.cli.Register(reg.procedure, reg.invoke, cloneDict(reg.options)); err != nil {
		if !s.alive() && !c.stopFlag.Load() {
			return true, nil
		}
		return false, c.requestError("register procedure '"+reg.procedure+"'", registerPrefix(reg.procedure), err)
	}
	reg.sess.Store(s)
	c.regs = append(c.regs, reg)
	return false, nil
}

// Unregister removes a registration; untracked first, like Unsubscribe.
func (c *Connection) Unregister(ctx context.Context, reg *Registration) error {
	if reg == nil || reg.conn != c {
		return nil
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if !slices.Contains(c.regs, reg) {
		return nil
	}
	c.regs = slices.DeleteFunc(c.regs, func(r *Registration) bool { return r == reg })
	reg.removed.Store(true)
	s := reg.sess.Swap(nil)
	if s == nil || s != c.currentSession() {
		return nil // died with its session: nothing to undo
	}
	err := c.requestError("unregister procedure '"+reg.procedure+"'", unregisterPrefix(reg.procedure), s.cli.Unregister(reg.procedure))
	if isGone(err, "wamp.error.no_such_registration") {
		// Evicted by another session's force_reregister, or lost with the
		// session: either way it is no longer registered.
		c.log.Debug("Ignoring stale registration handle", "procedure", reg.procedure, "error", err)
		return nil
	}
	return err
}

// isGone reports whether an UNSUBSCRIBE or UNREGISTER failed only because
// there is nothing left to undo: the session is gone, nexus no longer knows
// the handle, or the router answers goneURI.
func isGone(err error, goneURI string) bool {
	if err == nil {
		return false
	}
	var werr *Error
	if errors.As(err, &werr) {
		return werr.URI == goneURI
	}
	return errors.Is(err, ErrNotConnected) || errors.Is(err, ErrStopped) ||
		errors.Is(err, client.ErrNotSubscribed) || errors.Is(err, client.ErrNotRegistered)
}

// Subscription is a tracked subscription. The handle stays valid across
// reconnects.
type Subscription struct {
	conn    *Connection
	topic   string
	match   string
	handler EventHandler

	group   *subGroup      // set before the handle is returned
	queue   serialExecutor // ordered event delivery
	removed atomic.Bool
}

// Topic returns the subscribed topic (or pattern).
func (s *Subscription) Topic() string { return s.topic }

// Match returns the match policy ("" for exact).
func (s *Subscription) Match() string { return s.match }

// Active reports whether the subscription is currently live at the router —
// false while the connection is down or after a failed restore.
func (s *Subscription) Active() bool {
	if s == nil || s.removed.Load() || s.group == nil {
		return false
	}
	live := s.group.sess.Load()
	return live != nil && live == s.conn.currentSession() && live.alive()
}

// Unsubscribe is shorthand for Connection.Unsubscribe.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.conn.Unsubscribe(ctx, s)
}

// Registration is a tracked registration. The handle stays valid across
// reconnects.
type Registration struct {
	conn      *Connection
	procedure string
	handler   InvocationHandler

	options nxwamp.Dict
	sess    atomic.Pointer[session] // session holding the WAMP registration
	removed atomic.Bool
}

// Procedure returns the registered procedure URI.
func (r *Registration) Procedure() string { return r.procedure }

// Active reports whether the registration is currently live at the router.
func (r *Registration) Active() bool {
	if r == nil || r.removed.Load() {
		return false
	}
	live := r.sess.Load()
	if live == nil || live != r.conn.currentSession() || !live.alive() {
		return false
	}
	id, ok := live.cli.RegistrationID(r.procedure)
	return ok && !live.peer.isEvicted(id)
}

// Unregister is shorthand for Connection.Unregister.
func (r *Registration) Unregister(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.conn.Unregister(ctx, r)
}
