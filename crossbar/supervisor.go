package crossbar

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"time"

	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/transport"
	"github.com/gammazero/nexus/v3/transport/serialize"
	"github.com/gammazero/nexus/v3/wamp"
	"github.com/gammazero/nexus/v3/wamp/crsign"
)

// tunables are the timing knobs of a Connection. They default to the
// exported constants; tests shorten them.
type tunables struct {
	initialRetryDelay        time.Duration
	baseMaxRetryDelay        time.Duration
	noSuchRealmBackoffAfter  time.Duration
	noSuchRealmMaxRetryDelay time.Duration
	retryDelayGrowth         float64
	retryDelayJitter         float64

	retryFirstDelay time.Duration
	retryMaxDelay   time.Duration

	// connectTimeout bounds the TCP/TLS/WebSocket handshake of an attempt;
	// responseTimeout bounds each step of the WAMP join, every router
	// answer to SUBSCRIBE, REGISTER and acknowledged PUBLISH, and the
	// GOODBYE exchange of Stop.
	connectTimeout  time.Duration
	responseTimeout time.Duration
	// startTeardown bounds how long a failed Start waits for the supervisor
	// to exit before closing the connection forcibly.
	startTeardown time.Duration

	now  func() time.Time
	rand func() float64 // in [0, 1)

	// onRetry, if set, observes every scheduled reconnect (tests).
	onRetry func(reason string, delay time.Duration)
}

func defaultTunables() tunables {
	return tunables{
		initialRetryDelay:        InitialRetryDelay,
		baseMaxRetryDelay:        BaseMaxRetryDelay,
		noSuchRealmBackoffAfter:  NoSuchRealmBackoffAfter,
		noSuchRealmMaxRetryDelay: NoSuchRealmMaxRetryDelay,
		retryDelayGrowth:         RetryDelayGrowth,
		retryDelayJitter:         RetryDelayJitter,
		retryFirstDelay:          RetryFirstDelay,
		retryMaxDelay:            RetryMaxDelay,
		connectTimeout:           15 * time.Second,
		responseTimeout:          DefaultSessionWaitTimeout,
		startTeardown:            5 * time.Second,
		now:                      time.Now,
		rand:                     rand.Float64,
	}
}

// backoff is the reconnect delay policy: exponential growth from the initial
// delay, capped at baseMaxRetryDelay — or at noSuchRealmMaxRetryDelay once
// the realm has been missing for noSuchRealmBackoffAfter. Only the
// supervisor goroutine uses it.
type backoff struct {
	t *tunables

	delay time.Duration // base of the next delay, before jitter

	// The current streak of no_such_realm refusals.
	realmMissingSince time.Time
	realmMissing      bool
	slowed            bool
}

func newBackoff(t *tunables) *backoff {
	return &backoff{t: t, delay: t.initialRetryDelay}
}

// joined resets the policy after a successful join.
func (b *backoff) joined() {
	b.delay = b.t.initialRetryDelay
	b.resetStreak()
}

func (b *backoff) resetStreak() {
	b.realmMissing = false
	b.slowed = false
}

// observe records why the last attempt or session ended. It reports true
// exactly when the realm has now been missing long enough to slow down.
func (b *backoff) observe(reason string) bool {
	if reason != ErrURINoSuchRealm {
		b.resetStreak()
		return false
	}
	now := b.t.now()
	if !b.realmMissing {
		b.realmMissing = true
		b.realmMissingSince = now
		return false
	}
	if b.slowed || now.Sub(b.realmMissingSince) < b.t.noSuchRealmBackoffAfter {
		return false
	}
	b.slowed = true
	return true
}

func (b *backoff) maxDelay() time.Duration {
	if b.slowed {
		return b.t.noSuchRealmMaxRetryDelay
	}
	return b.t.baseMaxRetryDelay
}

// next returns the delay before the next attempt and grows the base.
func (b *backoff) next() time.Duration {
	ceiling := b.maxDelay()
	d := min(b.delay, ceiling)
	b.delay = min(time.Duration(float64(d)*b.t.retryDelayGrowth), ceiling)
	if j := b.t.retryDelayJitter; j > 0 {
		d = time.Duration(float64(d) * (1 + j*(2*b.t.rand()-1)))
	}
	return d
}

// supervise is the connection's only connect/retry loop. It runs from Start
// until the connection is stopped or fails for good, and closes c.done.
func (c *Connection) supervise() {
	defer close(c.done)
	bo := newBackoff(&c.t)
	for c.runCtx.Err() == nil {
		s, reason, err := c.dialAndJoin()
		if err == nil {
			bo.joined()
			reason = c.runSession(s)
		}
		if c.runCtx.Err() != nil {
			return
		}
		if c.cfg.FailOnAuthError && IsFatalAuthReason(reason) {
			c.failAuth(reason)
			return
		}
		if bo.observe(reason) {
			c.log.Warn(fmt.Sprintf("Realm %s still does not exist after %ss; slowing reconnect attempts to "+
				"at most one per %ss until it appears", c.cfg.Realm,
				formatSeconds(c.t.noSuchRealmBackoffAfter), formatSeconds(c.t.noSuchRealmMaxRetryDelay)))
		}
		delay := bo.next()
		if err != nil {
			c.log.Info("Connection attempt failed; retrying", "reason", reason, "error", err, "retry_in", delay)
		} else {
			c.log.Info("Reconnecting", "retry_in", delay)
		}
		if c.t.onRetry != nil {
			c.t.onRetry(reason, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-c.runCtx.Done():
			timer.Stop()
			return
		}
	}
}

// dialAndJoin makes one connection attempt. On failure it returns the
// router's close reason, if it sent one.
func (c *Connection) dialAndJoin() (*session, string, error) {
	authID, secret := c.credentials()

	ctx, cancel := context.WithTimeout(c.runCtx, c.t.connectTimeout)
	defer cancel()
	logger := nexusLogger{c.log}
	dead := newSignal()
	wsCfg := transport.WebsocketConfig{
		KeepAlive: c.keepAlive(),
		Dial: func(network, addr string) (net.Conn, error) {
			var d net.Dialer
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &watchedConn{Conn: conn, dead: dead}, nil
		},
	}
	inner, err := transport.ConnectWebsocketPeer(ctx, c.dialURL, serialize.MSGPACK, c.cfg.TLSConfig, logger, &wsCfg)
	if err != nil {
		if c.runCtx.Err() != nil {
			return nil, "", c.stoppedErr()
		}
		return nil, "", fmt.Errorf("connect to %s: %w", c.cfg.URL, err)
	}

	p := newObservedPeer(inner, dead, c.registrationEvicted)
	c.setPeer(p)
	// Stop must not wait for a join that is still waiting for the router.
	stopJoin := context.AfterFunc(c.runCtx, p.Close)
	cli, err := client.NewClient(p, client.Config{
		Realm:        c.cfg.Realm,
		HelloDetails: wamp.Dict{"authid": authID},
		AuthHandlers: map[string]client.AuthFunc{
			"wampcra": func(ch *wamp.Challenge) (string, wamp.Dict) {
				c.log.Debug("WAMP-CRA challenge received", "authmethod", ch.AuthMethod)
				return crsign.RespondChallenge(secret, ch, nil), wamp.Dict{}
			},
		},
		ResponseTimeout: c.t.responseTimeout,
		Serialization:   client.MSGPACK,
		Logger:          logger,
	})
	interrupted := !stopJoin()
	if err != nil {
		p.Close() // NewClient closed it already on most paths; Close is idempotent
		c.setPeer(nil)
		if c.runCtx.Err() != nil {
			return nil, "", c.stoppedErr()
		}
		reason, msg := p.abortReason()
		if reason != "" {
			if msg != "" {
				return nil, reason, fmt.Errorf("join refused: %s: %s", reason, msg)
			}
			return nil, reason, fmt.Errorf("join refused: %s", reason)
		}
		return nil, "", fmt.Errorf("join: %w", err)
	}
	s := newSession(cli, p)
	if interrupted {
		c.teardown(s, true)
		return nil, "", c.stoppedErr()
	}
	c.log.Debug("Joined realm", "authid", authID, "session", cli.ID())
	return s, "", nil
}

// runSession restores the tracked subscriptions and registrations on a
// freshly joined session, announces it, and blocks until it ends or the
// connection stops. It returns the close reason ("" when stopped).
func (c *Connection) runSession(s *session) string {
	if !c.establish(s) {
		c.markDown(s)
		c.teardown(s, s.alive())
		if c.runCtx.Err() != nil {
			return ""
		}
		reason := s.closeReason()
		c.log.Warn("Connection lost while restoring subscriptions and registrations", "reason", reason)
		return reason
	}
	c.log.Info("Connection to IronFlock app realm established", "url", c.cfg.URL)
	if cb := c.cfg.OnConnect; cb != nil {
		c.runCallback("OnConnect", cb)
	}

	select {
	case <-s.cli.Done():
		reason := s.closeReason()
		c.markDown(s)
		if c.runCtx.Err() != nil {
			c.teardown(s, false)
			return ""
		}
		c.log.Warn("Connection to IronFlock app realm closed", "reason", reason)
		if cb := c.cfg.OnDisconnect; cb != nil {
			c.runCallback("OnDisconnect", func() { cb(reason) })
		}
		c.teardown(s, false)
		return reason
	case <-c.runCtx.Done():
		c.markDown(s)
		c.teardown(s, true)
		c.log.Info("Connection to IronFlock app realm closed by client")
		return ""
	}
}

// establish restores every tracked subscription and registration onto s
// and then publishes s as the current session. Holding stateMu throughout
// keeps restores and subscription changes from interleaving, and no
// operation sees the session before its restore is complete.
func (c *Connection) establish(s *session) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.restore(s)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped || !s.alive() {
		return false
	}
	c.sess = s
	close(c.upCh)
	return true
}

// restore re-subscribes and re-registers every tracked entry on s. An entry
// that fails stays tracked, inactive, and is retried after the next join.
// c.stateMu must be held.
func (c *Connection) restore(s *session) {
	if n := len(c.groups); n > 0 {
		c.log.Info("Resubscribing", "subscriptions", n)
	}
	for _, g := range c.groups {
		if !s.alive() || c.runCtx.Err() != nil {
			return
		}
		err := s.cli.Subscribe(g.topic, g.onEvent, cloneDict(g.options))
		if err != nil {
			g.sess.Store(nil)
			c.log.Warn("Failed to restore subscription; retrying after the next reconnect", "topic", g.topic,
				"error", c.requestError("subscribe to topic '"+g.topic+"'", subscribePrefix(g.topic), err))
			continue
		}
		g.sess.Store(s)
	}
	if n := len(c.regs); n > 0 {
		c.log.Info("Re-registering", "procedures", n)
	}
	for _, r := range c.regs {
		if !s.alive() || c.runCtx.Err() != nil {
			return
		}
		err := s.cli.Register(r.procedure, r.invoke, cloneDict(r.options))
		if err != nil {
			r.sess.Store(nil)
			c.log.Warn("Failed to restore registration; retrying after the next reconnect", "procedure", r.procedure,
				"error", c.requestError("register procedure '"+r.procedure+"'", registerPrefix(r.procedure), err))
			continue
		}
		r.sess.Store(s)
	}
}

// markDown withdraws s as the current session.
func (c *Connection) markDown(s *session) {
	c.mu.Lock()
	if c.sess == s {
		c.sess = nil
		c.upCh = make(chan struct{})
	}
	c.mu.Unlock()
	s.markDown()
}

// teardown ends the session and closes its client and WebSocket. With
// graceful set it first says GOODBYE (see leave).
func (c *Connection) teardown(s *session, graceful bool) {
	if !graceful || !c.leave(s) {
		s.peer.Close()
	}
	// The receive loop ends once the WebSocket is closed or the router
	// answered the GOODBYE; Close then neither sends another GOODBYE nor
	// waits for an answer, it only waits for running invocation goroutines.
	<-s.cli.Done()
	_ = s.cli.Close()
	c.setPeer(nil)
}

// leave sends GOODBYE and waits for the router's answer, each within the
// response timeout. It reports whether the session ended. nexus' own Close
// does the same but keeps waiting to hand the GOODBYE to a writer that has
// already died with its connection (Stop racing a dropped socket), for twice
// the response timeout.
func (c *Connection) leave(s *session) bool {
	timer := time.NewTimer(c.t.responseTimeout)
	defer timer.Stop()
	select {
	case s.peer.Send() <- &wamp.Goodbye{Reason: wamp.CloseRealm, Details: wamp.Dict{}}:
	case <-s.cli.Done():
		return true
	case <-s.peer.Done():
		return false
	case <-timer.C:
		return false
	}
	select {
	case <-s.cli.Done():
		return true
	case <-timer.C:
		c.log.Debug("No GOODBYE from the router; closing the connection")
		return false
	}
}

// registrationEvicted logs a registration the router revoked because
// another session took its procedure over (force_reregister). The entry stays
// tracked, inactive, and is registered again after the next reconnect — the
// Python SDK's behaviour. It runs on the receive path, so the lookup of the
// procedure, which needs stateMu, happens on a goroutine of its own.
func (c *Connection) registrationEvicted(id wamp.ID) {
	go func() {
		c.stateMu.Lock()
		s := c.currentSession()
		procedure := ""
		for _, r := range c.regs {
			if s != nil && r.sess.Load() == s {
				if rid, ok := s.cli.RegistrationID(r.procedure); ok && rid == id {
					procedure = r.procedure
				}
			}
		}
		c.stateMu.Unlock()
		c.log.Warn("Registration taken over by another session (force_reregister); "+
			"it is registered again after the next reconnect", "procedure", procedure, "registration", id)
	}()
}

// failAuth stops the connection for good after a fatal auth denial.
func (c *Connection) failAuth(reason string) {
	c.mu.Lock()
	c.fatal = &AuthError{Realm: c.cfg.Realm, Reason: reason}
	c.stopped = true
	c.stopFlag.Store(true)
	c.cancelRun()
	c.mu.Unlock()
	c.log.Error("Authentication denied; not reconnecting", "reason", reason)
	if cb := c.cfg.OnAuthFailure; cb != nil {
		c.runCallback("OnAuthFailure", func() { cb(reason) })
	}
}

// credentials returns the WAMP-CRA credential of the next attempt: the
// explicit pair, or the injected per-app pair (re-read every attempt, so a
// rotated credential is picked up), or the legacy (serial, serial).
func (c *Connection) credentials() (authID, secret string) {
	if c.cfg.AuthID != "" && c.cfg.AuthSecret != "" {
		return c.cfg.AuthID, c.cfg.AuthSecret
	}
	return AppCredentials(c.cfg.SerialNumber)
}

func (c *Connection) keepAlive() time.Duration {
	switch {
	case c.cfg.KeepAlive < 0:
		return 0
	case c.cfg.KeepAlive == 0:
		return DefaultKeepAlive
	}
	return c.cfg.KeepAlive
}

func (c *Connection) setPeer(p *observedPeer) {
	c.mu.Lock()
	c.peer = p
	c.mu.Unlock()
}

// forceClose closes the WebSocket of the current attempt or session
// without waiting for the router (Stop past its deadline).
func (c *Connection) forceClose() {
	c.mu.Lock()
	p := c.peer
	c.mu.Unlock()
	if p != nil {
		p.Close()
	}
}

// toDialURL converts the configured router URL to the ws:// or wss:// form
// the WebSocket dialer accepts.
func toDialURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("crossbar: invalid router URL %q: %w", raw, err)
	}
	switch u.Scheme {
	case "ws", "wss":
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("crossbar: invalid router URL %q: scheme must be ws, wss, http or https", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("crossbar: invalid router URL %q: missing host", raw)
	}
	return u.String(), nil
}

func subscribePrefix(topic string) string    { return "subscribing to topic '" + topic + "': " }
func registerPrefix(procedure string) string { return "registering procedure '" + procedure + "': " }
func unsubscribePrefix(topic string) string  { return "unsubscribing to '" + topic + "': " }
func unregisterPrefix(procedure string) string {
	return "unregistering procedure '" + procedure + "': "
}

const publishPrefix = "waiting for published message: "
