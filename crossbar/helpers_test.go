package crossbar

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router"
	"github.com/gammazero/nexus/v3/router/auth"
	"github.com/gammazero/nexus/v3/wamp"
)

const (
	testRealm  = "realm-1-2-dev"
	testAuthID = "app-auth-id"
	testSecret = "app-secret"
	testSerial = "test-device"
)

// keyStore is a static WAMP-CRA key store for the test router.
type keyStore struct {
	mu    sync.Mutex
	users map[string]string // authid -> secret
}

func (k *keyStore) set(authID, secret string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.users[authID] = secret
}

func (k *keyStore) AuthKey(authid, _ string) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	secret, ok := k.users[authid]
	if !ok {
		return nil, errors.New("no such user")
	}
	return []byte(secret), nil
}

func (k *keyStore) PasswordInfo(string) (string, int, int) { return "", 0, 0 }
func (k *keyStore) AuthRole(string) (string, error)        { return "device", nil }
func (k *keyStore) Provider() string                       { return "static" }

// recordingAuth records the authid of every HELLO before delegating to the
// WAMP-CRA authenticator.
type recordingAuth struct {
	auth.Authenticator
	mu      sync.Mutex
	authIDs []string
}

func (a *recordingAuth) Authenticate(sid wamp.ID, details wamp.Dict, peer wamp.Peer) (*wamp.Welcome, error) {
	id, _ := wamp.AsString(details["authid"])
	a.mu.Lock()
	a.authIDs = append(a.authIDs, id)
	a.mu.Unlock()
	return a.Authenticator.Authenticate(sid, details, peer)
}

func (a *recordingAuth) attempts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.authIDs...)
}

// testAuthorizer denies selected operations and records REGISTER options.
type testAuthorizer struct {
	mu           sync.Mutex
	deny         map[string]bool // "publish:<topic>", "subscribe:<topic>", "register:<proc>"
	registerOpts map[string]wamp.Dict
}

func (a *testAuthorizer) setDeny(key string, deny bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deny[key] = deny
}

func (a *testAuthorizer) registered(proc string) wamp.Dict {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.registerOpts[proc]
}

func (a *testAuthorizer) Authorize(_ *wamp.Session, msg wamp.Message) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch m := msg.(type) {
	case *wamp.Publish:
		return !a.deny["publish:"+string(m.Topic)], nil
	case *wamp.Subscribe:
		return !a.deny["subscribe:"+string(m.Topic)], nil
	case *wamp.Register:
		opts := make(wamp.Dict, len(m.Options))
		for k, v := range m.Options {
			opts[k] = v
		}
		a.registerOpts[string(m.Procedure)] = opts
		return !a.deny["register:"+string(m.Procedure)], nil
	}
	return true, nil
}

// trackingListener remembers accepted connections so a test can drop them:
// httptest does not close hijacked (WebSocket) connections.
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, conn)
		l.mu.Unlock()
	}
	return conn, err
}

func (l *trackingListener) dropAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		_ = c.Close()
	}
	l.conns = nil
}

// testRouter is an in-process nexus router behind a WebSocket server.
type testRouter struct {
	t        *testing.T
	r        router.Router
	srv      *httptest.Server
	ln       *trackingListener
	url      string
	keys     *keyStore
	auth     *recordingAuth
	authz    *testAuthorizer
	protoMu  sync.Mutex
	protocol []string // Sec-WebSocket-Protocol of every upgrade request
	handlers sync.WaitGroup
	closed   atomic.Bool
}

// newTestRouter starts a router; withRealm controls whether testRealm exists.
func newTestRouter(t *testing.T, withRealm bool) *testRouter {
	t.Helper()
	keys := &keyStore{users: map[string]string{testAuthID: testSecret}}
	tr := &testRouter{
		t:     t,
		keys:  keys,
		auth:  &recordingAuth{Authenticator: auth.NewCRAuthenticator(keys, time.Second)},
		authz: &testAuthorizer{deny: map[string]bool{}, registerOpts: map[string]wamp.Dict{}},
	}
	cfg := &router.Config{}
	if withRealm {
		cfg.RealmConfigs = []*router.RealmConfig{tr.realmConfig()}
	}
	r, err := router.NewRouter(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	tr.r = r
	wss := router.NewWebsocketServer(r)
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// ServeHTTP returns once the session is attached (or refused);
		// httptest stops tracking the request when it is hijacked.
		tr.handlers.Add(1)
		defer tr.handlers.Done()
		tr.protoMu.Lock()
		tr.protocol = append(tr.protocol, req.Header.Get("Sec-Websocket-Protocol"))
		tr.protoMu.Unlock()
		wss.ServeHTTP(w, req)
	})
	tr.srv = httptest.NewUnstartedServer(handler)
	tr.ln = &trackingListener{Listener: tr.srv.Listener}
	tr.srv.Listener = tr.ln
	tr.srv.Start()
	tr.url = "ws" + strings.TrimPrefix(tr.srv.URL, "http") + "/ws-ua-usr"
	t.Cleanup(tr.close)
	return tr
}

func (tr *testRouter) realmConfig() *router.RealmConfig {
	return &router.RealmConfig{
		URI:            testRealm,
		Authenticators: []auth.Authenticator{tr.auth},
		Authorizer:     tr.authz,
		EnableMetaKill: true,
	}
}

func (tr *testRouter) addRealm() {
	tr.t.Helper()
	if err := tr.r.AddRealm(tr.realmConfig()); err != nil {
		tr.t.Fatalf("AddRealm: %v", err)
	}
}

func (tr *testRouter) removeRealm() { tr.r.RemoveRealm(testRealm) }

func (tr *testRouter) protocols() []string {
	tr.protoMu.Lock()
	defer tr.protoMu.Unlock()
	return append([]string(nil), tr.protocol...)
}

// close stops the server and the router. Connections must be stopped first,
// and no join may still be in progress: the nexus router races (and panics
// on a closed channel) when it is closed while attaching a session.
func (tr *testRouter) close() {
	if !tr.closed.CompareAndSwap(false, true) {
		return
	}
	tr.srv.Close()
	tr.ln.dropAll()
	tr.handlers.Wait()
	tr.r.Close()
}

// local returns a trusted in-process client on testRealm.
func (tr *testRouter) local(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.ConnectLocal(tr.r, client.Config{
		Realm:  testRealm,
		Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("local client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// logBuffer captures slog output for assertions.
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

func (b *logBuffer) count(substr string) int {
	return strings.Count(b.String(), substr)
}

// fastTunables shortens every delay for tests and disables jitter.
func fastTunables(c *Connection) {
	c.t.initialRetryDelay = 10 * time.Millisecond
	c.t.baseMaxRetryDelay = 20 * time.Millisecond
	c.t.retryDelayJitter = 0
	c.t.retryFirstDelay = 10 * time.Millisecond
	c.t.retryMaxDelay = 40 * time.Millisecond
	c.t.responseTimeout = 2 * time.Second
	c.t.connectTimeout = 2 * time.Second
}

type connOption func(*Config, *Connection)

// newTestConn returns a configured (not started) connection to tr. Stop is
// registered as cleanup.
func newTestConn(t *testing.T, tr *testRouter, opts ...connOption) (*Connection, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	c := NewConnection()
	fastTunables(c)
	cfg := Config{
		SwarmKey:     1,
		AppKey:       2,
		Stage:        StageDevelopment,
		URL:          tr.url,
		SerialNumber: testSerial,
		AuthID:       testAuthID,
		AuthSecret:   testSecret,
		Logger:       slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, o := range opts {
		o(&cfg, c)
	}
	if err := c.Configure(cfg); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return c, logs
}

// startTestConn returns a started connection.
func startTestConn(t *testing.T, tr *testRouter, opts ...connOption) (*Connection, *logBuffer) {
	t.Helper()
	c, logs := newTestConn(t, tr, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v\nlogs:\n%s", err, logs)
	}
	return c, logs
}

// eventually polls cond until it holds or timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// recv waits for a value from ch.
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// noRecv asserts nothing arrives on ch for a short while.
func noRecv[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %v", what, v)
	case <-time.After(100 * time.Millisecond):
	}
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// localPublish publishes from a trusted local client.
func localPublish(t *testing.T, cli *client.Client, topic string, args wamp.List, kwargs wamp.Dict) {
	t.Helper()
	if err := cli.Publish(topic, wamp.Dict{wamp.OptAcknowledge: true}, args, kwargs); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

var goroutineHeader = regexp.MustCompile(`^goroutine (\d+) `)

// interestingGoroutines returns the stacks of goroutines running SDK or
// nexus code (not test code), keyed by goroutine ID.
func interestingGoroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]string{}
	for _, g := range strings.Split(string(buf), "\n\n") {
		m := goroutineHeader.FindStringSubmatch(g)
		if m == nil {
			continue
		}
		relevant := false
		for _, line := range strings.Split(g, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "github.com/gammazero/nexus/") ||
				(strings.HasPrefix(line, "github.com/RecordEvolution/ironflock-go/crossbar.") &&
					!strings.Contains(line, "crossbar.Test") && !strings.Contains(line, "_test.go")) {
				relevant = true
			}
		}
		if relevant && !strings.Contains(g, "_test.go") {
			out[m[1]] = g
		}
	}
	return out
}
