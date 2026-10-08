package wamp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests against a real router: ironflock-router with the fake platform, as
// integration/ironflock-router/start.sh runs them. They run only when
// IRONFLOCK_TEST_ROUTER_URL is set, e.g. to ws://localhost:18082/ws-ua-usr.
//
// The connection joins realm-2-26-dev with the harness's per-app credential,
// which the router admits with the production `app` role. That role
// registers only device-function URIs <swarm>.<device>.<app>.<STAGE>.<name>,
// and the router's identity check reserves them to the device they name, so
// the tests register and call functions of device 42 (realRouterFunction)
// and publish and subscribe names of their own.
const (
	realRouterRealm  = "realm-2-26-dev"
	realRouterAuthID = "app-26-per-app"
	realRouterSecret = "per-app-secret"
)

// realRouterFunction returns the URI of the device function name of the
// harness's device 42.
func realRouterFunction(name string) string { return "2.42.26.DEV." + name }

func realRouterConn(t *testing.T, url string, mutate func(*Config)) (*Connection, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	c := NewConnection()
	c.t.initialRetryDelay = 50 * time.Millisecond
	c.t.retryFirstDelay = 50 * time.Millisecond
	cfg := Config{
		SwarmKey:     2,
		AppKey:       26,
		Stage:        StageDevelopment,
		URL:          url,
		SerialNumber: "router-real-test",
		AuthID:       realRouterAuthID,
		AuthSecret:   realRouterSecret,
		Logger:       slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	if err := c.Configure(cfg); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(ctxTimeout(t, 10*time.Second)); err != nil {
			t.Errorf("Stop: %v", err)
		}
		if t.Failed() {
			t.Logf("connection log:\n%s", logs)
		}
	})
	return c, logs
}

func TestRealRouter(t *testing.T) {
	routerURL := os.Getenv("IRONFLOCK_TEST_ROUTER_URL")
	if routerURL == "" {
		t.Skip("IRONFLOCK_TEST_ROUTER_URL not set")
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	// The connection under test reaches the router through a proxy that can
	// drop its TCP connection: a transport loss, which the reconnect subtest
	// needs (the app role may not kill sessions).
	proxy := newDropProxy(t, routerURL)
	connects := make(chan struct{}, 8)
	c, logs := realRouterConn(t, proxy.url, func(cfg *Config) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	if err := c.Start(ctxTimeout(t, 15*time.Second)); err != nil {
		t.Fatalf("Start: %v\n%s", err, logs)
	}
	recv(t, connects, "OnConnect")
	if c.Realm() != realRouterRealm || !c.IsOpen() {
		t.Fatalf("realm %q open %v", c.Realm(), c.IsOpen())
	}
	ctx := ctxTimeout(t, 60*time.Second)

	topic := "com.example.gosdk.topic." + suffix
	handler, events := eventCollector(16)
	sub, err := c.Subscribe(ctx, topic, handler, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	publish := func(t *testing.T, seq int) {
		t.Helper()
		f := false
		err := c.Publish(ctx, topic, []any{seq, "two", 3.5}, map[string]any{"foo": "bar", "nested": map[string]any{"n": 1}},
			&PublishOptions{Acknowledge: true, ExcludeMe: &f}, 0)
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	t.Run("publish subscribe round trip", func(t *testing.T) {
		publish(t, 1)
		ev := recv(t, events, "own event")
		if ev.Topic != topic || !reflect.DeepEqual(ev.Args, []any{int64(1), "two", 3.5}) {
			t.Fatalf("event = %+v", ev)
		}
		if !reflect.DeepEqual(ev.Kwargs, map[string]any{"foo": "bar", "nested": map[string]any{"n": int64(1)}}) {
			t.Fatalf("kwargs = %#v", ev.Kwargs)
		}
	})

	proc := realRouterFunction("gosdk_add_" + suffix)
	reg, err := c.Register(ctx, proc, func(_ context.Context, inv *Invocation) (any, error) {
		a, _ := inv.Args[0].(int64)
		b, _ := inv.Args[1].(int64)
		if b < 0 {
			return nil, &Error{URI: "app.error.negative", Args: []any{"b must be positive"}, Kwargs: map[string]any{"b": b}}
		}
		return Result{Args: []any{a + b}, Kwargs: map[string]any{"caller_known": inv.Details["caller"] != nil}}, nil
	}, nil)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Run("register call round trip", func(t *testing.T) {
		res, err := c.Call(ctx, proc, []any{20, 22}, nil, nil, 0)
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if res.Value() != int64(42) {
			t.Fatalf("result = %#v", res.Args)
		}
		_, err = c.Call(ctx, proc, []any{1, -1}, nil, nil, 0)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != "app.error.negative" ||
			!reflect.DeepEqual(werr.Args, []any{"b must be positive"}) ||
			!reflect.DeepEqual(werr.Kwargs, map[string]any{"b": int64(-1)}) {
			t.Fatalf("callee error = %#v", err)
		}
	})

	t.Run("identity check refuses another device's function", func(t *testing.T) {
		_, err := c.Register(ctx, "2.43.26.DEV.gosdk_other_"+suffix, echoHandler(""), nil)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != URINotAuthorized {
			t.Fatalf("Register of device 43's function = %v, want %s", err, URINotAuthorized)
		}
	})

	t.Run("force_reregister takes over", func(t *testing.T) {
		other, _ := realRouterConn(t, routerURL, nil)
		if err := other.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		_, err := other.Register(ctx, proc, func(context.Context, *Invocation) (any, error) { return "taken over", nil }, nil)
		if err != nil {
			t.Fatalf("force_reregister by a second session: %v", err)
		}
		eventually(t, 5*time.Second, "the evicted registration to turn inactive", func() bool { return !reg.Active() })
		res, err := c.Call(ctx, proc, []any{1, 1}, nil, nil, 0)
		if err != nil || res.Value() != "taken over" {
			t.Fatalf("Call after takeover = %v, %v", res, err)
		}
		if err := other.Stop(ctxTimeout(t, 10*time.Second)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reconnect restores", func(t *testing.T) {
		proxy.dropAll()
		recv(t, connects, "OnConnect after the transport was dropped")
		if !sub.Active() || !reg.Active() {
			t.Fatalf("not restored: sub=%v reg=%v", sub.Active(), reg.Active())
		}
		// The registration was taken over and released by the subtest
		// above; the restore registered it again.
		res, err := c.Call(ctx, proc, []any{2, 3}, nil, nil, 2*time.Second)
		if err != nil || res.Value() != int64(5) {
			t.Fatalf("Call after reconnect = %v, %v", res, err)
		}
		publish(t, 2)
		if ev := recv(t, events, "event after reconnect"); ev.Args[0] != int64(2) {
			t.Fatalf("event = %+v", ev)
		}
	})

	t.Run("unsubscribe", func(t *testing.T) {
		if err := sub.Unsubscribe(ctx); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		if sub.Active() {
			t.Fatal("still active")
		}
		publish(t, 3)
		noRecv(t, events, "event after unsubscribe")
	})

	t.Run("unregister", func(t *testing.T) {
		if err := reg.Unregister(ctx); err != nil {
			t.Fatalf("Unregister: %v", err)
		}
		_, err := c.Call(ctx, proc, []any{1, 2}, nil, nil, 0)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != URINoSuchProcedure {
			t.Fatalf("Call after Unregister = %v", err)
		}
	})

	t.Run("retry window reaches a late registration", func(t *testing.T) {
		late := realRouterFunction("gosdk_late_" + suffix)
		other, _ := realRouterConn(t, routerURL, nil)
		if err := other.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		const retried = "Procedure not registered yet; retrying"
		before := logs.count(retried)
		type outcome struct {
			res *Result
			err error
		}
		called := make(chan outcome, 1)
		go func() {
			res, err := c.Call(ctx, late, nil, nil, nil, 10*time.Second)
			called <- outcome{res, err}
		}()
		eventually(t, 5*time.Second, "a no_such_procedure retry", func() bool { return logs.count(retried) > before })
		if _, err := other.Register(ctx, late, func(context.Context, *Invocation) (any, error) { return "late", nil }, nil); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if got := recv(t, called, "the retried call"); got.err != nil || got.res.Value() != "late" {
			t.Fatalf("Call = %v, %v", got.res, got.err)
		}
	})

	t.Run("wrong secret is fatal with FailOnAuthError", func(t *testing.T) {
		var failures atomic.Int32
		bad, _ := realRouterConn(t, routerURL, func(cfg *Config) {
			cfg.AuthSecret = "wrong-secret"
			cfg.FailOnAuthError = true
			cfg.OnAuthFailure = func(string) { failures.Add(1) }
		})
		err := bad.Start(ctxTimeout(t, 15*time.Second))
		var authErr *AuthError
		if !errors.As(err, &authErr) || !IsFatalAuthReason(authErr.Reason) {
			t.Fatalf("Start = %v, want *AuthError", err)
		}
		eventually(t, 5*time.Second, "OnAuthFailure", func() bool { return failures.Load() == 1 })
		t.Logf("the router refused the credential with %s", authErr.Reason)
	})

	t.Run("missing realm is not fatal", func(t *testing.T) {
		var mu sync.Mutex
		var reasons []string
		missing, _ := realRouterConn(t, routerURL, func(cfg *Config) {
			cfg.SwarmKey, cfg.AppKey = 999999, 999999
			cfg.FailOnAuthError = true
			cfg.FirstConnectTimeout = 1500 * time.Millisecond
		})
		missing.t.onRetry = func(reason string, _ time.Duration) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		}
		err := missing.Start(context.Background())
		if !errors.Is(err, ErrNotConnected) || !strings.Contains(err.Error(), "within 1.5s") {
			t.Fatalf("Start = %v, want the first-connect timeout", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(reasons) == 0 || reasons[0] != URINoSuchRealm {
			t.Fatalf("refusal reasons = %v, want %s", reasons, URINoSuchRealm)
		}
	})
}

// dropProxy forwards TCP connections to a router and drops all of them on
// request: a transport loss the router did not cause.
type dropProxy struct {
	ln     net.Listener
	target string
	url    string // the router URL with the proxy as its host

	mu     sync.Mutex
	conns  []net.Conn
	closed bool
	wg     sync.WaitGroup
}

func newDropProxy(t *testing.T, routerURL string) *dropProxy {
	t.Helper()
	u, err := url.Parse(routerURL)
	if err != nil || u.Host == "" {
		t.Fatalf("router URL %q: %v", routerURL, err)
	}
	if u.Port() == "" {
		t.Fatalf("router URL %q: the proxy needs an explicit port", routerURL)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &dropProxy{ln: ln, target: u.Host}
	u.Host = ln.Addr().String()
	p.url = u.String()
	p.wg.Add(1)
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *dropProxy) serve() {
	defer p.wg.Done()
	for {
		down, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.DialTimeout("tcp", p.target, 5*time.Second)
		if err != nil {
			_ = down.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = down.Close()
			_ = up.Close()
			return
		}
		p.conns = append(p.conns, down, up)
		p.wg.Add(2)
		p.mu.Unlock()
		go p.pipe(up, down)
		go p.pipe(down, up)
	}
}

func (p *dropProxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

// dropAll closes every proxied connection, both sides.
func (p *dropProxy) dropAll() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (p *dropProxy) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	_ = p.ln.Close()
	p.dropAll()
	p.wg.Wait()
}
