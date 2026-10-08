package crossbar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests against a real Crossbar router. They run only when
// IRONFLOCK_TEST_CROSSBAR_URL is set, e.g. to ws://localhost:18080/ws-ua-usr
// for the router of ironflock-py/test/integration (realm realm-2-26-dev, a
// static WAMP-CRA user whose secret equals its authid).
const (
	realCrossbarAuthID = "06a0bf96-a539-4d6a-8471-ac7adc67616e"
	realCrossbarRealm  = "realm-2-26-dev"
)

func realCrossbarConn(t *testing.T, url string, mutate func(*Config)) (*Connection, *logBuffer) {
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
		SerialNumber: realCrossbarAuthID,
		AuthID:       realCrossbarAuthID,
		AuthSecret:   realCrossbarAuthID,
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

func TestCrossbarRealRouter(t *testing.T) {
	url := os.Getenv("IRONFLOCK_TEST_CROSSBAR_URL")
	if url == "" {
		t.Skip("IRONFLOCK_TEST_CROSSBAR_URL not set")
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	connects := make(chan struct{}, 8)
	c, logs := realCrossbarConn(t, url, func(cfg *Config) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	if err := c.Start(ctxTimeout(t, 15*time.Second)); err != nil {
		t.Fatalf("Start: %v\n%s", err, logs)
	}
	recv(t, connects, "OnConnect")
	if c.Realm() != realCrossbarRealm || !c.IsOpen() {
		t.Fatalf("realm %q open %v", c.Realm(), c.IsOpen())
	}
	ctx := ctxTimeout(t, 30*time.Second)

	topic := "test.gosdk.topic." + suffix
	handler, events := eventCollector(16)
	sub, err := c.Subscribe(ctx, topic, handler, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	publish := func(seq int) {
		t.Helper()
		f := false
		err := c.Publish(ctx, topic, []any{seq, "two", 3.5}, map[string]any{"foo": "bar", "nested": map[string]any{"n": 1}},
			&PublishOptions{Acknowledge: true, ExcludeMe: &f}, 0)
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	t.Run("publish subscribe round trip", func(t *testing.T) {
		publish(1)
		ev := recv(t, events, "own event")
		if ev.Topic != topic || !reflect.DeepEqual(ev.Args, []any{int64(1), "two", 3.5}) {
			t.Fatalf("event = %+v", ev)
		}
		if !reflect.DeepEqual(ev.Kwargs, map[string]any{"foo": "bar", "nested": map[string]any{"n": int64(1)}}) {
			t.Fatalf("kwargs = %#v", ev.Kwargs)
		}
	})

	proc := "test.gosdk.add." + suffix
	reg, err := c.Register(ctx, proc, func(_ context.Context, inv *Invocation) (any, error) {
		a, _ := inv.Args[0].(int64)
		b, _ := inv.Args[1].(int64)
		if b < 0 {
			return nil, &WampError{URI: "app.error.negative", Args: []any{"b must be positive"}, Kwargs: map[string]any{"b": b}}
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
		var werr *WampError
		if !errors.As(err, &werr) || werr.URI != "app.error.negative" ||
			!reflect.DeepEqual(werr.Args, []any{"b must be positive"}) ||
			!reflect.DeepEqual(werr.Kwargs, map[string]any{"b": int64(-1)}) {
			t.Fatalf("callee error = %#v", err)
		}
	})

	t.Run("force_reregister takes over", func(t *testing.T) {
		other, _ := realCrossbarConn(t, url, nil)
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
		killer, _ := realCrossbarConn(t, url, nil)
		if err := killer.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		sid := uint64(c.currentSession().cli.ID())
		if _, err := killer.Call(ctx, "wamp.session.kill", []any{sid}, map[string]any{"reason": "wamp.close.killed"}, nil, 0); err != nil {
			t.Skipf("cannot kill the session on this router: %v", err)
		}
		recv(t, connects, "OnConnect after the session was killed")
		if !sub.Active() || !reg.Active() {
			t.Fatalf("not restored: sub=%v reg=%v", sub.Active(), reg.Active())
		}
		// The registration was taken over and released by the subtest
		// above; the restore registered it again.
		res, err := c.Call(ctx, proc, []any{2, 3}, nil, nil, 2*time.Second)
		if err != nil || res.Value() != int64(5) {
			t.Fatalf("Call after reconnect = %v, %v", res, err)
		}
		publish(2)
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
		publish(3)
		noRecv(t, events, "event after unsubscribe")
	})

	t.Run("unregister", func(t *testing.T) {
		if err := reg.Unregister(ctx); err != nil {
			t.Fatalf("Unregister: %v", err)
		}
		_, err := c.Call(ctx, proc, []any{1, 2}, nil, nil, 0)
		var werr *WampError
		if !errors.As(err, &werr) || werr.URI != ErrURINoSuchProcedure {
			t.Fatalf("Call after Unregister = %v", err)
		}
	})

	t.Run("retry window reaches a late registration", func(t *testing.T) {
		late := "test.gosdk.late." + suffix
		other, _ := realCrossbarConn(t, url, nil)
		if err := other.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		go func() {
			time.Sleep(300 * time.Millisecond)
			_, _ = other.Register(context.Background(), late, func(context.Context, *Invocation) (any, error) { return "late", nil }, nil)
		}()
		res, err := c.Call(ctx, late, nil, nil, nil, 5*time.Second)
		if err != nil || res.Value() != "late" {
			t.Fatalf("Call = %v, %v", res, err)
		}
	})

	t.Run("wrong secret is fatal with FailOnAuthError", func(t *testing.T) {
		var failures atomic.Int32
		bad, _ := realCrossbarConn(t, url, func(cfg *Config) {
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
		t.Logf("Crossbar refused the credential with %s", authErr.Reason)
	})

	t.Run("missing realm is not fatal", func(t *testing.T) {
		var mu sync.Mutex
		var reasons []string
		missing, _ := realCrossbarConn(t, url, func(cfg *Config) {
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
		if len(reasons) == 0 || reasons[0] != ErrURINoSuchRealm {
			t.Fatalf("refusal reasons = %v, want %s", reasons, ErrURINoSuchRealm)
		}
	})
}
