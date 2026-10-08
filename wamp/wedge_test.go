package wamp

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gammazero/nexus/v3/client"
	nxwamp "github.com/gammazero/nexus/v3/wamp"
)

// A nexus client's receive loop can wedge for good: a reply handed over just
// as its waiter timed out parks it in runSignalReply, where nothing wakes it
// (finding 7; fixed in the fork, not yet pinned). The client then stops
// reading the connection, its Done never closes and its Close never returns.
//
// wedgeClient produces exactly that state, deterministically, in the
// current session's real client: it parks the receive loop in an event
// handler subscribed directly on the nexus client (nexus runs event handlers
// on the receive loop). The loop resumes when release is called, at the
// latest at cleanup, so nothing outlives the test.
func wedgeClient(t *testing.T, tr *testRouter, c *Connection) (release func()) {
	t.Helper()
	s := c.currentSession()
	if s == nil {
		t.Fatal("no session to wedge")
	}
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release) // before the connection's cleanup Stop, which runs later
	if err := s.cli.Subscribe("test.wedge", func(*nxwamp.Event) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}, nil); err != nil {
		t.Fatalf("subscribe the wedge: %v", err)
	}
	localPublish(t, tr.local(t), "test.wedge", nil, nil)
	recv(t, entered, "the receive loop to park")
	return release
}

// wedgeOptions shortens the timings of the abandonment path.
func wedgeOptions(connects chan<- struct{}, disconnects chan<- string) connOption {
	return func(cfg *Config, c *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
		cfg.OnDisconnect = func(reason string) { disconnects <- reason }
		c.t.responseTimeout = 500 * time.Millisecond
		c.t.clientGrace = 100 * time.Millisecond
	}
}

// The transport dies while the client is wedged: the session is given up
// within the grace and the connection reconnects with a fresh client.
func TestWedgedClientAbandonedWhenTransportDies(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	c, logs := startTestConn(t, tr, wedgeOptions(connects, disconnects))
	recv(t, connects, "OnConnect")
	old := c.currentSession()
	wedgeClient(t, tr, c)

	tr.ln.dropAll()
	if got := recv(t, disconnects, "OnDisconnect"); got != reasonTransportLost {
		t.Fatalf("OnDisconnect(%q), want %q", got, reasonTransportLost)
	}
	recv(t, connects, "OnConnect with a fresh client")
	if s := c.currentSession(); s == nil || s == old || !c.IsOpen() {
		t.Fatalf("not reconnected: session %p (old %p), open %v", s, old, c.IsOpen())
	}
	if err := c.Publish(ctxTimeout(t, 5*time.Second), "after.wedge", nil, nil, &PublishOptions{Acknowledge: true}, 0); err != nil {
		t.Fatalf("Publish on the new session: %v", err)
	}
	if !strings.Contains(logs.String(), "abandoning it") {
		t.Fatalf("abandoned client not logged:\n%s", logs)
	}
}

// The transport stays up but the wedged client takes no more messages (no
// keepalive here, so only the forwarder watchdog can notice): the next
// message the router sends stalls the forwarder, which closes the
// connection after the response timeout.
func TestForwarderStallClosesWedgedSession(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	c, logs := startTestConn(t, tr, wedgeOptions(connects, disconnects), func(cfg *Config, _ *Connection) {
		cfg.KeepAlive = -1
	})
	recv(t, connects, "OnConnect")
	old := c.currentSession()
	wedgeClient(t, tr, c)

	localPublish(t, tr.local(t), "test.wedge", nil, nil) // cannot be handed to the client
	if got := recv(t, disconnects, "OnDisconnect"); got != reasonTransportLost {
		t.Fatalf("OnDisconnect(%q), want %q", got, reasonTransportLost)
	}
	recv(t, connects, "OnConnect with a fresh client")
	if s := c.currentSession(); s == nil || s == old {
		t.Fatal("not reconnected")
	}
	if !strings.Contains(logs.String(), "stopped taking messages") {
		t.Fatalf("stall not logged:\n%s", logs)
	}
}

// The router's close reason survives a wedged client: the peer saw the
// GOODBYE even though the client never processed it. With FailOnAuthError a
// revoked grant stays fatal instead of turning into a transport loss and a
// reconnect.
func TestWedgedClientKeepsCloseReason(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	reasons := make(chan string, 4)
	c, _ := startTestConn(t, tr, wedgeOptions(connects, disconnects), func(cfg *Config, _ *Connection) {
		cfg.FailOnAuthError = true
		cfg.OnAuthFailure = func(reason string) { reasons <- reason }
	})
	recv(t, connects, "OnConnect")
	wedgeClient(t, tr, c)

	if n := tr.r.KillSessionsByAuthrole(testRealm, "device", URINotAuthorized, "revoked", 0); n != 1 {
		t.Fatalf("killed %d sessions", n)
	}
	if got := recv(t, reasons, "OnAuthFailure"); got != URINotAuthorized {
		t.Fatalf("OnAuthFailure(%q), want %q", got, URINotAuthorized)
	}
	err := c.WaitSession(context.Background(), 50*time.Millisecond)
	var authErr *AuthError
	if !errors.Is(err, ErrStopped) || !errors.As(err, &authErr) || authErr.Reason != URINotAuthorized {
		t.Fatalf("WaitSession = %v, want ErrStopped wrapping the *AuthError", err)
	}
	noRecv(t, connects, "a reconnect after a fatal close")
}

// Stop does not wait for a wedged client to end, which it never does.
func TestStopAbandonsWedgedClient(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	c, _ := startTestConn(t, tr, wedgeOptions(connects, disconnects))
	recv(t, connects, "OnConnect")
	wedgeClient(t, tr, c)

	begin := time.Now()
	if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatalf("Stop = %v after %v", err, time.Since(begin))
	}
	// The GOODBYE exchange waits for the response timeout, the client for
	// the grace.
	if el := time.Since(begin); el > 3*time.Second {
		t.Fatalf("Stop took %v", el)
	}
	if !closed(c.done) {
		t.Fatal("supervisor still running after Stop returned")
	}
	noRecv(t, disconnects, "OnDisconnect after Stop")
}

// A call in flight when its session is abandoned fails as a lost session:
// the wedged client would never end it, so it would hang for good.
func TestCallOnAbandonedSessionFails(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	c, _ := startTestConn(t, tr, wedgeOptions(connects, disconnects))
	recv(t, connects, "OnConnect")

	callee := tr.local(t)
	invoked := make(chan struct{}, 1)
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) }) // before the callee's Close, which waits for its handler
	if err := callee.Register("slow.proc", func(ctx context.Context, _ *nxwamp.Invocation) client.InvokeResult {
		invoked <- struct{}{}
		select {
		case <-hold:
		case <-ctx.Done():
		}
		return client.InvokeResult{Args: nxwamp.List{"late"}}
	}, nil); err != nil {
		t.Fatal(err)
	}
	called := make(chan error, 1)
	go func() {
		_, err := c.Call(context.Background(), "slow.proc", nil, nil, nil, 0)
		called <- err
	}()
	recv(t, invoked, "the invocation")
	wedgeClient(t, tr, c)

	tr.ln.dropAll()
	err := recv(t, called, "the call on the abandoned session")
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Call = %v, want a lost session (ErrNotConnected)", err)
	}
	recv(t, connects, "OnConnect with a fresh client")
}

// Once the wedged client resumes, nothing of the abandoned session is left.
func TestNoGoroutineLeaksAfterAbandonedClient(t *testing.T) {
	before := interestingGoroutines()
	func() {
		tr := newTestRouter(t, true)
		connects := make(chan struct{}, 4)
		disconnects := make(chan string, 4)
		c, _ := newTestConn(t, tr, wedgeOptions(connects, disconnects))
		if err := c.Start(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		recv(t, connects, "OnConnect")
		release := wedgeClient(t, tr, c)
		tr.ln.dropAll()
		recv(t, connects, "OnConnect with a fresh client")
		if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		release()
		tr.close()
	}()

	var leaked []string
	deadline := time.Now().Add(5 * time.Second)
	for {
		leaked = leaked[:0]
		for id, stack := range interestingGoroutines() {
			if _, ok := before[id]; !ok {
				leaked = append(leaked, stack)
			}
		}
		if len(leaked) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(leaked) > 0 {
		t.Fatalf("%d goroutines leaked:\n\n%s", len(leaked), strings.Join(leaked, "\n\n"))
	}
}
