package wamp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ironflock/nexus/v3/client"
	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

// Subscribe, Register, Unsubscribe, UnsubscribeTopic and Unregister wait
// for subscription changes in progress — another one's router round trip,
// the restore after a reconnect — within their context (findings 4, 31).
// The tests make the router hold a SUBSCRIBE (see testAuthorizer.hold), so
// the change in progress takes exactly as long as the test wants.

// queuedOpBound is how long an operation whose 50ms context ends while it
// is queued may take to return. Without a ctx-aware wait it returns only
// once the held change is released, which the test does not do before.
const queuedOpBound = 2 * time.Second

// holdSubscribe starts a Subscribe of topic that the router holds, and
// returns once the router holds it — while the Subscribe holds the
// connection's subscription state. The result arrives on the channel once
// release is called.
func holdSubscribe(t *testing.T, tr *testRouter, c *Connection, topic string) (release func(), result <-chan error) {
	t.Helper()
	release = tr.authz.hold(t, "subscribe:"+topic)
	done := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), topic, func(*Event) {}, nil)
		done <- err
	}()
	if key := recv(t, tr.authz.held, "the router to hold the SUBSCRIBE"); key != "subscribe:"+topic {
		t.Fatalf("the router holds %s", key)
	}
	return release, done
}

// expectCtxError runs op with a 50ms context and expects it to return that
// context's error promptly.
func expectCtxError(t *testing.T, name string, op func(ctx context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	begin := time.Now()
	done := make(chan error, 1)
	go func() { done <- op(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s = %v, want context.DeadlineExceeded", name, err)
		}
		if el := time.Since(begin); el > queuedOpBound {
			t.Fatalf("%s took %v", name, el)
		}
	case <-time.After(queuedOpBound):
		t.Fatalf("%s ignored its context while queued", name)
	}
}

// trackedCounts returns how many topics and procedures c tracks.
func trackedCounts(t *testing.T, c *Connection) (groups, regs int) {
	t.Helper()
	if err := c.state.lock(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatalf("state lock: %v", err)
	}
	defer c.state.unlock()
	return len(c.groupList()), len(c.regs)
}

// tracksTopic reports whether c tracks a subscription of topic.
func tracksTopic(t *testing.T, c *Connection, topic string) bool {
	t.Helper()
	if err := c.state.lock(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatalf("state lock: %v", err)
	}
	defer c.state.unlock()
	return c.findGroup(topic) != nil
}

// Subscribe and Register queued behind a change in progress give up when
// their context ends, and send nothing.
func TestQueuedSubscribeAndRegisterHonourCtx(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	release, slow := holdSubscribe(t, tr, c, "slow.a")

	expectCtxError(t, "Subscribe", func(ctx context.Context) error {
		_, err := c.Subscribe(ctx, "queued.topic", func(*Event) {}, nil)
		return err
	})
	expectCtxError(t, "Register", func(ctx context.Context) error {
		_, err := c.Register(ctx, "queued.proc", echoHandler(""), nil)
		return err
	})

	release()
	if err := recv(t, slow, "the held Subscribe"); err != nil {
		t.Fatalf("held Subscribe: %v", err)
	}
	// A round trip behind anything the abandoned operations might still send.
	if err := c.Publish(ctxTimeout(t, 5*time.Second), "barrier", nil, nil, &PublishOptions{Acknowledge: true}, 0); err != nil {
		t.Fatal(err)
	}
	if n, m := tr.authz.count("subscribe:queued.topic"), tr.authz.count("register:queued.proc"); n+m > 0 {
		t.Fatalf("abandoned operations reached the router: %d SUBSCRIBE, %d REGISTER", n, m)
	}
	if groups, regs := trackedCounts(t, c); groups != 1 || regs != 0 {
		t.Fatalf("tracked %d topics and %d procedures, want only slow.a", groups, regs)
	}
}

// Unsubscribe, UnsubscribeTopic and Unregister remove the subscription or
// registration at once, even when their context ends while they are queued:
// they return the context's error, and finish the removal at the router in
// the background.
func TestRemovalDetachesAtOnceWhenCtxEndsWhileQueued(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 10*time.Second)
	h1, ev1 := eventCollector(8)
	sub, err := c.Subscribe(ctx, "detach.one", h1, nil)
	if err != nil {
		t.Fatal(err)
	}
	h2, ev2 := eventCollector(8)
	h3, ev3 := eventCollector(8)
	for _, h := range []EventHandler{h2, h3} {
		if _, err := c.Subscribe(ctx, "detach.all", h, nil); err != nil {
			t.Fatal(err)
		}
	}
	invoked := make(chan struct{}, 4)
	reg, err := c.Register(ctx, "detach.proc", func(context.Context, *Invocation) (any, error) {
		invoked <- struct{}{}
		return "served", nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	release, slow := holdSubscribe(t, tr, c, "slow.b")
	expectCtxError(t, "Unsubscribe", func(ctx context.Context) error { return c.Unsubscribe(ctx, sub) })
	expectCtxError(t, "UnsubscribeTopic", func(ctx context.Context) error { return c.UnsubscribeTopic(ctx, "detach.all") })
	expectCtxError(t, "Unregister", func(ctx context.Context) error { return c.Unregister(ctx, reg) })
	if sub.Active() || reg.Active() {
		t.Fatalf("still active: subscription %v, registration %v", sub.Active(), reg.Active())
	}
	// Removing them again is a no-op.
	if err := c.Unsubscribe(ctx, sub); err != nil {
		t.Fatalf("second Unsubscribe = %v", err)
	}
	if err := c.Unregister(ctx, reg); err != nil {
		t.Fatalf("second Unregister = %v", err)
	}

	// The router still routes to the session, but nothing reaches the
	// handlers: the events are dropped, the call is refused as if the
	// procedure were gone.
	pub := tr.local(t)
	localPublish(t, pub, "detach.one", nxwamp.List{1}, nil)
	localPublish(t, pub, "detach.all", nxwamp.List{2}, nil)
	called := make(chan error, 1)
	go func() {
		_, err := pub.Call(ctx, "detach.proc", nil, nil, nil, nil)
		called <- err // the refusal waits at the router behind the held SUBSCRIBE
	}()
	noRecv(t, ev1, "an event after Unsubscribe")
	noRecv(t, ev2, "an event after UnsubscribeTopic")
	noRecv(t, ev3, "an event after UnsubscribeTopic")

	release()
	if err := recv(t, slow, "the held Subscribe"); err != nil {
		t.Fatal(err)
	}
	var rpcErr client.RPCError
	if err := recv(t, called, "the call"); !errors.As(err, &rpcErr) || rpcErr.Err.Error != nxwamp.ErrNoSuchProcedure {
		t.Fatalf("call of an unregistered procedure = %v, want %s", err, nxwamp.ErrNoSuchProcedure)
	}
	noRecv(t, invoked, "a call of the handler after Unregister")

	// The router forgets them once the background removal ran.
	eventually(t, 5*time.Second, "the router to forget the subscriptions and the registration", func() bool {
		return tr.lookup(t, nxwamp.MetaProcSubLookup, "detach.one") == 0 &&
			tr.lookup(t, nxwamp.MetaProcSubLookup, "detach.all") == 0 &&
			tr.lookup(t, nxwamp.MetaProcRegLookup, "detach.proc") == 0
	})
	if groups, regs := trackedCounts(t, c); groups != 1 || regs != 0 {
		t.Fatalf("tracked %d topics and %d procedures, want only slow.b", groups, regs)
	}
}

// The same behind the restore after a reconnect, which holds the
// subscription state while it waits for the router: the queued operations
// give up at their context's end, and the restore skips what was removed
// meanwhile.
func TestQueuedBehindRestoreHonoursCtx(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 10*time.Second)
	if _, err := c.Subscribe(ctx, "restore.slow", func(*Event) {}, nil); err != nil {
		t.Fatal(err)
	}
	later, err := c.Subscribe(ctx, "restore.later", func(*Event) {}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := c.Register(ctx, "restore.proc", echoHandler(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	release := tr.authz.hold(t, "subscribe:restore.slow")
	tr.ln.dropAll()
	if key := recv(t, tr.authz.held, "the router to hold the restore"); key != "subscribe:restore.slow" {
		t.Fatalf("the router holds %s", key)
	}
	expectCtxError(t, "Subscribe", func(ctx context.Context) error {
		_, err := c.Subscribe(ctx, "restore.new", func(*Event) {}, nil)
		return err
	})
	expectCtxError(t, "Unsubscribe", func(ctx context.Context) error { return c.Unsubscribe(ctx, later) })
	expectCtxError(t, "Unregister", func(ctx context.Context) error { return c.Unregister(ctx, reg) })

	release()
	recv(t, connects, "OnConnect after the restore")
	// The removed entries were not restored, and the abandoned Subscribe
	// sent nothing.
	if n := tr.authz.count("subscribe:restore.later"); n != 1 {
		t.Fatalf("SUBSCRIBE restore.later sent %d times, want only the first", n)
	}
	if n := tr.authz.count("register:restore.proc"); n != 1 {
		t.Fatalf("REGISTER restore.proc sent %d times, want only the first", n)
	}
	if n := tr.authz.count("subscribe:restore.new"); n != 0 {
		t.Fatal("the abandoned Subscribe reached the router")
	}
	eventually(t, 5*time.Second, "the background removal", func() bool {
		groups, regs := trackedCounts(t, c)
		return groups == 1 && regs == 0
	})
	if !tracksTopic(t, c, "restore.slow") {
		t.Fatal("restore.slow not tracked")
	}
}

// A procedure or topic removed while its removal waits in the background can
// be registered or subscribed again at once: the new registration is not
// refused as a duplicate, and each handler gets only what it should.
func TestRegisterAndSubscribeAgainWhileRemovalPending(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 10*time.Second)
	oldEvents, oldCalls := make(chan *Event, 8), make(chan struct{}, 8)
	sub, err := c.Subscribe(ctx, "again.topic", func(ev *Event) { oldEvents <- ev }, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := c.Register(ctx, "again.proc", func(context.Context, *Invocation) (any, error) {
		oldCalls <- struct{}{}
		return "old", nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	release, slow := holdSubscribe(t, tr, c, "slow.c")
	expectCtxError(t, "Unsubscribe", func(ctx context.Context) error { return c.Unsubscribe(ctx, sub) })
	expectCtxError(t, "Unregister", func(ctx context.Context) error { return c.Unregister(ctx, reg) })
	type registered struct {
		reg *Registration
		err error
	}
	again := make(chan registered, 1)
	go func() {
		r, err := c.Register(ctx, "again.proc", func(context.Context, *Invocation) (any, error) { return "new", nil }, nil)
		again <- registered{r, err}
	}()
	newHandler, newEvents := eventCollector(8)
	subscribed := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(ctx, "again.topic", newHandler, nil)
		subscribed <- err
	}()

	release()
	if err := recv(t, slow, "the held Subscribe"); err != nil {
		t.Fatal(err)
	}
	if r := recv(t, again, "Register again"); r.err != nil || !r.reg.Active() {
		t.Fatalf("Register again = %v (active %v)", r.err, r.reg != nil && r.reg.Active())
	}
	if err := recv(t, subscribed, "Subscribe again"); err != nil {
		t.Fatalf("Subscribe again = %v", err)
	}
	pub := tr.local(t)
	eventually(t, 5*time.Second, "the removals to finish", func() bool {
		groups, regs := trackedCounts(t, c)
		return groups == 2 && regs == 1 // slow.c and again.topic; again.proc
	})
	res, err := localCall(t, pub, "again.proc")
	if err != nil || res.Arguments[0] != "new" {
		t.Fatalf("call = %v, %v", res, err)
	}
	localPublish(t, pub, "again.topic", nxwamp.List{"x"}, nil)
	recv(t, newEvents, "the event for the new subscription")
	noRecv(t, oldEvents, "an event for the removed subscription")
	noRecv(t, oldCalls, "a call of the removed registration's handler")
}

// When Register gets the subscription state before the queued removal of a
// registration of the same procedure, it finishes that removal itself
// instead of refusing the procedure as registered already.
func TestRegisterFinishesPendingRemovalOfSameProcedure(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 10*time.Second)
	old, err := c.Register(ctx, "pending.proc", func(context.Context, *Invocation) (any, error) { return "old", nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	// What Unregister does at once; its removal then waits for the state.
	old.removed.Store(true)

	reg, err := c.Register(ctx, "pending.proc", func(context.Context, *Invocation) (any, error) { return "new", nil }, nil)
	if err != nil {
		t.Fatalf("Register while the removal is pending = %v", err)
	}
	if n := tr.authz.count("UNREGISTER"); n != 1 {
		t.Fatalf("the router saw %d UNREGISTER, want the old registration's", n)
	}
	pub := tr.local(t)
	if res, err := localCall(t, pub, "pending.proc"); err != nil || res.Arguments[0] != "new" {
		t.Fatalf("call = %v, %v", res, err)
	}
	// The queued removal finds nothing left to do.
	if err := c.untrack(ctx, "unregister procedure 'pending.proc'", func() error { return c.untrackRegistration(old) }); err != nil {
		t.Fatalf("the pending removal = %v", err)
	}
	if _, regs := trackedCounts(t, c); regs != 1 || !reg.Active() {
		t.Fatalf("tracked %d procedures; new registration active %v", regs, reg.Active())
	}
	if res, err := localCall(t, pub, "pending.proc"); err != nil || res.Arguments[0] != "new" {
		t.Fatalf("call after the pending removal = %v, %v", res, err)
	}
}
