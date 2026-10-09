package wamp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/transport"
	nxwamp "github.com/gammazero/nexus/v3/wamp"
)

// progressCallee registers proc on a trusted local client. Its handler sends
// n progressive results — [i] with {"chunk": i} — and then the final result
// "done". sent, if set, receives a value once all n are sent; the final
// result waits until finish, if set, is closed.
func progressCallee(t *testing.T, tr *testRouter, proc string, n int, sent chan<- struct{}, finish <-chan struct{}) {
	t.Helper()
	callee := tr.local(t)
	if err := callee.Register(proc, func(ctx context.Context, _ *nxwamp.Invocation) client.InvokeResult {
		for i := range n {
			if err := callee.SendProgress(ctx, nxwamp.List{i}, nxwamp.Dict{"chunk": i}); err != nil {
				return client.InvokeResult{Err: "test.error.progress", Args: nxwamp.List{err.Error()}}
			}
		}
		if sent != nil {
			sent <- struct{}{}
		}
		if finish != nil {
			select {
			case <-finish:
			case <-ctx.Done():
				return client.InvokeResult{Err: nxwamp.ErrCanceled}
			}
		}
		return client.InvokeResult{Args: nxwamp.List{"done"}}
	}, nil); err != nil {
		t.Fatal(err)
	}
}

// closer returns a channel and a function that closes it once; the test
// closes it at cleanup at the latest.
func closer(t *testing.T) (chan struct{}, func()) {
	ch := make(chan struct{})
	var once sync.Once
	closeIt := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(closeIt)
	return ch, closeIt
}

// Progressive results reach OnProgress in order, each one before Call returns
// the final result (finding 1: fleetdb streams a large read this way).
func TestCallPassesProgressiveResultsInOrder(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	const n = 200
	progressCallee(t, tr, "progress.proc", n, nil, nil)

	// Written without a lock: under -race, OnProgress calls that are not
	// ordered one after the other, and all of them before Call returns, are
	// reported.
	var got []int64
	var bad []string
	res, err := c.Call(ctxTimeout(t, 10*time.Second), "progress.proc", nil, nil, &CallOptions{OnProgress: func(r *Result) {
		v, _ := r.Value().(int64)
		got = append(got, v)
		if r.Kwargs["chunk"] != v || r.Details["progress"] != true {
			bad = append(bad, fmt.Sprintf("%+v", r))
		}
	}}, 0)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res.Value() != "done" || res.Details["progress"] != nil {
		t.Fatalf("final result = %+v", res)
	}
	if len(got) != n {
		t.Fatalf("OnProgress called %d times, want %d", len(got), n)
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("progressive result %d is %d: not in order", i, v)
		}
	}
	if len(bad) > 0 {
		t.Fatalf("malformed progressive results: %v", bad)
	}
}

// OnProgress never runs on the connection's receive loop: while it blocks,
// events are delivered, and the forwarder watchdog — which closes a
// connection whose client takes no message for the response timeout — has
// nothing to report, although the callee keeps sending.
func TestSlowOnProgressDoesNotHoldUpTheConnection(t *testing.T) {
	tr := newTestRouter(t, true)
	disconnects := make(chan string, 4)
	const stallAfter = 500 * time.Millisecond
	c, logs := startTestConn(t, tr, func(cfg *Config, c *Connection) {
		cfg.OnDisconnect = func(reason string) { disconnects <- reason }
		c.t.responseTimeout = stallAfter
	})
	handler, events := eventCollector(4)
	if _, err := c.Subscribe(ctxTimeout(t, 5*time.Second), "while.progress", handler, nil); err != nil {
		t.Fatal(err)
	}
	const n = 10
	sent := make(chan struct{}, 1)
	progressCallee(t, tr, "slow.progress", n, sent, nil)

	entered := make(chan struct{})
	release, releaseNow := closer(t)
	var got []int64
	called := make(chan error, 1)
	go func() {
		_, err := c.Call(ctxTimeout(t, 30*time.Second), "slow.progress", nil, nil, &CallOptions{OnProgress: func(r *Result) {
			if len(got) == 0 {
				close(entered)
				<-release
			}
			v, _ := r.Value().(int64)
			got = append(got, v)
		}}, 0)
		called <- err
	}()
	recv(t, entered, "the first OnProgress call")
	recv(t, sent, "the callee to send every progressive result")
	localPublish(t, tr.local(t), "while.progress", nxwamp.List{"x"}, nil)
	recv(t, events, "an event while OnProgress blocks")
	select {
	case reason := <-disconnects:
		t.Fatalf("the connection was closed (%s) while OnProgress blocked\n%s", reason, logs)
	case <-time.After(2 * stallAfter):
	}

	releaseNow()
	if err := recv(t, called, "Call"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if len(got) != n {
		t.Fatalf("OnProgress called %d times, want %d", len(got), n)
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("progressive result %d is %d: not in order", i, v)
		}
	}
}

// Once Call has failed — here its ctx ended — the progressive results not yet
// passed on are dropped: Call waits for the OnProgress call under way, and
// OnProgress is not called again.
func TestOnProgressEndsWithTheCall(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	sent := make(chan struct{}, 1)
	progressCallee(t, tr, "endless.progress", 20, sent, make(chan struct{})) // finishes only when cancelled

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	release, releaseNow := closer(t)
	var calls atomic.Int32
	var returned atomic.Bool
	late := make(chan struct{}, 1)
	called := make(chan error, 1)
	go func() {
		_, err := c.Call(ctx, "endless.progress", nil, nil, &CallOptions{OnProgress: func(*Result) {
			if returned.Load() {
				select {
				case late <- struct{}{}:
				default:
				}
			}
			if calls.Add(1) == 1 {
				close(entered)
				<-release
			}
		}}, 0)
		returned.Store(true)
		called <- err
	}()
	recv(t, entered, "the first OnProgress call")
	recv(t, sent, "the callee to send its progressive results")
	cancel()
	noRecv(t, called, "Call's return while OnProgress runs")
	releaseNow()
	if err := recv(t, called, "Call"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Call = %v, want context.Canceled", err)
	}
	noRecv(t, late, "an OnProgress call after Call returned")
	if n := calls.Load(); n != 1 {
		t.Fatalf("OnProgress called %d times, want only the call under way", n)
	}
}

// rawCallee joins testRealm as a callee the test plays message by message,
// and registers proc: the test reads the INVOCATIONs from the returned peer
// and writes the YIELDs.
func rawCallee(t *testing.T, tr *testRouter, proc string) nxwamp.Peer {
	t.Helper()
	calleeSide, routerSide := transport.LinkedPeers()
	go func() { _ = tr.r.Attach(routerSide) }()
	t.Cleanup(calleeSide.Close) // before the router closes
	calleeSide.Send() <- &nxwamp.Hello{Realm: testRealm, Details: nxwamp.Dict{
		"roles": nxwamp.Dict{"callee": nxwamp.Dict{"features": nxwamp.Dict{"progressive_call_results": true}}},
	}}
	if msg := recv(t, calleeSide.Recv(), "WELCOME"); msg.MessageType() != nxwamp.WELCOME {
		t.Fatalf("the router answered HELLO with %v", msg)
	}
	calleeSide.Send() <- &nxwamp.Register{Request: 1, Procedure: nxwamp.URI(proc), Options: nxwamp.Dict{}}
	if msg := recv(t, calleeSide.Recv(), "REGISTERED"); msg.MessageType() != nxwamp.REGISTERED {
		t.Fatalf("the router answered REGISTER with %v", msg)
	}
	return calleeSide
}

// A progressive result is never mistaken for the call's result: a callee
// that sends one although the call did not ask for it fails the call
// (finding 1).
func TestUnaskedProgressiveResultIsAnError(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	callee := rawCallee(t, tr, "unasked.progress")
	called := make(chan error, 1)
	go func() {
		res, err := c.Call(ctxTimeout(t, 5*time.Second), "unasked.progress", nil, nil, nil, 0)
		if err == nil {
			err = fmt.Errorf("no error, result %+v", res)
		}
		called <- err
	}()
	inv, ok := recv(t, callee.Recv(), "the INVOCATION").(*nxwamp.Invocation)
	if !ok {
		t.Fatal("no INVOCATION")
	}
	if inv.Details[nxwamp.OptReceiveProgress] == true {
		t.Fatal("the call asked for progressive results")
	}
	callee.Send() <- &nxwamp.Yield{Request: inv.Request, Options: nxwamp.Dict{nxwamp.OptProgress: true},
		Arguments: nxwamp.List{"chunk"}}
	if err := recv(t, called, "Call"); !strings.Contains(err.Error(), "OnProgress") {
		t.Fatalf("Call = %v, want an error naming OnProgress", err)
	}
	// The final result comes once the call is over: a nexus client whose
	// receive loop hands a result to a call that is just returning wedges
	// (review finding 7), which is why receive_progress without OnProgress
	// is refused (see the next test).
	callee.Send() <- &nxwamp.Yield{Request: inv.Request, Options: nxwamp.Dict{}, Arguments: nxwamp.List{"done"}}
	if err := c.Publish(ctxTimeout(t, 5*time.Second), "after.call", nil, nil, &PublishOptions{Acknowledge: true}, 0); err != nil {
		t.Fatalf("Publish after the call: %v", err)
	}
}

// receive_progress in Extra without OnProgress would let the callee's
// progressive results in with nothing to take them: nexus would answer the
// call with the first one, and the rest race the end of the call's wait.
// The call is refused before it is sent.
func TestReceiveProgressWithoutOnProgressIsRefused(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	var invoked atomic.Int32
	callee := tr.local(t)
	if err := callee.Register("asked.progress", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
		invoked.Add(1)
		return client.InvokeResult{Args: nxwamp.List{"done"}}
	}, nil); err != nil {
		t.Fatal(err)
	}
	ctx := ctxTimeout(t, 5*time.Second)
	_, err := c.Call(ctx, "asked.progress", nil, nil, &CallOptions{Extra: map[string]any{"receive_progress": true}}, 0)
	if err == nil || !strings.Contains(err.Error(), "OnProgress") {
		t.Fatalf("Call = %v, want an error naming OnProgress", err)
	}
	if n := invoked.Load(); n != 0 {
		t.Fatalf("the refused call reached the callee %d times", n)
	}
	// receive_progress false, or with OnProgress, is fine.
	for _, opts := range []*CallOptions{
		{Extra: map[string]any{"receive_progress": false}},
		{Extra: map[string]any{"receive_progress": true}, OnProgress: func(*Result) {}},
	} {
		if res, err := c.Call(ctx, "asked.progress", nil, nil, opts, 0); err != nil || res.Value() != "done" {
			t.Fatalf("Call with %+v = %+v, %v", opts.Extra, res, err)
		}
	}
}

// When the callee's RESULT crosses the caller's CANCEL, the router has
// nothing left to cancel and answers nothing: Call must still return at its
// ctx's end, with ctx's error — not wait out the response timeout for an
// answer that never comes, nor turn into "timeout waiting for reply"
// (finding 33).
func TestCallReturnsAtCtxEndWhenResultCrossesCancel(t *testing.T) {
	tests := []struct {
		name     string
		ctx      func() (context.Context, context.CancelFunc)
		cancelIt bool // cancel ctx once the callee runs; else it expires
		wantErr  error
	}{
		{"cancelled", func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			true, context.Canceled},
		{"deadline", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 100*time.Millisecond)
		}, false, context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTestRouter(t, true)
			const responseTimeout = 5 * time.Second
			c, _ := startTestConn(t, tr, func(_ *Config, c *Connection) { c.t.responseTimeout = responseTimeout })
			callee := tr.local(t)
			invoked := make(chan struct{}, 1)
			answer, answerNow := closer(t)
			if err := callee.Register("crossing.proc", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
				invoked <- struct{}{}
				<-answer // answers although the call is being cancelled
				return client.InvokeResult{Args: nxwamp.List{"late"}}
			}, nil); err != nil {
				t.Fatal(err)
			}
			// The router takes the CANCEL only once the call is over.
			releaseCancel := tr.authz.hold(t, "CANCEL")

			ctx, cancel := tt.ctx()
			defer cancel()
			type outcome struct {
				err error
				at  time.Time
			}
			called := make(chan outcome, 1)
			go func() {
				_, err := c.Call(ctx, "crossing.proc", nil, nil, nil, 0)
				called <- outcome{err, time.Now()}
			}()
			recv(t, invoked, "the invocation")
			if tt.cancelIt {
				cancel()
			}
			<-ctx.Done()
			ended := time.Now()
			if key := recv(t, tr.authz.held, "the router to hold the CANCEL"); key != "CANCEL" {
				t.Fatalf("the router holds %s", key)
			}
			answerNow() // the RESULT crosses the CANCEL

			got := recv(t, called, "Call")
			if !errors.Is(got.err, tt.wantErr) {
				t.Fatalf("Call = %v, want %v", got.err, tt.wantErr)
			}
			if el := got.at.Sub(ended); el > time.Second {
				t.Fatalf("Call returned %v after its ctx ended", el)
			}
			releaseCancel()
			if !c.IsOpen() {
				t.Fatal("connection lost")
			}
		})
	}
}

// Once its ctx has ended, a call returns ctx's error whatever came in — a
// result that arrived just then included, as Go's convention has it.
// Otherwise a cancellation it did not ask for is the session's end.
func TestCallOutcome(t *testing.T) {
	live := context.Background()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	result := &nxwamp.Result{Arguments: nxwamp.List{"ok"}}
	refusal := client.RPCError{Err: &nxwamp.Error{Error: nxwamp.ErrNoSuchProcedure}}
	tests := []struct {
		name    string
		ctx     context.Context
		res     *nxwamp.Result
		err     error
		wantRes *nxwamp.Result
		wantErr error
	}{
		{"result", live, result, nil, result, nil},
		{"refusal", live, nil, refusal, nil, refusal},
		{"cancelled by the session's end", live, nil, context.Canceled, nil, client.ErrNotConn},
		{"result as ctx ends", cancelled, result, nil, nil, context.Canceled},
		{"reply timeout as ctx ends", expired, nil, client.ErrReplyTimeout, nil, context.DeadlineExceeded},
		{"ctx ended first", expired, nil, nil, nil, context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := callOutcome(tt.ctx, tt.res, tt.err)
			if res != tt.wantRes || !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Fatalf("callOutcome = %v, %v; want %v, %v", res, err, tt.wantRes, tt.wantErr)
			}
		})
	}
}
