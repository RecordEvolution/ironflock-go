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
	nxwamp "github.com/gammazero/nexus/v3/wamp"
)

// The message the router sends right behind its answer to a SUBSCRIBE or
// REGISTER must reach the handler being installed. nexus installs it only
// once Subscribe or Register has returned, while its receive loop goes
// straight on to the next message (findings 0, 5, 6).

// callOutcomes counts what polling callers got.
type callOutcomes struct {
	mu     sync.Mutex
	counts map[string]int
	first  map[string]error
}

func (o *callOutcomes) add(err error) (served bool) {
	key := "ok"
	if err != nil {
		key = err.Error()
		var rpcErr client.RPCError
		if errors.As(err, &rpcErr) {
			key = string(rpcErr.Err.Error)
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.counts == nil {
		o.counts, o.first = map[string]int{}, map[string]error{}
	}
	o.counts[key]++
	if _, ok := o.first[key]; !ok {
		o.first[key] = err
	}
	return err == nil
}

func (o *callOutcomes) get(key string) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts[key], o.first[key]
}

func (o *callOutcomes) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return fmt.Sprint(o.counts)
}

// pollUntilServed calls proc from cli in a tight loop until a call succeeds
// or done is closed, as a caller does that waits for a procedure to appear.
func pollUntilServed(ctx context.Context, cli *client.Client, proc string, out *callOutcomes, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}
		_, err := cli.Call(ctx, proc, nil, nil, nil, nil)
		if out.add(err) || ctx.Err() != nil {
			return
		}
	}
}

const noHandlerForRegistration = "no handler for registration"

// Callers polling a procedure while the connection registers it are served
// as soon as it is registered, never refused because the INVOCATION came
// right behind REGISTERED.
func TestRegisterRaceWithPollingCallers(t *testing.T) {
	tr := newTestRouter(t, true)
	c, logs := startTestConn(t, tr)
	ctx := ctxTimeout(t, 60*time.Second)
	callers := make([]*client.Client, 4)
	for i := range callers {
		callers[i] = tr.local(t)
	}
	var out callOutcomes
	const rounds = 50
	for i := range rounds {
		proc := fmt.Sprintf("race.proc.%d", i)
		var wg sync.WaitGroup
		for _, cli := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				pollUntilServed(ctx, cli, proc, &out, nil)
			}()
		}
		if _, err := c.Register(ctx, proc, func(context.Context, *Invocation) (any, error) { return "ok", nil }, nil); err != nil {
			t.Fatalf("round %d: Register: %v", i, err)
		}
		wg.Wait()
	}
	if n, first := out.get(string(nxwamp.ErrInvalidArgument)); n > 0 {
		t.Fatalf("%d calls refused right after REGISTERED (outcomes %v); first: %v", n, &out, first)
	}
	if n, _ := out.get("ok"); n != rounds*len(callers) {
		t.Fatalf("served %d calls, want %d (outcomes %v)", n, rounds*len(callers), &out)
	}
	if n := logs.count(noHandlerForRegistration); n > 0 {
		t.Fatalf("the client refused %d invocations for want of a handler", n)
	}
}

// The same while the restore after a reconnect registers the procedures
// again: callers keep calling them across the outage.
func TestRegisterRaceDuringRestore(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, logs := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 60*time.Second)
	const procs, reconnects = 10, 5
	for i := range procs {
		proc := fmt.Sprintf("restore.race.%d", i)
		if _, err := c.Register(ctx, proc, func(context.Context, *Invocation) (any, error) { return "ok", nil }, nil); err != nil {
			t.Fatal(err)
		}
	}

	var out callOutcomes
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 2 * procs {
		cli := tr.local(t)
		proc := fmt.Sprintf("restore.race.%d", i%procs)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				out.add(func() error { _, err := cli.Call(ctx, proc, nil, nil, nil, nil); return err }())
			}
		}()
	}
	for range reconnects {
		tr.ln.dropAll()
		recv(t, connects, "OnConnect after the reconnect")
		served, _ := out.get("ok")
		eventually(t, 5*time.Second, "calls served after the restore", func() bool {
			n, _ := out.get("ok")
			return n > served+2*procs
		})
	}
	close(stop)
	wg.Wait()
	if n, first := out.get(string(nxwamp.ErrInvalidArgument)); n > 0 {
		t.Fatalf("%d calls refused right after a re-registration (outcomes %v); first: %v", n, &out, first)
	}
	if n, _ := out.get(string(nxwamp.ErrNoSuchProcedure)); n == 0 {
		t.Fatalf("the callers never called while the procedures were missing (outcomes %v)", &out)
	}
	if n := logs.count(noHandlerForRegistration); n > 0 {
		t.Fatalf("the client refused %d invocations for want of a handler", n)
	}
	t.Logf("outcomes: %v", &out)
}

// firstEvents makes the router send every SUBSCRIBED with an event of that
// subscription right behind it — as a router does for a retained event, or
// for the next event of a busy topic.
func firstEvents(tr *testRouter) {
	var publication atomic.Int64
	tr.injectAfter(func(msg nxwamp.Message) []nxwamp.Message {
		subscribed, ok := msg.(*nxwamp.Subscribed)
		if !ok {
			return nil
		}
		return []nxwamp.Message{&nxwamp.Event{
			Subscription: subscribed.Subscription,
			Publication:  nxwamp.ID(publication.Add(1)),
			Details:      nxwamp.Dict{},
			Arguments:    nxwamp.List{"first"},
		}}
	})
}

// An event right behind SUBSCRIBED reaches the new subscription's handler,
// on Subscribe and on the restore after a reconnect.
func TestFirstEventRightBehindSubscribedIsDelivered(t *testing.T) {
	tr := newTestRouter(t, true)
	firstEvents(tr)
	connects := make(chan struct{}, 8)
	c, logs := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 60*time.Second)

	const topics = 50
	events := make([]chan *Event, topics)
	for i := range topics {
		var handler EventHandler
		handler, events[i] = eventCollector(4)
		if _, err := c.Subscribe(ctx, fmt.Sprintf("first.event.%d", i), handler, nil); err != nil {
			t.Fatal(err)
		}
		if ev := recv(t, events[i], fmt.Sprintf("the first event of subscription %d", i)); ev.Args[0] != "first" {
			t.Fatalf("event = %+v", ev)
		}
	}

	tr.ln.dropAll()
	recv(t, connects, "OnConnect after the reconnect")
	for i := range topics {
		if ev := recv(t, events[i], fmt.Sprintf("the first event of restored subscription %d", i)); ev.Args[0] != "first" {
			t.Fatalf("event = %+v", ev)
		}
	}
	if n := logs.count("No handler registered for subscription"); n > 0 {
		t.Fatalf("the client dropped %d events for want of a handler", n)
	}
}

// A call that overtook the callee's registration is refused by the callee's
// nexus client with wamp.error.invalid_argument "client has no handler for
// registration N". It never reached a handler, so the retry window retries it
// like a call that found no callee at all.
func TestCallRetriesRefusalOfUninstalledRegistration(t *testing.T) {
	tr := newTestRouter(t, true)
	c, logs := startTestConn(t, tr)
	callee := tr.local(t)
	var calls atomic.Int32
	refuse := func(n int32, text string) {
		t.Helper()
		calls.Store(0)
		proc := fmt.Sprintf("racy.proc.%d", n)
		if err := callee.Register(proc, func(context.Context, *nxwamp.Invocation) client.InvokeResult {
			if calls.Add(1) <= n {
				return client.InvokeResult{Err: nxwamp.ErrInvalidArgument, Args: nxwamp.List{text}}
			}
			return client.InvokeResult{Args: nxwamp.List{"served"}}
		}, nil); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("retried within the window", func(t *testing.T) {
		refuse(2, "client has no handler for registration 4711")
		res, err := c.Call(context.Background(), "racy.proc.2", nil, nil, nil, 3*time.Second)
		if err != nil || res.Value() != "served" {
			t.Fatalf("Call = %v, %v", res, err)
		}
		if n := calls.Load(); n != 3 {
			t.Fatalf("callee reached %d times, want 3", n)
		}
		if !strings.Contains(logs.String(), "retrying") {
			t.Fatal("retry not logged")
		}
	})
	t.Run("not retried without a window", func(t *testing.T) {
		refuse(3, "client has no handler for registration 4712")
		_, err := c.Call(context.Background(), "racy.proc.3", nil, nil, nil, 0)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != string(nxwamp.ErrInvalidArgument) {
			t.Fatalf("Call = %v, want *Error invalid_argument", err)
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("callee reached %d times, want 1", n)
		}
	})
	t.Run("other invalid arguments are not retried", func(t *testing.T) {
		refuse(4, "bad input")
		_, err := c.Call(context.Background(), "racy.proc.4", nil, nil, nil, 3*time.Second)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != string(nxwamp.ErrInvalidArgument) || werr.Args[0] != "bad input" {
			t.Fatalf("Call = %v, want *Error invalid_argument", err)
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("callee reached %d times, want 1", n)
		}
	})
}
