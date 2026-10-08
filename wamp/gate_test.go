package wamp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/transport"
	nxwamp "github.com/gammazero/nexus/v3/wamp"
)

// newReadAheadPeer returns a fake inner peer whose connection has received
// messages ahead of the forwarder, as a WebSocket reader does: receive never
// blocks.
func newReadAheadPeer() *fakePeer {
	f := newFakePeer()
	f.recv = make(chan nxwamp.Message, 16)
	return f
}

// sendThrough sends msg through p, as the client does, and checks that it
// reaches the inner peer.
func sendThrough(t *testing.T, p *observedPeer, inner *fakePeer, msg nxwamp.Message) {
	t.Helper()
	go func() {
		select {
		case p.Send() <- msg:
		case <-p.Done():
		}
	}()
	if got := recv(t, inner.send, "the message at the inner peer"); got != msg {
		t.Fatalf("inner peer got %v, want %v", got, msg)
	}
}

// receive makes the connection of a read-ahead peer receive msgs.
func receive(inner *fakePeer, msgs ...nxwamp.Message) {
	for _, m := range msgs {
		inner.recv <- m
	}
}

func expectRecv[T nxwamp.Message](t *testing.T, p *observedPeer, what string) T {
	t.Helper()
	msg := recv(t, p.Recv(), what)
	m, ok := msg.(T)
	if !ok {
		t.Fatalf("%s: got %v", what, msg)
	}
	return m
}

// While a SUBSCRIBE or REGISTER is in flight, the message right behind the
// answer to it waits until the call that sent it has returned: nexus
// installs the handler only then.
func TestReplyGateHoldsTheMessageBehindTheAnswer(t *testing.T) {
	answers := []struct {
		name    string
		request nxwamp.Message
		answer  nxwamp.Message
	}{
		{"SUBSCRIBED", &nxwamp.Subscribe{Request: 7, Topic: "t"}, &nxwamp.Subscribed{Request: 7, Subscription: 70}},
		{"REGISTERED", &nxwamp.Register{Request: 7, Procedure: "p"}, &nxwamp.Registered{Request: 7, Registration: 70}},
		{"ERROR for SUBSCRIBE", &nxwamp.Subscribe{Request: 7, Topic: "t"},
			&nxwamp.Error{Type: nxwamp.SUBSCRIBE, Request: 7, Error: nxwamp.ErrNotAuthorized}},
		{"ERROR for REGISTER", &nxwamp.Register{Request: 7, Procedure: "p"},
			&nxwamp.Error{Type: nxwamp.REGISTER, Request: 7, Error: nxwamp.ErrNotAuthorized}},
	}
	for _, tt := range answers {
		t.Run(tt.name, func(t *testing.T) {
			inner := newReadAheadPeer()
			p := newObservedPeer(inner, newSignal(), peerOptions{holdLimit: time.Minute})
			defer p.Close()
			disarm := p.gate.arm()
			sendThrough(t, p, inner, tt.request)
			receive(inner, tt.answer, &nxwamp.Event{Subscription: 70})
			if got := recv(t, p.Recv(), "the answer"); got != tt.answer {
				t.Fatalf("got %v, want the answer", got)
			}
			noRecv(t, p.Recv(), "the next message before the call returned")
			disarm()
			expectRecv[*nxwamp.Event](t, p, "the next message")
		})
	}
}

// Only the answer to the call in flight holds the next message: not a late
// answer to an earlier request whose call gave up waiting, not other
// answers, and nothing while no call is in flight.
func TestReplyGateHoldsOnlyForTheCallInFlight(t *testing.T) {
	inner := newReadAheadPeer()
	p := newObservedPeer(inner, newSignal(), peerOptions{holdLimit: time.Minute})
	defer p.Close()

	receive(inner, &nxwamp.Subscribed{Request: 3, Subscription: 30}, &nxwamp.Event{Subscription: 30})
	expectRecv[*nxwamp.Subscribed](t, p, "an answer while nothing is in flight")
	expectRecv[*nxwamp.Event](t, p, "the message behind it")

	disarm := p.gate.arm()
	sendThrough(t, p, inner, &nxwamp.Call{Request: 8, Procedure: "c"})
	sendThrough(t, p, inner, &nxwamp.Subscribe{Request: 9, Topic: "t"})
	receive(inner,
		&nxwamp.Subscribed{Request: 5, Subscription: 50}, // late
		&nxwamp.Error{Type: nxwamp.CALL, Request: 8, Error: nxwamp.ErrNoSuchProcedure},
		&nxwamp.Event{Subscription: 50},
		&nxwamp.Subscribed{Request: 9, Subscription: 90},
		&nxwamp.Event{Subscription: 90},
	)
	expectRecv[*nxwamp.Subscribed](t, p, "the late answer")
	expectRecv[*nxwamp.Error](t, p, "the CALL's error")
	expectRecv[*nxwamp.Event](t, p, "the message behind the late answer and the CALL's error")
	expectRecv[*nxwamp.Subscribed](t, p, "the answer to the call in flight")
	noRecv(t, p.Recv(), "the next message before the call returned")
	disarm()
	expectRecv[*nxwamp.Event](t, p, "the next message")

	// A disarm that comes late, after the gate was armed again, leaves the
	// new call armed.
	first := p.gate.arm()
	second := p.gate.arm()
	first()
	sendThrough(t, p, inner, &nxwamp.Register{Request: 10, Procedure: "p"})
	receive(inner, &nxwamp.Registered{Request: 10, Registration: 100}, &nxwamp.Event{})
	expectRecv[*nxwamp.Registered](t, p, "the answer")
	noRecv(t, p.Recv(), "the next message before the second call returned")
	second()
	expectRecv[*nxwamp.Event](t, p, "the next message")
}

// The hold ends at the latest after holdLimit, and at once when the peer
// closes.
func TestReplyGateHoldIsBounded(t *testing.T) {
	t.Run("hold limit", func(t *testing.T) {
		inner := newReadAheadPeer()
		const limit = 50 * time.Millisecond
		p := newObservedPeer(inner, newSignal(), peerOptions{holdLimit: limit})
		defer p.Close()
		defer p.gate.arm()() // never returns before the end of the test
		sendThrough(t, p, inner, &nxwamp.Subscribe{Request: 1, Topic: "t"})
		receive(inner, &nxwamp.Subscribed{Request: 1, Subscription: 10}, &nxwamp.Event{})
		expectRecv[*nxwamp.Subscribed](t, p, "the answer")
		begin := time.Now()
		expectRecv[*nxwamp.Event](t, p, "the next message after the hold limit")
		if el := time.Since(begin); el < limit/2 {
			t.Fatalf("the next message came after %v, before the hold limit", el)
		}
	})
	t.Run("close", func(t *testing.T) {
		inner := newReadAheadPeer()
		p := newObservedPeer(inner, newSignal(), peerOptions{holdLimit: time.Minute})
		defer p.gate.arm()()
		sendThrough(t, p, inner, &nxwamp.Subscribe{Request: 1, Topic: "t"})
		receive(inner, &nxwamp.Subscribed{Request: 1, Subscription: 10})
		expectRecv[*nxwamp.Subscribed](t, p, "the answer")
		p.Close() // the forwarder exits, holding or not
		eventually(t, 5*time.Second, "Recv to close", func() bool {
			select {
			case _, ok := <-p.Recv():
				return !ok
			default:
				return false
			}
		})
	})
	t.Run("connection gone", func(t *testing.T) {
		inner := newReadAheadPeer()
		dead := newSignal()
		p := newObservedPeer(inner, dead, peerOptions{holdLimit: time.Minute})
		defer p.Close()
		defer p.gate.arm()()
		sendThrough(t, p, inner, &nxwamp.Subscribe{Request: 1, Topic: "t"})
		receive(inner, &nxwamp.Subscribed{Request: 1, Subscription: 10}, &nxwamp.Event{})
		expectRecv[*nxwamp.Subscribed](t, p, "the answer")
		dead.fire()
		expectRecv[*nxwamp.Event](t, p, "the next message once the connection is gone")
	})
}

// lineLog is a nexus client logger that hands every line to a channel.
type lineLog struct{ lines chan string }

func newLineLog() *lineLog { return &lineLog{lines: make(chan string, 1024)} }

func (l *lineLog) put(s string) {
	select {
	case l.lines <- strings.TrimSpace(s):
	default:
	}
}

func (l *lineLog) Print(v ...any)                 { l.put(fmt.Sprint(v...)) }
func (l *lineLog) Println(v ...any)               { l.put(fmt.Sprintln(v...)) }
func (l *lineLog) Printf(format string, v ...any) { l.put(fmt.Sprintf(format, v...)) }

// scriptedSession joins a nexus client, through an observedPeer as
// dialAndJoin does, to a router the test plays message by message: it can
// write an answer and the message behind it back to back, which a real
// router does only now and then.
func scriptedSession(t *testing.T) (*session, nxwamp.Peer, *lineLog) {
	t.Helper()
	clientSide, routerSide := transport.LinkedPeers()
	p := newObservedPeer(clientSide, newSignal(), peerOptions{holdLimit: 5 * time.Second})
	go func() {
		<-routerSide.Recv() // HELLO
		routerSide.Send() <- &nxwamp.Welcome{ID: 1, Details: nxwamp.Dict{
			"roles": nxwamp.Dict{"broker": nxwamp.Dict{}, "dealer": nxwamp.Dict{}},
		}}
	}()
	logs := newLineLog()
	cli, err := client.NewClient(p, client.Config{Realm: "scripted", Logger: logs, ResponseTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	t.Cleanup(func() {
		go func() { // answer the GOODBYE of Close, as a router does
			for msg := range routerSide.Recv() {
				if _, ok := msg.(*nxwamp.Goodbye); ok {
					routerSide.Send() <- &nxwamp.Goodbye{Reason: nxwamp.CloseGoodbyeAndOut, Details: nxwamp.Dict{}}
				}
			}
		}()
		_ = cli.Close()
		p.Close()
		routerSide.Close()
	})
	return newSession(cli, p), routerSide, logs
}

func expectSent[T nxwamp.Message](t *testing.T, routerSide nxwamp.Peer) T {
	t.Helper()
	msg := recv(t, routerSide.Recv(), "a message from the client")
	m, ok := msg.(T)
	if !ok {
		t.Fatalf("the client sent %v", msg)
	}
	return m
}

// An EVENT right behind SUBSCRIBED reaches the handler being subscribed.
func TestEventRightBehindSubscribedReachesTheHandler(t *testing.T) {
	s, routerSide, logs := scriptedSession(t)
	for i := range 200 {
		subID := nxwamp.ID(1000 + i)
		events := make(chan *nxwamp.Event, 1)
		subscribed := make(chan error, 1)
		go func() {
			subscribed <- s.subscribe(fmt.Sprintf("first.%d", i), func(ev *nxwamp.Event) { events <- ev }, nil)
		}()
		sub := expectSent[*nxwamp.Subscribe](t, routerSide)
		routerSide.Send() <- &nxwamp.Subscribed{Request: sub.Request, Subscription: subID}
		routerSide.Send() <- &nxwamp.Event{Subscription: subID, Publication: nxwamp.ID(i + 1), Details: nxwamp.Dict{}}
		if err := recv(t, subscribed, "Subscribe"); err != nil {
			t.Fatalf("round %d: Subscribe: %v", i, err)
		}
		select {
		case ev := <-events:
			if ev.Subscription != subID {
				t.Fatalf("round %d: event of subscription %v", i, ev.Subscription)
			}
		case line := <-logs.lines:
			t.Fatalf("round %d: the client dropped the event: %s", i, line)
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the event was neither delivered nor dropped", i)
		}
	}
}

// An INVOCATION right behind REGISTERED is answered by the handler being
// registered, not refused with "client has no handler for registration".
func TestInvocationRightBehindRegisteredReachesTheHandler(t *testing.T) {
	s, routerSide, _ := scriptedSession(t)
	for i := range 200 {
		regID := nxwamp.ID(1000 + i)
		registered := make(chan error, 1)
		go func() {
			registered <- s.register(fmt.Sprintf("first.%d", i), func(context.Context, *nxwamp.Invocation) client.InvokeResult {
				return client.InvokeResult{Args: nxwamp.List{"ok"}}
			}, nil)
		}()
		reg := expectSent[*nxwamp.Register](t, routerSide)
		routerSide.Send() <- &nxwamp.Registered{Request: reg.Request, Registration: regID}
		routerSide.Send() <- &nxwamp.Invocation{Request: nxwamp.ID(i + 1), Registration: regID, Details: nxwamp.Dict{}}
		if err := recv(t, registered, "Register"); err != nil {
			t.Fatalf("round %d: Register: %v", i, err)
		}
		switch msg := recv(t, routerSide.Recv(), "the answer to the invocation").(type) {
		case *nxwamp.Yield:
			if msg.Request != nxwamp.ID(i+1) {
				t.Fatalf("round %d: YIELD for request %v", i, msg.Request)
			}
		case *nxwamp.Error:
			t.Fatalf("round %d: the invocation was refused: %s %v", i, msg.Error, msg.Arguments)
		default:
			t.Fatalf("round %d: the client answered %v", i, msg)
		}
	}
}
