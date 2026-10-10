package wamp

import (
	"net"
	"sync"
	"testing"
	"time"

	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

// fakePeer is a controllable inner peer.
type fakePeer struct {
	recv      chan nxwamp.Message
	send      chan nxwamp.Message
	done      chan struct{}
	closeOnce sync.Once
	recvOnce  sync.Once
}

func newFakePeer() *fakePeer {
	return &fakePeer{recv: make(chan nxwamp.Message), send: make(chan nxwamp.Message), done: make(chan struct{})}
}

func (f *fakePeer) Recv() <-chan nxwamp.Message { return f.recv }
func (f *fakePeer) Send() chan<- nxwamp.Message { return f.send }
func (f *fakePeer) Done() <-chan struct{}       { return f.done }
func (f *fakePeer) IsLocal() bool               { return false }
func (f *fakePeer) Close() {
	f.closeOnce.Do(func() { close(f.done) })
	f.endRecv()
}
func (f *fakePeer) endRecv() { f.recvOnce.Do(func() { close(f.recv) }) }

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestObservedPeerRecordsAbortAndEvictions(t *testing.T) {
	inner := newFakePeer()
	evicted := make(chan nxwamp.ID, 2)
	p := newObservedPeer(inner, newSignal(), peerOptions{onEvicted: func(id nxwamp.ID) { evicted <- id }})

	msgs := []nxwamp.Message{
		&nxwamp.Challenge{AuthMethod: "wampcra"},
		&nxwamp.Unregistered{Request: 7}, // the answer to our own UNREGISTER
		&nxwamp.Unregistered{Details: nxwamp.Dict{"registration": nxwamp.ID(42), "reason": "wamp.error.unregistered"}},
		&nxwamp.Abort{Reason: nxwamp.ErrNotAuthorized, Details: nxwamp.Dict{"message": "denied"}},
		&nxwamp.Abort{Reason: nxwamp.ErrNoSuchRealm},
		&nxwamp.Goodbye{Reason: nxwamp.CloseSystemShutdown},
		&nxwamp.Goodbye{Reason: nxwamp.CloseGoodbyeAndOut},
	}
	go func() {
		for _, m := range msgs {
			inner.recv <- m
		}
	}()
	for i, want := range msgs {
		if got := <-p.Recv(); got != want {
			t.Fatalf("message %d = %v, want %v", i, got, want)
		}
	}
	if reason, msg := p.abortReason(); reason != URINotAuthorized || msg != "denied" {
		t.Fatalf("abortReason = %q, %q (want the first ABORT)", reason, msg)
	}
	if reason := p.goodbyeReason(); reason != string(nxwamp.CloseSystemShutdown) {
		t.Fatalf("goodbyeReason = %q (want the first GOODBYE)", reason)
	}
	if got := recv(t, evicted, "eviction"); got != 42 {
		t.Fatalf("evicted %v", got)
	}
	if !p.isEvicted(42) || p.isEvicted(7) {
		t.Fatal("eviction bookkeeping")
	}
	noRecv(t, evicted, "eviction for a solicited UNREGISTERED")

	inner.endRecv() // connection gone
	if _, ok := <-p.Recv(); ok {
		t.Fatal("Recv not closed")
	}
	if !closed(p.Done()) {
		t.Fatal("Done not closed after the connection went away")
	}
}

// The deadlock this guards against: the client's receive loop blocks sending
// to a dead writer while the forwarder blocks handing it the next message.
// The watched connection's signal must release the sender regardless.
func TestObservedPeerDoneFiresWhileForwarderIsBlocked(t *testing.T) {
	inner := newFakePeer()
	dead := newSignal()
	p := newObservedPeer(inner, dead, peerOptions{})
	inner.recv <- &nxwamp.Event{} // nobody reads p.Recv(): the forwarder blocks

	sendResult := make(chan bool, 1)
	go func() { // a sender stuck on a writer nobody serves
		// The peer's sender takes the first message and then waits for the
		// writer itself; the second one is stuck.
		for range 2 {
			select {
			case p.Send() <- &nxwamp.Error{}:
			case <-p.Done():
				sendResult <- false
				return
			}
		}
		sendResult <- true
	}()
	noRecv(t, sendResult, "send outcome before the transport died")
	dead.fire() // watchedConn: read or write error
	if recv(t, sendResult, "send outcome") {
		t.Fatal("send completed on a dead peer")
	}

	// A dead connection still delivers what it had received.
	if _, ok := (<-p.Recv()).(*nxwamp.Event); !ok {
		t.Fatal("pending event dropped")
	}
	p.Close()
	if _, ok := <-p.Recv(); ok {
		t.Fatal("Recv not closed after Close")
	}
	if !closed(inner.Done()) {
		t.Fatal("inner peer not closed")
	}
}

// A client that leaves a received message untaken for stallAfter is wedged:
// the peer reports it and closes itself, which ends the session. One that
// takes its messages in time is left alone.
func TestObservedPeerClosesWhenClientStalls(t *testing.T) {
	t.Run("stalled", func(t *testing.T) {
		inner := newFakePeer()
		stalls := make(chan time.Duration, 1)
		p := newObservedPeer(inner, newSignal(), peerOptions{
			stallAfter: 50 * time.Millisecond,
			onStall:    func(d time.Duration) { stalls <- d },
		})
		inner.recv <- &nxwamp.Event{} // nobody takes it
		if got := recv(t, stalls, "the stall report"); got != 50*time.Millisecond {
			t.Fatalf("stalled for %v", got)
		}
		eventually(t, 5*time.Second, "the peer to close", func() bool { return closed(p.Done()) && closed(inner.Done()) })
		if _, ok := <-p.Recv(); ok {
			t.Fatal("Recv delivered after the stall")
		}
	})
	t.Run("taken in time", func(t *testing.T) {
		inner := newFakePeer()
		stalls := make(chan time.Duration, 1)
		p := newObservedPeer(inner, newSignal(), peerOptions{
			stallAfter: 200 * time.Millisecond,
			onStall:    func(d time.Duration) { stalls <- d },
		})
		defer p.Close()
		for range 3 {
			go func() { inner.recv <- &nxwamp.Event{} }()
			time.Sleep(20 * time.Millisecond) // a busy, not a wedged, receive loop
			<-p.Recv()
		}
		noRecv(t, stalls, "a stall report")
		if closed(p.Done()) {
			t.Fatal("closed a peer whose client takes its messages")
		}
	})
}

// The receive loop may wait legitimately while the uplink is backed up: nexus
// sends from it (a CANCEL that its waiting call must hand over before it can
// take its RESULT, an ERROR for an INVOCATION), and each send waits for the
// WebSocket writer. So the watchdog does not count the time the peer's
// sender is blocked handing a message to the writer (finding 32). Once the
// writer takes messages again, a client that takes none is wedged as ever.
func TestStallWatchdogDoesNotCountUplinkBackpressure(t *testing.T) {
	inner := newFakePeer() // nobody receives what it is sent: the writer is busy
	const stallAfter = 100 * time.Millisecond
	stalls := make(chan time.Duration, 1)
	p := newObservedPeer(inner, newSignal(), peerOptions{
		stallAfter: stallAfter,
		onStall:    func(d time.Duration) { stalls <- d },
	})
	defer p.Close()
	p.Send() <- &nxwamp.Publish{Request: 1} // the sender takes it and waits for the writer
	go func() { inner.recv <- &nxwamp.Event{} }()

	select {
	case <-stalls:
		t.Fatal("stall reported while the uplink was backed up")
	case <-time.After(3 * stallAfter):
	}
	if closed(p.Done()) {
		t.Fatal("closed while the uplink was backed up")
	}

	<-inner.send // the writer takes the message: the uplink moves again
	unblocked := time.Now()
	recv(t, stalls, "the stall report once the writer is idle")
	if el := time.Since(unblocked); el < stallAfter/2 {
		t.Fatalf("stall reported %v after the uplink moved again, want about %v", el, stallAfter)
	}
	eventually(t, 5*time.Second, "the peer to close", func() bool { return closed(p.Done()) })
}

// With a real nexus client: its receive loop waits to send an ERROR behind a
// backed-up uplink, so the next message waits in the forwarder — longer
// than the watchdog's bound. Once the uplink moves, all of it is delivered
// and the connection was never closed.
func TestStallWatchdogSparesReceiveLoopWaitingOnUplink(t *testing.T) {
	const stallAfter = 100 * time.Millisecond
	stalls := make(chan time.Duration, 1)
	s, routerSide, _ := scriptedSessionWith(t, peerOptions{
		stallAfter: stallAfter,
		onStall:    func(d time.Duration) { stalls <- d },
		holdLimit:  5 * time.Second,
	})
	events := make(chan *nxwamp.Event, 1)
	subscribed := make(chan error, 1)
	go func() { subscribed <- s.subscribe("t", func(ev *nxwamp.Event) { events <- ev }, nil) }()
	sub := expectSent[*nxwamp.Subscribe](t, routerSide)
	routerSide.Send() <- &nxwamp.Subscribed{Request: sub.Request, Subscription: 70}
	if err := recv(t, subscribed, "Subscribe"); err != nil {
		t.Fatal(err)
	}

	// The router stops reading. The transport takes one publication, the
	// sender a second one and waits for the writer: the uplink is backed up.
	for range 2 {
		if err := s.cli.Publish("up", nil, nxwamp.List{"payload"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// The receive loop answers an INVOCATION of a registration it does not
	// know with an ERROR, which waits behind the publications; the EVENT
	// behind it waits for the receive loop.
	routerSide.Send() <- &nxwamp.Invocation{Request: 9, Registration: 999, Details: nxwamp.Dict{}}
	routerSide.Send() <- &nxwamp.Event{Subscription: 70, Publication: 1, Details: nxwamp.Dict{}}
	select {
	case <-stalls:
		t.Fatal("stall reported while the receive loop waited on the backed-up uplink")
	case <-events:
		t.Fatal("the scenario did not hold up the receive loop")
	case <-time.After(3 * stallAfter):
	}

	// The uplink moves again.
	expectSent[*nxwamp.Publish](t, routerSide)
	expectSent[*nxwamp.Publish](t, routerSide)
	if e := expectSent[*nxwamp.Error](t, routerSide); e.Type != nxwamp.INVOCATION || e.Request != 9 {
		t.Fatalf("the client answered %v", e)
	}
	recv(t, events, "the event once the uplink moved")
	noRecv(t, stalls, "a stall report")
	if !s.alive() {
		t.Fatal("the session is gone")
	}
}

func TestObservedPeerCloseFiresDone(t *testing.T) {
	inner := newFakePeer()
	p := newObservedPeer(inner, newSignal(), peerOptions{})
	p.Close()
	p.Close() // idempotent
	if !closed(p.Done()) {
		t.Fatal("Done not closed by Close")
	}
	if _, ok := <-p.Recv(); ok {
		t.Fatal("Recv not closed after Close")
	}
}

func TestWatchedConnFiresOnFailure(t *testing.T) {
	t.Run("read error", func(t *testing.T) {
		a, b := net.Pipe()
		dead := newSignal()
		c := &watchedConn{Conn: a, dead: dead}
		go func() { _, _ = b.Write([]byte("x")); _ = b.Close() }()
		buf := make([]byte, 1)
		if _, err := c.Read(buf); err != nil {
			t.Fatal(err)
		}
		if closed(dead.done()) {
			t.Fatal("fired on a successful read")
		}
		if _, err := c.Read(buf); err == nil {
			t.Fatal("read after remote close succeeded")
		}
		if !closed(dead.done()) {
			t.Fatal("not fired on a read error")
		}
		_ = c.Close()
	})
	t.Run("write error", func(t *testing.T) {
		a, b := net.Pipe()
		dead := newSignal()
		c := &watchedConn{Conn: a, dead: dead}
		_ = b.Close()
		if _, err := c.Write([]byte("x")); err == nil {
			t.Fatal("write to a closed pipe succeeded")
		}
		if !closed(dead.done()) {
			t.Fatal("not fired on a write error")
		}
	})
	t.Run("close", func(t *testing.T) {
		a, b := net.Pipe()
		defer func() { _ = b.Close() }()
		dead := newSignal()
		c := &watchedConn{Conn: a, dead: dead}
		_ = c.Close()
		if !closed(dead.done()) {
			t.Fatal("not fired on Close")
		}
	})
}
