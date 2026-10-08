package wamp

import (
	"net"
	"sync"
	"testing"
	"time"

	nxwamp "github.com/gammazero/nexus/v3/wamp"
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
