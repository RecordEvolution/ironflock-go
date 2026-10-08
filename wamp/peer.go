package wamp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/gammazero/nexus/v3/client"
	nxwamp "github.com/gammazero/nexus/v3/wamp"
)

// observedPeer wraps the WebSocket peer of one connection attempt to record
// the router's ABORT. nexus turns an ABORT that answers AUTHENTICATE — the
// refused credential — into a plain error without the reason URI, and the
// reason decides whether a failure is fatal (IsFatalAuthReason) or a missing
// realm (the slow-down streak).
//
// It also notes registrations the router revokes on its own — another
// session took the procedure over with force_reregister — which nexus
// ignores.
//
// Done closes when the peer is closed or its connection is gone, not only on
// Close: the nexus client abandons a send only when Done closes or its own
// receive loop has exited, so a receive loop that is itself sending (it
// answers an INVOCATION it cannot route, for one) to a writer that died with
// the connection would otherwise block forever — and with it the end of the
// session the supervisor waits for. "Gone" is signalled by the TCP
// connection itself (see watchedConn): the receive side cannot be relied on
// for it, as the forwarder below may be blocked handing a message to that
// very receive loop.
//
// A forwarder goroutine moves every received message to the channel Recv
// returns, noting ABORTs and revocations on the way. (Intercepting only the
// handshake and then handing out the WebSocket peer's own channel would race:
// the client looks up Recv() again for each handshake message and once more
// when its session starts.) The forwarder exits when the WebSocket peer's
// receive channel closes, which Close guarantees. Send and IsLocal are the
// wrapped peer's.
type observedPeer struct {
	nxwamp.Peer

	in        chan nxwamp.Message
	onEvicted func(registration nxwamp.ID) // called on the forwarder; must not block

	dead    *signal // Done: closed, or the connection is gone
	closing *signal // Close was called: nobody reads Recv any more

	mu      sync.Mutex
	abort   *nxwamp.Abort
	evicted map[nxwamp.ID]struct{}
}

// newObservedPeer wraps inner. dead is fired by the connection's
// watchedConn; the peer also fires it on Close and when inner's receive
// channel closes.
func newObservedPeer(inner nxwamp.Peer, dead *signal, onEvicted func(nxwamp.ID)) *observedPeer {
	p := &observedPeer{Peer: inner, in: make(chan nxwamp.Message), onEvicted: onEvicted, dead: dead, closing: newSignal()}
	go p.forward()
	return p
}

// Recv implements nxwamp.Peer.
func (p *observedPeer) Recv() <-chan nxwamp.Message { return p.in }

// Done implements nxwamp.Peer: it is closed by Close and when the connection
// is gone.
func (p *observedPeer) Done() <-chan struct{} { return p.dead.done() }

// Close implements nxwamp.Peer. It is idempotent.
func (p *observedPeer) Close() {
	p.closing.fire()
	p.dead.fire() // before closing the WebSocket, so blocked senders give up
	p.Peer.Close()
}

func (p *observedPeer) forward() {
	defer close(p.in)
	defer p.dead.fire()
	for msg := range p.Peer.Recv() {
		p.observe(msg)
		// Not Done: a dead connection still delivers what it received
		// before it died — typically the router's GOODBYE with the reason.
		select {
		case p.in <- msg:
		case <-p.closing.done():
			// Nobody reads any more. Keep draining so the WebSocket peer's
			// receiver can exit.
		}
	}
}

func (p *observedPeer) observe(msg nxwamp.Message) {
	switch m := msg.(type) {
	case *nxwamp.Abort:
		p.mu.Lock()
		if p.abort == nil {
			p.abort = m
		}
		p.mu.Unlock()
	case *nxwamp.Unregistered:
		// Request 0: not the answer to an UNREGISTER of ours, but the
		// router revoking the registration named in the details.
		if m.Request != 0 {
			return
		}
		id, ok := nxwamp.AsID(m.Details["registration"])
		if !ok {
			return
		}
		p.mu.Lock()
		if p.evicted == nil {
			p.evicted = map[nxwamp.ID]struct{}{}
		}
		p.evicted[id] = struct{}{}
		p.mu.Unlock()
		if p.onEvicted != nil {
			p.onEvicted(id)
		}
	}
}

// isEvicted reports whether the router revoked the registration id.
func (p *observedPeer) isEvicted(id nxwamp.ID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.evicted[id]
	return ok
}

// abortReason returns the reason URI and message of the router's ABORT, if
// it sent one.
func (p *observedPeer) abortReason() (reason, message string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.abort == nil {
		return "", ""
	}
	message, _ = nxwamp.AsString(p.abort.Details[nxwamp.OptMessage])
	return string(p.abort.Reason), message
}

// signal is a broadcast that fires once.
type signal struct {
	ch   chan struct{}
	once sync.Once
}

func newSignal() *signal { return &signal{ch: make(chan struct{})} }

func (s *signal) fire()                 { s.once.Do(func() { close(s.ch) }) }
func (s *signal) done() <-chan struct{} { return s.ch }

// watchedConn is the TCP connection under a session's WebSocket. Its first
// read or write error — the WebSocket treats either as permanent — or its
// Close fires dead, the moment the session's transport is gone.
type watchedConn struct {
	net.Conn
	dead *signal
}

func (c *watchedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err != nil {
		c.dead.fire()
	}
	return n, err
}

func (c *watchedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if err != nil {
		c.dead.fire()
	}
	return n, err
}

func (c *watchedConn) Close() error {
	c.dead.fire()
	return c.Conn.Close()
}

// reasonTransportLost is the close reason of a session whose transport went
// away without a WAMP GOODBYE or ABORT (autobahn's name for it).
const reasonTransportLost = "wamp.close.transport_lost"

// session is one joined WAMP session.
type session struct {
	cli  *client.Client
	peer *observedPeer
	// down is closed once the supervisor has withdrawn the session.
	down     chan struct{}
	downOnce sync.Once
}

func newSession(cli *client.Client, peer *observedPeer) *session {
	return &session{cli: cli, peer: peer, down: make(chan struct{})}
}

func (s *session) alive() bool { return s.cli.Connected() }

func (s *session) markDown() { s.downOnce.Do(func() { close(s.down) }) }

// closeReason is why the session ended; call it after cli.Done().
func (s *session) closeReason() string {
	if gb := s.cli.RouterGoodbye(); gb != nil && gb.Reason != "" {
		return string(gb.Reason)
	}
	if reason, _ := s.peer.abortReason(); reason != "" {
		return reason
	}
	return reasonTransportLost
}

// nexusLogger routes nexus' internal logging to the connection's logger at
// debug level.
type nexusLogger struct{ log *slog.Logger }

func (l nexusLogger) emit(msg func() string) {
	if l.log.Enabled(context.Background(), slog.LevelDebug) {
		l.log.Debug(strings.TrimSpace(msg()), "component", "nexus")
	}
}

func (l nexusLogger) Print(v ...any)   { l.emit(func() string { return fmt.Sprint(v...) }) }
func (l nexusLogger) Println(v ...any) { l.emit(func() string { return fmt.Sprintln(v...) }) }
func (l nexusLogger) Printf(format string, v ...any) {
	l.emit(func() string { return fmt.Sprintf(format, v...) })
}
