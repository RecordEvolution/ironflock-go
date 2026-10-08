package wamp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

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
// returns, noting ABORTs, GOODBYEs and revocations on the way. (Intercepting
// only the handshake and then handing out the WebSocket peer's own channel
// would race: the client looks up Recv() again for each handshake message and
// once more when its session starts.) It also watches the client take them
// (see deliver), and holds back the message right behind the answer to a
// SUBSCRIBE or REGISTER until the handler is installed (see replyGate). The
// forwarder exits when the WebSocket peer's receive channel closes, which
// Close guarantees.
//
// A sender goroutine moves every message the client sends from the channel
// Send returns to the WebSocket peer's, noting the request IDs replyGate
// needs on the way. It exits once Done is closed: the client gives up a
// send then. IsLocal is the wrapped peer's.
type observedPeer struct {
	nxwamp.Peer

	in   chan nxwamp.Message // Recv: filled by the forwarder
	out  chan nxwamp.Message // Send: drained by the sender
	opts peerOptions
	gate replyGate

	dead    *signal // Done: closed, or the connection is gone
	closing *signal // Close was called: nobody reads Recv any more

	mu      sync.Mutex
	abort   *nxwamp.Abort
	goodbye *nxwamp.Goodbye
	evicted map[nxwamp.ID]struct{}
}

// peerOptions configure an observedPeer.
type peerOptions struct {
	// conn is the network connection under the WebSocket peer, closed first
	// by closeNow; nil when there is none.
	conn *watchedConn
	// onEvicted is called with a registration the router revoked. It runs on
	// the forwarder and must not block.
	onEvicted func(registration nxwamp.ID)
	// stallAfter bounds how long the client may leave a received message
	// untaken (0: no bound). Past it the peer calls onStall, if set, and
	// closes itself.
	stallAfter time.Duration
	onStall    func(stalled time.Duration)
	// holdLimit bounds how long the forwarder holds back a message for
	// replyGate (0: no bound).
	holdLimit time.Duration
}

// newObservedPeer wraps inner. dead is fired by the connection's
// watchedConn; the peer also fires it on Close and when inner's receive
// channel closes.
func newObservedPeer(inner nxwamp.Peer, dead *signal, opts peerOptions) *observedPeer {
	p := &observedPeer{
		Peer:    inner,
		in:      make(chan nxwamp.Message),
		out:     make(chan nxwamp.Message),
		opts:    opts,
		dead:    dead,
		closing: newSignal(),
	}
	go p.forward()
	go p.send()
	return p
}

// Recv implements nxwamp.Peer.
func (p *observedPeer) Recv() <-chan nxwamp.Message { return p.in }

// Send implements nxwamp.Peer. Like the WebSocket peer's own, the channel is
// never closed: senders select on Done as well.
func (p *observedPeer) Send() chan<- nxwamp.Message { return p.out }

// Done implements nxwamp.Peer: it is closed by Close and when the connection
// is gone.
func (p *observedPeer) Done() <-chan struct{} { return p.dead.done() }

// Close implements nxwamp.Peer. It is idempotent.
func (p *observedPeer) Close() {
	p.closing.fire()
	p.dead.fire() // before closing the WebSocket, so blocked senders give up
	p.Peer.Close()
}

// closeNow is Close without waiting for the network, for a connection the
// SDK gives up on: it closes the connection under the WebSocket first. The
// WebSocket peer's Close waits for its writer, and a write stuck on a full
// socket buffer (a router that stopped reading, a dead link) gives up only
// at its 60s deadline. No WebSocket close frame is sent.
func (p *observedPeer) closeNow() {
	p.closing.fire()
	p.dead.fire()
	if p.opts.conn != nil {
		_ = p.opts.conn.Close()
	}
	p.Peer.Close()
}

func (p *observedPeer) forward() {
	defer close(p.in)
	defer p.dead.fire()
	for msg := range p.Peer.Recv() {
		p.observe(msg)
		returned := p.gate.answering(msg)
		p.deliver(msg)
		if returned != nil {
			p.hold(returned)
		}
	}
}

// send moves the client's messages to the WebSocket peer, in order. Once the
// peer is dead nothing can be sent any more: the sender exits, and a message
// it holds is dropped, as if it had been written to the dead connection.
func (p *observedPeer) send() {
	to := p.Peer.Send()
	for {
		select {
		case msg := <-p.out:
			p.gate.sent(msg)
			select {
			case to <- msg:
			case <-p.dead.done():
				return
			}
		case <-p.dead.done():
			return
		}
	}
}

// hold waits, before the forwarder hands over the next message, until the
// call whose answer it has just handed over has returned (see replyGate).
// nexus gives the answer straight to the waiting call, so that happens at
// once. The wait also ends when the peer closes or its connection is gone,
// and after holdLimit at the latest.
func (p *observedPeer) hold(returned <-chan struct{}) {
	var limit <-chan time.Time
	if p.opts.holdLimit > 0 {
		timer := time.NewTimer(p.opts.holdLimit)
		defer timer.Stop()
		limit = timer.C
	}
	select {
	case <-returned:
	case <-p.closing.done():
	case <-p.dead.done():
	case <-limit:
	}
}

// replyGate makes sure that what the router sends right behind its answer
// to a SUBSCRIBE or REGISTER reaches the handler being installed. nexus
// installs it only once the call that sent the request has returned, on
// that call's goroutine, while its receive loop goes straight on to the next
// message: an EVENT right behind SUBSCRIBED (the next event of a busy topic)
// would be dropped, and an INVOCATION right behind REGISTERED (a caller
// polling for the procedure) refused with wamp.error.invalid_argument
// "client has no handler for registration". So while such a call is in
// flight (armed), the forwarder holds back the message behind the answer to
// its request — SUBSCRIBED, REGISTERED or ERROR — until the call has
// returned.
//
// The connection has at most one such call in flight per session (they hold
// its subscription state), so the first SUBSCRIBE or REGISTER sent while the
// gate is armed is the call's. Matching its request ID keeps a late answer
// to an earlier request, whose call gave up waiting, from holding anything
// back. With a nexus client that installs the handler before it reads on,
// the gate only costs a moment per SUBSCRIBE or REGISTER.
type replyGate struct {
	mu       sync.Mutex
	armed    bool
	request  nxwamp.ID     // the armed call's request, once sent; 0 before
	returned chan struct{} // closed when the armed call has returned
}

// arm marks a SUBSCRIBE or REGISTER call as in flight. Call disarm once the
// call has returned. Calls must not overlap; if they do, the gate serves the
// last one armed.
func (g *replyGate) arm() (disarm func()) {
	returned := make(chan struct{})
	g.mu.Lock()
	g.armed, g.request, g.returned = true, 0, returned
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		if g.returned == returned {
			g.armed, g.request, g.returned = false, 0, nil
		}
		g.mu.Unlock()
		close(returned)
	}
}

// sent notes a message the client sends: an armed gate takes the request ID
// of the first SUBSCRIBE or REGISTER.
func (g *replyGate) sent(msg nxwamp.Message) {
	var request nxwamp.ID
	switch m := msg.(type) {
	case *nxwamp.Subscribe:
		request = m.Request
	case *nxwamp.Register:
		request = m.Request
	default:
		return
	}
	g.mu.Lock()
	if g.armed && g.request == 0 {
		g.request = request
	}
	g.mu.Unlock()
}

// answering returns the channel that is closed when the armed call has
// returned, if msg is the answer to its request, and nil otherwise.
func (g *replyGate) answering(msg nxwamp.Message) <-chan struct{} {
	var request nxwamp.ID
	switch m := msg.(type) {
	case *nxwamp.Subscribed:
		request = m.Request
	case *nxwamp.Registered:
		request = m.Request
	case *nxwamp.Error:
		if m.Type != nxwamp.SUBSCRIBE && m.Type != nxwamp.REGISTER {
			return nil
		}
		request = m.Request
	default:
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed || g.request == 0 || g.request != request {
		return nil
	}
	return g.returned
}

// deliver hands msg to the client's receive loop. It does not give up when
// the connection is gone: a dead connection still delivers what it received
// before it died — typically the router's GOODBYE with the reason. Once the
// peer is closing nobody reads any more, and msg is dropped, so the
// forwarder keeps draining and the WebSocket peer's receiver can exit.
//
// The receive loop takes each message within moments: the SDK runs nothing
// on it that blocks (event handlers only queue), and nexus blocks it only
// briefly, to hand a reply to its waiter or a message to the WebSocket
// writer. A loop that leaves a message untaken for stallAfter is wedged — a
// nexus client can park it for good (see Connection.awaitEnd) — and would
// keep its session open with nothing processed: the peer is closed, which
// ends the session.
func (p *observedPeer) deliver(msg nxwamp.Message) {
	select {
	case p.in <- msg:
		return
	case <-p.closing.done():
		return
	default:
	}
	var stalled <-chan time.Time
	if p.opts.stallAfter > 0 {
		timer := time.NewTimer(p.opts.stallAfter)
		defer timer.Stop()
		stalled = timer.C
	}
	select {
	case p.in <- msg:
	case <-p.closing.done():
	case <-stalled:
		if p.opts.onStall != nil {
			p.opts.onStall(p.opts.stallAfter)
		}
		// Close on a goroutine of its own: closing the WebSocket peer waits
		// for its receiver, which may itself be blocked handing this
		// goroutine the next message. Closing fires at once, so the wait
		// below ends right away unless the loop takes msg after all.
		go p.closeNow()
		select {
		case p.in <- msg:
		case <-p.closing.done():
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
	case *nxwamp.Goodbye:
		p.mu.Lock()
		if p.goodbye == nil {
			p.goodbye = m
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
		if p.opts.onEvicted != nil {
			p.opts.onEvicted(id)
		}
	}
}

// goodbyeReason returns the reason URI of the router's GOODBYE, if it sent
// one.
func (p *observedPeer) goodbyeReason() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.goodbye == nil {
		return ""
	}
	return string(p.goodbye.Reason)
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
	// over is cancelled once the supervisor is done with the session (see
	// Connection.teardown); it ends calls still waiting on it.
	over    context.Context
	endOver context.CancelFunc
}

func newSession(cli *client.Client, peer *observedPeer) *session {
	over, endOver := context.WithCancel(context.Background())
	return &session{cli: cli, peer: peer, down: make(chan struct{}), over: over, endOver: endOver}
}

// alive reports whether the session is usable: its client runs and its
// connection is up. A client notices a lost connection only once it has
// processed what was received before; a wedged one never does.
func (s *session) alive() bool {
	select {
	case <-s.peer.Done():
		return false
	default:
		return s.cli.Connected()
	}
}

func (s *session) markDown() { s.downOnce.Do(func() { close(s.down) }) }

// subscribe is cli.Subscribe, with what the router sends right behind the
// answer held back until fn is installed (see replyGate). Calls of subscribe
// and register must not overlap on a session.
func (s *session) subscribe(topic string, fn client.EventHandler, options nxwamp.Dict) error {
	disarm := s.peer.gate.arm()
	defer disarm()
	return s.cli.Subscribe(topic, fn, options)
}

// register is cli.Register, with what the router sends right behind the
// answer held back until fn is installed (see replyGate).
func (s *session) register(procedure string, fn client.InvocationHandler, options nxwamp.Dict) error {
	disarm := s.peer.gate.arm()
	defer disarm()
	return s.cli.Register(procedure, fn, options)
}

// call is cli.Call, released with client.ErrNotConn when the session is
// over. A client ends its calls itself when its receive loop exits, but a
// wedged loop never exits, and the calls would wait on it forever.
func (s *session) call(ctx context.Context, procedure string, options nxwamp.Dict, args nxwamp.List, kwargs nxwamp.Dict) (*nxwamp.Result, error) {
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.over, cancel)
	defer stop()
	res, err := s.cli.Call(callCtx, procedure, options, args, kwargs, nil)
	if errors.Is(err, context.Canceled) && ctx.Err() == nil {
		return nil, client.ErrNotConn // cancelled by the session's end, not by the caller
	}
	return res, err
}

// closeReason is why the session ended: the router's GOODBYE or ABORT
// reason, else wamp.close.transport_lost. Call it once the session has
// ended; a reason the client has not processed is taken from the peer.
func (s *session) closeReason() string {
	if gb := s.cli.RouterGoodbye(); gb != nil && gb.Reason != "" {
		return string(gb.Reason)
	}
	if reason := s.peer.goodbyeReason(); reason != "" {
		return reason
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
