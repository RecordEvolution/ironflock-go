package crossbar

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/wamp"
)

// serialExecutor runs submitted functions one at a time, in submission
// order, on a goroutine that exists only while there is work: submit never
// blocks, and an idle executor holds no goroutine.
type serialExecutor struct {
	mu      sync.Mutex
	queue   []func()
	running bool
	closed  bool
}

// submit queues f. It reports false (and drops f) once the executor is
// closed.
func (e *serialExecutor) submit(f func()) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	e.queue = append(e.queue, f)
	if !e.running {
		e.running = true
		go e.run()
	}
	return true
}

// close drops queued functions and rejects new ones. A function already
// running finishes.
func (e *serialExecutor) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	clear(e.queue)
	e.queue = nil
}

func (e *serialExecutor) run() {
	for {
		e.mu.Lock()
		if e.closed || len(e.queue) == 0 {
			e.running = false
			e.queue = nil
			e.mu.Unlock()
			return
		}
		f := e.queue[0]
		e.queue[0] = nil
		e.queue = e.queue[1:]
		e.mu.Unlock()
		f()
	}
}

// subGroup is the WAMP subscription behind every Subscription of one topic.
// nexus keys event handlers by topic, so handlers of the same topic must
// share one WAMP subscription; the group fans its events out.
//
// handles is copy-on-write (written under Connection.stateMu) because
// onEvent runs on the nexus receive loop and must not wait for stateMu,
// which is held across router round trips.
type subGroup struct {
	topic   string
	match   string
	options wamp.Dict

	sess    atomic.Pointer[session] // session holding the WAMP subscription
	handles atomic.Pointer[[]*Subscription]
}

func (g *subGroup) list() []*Subscription {
	if hs := g.handles.Load(); hs != nil {
		return *hs
	}
	return nil
}

// add and remove must be called with Connection.stateMu held.
func (g *subGroup) add(s *Subscription) {
	hs := append(slices.Clone(g.list()), s)
	g.handles.Store(&hs)
}

func (g *subGroup) remove(s *Subscription) (remaining int) {
	hs := slices.DeleteFunc(slices.Clone(g.list()), func(h *Subscription) bool { return h == s })
	g.handles.Store(&hs)
	return len(hs)
}

// onEvent is the nexus event handler. It runs on the client's receive loop,
// so it only queues: a handler running here could not even wait for the
// result of its own Call.
func (g *subGroup) onEvent(raw *wamp.Event) {
	for _, s := range g.list() {
		s.deliver(raw)
	}
}

func (s *Subscription) deliver(raw *wamp.Event) {
	if s.removed.Load() {
		return
	}
	ev := &Event{
		Topic:   s.topic,
		Args:    normalizeList(raw.Arguments),
		Kwargs:  normalizeDict(raw.ArgumentsKw),
		Details: normalizeDict(raw.Details),
	}
	if topic, ok := ev.Details["topic"].(string); ok && topic != "" {
		ev.Topic = topic
	}
	s.queue.submit(func() { s.conn.runEventHandler(s, ev) })
}

func (c *Connection) runEventHandler(s *Subscription, ev *Event) {
	if s.removed.Load() || c.stopFlag.Load() {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("Event handler panicked", "topic", s.topic, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	s.handler(ev)
}

// invoke is the nexus invocation handler of a registration. nexus runs it on
// a goroutine of its own per invocation.
func (r *Registration) invoke(ctx context.Context, raw *wamp.Invocation) client.InvokeResult {
	inv := &Invocation{
		Procedure: r.procedure,
		Args:      normalizeList(raw.Arguments),
		Kwargs:    normalizeDict(raw.ArgumentsKw),
		Details:   normalizeDict(raw.Details),
	}
	if p, ok := inv.Details[wamp.OptProcedure].(string); ok && p != "" {
		inv.Procedure = p
	}
	value, err := r.conn.runInvocationHandler(ctx, r, inv)
	return invokeResult(value, err)
}

func (c *Connection) runInvocationHandler(ctx context.Context, r *Registration, inv *Invocation) (value any, err error) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("Procedure handler panicked", "procedure", r.procedure, "panic", p, "stack", string(debug.Stack()))
			value, err = nil, fmt.Errorf("procedure '%s' panicked: %v", inv.Procedure, p)
		}
	}()
	return r.handler(ctx, inv)
}

// invokeResult maps an InvocationHandler's return values to the YIELD or
// ERROR sent back to the caller.
func invokeResult(value any, err error) client.InvokeResult {
	if err != nil {
		var werr *WampError
		if errors.As(err, &werr) && werr != nil && werr.URI != "" {
			return client.InvokeResult{Err: wamp.URI(werr.URI), Args: wireArgs(werr.Args, werr.Kwargs), Kwargs: werr.Kwargs}
		}
		return client.InvokeResult{Err: ErrURIRuntimeError, Args: wamp.List{err.Error()}}
	}
	switch v := value.(type) {
	case nil:
		return client.InvokeResult{}
	case Result:
		return client.InvokeResult{Args: wireArgs(v.Args, v.Kwargs), Kwargs: v.Kwargs}
	case *Result:
		if v == nil {
			return client.InvokeResult{}
		}
		return client.InvokeResult{Args: wireArgs(v.Args, v.Kwargs), Kwargs: v.Kwargs}
	}
	return client.InvokeResult{Args: wamp.List{value}}
}

// runCallback runs a lifecycle callback on the callback executor, in order
// with the other lifecycle callbacks, recovering a panic.
func (c *Connection) runCallback(name string, f func()) {
	c.callbacks.submit(func() {
		defer func() {
			if p := recover(); p != nil {
				c.log.Error("Connection callback panicked", "callback", name, "panic", p, "stack", string(debug.Stack()))
			}
		}()
		f()
	})
}
