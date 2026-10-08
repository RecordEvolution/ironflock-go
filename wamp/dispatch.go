package wamp

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gammazero/nexus/v3/client"
	nxwamp "github.com/gammazero/nexus/v3/wamp"
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

// DeliveryGroup makes several subscriptions deliver their events as one
// subscription does: one at a time, in the order they arrive, across all of
// them — a handler of one never runs while a handler of another one is
// running. Pass the same group as SubscribeOptions.Group to each; the
// subscriptions may have different topics and handlers. Events still queued
// for a subscription when it is unsubscribed are dropped.
//
// A slow handler delays the events of every subscription in the group. A
// group holds a goroutine only while it has events to deliver.
type DeliveryGroup struct {
	queue serialExecutor
}

// NewDeliveryGroup returns a new, empty DeliveryGroup.
func NewDeliveryGroup() *DeliveryGroup { return &DeliveryGroup{} }

// subGroup is the WAMP subscription behind every Subscription of one topic.
// nexus keys event handlers by topic, so handlers of the same topic must
// share one WAMP subscription; the group fans its events out.
//
// handles is copy-on-write (written while Connection.state is held) because
// onEvent runs on the nexus receive loop and must not wait for that lock,
// which is held across router round trips.
type subGroup struct {
	topic   string
	match   string
	options nxwamp.Dict

	sess    atomic.Pointer[session] // session holding the WAMP subscription
	handles atomic.Pointer[[]*Subscription]
}

func (g *subGroup) list() []*Subscription {
	if hs := g.handles.Load(); hs != nil {
		return *hs
	}
	return nil
}

// wanted reports whether a handler of the group has not been unsubscribed.
// Once none is left the group is removed as soon as its removal gets the
// connection's state lock.
func (g *subGroup) wanted() bool {
	return slices.ContainsFunc(g.list(), func(s *Subscription) bool { return !s.removed.Load() })
}

// add and remove must be called with Connection.state held.
func (g *subGroup) add(s *Subscription) {
	hs := append(slices.Clone(g.list()), s)
	g.handles.Store(&hs)
}

func (g *subGroup) remove(s *Subscription) {
	hs := slices.DeleteFunc(slices.Clone(g.list()), func(h *Subscription) bool { return h == s })
	g.handles.Store(&hs)
}

// onEvent is the nexus event handler. It runs on the client's receive loop,
// so it only queues: a handler running here could not even wait for the
// result of its own Call.
func (g *subGroup) onEvent(raw *nxwamp.Event) {
	for _, s := range g.list() {
		s.deliver(raw)
	}
}

// deliver queues raw for the subscription's handler: on its own executor, or
// its DeliveryGroup's.
func (s *Subscription) deliver(raw *nxwamp.Event) {
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
// a goroutine of its own per invocation. Once the registration is removed it
// refuses the call as the router does when no callee has the procedure (see
// Connection.Unregister).
func (r *Registration) invoke(ctx context.Context, raw *nxwamp.Invocation) client.InvokeResult {
	if r.removed.Load() {
		return client.InvokeResult{Err: URINoSuchProcedure, Args: nxwamp.List{
			fmt.Sprintf("procedure '%s' has been unregistered", r.procedure)}}
	}
	inv := &Invocation{
		Procedure: r.procedure,
		Args:      normalizeList(raw.Arguments),
		Kwargs:    normalizeDict(raw.ArgumentsKw),
		Details:   normalizeDict(raw.Details),
	}
	if p, ok := inv.Details[nxwamp.OptProcedure].(string); ok && p != "" {
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
		var werr *Error
		if errors.As(err, &werr) && werr != nil && werr.URI != "" {
			return client.InvokeResult{Err: nxwamp.URI(werr.URI), Args: wireArgs(werr.Args, werr.Kwargs), Kwargs: werr.Kwargs}
		}
		return client.InvokeResult{Err: URIRuntimeError, Args: nxwamp.List{err.Error()}}
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
	return client.InvokeResult{Args: nxwamp.List{value}}
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
