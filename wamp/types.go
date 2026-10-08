package wamp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RecordEvolution/ironflock-go/internal/finite"
	"github.com/RecordEvolution/ironflock-go/internal/jsontext"
)

// Event is one event delivered to a subscription handler.
type Event struct {
	// Topic is the concrete topic the event was published to. For pattern
	// subscriptions it is taken from the event details; otherwise it is the
	// subscribed topic.
	Topic string
	// Args are the positional payload arguments.
	Args []any
	// Kwargs are the keyword payload arguments.
	Kwargs map[string]any
	// Details are the WAMP event details (e.g. "publisher", "topic").
	Details map[string]any
}

// Row returns the first positional argument as a row map — the shape of a
// table row delivered by SubscribeToTable. It returns nil when the event has
// no positional arguments or the first one is not a map.
func (e *Event) Row() map[string]any {
	if e == nil || len(e.Args) == 0 {
		return nil
	}
	row, _ := e.Args[0].(map[string]any)
	return row
}

// EventHandler handles events of a subscription.
//
// Each subscription delivers its events in order, one at a time, on its own
// goroutine — never on the connection's receive loop — so a handler may call
// any other method of the connection (Call, Publish, Subscribe, ...). A slow
// handler delays only later events of its own subscription; they are queued,
// not dropped. Subscriptions that share a DeliveryGroup (see
// SubscribeOptions.Group) deliver one event at a time across all of them
// instead. A panicking handler is recovered and logged.
type EventHandler func(ev *Event)

// Invocation is one call of a registered procedure.
type Invocation struct {
	// Procedure is the registered procedure URI (the concrete URI for pattern
	// registrations, when the router discloses it).
	Procedure string
	Args      []any
	Kwargs    map[string]any
	Details   map[string]any
}

// InvocationHandler implements a registered procedure.
//
// The returned value becomes the call result:
//   - nil: a result without arguments
//   - a Result or *Result: sent as its Args and Kwargs
//   - anything else: sent as the single positional result argument
//
// A returned *Error is sent with its URI, Args and Kwargs. Any other error
// is sent as wamp.error.runtime_error with the error text as its argument.
// ctx is cancelled when the caller cancels the call or the session ends.
type InvocationHandler func(ctx context.Context, inv *Invocation) (any, error)

// Result is the outcome of a remote procedure call.
type Result struct {
	Args    []any
	Kwargs  map[string]any
	Details map[string]any
}

// Value returns the first positional result argument, or nil. Most
// procedures return exactly one value.
func (r *Result) Value() any {
	if r == nil || len(r.Args) == 0 {
		return nil
	}
	return r.Args[0]
}

// Decode decodes Value() into dst (a pointer) through encoding/json, so
// struct fields are matched by their json tags.
//
// NaN and ±Inf (which the Python SDK sends as they are, e.g. for a failed
// sensor read) decode as JSON null does: the field keeps its zero value, or
// is nil when it is a pointer — declare it as *float64 to tell such a value
// from 0.
func (r *Result) Decode(dst any) error {
	data, err := json.Marshal(finite.OrNil(r.Value()))
	if err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	return nil
}

// Error is an error the router or a callee answered with. URI is the WAMP
// error URI (e.g. URINoSuchProcedure); Args and Kwargs carry the error
// payload.
type Error struct {
	URI    string
	Args   []any
	Kwargs map[string]any
}

// Error implements error: "<uri>" followed by Args, if any, as compact JSON
// without HTML escaping (as JavaScript's JSON.stringify writes it), e.g.
// `app.error.invalid: ["a<b"]`. Args JSON cannot encode (NaN, say) are
// printed as Go values.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if len(e.Args) == 0 {
		return e.URI
	}
	return e.URI + ": " + jsontext.Compact(e.Args)
}

// Well-known WAMP error URIs.
const (
	URINoSuchProcedure        = "wamp.error.no_such_procedure"
	URINoSuchRealm            = "wamp.error.no_such_realm"
	URINotAuthorized          = "wamp.error.not_authorized"
	URIAuthorizationFailed    = "wamp.error.authorization_failed"
	URIAuthenticationFailed   = "wamp.error.authentication_failed"
	URINoAuthMethod           = "wamp.error.no_auth_method"
	URIRuntimeError           = "wamp.error.runtime_error"
	URICanceled               = "wamp.error.canceled"
	URIProcedureAlreadyExists = "wamp.error.procedure_already_exists"
)

// IsFatalAuthReason reports whether reason, a WAMP close or abort reason,
// means that the router rejected the credentials or role: URINotAuthorized,
// URIAuthorizationFailed, URIAuthenticationFailed or URINoAuthMethod.
// Retrying cannot succeed until the credential or grant changes.
func IsFatalAuthReason(reason string) bool {
	switch reason {
	case URINotAuthorized, URIAuthorizationFailed, URIAuthenticationFailed, URINoAuthMethod:
		return true
	}
	return false
}

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	// Match is the topic matching policy: "" or "exact" (default), "prefix"
	// or "wildcard".
	Match string
	// Group, if set, delivers the subscription's events through a
	// DeliveryGroup it shares with other subscriptions: one event at a time
	// across all of them, in the order they arrive.
	Group *DeliveryGroup
	// Extra holds additional WAMP SUBSCRIBE options, sent as-is.
	Extra map[string]any
}

// RegisterOptions configures a registration.
type RegisterOptions struct {
	// Match is the procedure matching policy: "" or "exact" (default),
	// "prefix" or "wildcard".
	Match string
	// Invoke is the invocation policy. The IronFlock router accepts only
	// single registrations, so leave it empty (or "single").
	Invoke string
	// ForceReregister lets this registration take over the procedure from a
	// previous session of the same app (e.g. after a crash). Defaults to
	// true when nil.
	ForceReregister *bool
	// Extra holds additional WAMP REGISTER options, sent as-is.
	Extra map[string]any
}

// CallOptions configures a call.
type CallOptions struct {
	// Timeout asks the router (dealer) to cancel the call after this long.
	// Use the context for a client-side deadline.
	Timeout time.Duration
	// DiscloseMe asks the router to disclose the caller to the callee.
	DiscloseMe bool
	// Extra holds additional WAMP CALL options, sent as-is.
	Extra map[string]any
}

// PublishOptions configures a publication.
type PublishOptions struct {
	// Acknowledge waits for the router to confirm (or reject) the publication.
	Acknowledge bool
	// ExcludeMe controls whether the publisher receives its own event when
	// subscribed. The router default (exclude) applies when nil.
	ExcludeMe *bool
	// Extra holds additional WAMP PUBLISH options, sent as-is.
	Extra map[string]any
}
