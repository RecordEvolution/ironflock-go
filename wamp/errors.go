package wamp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ironflock/nexus/v3/client"
)

// notConnectedError is the session-wait timeout. It wraps ErrNotConnected.
type notConnectedError struct {
	realm string
	wait  time.Duration
}

func (e *notConnectedError) Error() string {
	return fmt.Sprintf("Not connected to the IronFlock router (realm '%s', no session after %ss). "+
		"Ensure Start() was called and the connection is established.", e.realm, formatSeconds(e.wait))
}

func (e *notConnectedError) Unwrap() error { return ErrNotConnected }

// connectTimeoutError is Start's FirstConnectTimeout expiring. It wraps
// ErrNotConnected.
type connectTimeoutError struct {
	realm   string
	timeout time.Duration
}

func (e *connectTimeoutError) Error() string {
	return fmt.Sprintf("failed to connect to realm %s within %ss", e.realm, formatSeconds(e.timeout))
}

func (e *connectTimeoutError) Unwrap() error { return ErrNotConnected }

// errAlreadyStarted is returned by a second Start.
var errAlreadyStarted = errors.New("wamp: connection already started")

// stoppedErrLocked is the error of an operation on a stopped connection:
// ErrStopped, joined with the *AuthError when a fatal auth denial stopped
// it. c.mu must be held.
func (c *Connection) stoppedErrLocked() error {
	if c.fatal != nil {
		return fmt.Errorf("%w: %w", ErrStopped, c.fatal)
	}
	return ErrStopped
}

func (c *Connection) stoppedErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stoppedErrLocked()
}

// opError wraps a failure of op that is not a WAMP refusal.
func (c *Connection) opError(op string, err error) error {
	switch {
	case errors.Is(err, client.ErrNotConn):
		if c.stopFlag.Load() {
			return c.stoppedErr()
		}
		return fmt.Errorf("wamp: %s: session lost: %w", op, ErrNotConnected)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	}
	return fmt.Errorf("wamp: %s: %w", op, err)
}

// requestError converts the error of a nexus SUBSCRIBE, REGISTER, PUBLISH,
// UNSUBSCRIBE or UNREGISTER request: a router refusal becomes an *Error (see
// parseRefusal), anything else is wrapped by opError.
func (c *Connection) requestError(op, nexusPrefix string, err error) error {
	if err == nil {
		return nil
	}
	if werr := parseRefusal(err.Error(), nexusPrefix); werr != nil {
		return werr
	}
	return c.opError(op, err)
}

// callError converts the error of a nexus CALL.
func (c *Connection) callError(procedure string, err error) error {
	var rpcErr client.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Err != nil {
		return newError(rpcErr.Err)
	}
	return c.opError("call of procedure '"+procedure+"'", err)
}
