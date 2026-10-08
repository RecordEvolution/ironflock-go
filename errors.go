package ironflock

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/RecordEvolution/ironflock-go/crossbar"
)

// ErrInvalidArgument is wrapped by every error about invalid parameters
// (the Python SDK's ValueError): test with errors.Is.
var ErrInvalidArgument = errors.New("ironflock: invalid argument")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// OperationError reports a failed SDK operation. Op names the operation and
// its target (e.g. "Publish to topic 'x'"); Err is the cause — a
// *crossbar.WampError when the router or callee refused, which errors.As
// finds through it.
type OperationError struct {
	Op  string
	Err error
	// hint replaces the generic cause in the message when set.
	hint string
}

// Error implements error:
//
//	<Op> failed with WAMP error '<uri>'[ — <json args>]
//	<Op> failed: <hint or cause>
func (e *OperationError) Error() string {
	if e.hint != "" {
		return fmt.Sprintf("%s failed: %s", e.Op, e.hint)
	}
	var werr *crossbar.WampError
	if errors.As(e.Err, &werr) {
		detail := ""
		if len(werr.Args) > 0 {
			if data, err := json.Marshal(werr.Args); err == nil {
				detail = " — " + string(data)
			} else {
				detail = fmt.Sprintf(" — %v", werr.Args)
			}
		}
		return fmt.Sprintf("%s failed with WAMP error '%s'%s", e.Op, werr.URI, detail)
	}
	return fmt.Sprintf("%s failed: %v", e.Op, e.Err)
}

// Unwrap returns the cause.
func (e *OperationError) Unwrap() error { return e.Err }

// WampURI returns the WAMP error URI behind err, or "" when err is not a
// router or callee refusal.
func WampURI(err error) string {
	var werr *crossbar.WampError
	if errors.As(err, &werr) {
		return werr.URI
	}
	return ""
}

// operationFailed wraps err for operation op, leaving invalid-argument and
// cross-app errors as they are.
func operationFailed(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInvalidArgument) {
		return err
	}
	var cerr *CrossAppAccessError
	if errors.As(err, &cerr) {
		return err
	}
	return &OperationError{Op: op, Err: err}
}

// historyHint is the explanation for the most common history failure: the
// procedure is not registered because the table is not declared in the
// data-template or the app's data backend is not running.
const historyHint = "Check that the table is declared in the app's data-template and that the app's data backend is running."

// secretRPCHint replaces historyHint for the secret-column procedures.
const secretRPCHint = "The app's data backend does not support secret columns yet — update it, then declare the column with `secret: true` in the app's data-template."

// historyFailed wraps a failed history-style RPC, spelling out the
// not-registered case. secret selects the secret-column hint.
func historyFailed(op, topic string, err error, secret bool) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInvalidArgument) {
		return err
	}
	text := err.Error()
	notRegistered := strings.Contains(WampURI(err), "no_such_procedure") ||
		strings.Contains(text, "no_such_procedure") ||
		strings.Contains(text, "no callee registered")
	if notRegistered {
		if secret {
			return &OperationError{Op: op, Err: err,
				hint: fmt.Sprintf("procedure '%s' is not registered. %s", topic, secretRPCHint)}
		}
		return &OperationError{Op: op, Err: err,
			hint: fmt.Sprintf("history procedure '%s' is not registered. %s", topic, historyHint)}
	}
	return &OperationError{Op: op, Err: err}
}

// Cross-app access error codes (CrossAppAccessError.Code).
const (
	CodeNoGrant              = "NO_GRANT"
	CodeProviderNotInstalled = "PROVIDER_NOT_INSTALLED"
	CodeUnknownApp           = "UNKNOWN_APP"
	CodePrivateTable         = "PRIVATE_TABLE"
	CodeSecretColumn         = "SECRET_COLUMN"
	CodeNotAuthorized        = "NOT_AUTHORIZED"
)

// CrossAppAccessError reports that reading another app's data backend was
// declined or misused. Branch on Code.
type CrossAppAccessError struct {
	// Code is one of NO_GRANT, PROVIDER_NOT_INSTALLED, UNKNOWN_APP,
	// PRIVATE_TABLE, SECRET_COLUMN or NOT_AUTHORIZED.
	Code    string
	Message string
	// Err is the underlying cause, if any.
	Err error
}

func (e *CrossAppAccessError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the underlying cause.
func (e *CrossAppAccessError) Unwrap() error { return e.Err }

// crossAppCodes maps WAMP error URIs to cross-app access codes.
var crossAppCodes = map[string]string{
	"sys.appaccess.error.no_grant":               CodeNoGrant,
	"sys.appaccess.error.provider_not_installed": CodeProviderNotInstalled,
	"sys.appaccess.error.unknown_app":            CodeUnknownApp,
	crossbar.ErrURINotAuthorized:                 CodeNotAuthorized,
	crossbar.ErrURIAuthorizationFailed:           CodeNotAuthorized,
	crossbar.ErrURIAuthenticationFail:            CodeNotAuthorized,
}

// mapCrossAppError maps a WAMP refusal to a *CrossAppAccessError, or returns
// nil when the URI is not a cross-app access condition. The message is the
// URI followed by ": <json of the first error argument>", if any.
func mapCrossAppError(err error) *CrossAppAccessError {
	var werr *crossbar.WampError
	if !errors.As(err, &werr) {
		return nil
	}
	code, ok := crossAppCodes[werr.URI]
	if !ok {
		return nil
	}
	msg := werr.URI
	if len(werr.Args) > 0 {
		if data, jerr := json.Marshal(werr.Args[0]); jerr == nil {
			msg += ": " + string(data)
		} else {
			msg += fmt.Sprintf(": %v", werr.Args[0])
		}
	}
	return &CrossAppAccessError{Code: code, Message: msg, Err: err}
}
