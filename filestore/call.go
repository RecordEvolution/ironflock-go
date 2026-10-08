package filestore

import (
	"context"
	"errors"
	"fmt"

	"github.com/RecordEvolution/ironflock-go/internal/jsontext"
	"github.com/RecordEvolution/ironflock-go/wamp"
)

// errNoCaller is returned by every call of a FileStore built without a
// Caller.
var errNoCaller = errors.New("filestore: no connection to call the file service through")

// wampErrorCodes maps router-level rejections — which, unlike service
// failures, do arrive as WAMP errors — to file error codes.
// no_such_procedure means the deployment has no file service (or one older
// than the data plane), which an app must be able to tell apart from "you
// may not do this".
var wampErrorCodes = map[string]string{
	wamp.URINoSuchProcedure:      CodeNotAvailable,
	wamp.URINotAuthorized:        CodeNotAuthorized,
	wamp.URIAuthorizationFailed:  CodeNotAuthorized,
	wamp.URIAuthenticationFailed: CodeNotAuthorized,
}

// call calls a file service procedure and unwraps the response envelope
// {"success": bool, "code"?: str, "reason"?: str, "payload"?: object}.
//
// payload is sent as the single positional argument; nil sends no
// arguments. No retry window is passed: the call waits for the session for
// the connection's default time and is not retried, so a missing procedure
// is reported at once as CodeNotAvailable.
//
// Failures the service reports in the envelope are returned as *Error with
// the code passed through verbatim (CodeInternal when absent). Router
// rejections are mapped by mapWampError; every other error (no session,
// cancelled context, an unmapped WAMP error as *wamp.Error) is
// returned unchanged. A successful call returns the envelope's payload, or
// an empty map when it has none.
func (s *FileStore) call(ctx context.Context, uri string, payload map[string]any) (map[string]any, error) {
	if s.caller == nil {
		return nil, errNoCaller
	}
	var args []any
	if payload != nil {
		args = []any{payload}
	}
	res, err := s.caller.Call(ctx, uri, args, nil, nil, 0)
	if err != nil {
		if mapped := mapWampError(err); mapped != nil {
			return nil, mapped
		}
		return nil, err
	}

	result := res.Value()
	env, ok := asMap(result)
	if !ok {
		return nil, newError(CodeInternal, fmt.Sprintf("%s returned %s, expected an object", uri, jsonType(result)))
	}
	if !toBool(env["success"]) {
		// Code is the contract, reason is prose. A code this release does not
		// know passes through, so a newer server can add codes without
		// breaking this client.
		code := toString(env["code"])
		if code == "" {
			code = CodeInternal
		}
		return nil, newError(code, toString(env["reason"]))
	}
	if p, ok := asMap(env["payload"]); ok {
		return p, nil
	}
	return map[string]any{}, nil
}

// mapWampError maps a router-level rejection (a *wamp.Error anywhere
// in err's chain) with one of the URIs in wampErrorCodes to an *Error, or
// returns nil when err is not such a rejection (or is already an *Error).
// The reason carries the first error argument as detail, in compact JSON
// like JavaScript's JSON.stringify (no HTML escaping).
func mapWampError(err error) *Error {
	var fe *Error
	if errors.As(err, &fe) {
		return nil // already a file error
	}
	var we *wamp.Error
	if !errors.As(err, &we) || we == nil {
		return nil
	}
	code, ok := wampErrorCodes[we.URI]
	if !ok {
		return nil
	}
	detail := ""
	if len(we.Args) > 0 {
		detail = ": " + jsontext.Compact(we.Args[0])
	}
	if code == CodeNotAvailable {
		return wrapError(code, "the file service is not available on this deployment"+detail, err)
	}
	return wrapError(code, we.URI+detail, err)
}
