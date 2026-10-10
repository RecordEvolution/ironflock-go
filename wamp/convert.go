package wamp

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

// normalizeValue converts a decoded WAMP payload value into the JSON-like Go
// types the SDK hands to applications: nil, bool, string, int64 (uint64 only
// above math.MaxInt64), float64, []byte, []any and map[string]any, applied
// recursively.
//
// The msgpack decoder produces integers of several widths and the
// nxwamp.List / nxwamp.Dict named types, which defeat type assertions such
// as v.(map[string]any); everything a handler sees goes through here.
func normalizeValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case bool, string, int64, float64:
		return x
	case []byte:
		return x
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case uint:
		return normalizeUint(uint64(x))
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return normalizeUint(x)
	case float32:
		return float64(x)
	case nxwamp.List:
		return normalizeList(x)
	case []any:
		return normalizeList(x)
	case nxwamp.Dict:
		return normalizeDict(x)
	case map[string]any:
		return normalizeDict(x)
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[mapKey(k)] = normalizeValue(val)
		}
		return out
	case time.Time:
		return x
	}
	return normalizeReflect(v)
}

// normalizeReflect handles the types normalizeValue does not list: named
// basic types, typed slices and maps, pointers. Structs (other than the ones
// handled above) are returned unchanged.
func normalizeReflect(v any) any {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return normalizeUint(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return rv.Float()
	case reflect.String:
		return rv.String()
	case reflect.Slice:
		if rv.IsNil() {
			return nil
		}
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return append([]byte(nil), rv.Bytes()...)
		}
		return normalizeSeq(rv)
	case reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(b), rv)
			return b
		}
		return normalizeSeq(rv)
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[mapKey(iter.Key().Interface())] = normalizeValue(iter.Value().Interface())
		}
		return out
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return normalizeValue(rv.Elem().Interface())
	}
	return v
}

func normalizeSeq(rv reflect.Value) []any {
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = normalizeValue(rv.Index(i).Interface())
	}
	return out
}

func normalizeUint(u uint64) any {
	if u <= math.MaxInt64 {
		return int64(u)
	}
	return u
}

func mapKey(k any) string {
	if s, ok := k.(string); ok {
		return s
	}
	if b, ok := k.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(k)
}

// normalizeList normalizes a positional payload; nil stays nil.
func normalizeList(l []any) []any {
	if l == nil {
		return nil
	}
	out := make([]any, len(l))
	for i, v := range l {
		out[i] = normalizeValue(v)
	}
	return out
}

// normalizeDict normalizes a keyword payload or details dict; nil stays nil.
func normalizeDict(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = normalizeValue(v)
	}
	return out
}

// newResult converts a WAMP RESULT message into a *Result.
func newResult(r *nxwamp.Result) *Result {
	return &Result{
		Args:    normalizeList(r.Arguments),
		Kwargs:  normalizeDict(r.ArgumentsKw),
		Details: normalizeDict(r.Details),
	}
}

// isProgressive reports whether r is a progressive result: one the callee
// sends before its final result.
func isProgressive(r *nxwamp.Result) bool {
	progress, _ := r.Details[nxwamp.OptProgress].(bool)
	return progress
}

// newError converts a WAMP ERROR message into an *Error.
func newError(e *nxwamp.Error) *Error {
	return &Error{
		URI:    string(e.Error),
		Args:   normalizeList(e.Arguments),
		Kwargs: normalizeDict(e.ArgumentsKw),
	}
}

// parseRefusal recovers the router's refusal from the error text nexus
// produces for SUBSCRIBE, REGISTER, PUBLISH (acknowledged), UNSUBSCRIBE and
// UNREGISTER, which — unlike a CALL error — it does not return typed: the
// text is "<prefix><uri>[: <args>][: <kwargs>]". The URI is exact; the rest
// (nexus joins arguments with ", ") is kept as the single string argument.
// It returns nil when text is not such an error.
func parseRefusal(text, prefix string) *Error {
	rest, ok := strings.CutPrefix(text, prefix)
	if !ok {
		return nil
	}
	uri, detail, _ := strings.Cut(rest, ": ")
	if !looksLikeURI(uri) {
		return nil
	}
	werr := &Error{URI: uri}
	if detail != "" {
		werr.Args = []any{detail}
	}
	return werr
}

func looksLikeURI(s string) bool {
	if s == "" || !strings.Contains(s, ".") {
		return false
	}
	return strings.IndexFunc(s, unicode.IsSpace) < 0
}

// validateURI rejects topics and procedures the router could never accept
// and that are almost certainly a programming error: empty, or with leading
// or trailing whitespace (the Python SDK's topic rule for low-level calls).
func validateURI(kind, uri string) error {
	if uri == "" {
		return fmt.Errorf("wamp: %s must be a non-empty string", kind)
	}
	if strings.TrimSpace(uri) != uri {
		return fmt.Errorf("wamp: %s %q must not have leading or trailing whitespace", kind, uri)
	}
	return nil
}

// matchPolicy returns the normalized match policy ("" for exact) of the
// explicit option or, failing that, of a "match" entry in extra.
func matchPolicy(match string, extra map[string]any) (string, error) {
	if match == "" {
		if s, ok := extra[nxwamp.OptMatch].(string); ok {
			match = s
		}
	}
	switch match {
	case "", nxwamp.MatchExact:
		return "", nil
	case nxwamp.MatchPrefix, nxwamp.MatchWildcard:
		return match, nil
	}
	return "", fmt.Errorf("wamp: invalid match policy %q (want \"exact\", \"prefix\" or \"wildcard\")", match)
}

func cloneDict(m map[string]any) nxwamp.Dict {
	d := make(nxwamp.Dict, len(m)+3)
	for k, v := range m {
		d[k] = v
	}
	return d
}

// subscribeOptions builds the WAMP SUBSCRIBE options. Explicit fields win
// over Extra; the caller's options are never modified.
func subscribeOptions(o *SubscribeOptions, match string) nxwamp.Dict {
	if o == nil {
		return nxwamp.Dict{}
	}
	d := cloneDict(o.Extra)
	delete(d, nxwamp.OptMatch)
	if match != "" {
		d[nxwamp.OptMatch] = match
	}
	return d
}

// registerOptions builds the WAMP REGISTER options. force_reregister
// defaults to true unless ForceReregister (or Extra) sets it explicitly.
func registerOptions(o *RegisterOptions, match string) nxwamp.Dict {
	if o == nil {
		return nxwamp.Dict{"force_reregister": true}
	}
	d := cloneDict(o.Extra)
	delete(d, nxwamp.OptMatch)
	if match != "" {
		d[nxwamp.OptMatch] = match
	}
	if o.Invoke != "" {
		d[nxwamp.OptInvoke] = o.Invoke
	}
	switch {
	case o.ForceReregister != nil:
		d["force_reregister"] = *o.ForceReregister
	default:
		if _, set := d["force_reregister"]; !set {
			d["force_reregister"] = true
		}
	}
	return d
}

// callOptions builds the WAMP CALL options.
func callOptions(o *CallOptions) nxwamp.Dict {
	if o == nil {
		return nxwamp.Dict{}
	}
	d := cloneDict(o.Extra)
	if o.Timeout > 0 {
		ms := o.Timeout.Milliseconds()
		if ms == 0 {
			ms = 1
		}
		d[nxwamp.OptTimeout] = ms
	}
	if o.DiscloseMe {
		d[nxwamp.OptDiscloseMe] = true
	}
	return d
}

// publishOptions builds the WAMP PUBLISH options.
func publishOptions(o *PublishOptions) nxwamp.Dict {
	if o == nil {
		return nxwamp.Dict{}
	}
	d := cloneDict(o.Extra)
	if o.Acknowledge {
		d[nxwamp.OptAcknowledge] = true
	}
	if o.ExcludeMe != nil {
		d[nxwamp.OptExcludeMe] = *o.ExcludeMe
	}
	return d
}

// formatSeconds renders d the way the Python SDK formats seconds ("{:g}"):
// 10s → "10", 50ms → "0.05".
func formatSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'g', -1, 64)
}

// wireArgs returns the positional arguments of an outgoing message. WAMP
// requires Arguments to be a list when ArgumentsKw follows; a router may
// abort the session otherwise. nexus encodes a nil Arguments field followed
// by a non-empty ArgumentsKw as nil, not as an empty list, so wireArgs
// returns [] in that case, as autobahn sends it.
func wireArgs(args []any, kwargs map[string]any) nxwamp.List {
	if args == nil && len(kwargs) > 0 {
		return nxwamp.List{}
	}
	return nxwamp.List(args)
}
