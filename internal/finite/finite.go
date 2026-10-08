// Package finite prepares decoded payload values for encoding/json, which
// cannot encode the non-finite floats a payload may carry: the Python SDK
// sends NaN and ±Inf as they are (msgpack float64), for example for a failed
// sensor read.
package finite

import (
	"math"
	"reflect"
)

// trackDepth is the nesting depth from which OrNil remembers the maps and
// slices on its current path, to notice a cycle (encoding/json's
// startDetectingCyclesAfter). Decoded payloads are trees, never that deep;
// a cyclic value is something only a caller can build.
const trackDepth = 1000

// OrNil returns v with every NaN, +Inf and -Inf replaced by nil, so that
// encoding/json encodes it (as null, which is how JavaScript's JSON.stringify
// writes them) instead of failing on the whole value.
//
// v is a decoded payload value: nil, bool, string, int64, uint64, float64,
// float32, []byte, []any and map[string]any, nested to any depth; a
// []map[string]any (rows) is accepted as well. A value of any other type is
// returned as it is.
//
// v is never modified. Only the maps and slices on the way to a replaced
// value are copied; everything else is shared with v, so a value without
// non-finite floats is returned as it is, without allocating. A cyclic v is
// returned as it is, for encoding/json to report the cycle.
func OrNil(v any) any {
	var w walker
	out, _ := w.value(v)
	if w.cycle {
		return v
	}
	return out
}

// walker carries the state of one OrNil call.
type walker struct {
	depth int              // maps and slices entered
	path  map[any]struct{} // those on the current path, once deeper than trackDepth
	cycle bool             // a container contains itself: give up
}

// value returns v with its non-finite floats replaced by nil, and whether it
// replaced any (the result is then a new value, not v).
func (w *walker) value(v any) (any, bool) {
	switch x := v.(type) {
	case float64:
		if nonFinite(x) {
			return nil, true
		}
	case float32:
		if nonFinite(float64(x)) {
			return nil, true
		}
	case []any:
		if s, changed := w.list(x); changed {
			return s, true
		}
	case map[string]any:
		if m, changed := w.object(x); changed {
			return m, true
		}
	case []map[string]any:
		if rows, changed := w.rows(x); changed {
			return rows, true
		}
	}
	return v, false
}

func (w *walker) list(s []any) ([]any, bool) {
	var key any
	if w.depth++; w.depth > trackDepth {
		key = sliceKey{reflect.ValueOf(s).UnsafePointer(), len(s)}
		if !w.track(key) {
			return s, false
		}
	}
	defer w.leave(key)
	for i, e := range s {
		r, changed := w.value(e)
		if w.cycle {
			return s, false
		}
		if !changed {
			continue
		}
		out := make([]any, len(s))
		copy(out, s[:i])
		out[i] = r
		for j := i + 1; j < len(s); j++ {
			if out[j], _ = w.value(s[j]); w.cycle {
				return s, false
			}
		}
		return out, true
	}
	return s, false
}

func (w *walker) object(m map[string]any) (map[string]any, bool) {
	var key any
	if w.depth++; w.depth > trackDepth {
		key = reflect.ValueOf(m).UnsafePointer()
		if !w.track(key) {
			return m, false
		}
	}
	defer w.leave(key)
	for k, e := range m {
		r, changed := w.value(e)
		if w.cycle {
			return m, false
		}
		if !changed {
			continue
		}
		out := make(map[string]any, len(m))
		for k2, e2 := range m {
			if k2 == k {
				out[k2] = r
			} else if out[k2], _ = w.value(e2); w.cycle {
				return m, false
			}
		}
		return out, true
	}
	return m, false
}

func (w *walker) rows(rows []map[string]any) ([]map[string]any, bool) {
	var key any
	if w.depth++; w.depth > trackDepth {
		key = sliceKey{reflect.ValueOf(rows).UnsafePointer(), len(rows)}
		if !w.track(key) {
			return rows, false
		}
	}
	defer w.leave(key)
	for i, row := range rows {
		r, changed := w.object(row)
		if w.cycle {
			return rows, false
		}
		if !changed {
			continue
		}
		out := make([]map[string]any, len(rows))
		copy(out, rows[:i])
		out[i] = r
		for j := i + 1; j < len(rows); j++ {
			if out[j], _ = w.object(rows[j]); w.cycle {
				return rows, false
			}
		}
		return out, true
	}
	return rows, false
}

// sliceKey identifies a slice on the current path: its first element and
// length, as encoding/json identifies one.
type sliceKey struct {
	data any
	n    int
}

// track records the map or slice identified by key on the current path. It
// reports false, and gives up the walk, when key is on the path already: the
// value contains itself.
func (w *walker) track(key any) bool {
	if _, seen := w.path[key]; seen {
		w.cycle = true
		return false
	}
	if w.path == nil {
		w.path = make(map[any]struct{})
	}
	w.path[key] = struct{}{}
	return true
}

// leave is deferred by every map or slice walk that got past its track.
func (w *walker) leave(key any) {
	if key != nil {
		delete(w.path, key)
	}
	w.depth--
}

func nonFinite(f float64) bool { return math.IsNaN(f) || math.IsInf(f, 0) }
