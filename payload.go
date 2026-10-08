package ironflock

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// Row is one table row: column name to value.
type Row = map[string]any

// Kwargs marks WAMP keyword arguments in a variadic argument list. Positional
// arguments of Publish, PublishToTable, AppendToTable, Call and
// CallDeviceFunction are sent as WAMP args; a Kwargs value among them is sent
// as WAMP kwargs instead (several Kwargs are merged, later keys win):
//
//	ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"temperature": 22.5})
//	ifl.PublishToTable(ctx, "inspections", ironflock.Kwargs{"part_id": "1"})
//	ifl.Call(ctx, "proc", 1, 2, ironflock.Kwargs{"scale": 10})
type Kwargs map[string]any

// splitArgs separates positional arguments from Kwargs and CallOptions
// markers, and normalizes every value to the JSON-like shapes the platform
// expects (see normalize).
func splitArgs(args []any) (positional []any, kwargs map[string]any, callOpts *CallOptions, err error) {
	for _, a := range args {
		switch v := a.(type) {
		case Kwargs:
			if kwargs == nil {
				kwargs = make(map[string]any, len(v))
			}
			for k, val := range v {
				nv, err := normalize(val)
				if err != nil {
					return nil, nil, nil, invalidf("kwarg %q: %v", k, err)
				}
				kwargs[k] = nv
			}
		case CallOptions:
			o := v
			callOpts = &o
		case *CallOptions:
			callOpts = v
		default:
			nv, err := normalize(a)
			if err != nil {
				return nil, nil, nil, invalidf("argument %d: %v", len(positional), err)
			}
			positional = append(positional, nv)
		}
	}
	return positional, kwargs, callOpts, nil
}

// normalize converts a payload value to the JSON-like shapes every IronFlock
// SDK sends: nil, bool, string, int64 (uint64 only above math.MaxInt64),
// float64, []byte, []any and map[string]any.
//
// Structs (and pointers to them), time.Time, typed maps and typed slices are
// converted through encoding/json, so struct fields are named by their json
// tags and time.Time becomes an RFC 3339 string — the shape the data backend
// reads from Python and JavaScript apps. Numbers that went through JSON come
// out as int64 when integral (2.0 travels as 2, as it does from JavaScript)
// and float64 otherwise.
func normalize(v any) (any, error) {
	switch x := v.(type) {
	case nil, bool, string, []byte, int64, float64:
		return x, nil
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case uint:
		return unsigned(uint64(x)), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return unsigned(x), nil
	case float32:
		return float64(x), nil
	case json.Number:
		return jsonNumber(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			n, err := normalize(e)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			n, err := normalize(e)
			if err != nil {
				return nil, err
			}
			out[k] = n
		}
		return out, nil
	case Kwargs:
		return normalize(map[string]any(x))
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	case *time.Time:
		if x == nil {
			return nil, nil
		}
		return x.UTC().Format(time.RFC3339Nano), nil

	case []map[string]any:
		out := make([]any, len(x))
		for i, e := range x {
			n, err := normalize(e)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil, nil
	}
	switch rv.Kind() {
	case reflect.Func, reflect.Chan, reflect.Complex64, reflect.Complex128, reflect.UnsafePointer:
		return nil, fmt.Errorf("unsupported value of type %T", v)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cannot encode value of type %T: %v", v, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("cannot encode value of type %T: %v", v, err)
	}
	return fromJSON(out)
}

// unsigned returns u as int64 when it fits, as every integer travels, and as
// uint64 only above math.MaxInt64.
func unsigned(u uint64) any {
	if u <= math.MaxInt64 {
		return int64(u)
	}
	return u
}

// fromJSON converts a value decoded with json.Decoder.UseNumber, turning
// numbers into int64 when integral and float64 otherwise.
func fromJSON(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		return jsonNumber(x)
	case []any:
		for i, e := range x {
			n, err := fromJSON(e)
			if err != nil {
				return nil, err
			}
			x[i] = n
		}
		return x, nil
	case map[string]any:
		for k, e := range x {
			n, err := fromJSON(e)
			if err != nil {
				return nil, err
			}
			x[k] = n
		}
		return x, nil
	default:
		return x, nil
	}
}

// jsonNumber converts a JSON number to int64 when it is an integer that fits,
// to uint64 for a larger unsigned integer (so it keeps its precision), and to
// float64 otherwise.
func jsonNumber(n json.Number) (any, error) {
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
		return u, nil
	}
	f, err := n.Float64()
	if err != nil {
		return nil, fmt.Errorf("invalid number %q", n.String())
	}
	return f, nil
}

// normalizeRows validates a bulk batch — a non-empty slice whose elements
// are rows (maps with string keys, or structs) — and returns it as []any of
// map[string]any.
func normalizeRows(rows any) ([]any, error) {
	if rows == nil {
		return nil, fmt.Errorf("rows must be a non-empty list of rows")
	}
	rv := reflect.ValueOf(rows)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("rows must be a non-empty list of rows, got %T", rows)
	}
	if rv.Len() == 0 {
		return nil, fmt.Errorf("rows must be a non-empty list of rows")
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		n, err := normalize(rv.Index(i).Interface())
		if err != nil {
			return nil, fmt.Errorf("row at index %d: %v", i, err)
		}
		row, ok := n.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("row at index %d must be a map or struct, got %T", i, rv.Index(i).Interface())
		}
		out[i] = row
	}
	return out, nil
}

// DecodeRows converts rows (as returned by GetHistory and friends) into
// typed values through encoding/json, matching columns to json tags.
func DecodeRows[T any](rows []Row) ([]T, error) {
	data, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	var out []T
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	return out, nil
}

// Decode converts any payload value (an event argument, a call result) into
// T through encoding/json.
func Decode[T any](v any) (T, error) {
	var out T
	data, err := json.Marshal(v)
	if err != nil {
		return out, fmt.Errorf("decode: %w", err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("decode: %w", err)
	}
	return out, nil
}
