package ironflock

import (
	"bytes"
	"cmp"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/RecordEvolution/ironflock-go/internal/finite"
	"github.com/RecordEvolution/ironflock-go/internal/jsontext"
)

// Row is one table row: column name to value.
type Row = map[string]any

// Kwargs marks WAMP keyword arguments in a variadic argument list. Positional
// arguments of Publish, PublishToTable, AppendToTable, Call and
// CallDeviceFunction are sent as WAMP args; a Kwargs value among them is sent
// as WAMP kwargs instead (several Kwargs are merged, later keys win):
//
//	ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"tsp": time.Now(), "temperature": 22.5})
//	ifl.Publish(ctx, "com.myapp.status", ironflock.Kwargs{"state": "ready"})
//	ifl.Call(ctx, "proc", 1, 2, ironflock.Kwargs{"scale": 10})
//
// A table write's row is its first positional argument: the data backend
// reads the kwargs only into columns the table's data template maps to them.
type Kwargs map[string]any

// splitArgs separates positional arguments from Kwargs and CallOptions
// markers, and normalizes every value to the JSON-like shapes the platform
// expects (see normalize).
func splitArgs(args []any) (positional []any, kwargs map[string]any, callOpts *CallOptions, err error) {
	for _, a := range args {
		switch v := a.(type) {
		case Kwargs:
			kw, err := normalizeKwargs(v)
			if err != nil {
				return nil, nil, nil, err
			}
			if kwargs == nil {
				kwargs = kw
				continue
			}
			for k, val := range kw {
				kwargs[k] = val
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

// normalizeKwargs normalizes one Kwargs value: its values as normalize does,
// its keys as normalize does map keys.
func normalizeKwargs(kw Kwargs) (map[string]any, error) {
	obj := newObject(len(kw))
	for k, val := range kw {
		nv, err := normalize(val)
		if err != nil {
			return nil, invalidf("kwarg %q: %v", k, err)
		}
		obj.set(k, nv)
	}
	return obj.m, nil
}

// normalize converts a payload value to the JSON-like shapes every IronFlock
// SDK sends: nil, bool, string, int64 (uint64 only above math.MaxInt64),
// float64, []byte, []any and map[string]any. The result shares no mutable
// memory with v.
//
// Structs, maps, slices, arrays, pointers and interfaces are converted by the
// rules of encoding/json, so a value has the shape json.Marshal would give it:
// struct fields are named by their json tags, with the "-", omitempty,
// omitzero and ",string" options and the rules for embedded structs; map keys
// are strings, integers (as decimal strings) or encoding.TextMarshalers; a
// nil slice, map or pointer, Row(nil) included, becomes nil (JSON null), not
// an empty list or object; a value with a MarshalJSON or MarshalText method
// is encoded through it (the JSON a MarshalJSON method returns is decoded, a
// number written as an integer becoming int64, or uint64 above
// math.MaxInt64, any other float64), the method called wherever
// encoding/json calls it: a method with a pointer receiver only on an
// addressable value, and any on a nil pointer held in an interface of the
// method's type (a field of type json.Marshaler, say); strings are coerced
// to valid UTF-8, each invalid byte becoming U+FFFD.
// Unlike encoding/json:
//
//   - Floats stay float64, NaN and ±Inf included, as the Python SDK sends
//     them; an integral float such as 2.0 stays a float. A float32 is widened
//     through its shortest decimal form, so float32(0.1) is sent as 0.1.
//     Only a field with the ",string" option, which asks for the JSON number
//     in a string, cannot hold NaN or ±Inf.
//   - []byte stays []byte (msgpack bin, like Python bytes) instead of
//     becoming a base64 string.
//   - A time.Time becomes an RFC 3339 string in UTC
//     ("2026-01-02T03:04:05.123Z") instead of keeping its zone offset,
//     wherever it is: a value or a map key, behind a pointer, in an
//     interface, or embedded in a struct that is encoded through time.Time's
//     promoted MarshalJSON (as that time, its other fields dropped, as
//     encoding/json encodes it). Map keys naming the same instant become one
//     entry, the one encoding/json would write last. The Python and
//     JavaScript SDKs send their timestamps in UTC too.
//
// Like encoding/json, normalize fails on channels, functions, complex
// numbers, maps with other key types, and values that contain themselves.
// Where encoding/json panics, normalize does not: it fails on a method it
// could only call through an unexported embedded field, and sends nil (as a
// map key, "") where a method called on a nil pointer, or promoted through a
// nil embedded pointer or interface, fails with a run-time error on it.
func normalize(v any) (any, error) {
	var e encoder
	return e.anyValue(v)
}

// trackDepth is the nesting depth (pointers, maps and slices) from which the
// walk remembers the containers on its current path, to fail on a value that
// contains itself instead of recursing until the stack overflows. It is
// encoding/json's startDetectingCyclesAfter.
const trackDepth = 1000

// encoder carries the state of one normalize walk.
type encoder struct {
	depth int              // pointers, maps and slices entered
	path  map[any]struct{} // those on the current path, once deeper than trackDepth
}

// anyValue converts v, a value held in an interface: common payload types
// directly, any other type by reflection.
func (e *encoder) anyValue(v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string:
		return jsontext.ValidUTF8(x), nil
	case bool, int64, float64:
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
		return widen(x), nil
	case []byte:
		if x == nil {
			return nil, nil
		}
		return bytes.Clone(x), nil
	case time.Time:
		return utc(x), nil
	case map[string]any:
		return e.stringMap(x)
	case []any:
		return e.list(x)
	}
	// A value held in an interface is not addressable: as for encoding/json,
	// a MarshalJSON or MarshalText method with a pointer receiver is not
	// used for it.
	return e.value(reflect.ValueOf(v), false)
}

// value converts v by encoding/json's rules (with normalize's exceptions).
// quoted is the ",string" option of the struct field v is (or is pointed to
// by).
func (e *encoder) value(v reflect.Value, quoted bool) (any, error) {
	switch v.Kind() {
	case reflect.Invalid:
		return nil, nil
	case reflect.Pointer:
		return e.pointer(v, quoted)
	case reflect.Interface:
		return e.iface(v)
	}
	t := v.Type()
	switch t {
	case timeType:
		return timeString(v)
	case anyMapType: // e.g. the rows of a []Row: no reflection needed
		if v.CanInterface() {
			return e.stringMap(v.Interface().(map[string]any))
		}
	case anySliceType:
		if v.CanInterface() {
			return e.list(v.Interface().([]any))
		}
	}
	ti := infoOf(t)
	if ti.marshalJSON == valueMethod || ti.marshalJSON == pointerMethod && v.CanAddr() {
		return marshal(v, ti.marshalJSON, false)
	}
	if ti.marshalText == valueMethod || ti.marshalText == pointerMethod && v.CanAddr() {
		return marshal(v, ti.marshalText, true)
	}
	switch v.Kind() {
	case reflect.Bool:
		if quoted {
			return strconv.FormatBool(v.Bool()), nil
		}
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if quoted {
			return strconv.FormatInt(v.Int(), 10), nil
		}
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if quoted {
			return strconv.FormatUint(v.Uint(), 10), nil
		}
		return unsigned(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return floatValue(v.Float(), t.Bits(), quoted)
	case reflect.String:
		if t == numberType {
			return numberValue(v.String(), quoted)
		}
		if quoted {
			// The JSON encoding of the string, as a string.
			b, _ := json.Marshal(v.String())
			return string(b), nil
		}
		return jsontext.ValidUTF8(v.String()), nil
	case reflect.Struct:
		return e.structValue(v, ti.fields)
	case reflect.Map:
		return e.mapValue(v, ti)
	case reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
		if ti.bytes {
			return bytes.Clone(v.Bytes()), nil
		}
		key, err := e.enter(v)
		if err != nil {
			return nil, err
		}
		out, err := e.elems(v)
		e.leave(key)
		return out, err
	case reflect.Array:
		return e.elems(v)
	}
	return nil, failf("unsupported type %s", t)
}

// pointer converts the pointer v: nil, or the value it points to. A method
// encoding/json would call on the pointer is called on that value, which is
// addressable.
func (e *encoder) pointer(v reflect.Value, quoted bool) (any, error) {
	if v.IsNil() {
		return nil, nil
	}
	key, err := e.enter(v)
	if err != nil {
		return nil, err
	}
	out, err := e.value(v.Elem(), quoted)
	e.leave(key)
	return out, err
}

// iface converts v, a value of interface type.
func (e *encoder) iface(v reflect.Value) (any, error) {
	if v.IsNil() {
		return nil, nil
	}
	ti := infoOf(v.Type())
	if ti.marshalJSON == noMethod && ti.marshalText == noMethod {
		if !v.CanInterface() {
			return e.value(v.Elem(), false)
		}
		return e.anyValue(v.Interface())
	}
	// The interface type is a json.Marshaler or encoding.TextMarshaler itself
	// (a field of type json.Marshaler, say): encoding/json calls that method
	// on whatever is inside, a nil pointer included.
	return marshal(v, valueMethod, ti.marshalJSON == noMethod)
}

// structValue converts the struct v, whose encoded fields are fields.
func (e *encoder) structValue(v reflect.Value, fields []structField) (any, error) {
	out := make(map[string]any, len(fields))
	for i := range fields {
		f := &fields[i]
		fv, ok := fieldByIndex(v, f.index)
		if !ok {
			continue
		}
		if f.omitEmpty && isEmptyValue(fv) {
			continue
		}
		if f.omitZero {
			zero := false
			if f.isZero == nil {
				zero = fv.IsZero()
			} else {
				var err error
				if zero, err = f.isZero(fv); err != nil {
					return nil, at(err, keySegment(f.name))
				}
			}
			if zero {
				continue
			}
		}
		val, err := e.value(fv, f.quoted)
		if err != nil {
			return nil, at(err, keySegment(f.name))
		}
		out[f.name] = val
	}
	return out, nil
}

// mapValue converts the map v, of a type described by ti.
func (e *encoder) mapValue(v reflect.Value, ti *typeInfo) (any, error) {
	if !ti.keysOK {
		return nil, failf("unsupported type %s", v.Type())
	}
	if v.IsNil() {
		return nil, nil
	}
	key, err := e.enter(v)
	if err != nil {
		return nil, err
	}
	obj := newObject(v.Len())
	for it := v.MapRange(); it.Next(); {
		k, orig, err := keyName(it.Key())
		if err != nil {
			return nil, err
		}
		val, err := e.value(it.Value(), false)
		if err != nil {
			return nil, at(err, keySegment(k))
		}
		obj.setAs(orig, jsontext.ValidUTF8(k), val)
	}
	e.leave(key)
	return obj.m, nil
}

// stringMap is mapValue for a map[string]any, without reflection.
func (e *encoder) stringMap(m map[string]any) (any, error) {
	if m == nil {
		return nil, nil
	}
	var key any
	if e.depth++; e.depth > trackDepth {
		var err error
		if key, err = e.track(reflect.ValueOf(m)); err != nil {
			return nil, err
		}
	}
	obj := newObject(len(m))
	for k, x := range m {
		val, err := e.anyValue(x)
		if err != nil {
			return nil, at(err, keySegment(k))
		}
		obj.set(k, val)
	}
	e.leave(key)
	return obj.m, nil
}

// list is the slice conversion for a []any, without reflection.
func (e *encoder) list(s []any) (any, error) {
	if s == nil {
		return nil, nil
	}
	var key any
	if e.depth++; e.depth > trackDepth {
		var err error
		if key, err = e.track(reflect.ValueOf(s)); err != nil {
			return nil, err
		}
	}
	out := make([]any, len(s))
	for i, x := range s {
		val, err := e.anyValue(x)
		if err != nil {
			return nil, at(err, indexSegment(i))
		}
		out[i] = val
	}
	e.leave(key)
	return out, nil
}

// elems converts the elements of the slice or array v.
func (e *encoder) elems(v reflect.Value) ([]any, error) {
	out := make([]any, v.Len())
	for i := range out {
		val, err := e.value(v.Index(i), false)
		if err != nil {
			return nil, at(err, indexSegment(i))
		}
		out[i] = val
	}
	return out, nil
}

// enter is called before descending into the pointer, map or slice v; the
// returned key is passed to leave. Deeper than trackDepth, it records v on
// the current path and fails if v is on it already.
func (e *encoder) enter(v reflect.Value) (any, error) {
	if e.depth++; e.depth <= trackDepth {
		return nil, nil
	}
	return e.track(v)
}

// track records the pointer, map or slice v on the current path, failing if
// v is on it already: the value contains itself. It identifies v as
// encoding/json does.
func (e *encoder) track(v reflect.Value) (any, error) {
	var key any
	switch v.Kind() {
	case reflect.Map:
		key = v.UnsafePointer()
	case reflect.Slice:
		key = sliceKey{v.UnsafePointer(), v.Len()}
	default:
		key = pointerKey{v.UnsafePointer(), v.Type()}
	}
	if _, seen := e.path[key]; seen {
		return nil, failf("encountered a cycle via %s", v.Type())
	}
	if e.path == nil {
		e.path = make(map[any]struct{})
	}
	e.path[key] = struct{}{}
	return key, nil
}

// leave undoes enter.
func (e *encoder) leave(key any) {
	if key != nil {
		delete(e.path, key)
	}
	e.depth--
}

// sliceKey identifies a slice: its first element and its length.
type sliceKey struct {
	data any // an unsafe.Pointer
	n    int
}

// pointerKey identifies a pointer: its address and type.
type pointerKey struct {
	addr any // an unsafe.Pointer
	typ  reflect.Type
}

// fieldByIndex returns the field of the struct v at index, following
// embedded pointers. It reports false when one of them is nil: encoding/json
// then leaves the field out.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

// keyName returns the object key the map key k is sent as: the key
// encoding/json makes of it, except that a time.Time key — also behind a
// pointer, in an interface, or embedded in the key type (see callMarshaler)
// — is in UTC, as a time.Time value is. orig is the key encoding/json makes
// of it, which decides between keys that come out the same.
func keyName(k reflect.Value) (key, orig string, err error) {
	if k.Kind() == reflect.String {
		return k.String(), k.String(), nil
	}
	if k.CanInterface() {
		x := k.Interface()
		if _, ok := x.(encoding.TextMarshaler); ok {
			if k.Kind() == reflect.Pointer && k.IsNil() {
				return "", "", nil
			}
			m, err := callMarshaler(x, true)
			switch {
			case err != nil:
				return "", "", failf("error calling MarshalText for a map key of type %s: %v", k.Type(), err)
			case m.isNil:
				return "", "", nil // as for a nil pointer key
			case m.isTime:
				return utc(m.t), m.t.Format(time.RFC3339Nano), nil
			}
			return string(m.b), string(m.b), nil
		}
	} else if k.Type().Implements(textMarshalerType) {
		return "", "", unreachableMethod(k.Type(), "MarshalText")
	}
	switch k.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := strconv.FormatInt(k.Int(), 10)
		return s, s, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		s := strconv.FormatUint(k.Uint(), 10)
		return s, s, nil
	}
	return "", "", failf("unsupported map key type %s", k.Type())
}

// object builds the map[string]any a map converts to. Its keys are coerced
// to valid UTF-8 as strings are, and a time.Time key is in UTC. When two keys
// come out the same, the entry encoding/json would keep wins: it writes the
// keys in sorted order, and a decoder keeps the last of duplicate keys. So
// the result does not depend on the order of map iteration (unless the keys
// encoding/json makes are the same too, as for two pointer keys to equal
// values; then neither does encoding/json's).
type object struct {
	m    map[string]any
	orig map[string]string // the original key of each entry whose key differs from it
}

func newObject(n int) object { return object{m: make(map[string]any, n)} }

// set stores val under key, coerced to valid UTF-8.
func (o *object) set(key string, val any) { o.setAs(key, jsontext.ValidUTF8(key), val) }

// setAs stores val under key, which the original key orig is sent as.
func (o *object) setAs(orig, key string, val any) {
	if key == orig && o.orig == nil {
		// No key differs from its original, so no two keys can be the same.
		o.m[key] = val
		return
	}
	if _, dup := o.m[key]; dup {
		prev := key
		if p, ok := o.orig[key]; ok {
			prev = p
		}
		if orig < prev {
			return
		}
	}
	o.m[key] = val
	if key != orig {
		if o.orig == nil {
			o.orig = make(map[string]string)
		}
		o.orig[key] = orig
	} else {
		delete(o.orig, key)
	}
}

// utc returns t as an RFC 3339 string in UTC, with as many fractional
// digits as it needs.
func utc(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// timeString converts v, a time.Time.
func timeString(v reflect.Value) (any, error) {
	if !v.CanInterface() {
		return nil, unreachableMethod(v.Type(), "MarshalJSON")
	}
	return utc(v.Interface().(time.Time)), nil
}

// floatValue converts a float of the given bit size.
func floatValue(f float64, bits int, quoted bool) (any, error) {
	if quoted {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, failf("%s cannot be sent in a field with the \",string\" option", strconv.FormatFloat(f, 'g', -1, bits))
		}
		// The JSON number, as a string.
		var b []byte
		if bits == 32 {
			b, _ = json.Marshal(float32(f))
		} else {
			b, _ = json.Marshal(f)
		}
		return string(b), nil
	}
	if bits == 32 {
		return widen(float32(f)), nil
	}
	return f, nil
}

// widen converts f to the float64 nearest to f's shortest decimal form, the
// value encoding/json writes and fmt prints for it: float32(0.1) becomes 0.1,
// not 0.10000000149011612. NaN and ±Inf stay what they are.
func widen(f float32) float64 {
	w := float64(f)
	if math.IsNaN(w) || math.IsInf(w, 0) {
		return w
	}
	w, _ = strconv.ParseFloat(strconv.FormatFloat(w, 'g', -1, 32), 64)
	return w
}

// numberValue converts a json.Number as encoding/json encodes it.
func numberValue(s string, quoted bool) (any, error) {
	if s == "" {
		s = "0" // encoding/json writes the zero Number as 0
	}
	if !isNumberLiteral(s) {
		return nil, failf("invalid number literal %q", s)
	}
	if quoted {
		return s, nil
	}
	n, err := jsonNumber(json.Number(s))
	if err != nil {
		return nil, failf("%v", err)
	}
	return n, nil
}

// isNumberLiteral reports whether s is a JSON number. A JSON value that
// starts with '-' or a digit and ends with a digit is a number: json.Valid
// checks the rest of the grammar.
func isNumberLiteral(s string) bool {
	return s != "" && (s[0] == '-' || '0' <= s[0] && s[0] <= '9') &&
		'0' <= s[len(s)-1] && s[len(s)-1] <= '9' && json.Valid([]byte(s))
}

// marshal encodes v through its MarshalJSON method, or through its
// MarshalText method if text is set, as encoding/json does (see
// callMarshaler); how says whether the method is called on v or on v's
// address. For v of interface type, it is the method of the value inside.
func marshal(v reflect.Value, how method, text bool) (any, error) {
	name := "MarshalJSON"
	if text {
		name = "MarshalText"
	}
	if !v.CanInterface() {
		return nil, unreachableMethod(v.Type(), name)
	}
	rcv := v
	if how == pointerMethod {
		rcv = v.Addr()
	}
	m, err := callMarshaler(rcv.Interface(), text)
	if err != nil {
		return nil, failf("error calling %s for type %s: %v", name, v.Type(), err)
	}
	switch {
	case m.isNil:
		return nil, nil
	case m.isTime:
		return utc(m.t), nil
	case text:
		return jsontext.ValidUTF8(string(m.b)), nil
	}
	out, err := fromJSONText(m.b)
	if err != nil {
		return nil, failf("error calling MarshalJSON for type %s: %v", v.Type(), err)
	}
	return out, nil
}

// marshaled is what callMarshaler encodes a value as: the output of its
// method, a time.Time, or nil.
type marshaled struct {
	b      []byte
	t      time.Time
	isTime bool
	isNil  bool
}

// callMarshaler calls the MarshalJSON method of x, or its MarshalText method
// if text is set, as encoding/json calls it, except that:
//
//   - A time.Time or *time.Time comes back as the time, to be sent in UTC.
//     So does a value whose method returns just what the method of the
//     time.Time it embeds returns, as time.Time's promoted method does:
//     encoding/json encodes such a value as that time.
//   - Where encoding/json calls the method on a nil pointer — x is one, or
//     the method is promoted through a nil embedded pointer or interface —
//     and the method fails on it with a run-time error, as a value receiver
//     or a dereferencing pointer receiver does, the value comes back as nil
//     instead of panicking (fmt prints "<nil>" for such a value). As reflect
//     does not say whether a method is promoted, a run-time error while an
//     embedded pointer or interface that has the method is nil is taken as
//     one. Any other panic is passed on, as encoding/json passes it on.
func callMarshaler(x any, text bool) (marshaled, error) {
	switch t := x.(type) {
	case time.Time:
		return marshaled{t: t, isTime: true}, nil
	case *time.Time:
		if t == nil {
			return marshaled{isNil: true}, nil // its methods have value receivers
		}
		return marshaled{t: *t, isTime: true}, nil
	}
	iface, ok := marshalerType, false
	if text {
		iface = textMarshalerType
		_, ok = x.(encoding.TextMarshaler)
	} else {
		_, ok = x.(json.Marshaler)
	}
	if !ok {
		return marshaled{isNil: true}, nil // encoding/json writes null
	}
	var nilRcv func() bool // whether a run-time error is the method failing on a nil pointer
	v := reflect.ValueOf(x)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if v.Type().Elem().Implements(iface) {
				// A method of the type pointed to: called on the nil pointer,
				// it panics whatever it does.
				return marshaled{isNil: true}, nil
			}
			nilRcv = func() bool { return true }
		} else {
			v = v.Elem()
		}
	}
	var ti *typeInfo
	if v.Kind() == reflect.Struct {
		ti = infoOf(v.Type())
		if len(ti.nilable) > 0 {
			nilRcv = func() bool { return nilOn(v, ti.nilable) }
		}
	}
	b, isNil, err := call(x, text, nilRcv)
	if isNil {
		return marshaled{isNil: true}, nil
	}
	if ti != nil && ti.timeIndex != nil {
		if t, ok := timeAt(v, ti.timeIndex); ok {
			tb, _, terr := call(t, text, nil)
			if bytes.Equal(b, tb) && sameError(err, terr) {
				return marshaled{t: t, isTime: true}, nil
			}
		}
	}
	return marshaled{b: b}, err
}

// call calls the MarshalJSON method of x, or its MarshalText method if text
// is set. With nilRcv, a run-time error the method panics with, if nilRcv
// says a nil pointer is involved, makes it report isNil instead; any other
// panic is passed on.
func call(x any, text bool, nilRcv func() bool) (b []byte, isNil bool, err error) {
	if nilRcv != nil {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(runtime.Error); !ok || !nilRcv() {
					panic(r)
				}
				b, isNil, err = nil, true, nil
			}
		}()
	}
	if text {
		b, err = x.(encoding.TextMarshaler).MarshalText()
	} else {
		b, err = x.(json.Marshaler).MarshalJSON()
	}
	return b, false, err
}

// sameError reports whether a and b are both nil, or say the same.
func sameError(a, b error) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Error() == b.Error()
}

// timeAt returns the time.Time (or *time.Time) at index in the struct v,
// following embedded pointers; false when a nil pointer is on the way.
func timeAt(v reflect.Value, index []int) (time.Time, bool) {
	f, ok := fieldByIndex(v, index)
	if !ok {
		return time.Time{}, false
	}
	if f.Kind() == reflect.Pointer {
		if f.IsNil() {
			return time.Time{}, false
		}
		f = f.Elem()
	}
	if !f.CanInterface() {
		return time.Time{}, false
	}
	t, ok := f.Interface().(time.Time)
	return t, ok
}

// nilOn reports whether one of the embedded pointers and interfaces at
// indexes in the struct v is nil, holds a nil pointer, or lies behind a nil
// pointer.
func nilOn(v reflect.Value, indexes [][]int) bool {
	for _, index := range indexes {
		f, ok := fieldByIndex(v, index)
		if !ok {
			return true
		}
		if f.Kind() == reflect.Interface && !f.IsNil() {
			f = f.Elem()
		}
		if (f.Kind() == reflect.Pointer || f.Kind() == reflect.Interface) && f.IsNil() {
			return true
		}
	}
	return false
}

// fromJSONText decodes the JSON text b, converting numbers as fromJSON does.
func fromJSONText(b []byte) (any, error) {
	if !json.Valid(b) {
		var discard any
		if err := json.Unmarshal(b, &discard); err != nil {
			return nil, err // says what is wrong where
		}
		return nil, errors.New("invalid JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
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

var (
	marshalerType     = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
	isZeroerType      = reflect.TypeFor[interface{ IsZero() bool }]()
	timeType          = reflect.TypeFor[time.Time]()
	numberType        = reflect.TypeFor[json.Number]()
	anyMapType        = reflect.TypeFor[map[string]any]()
	anySliceType      = reflect.TypeFor[[]any]()
)

// method says how a type has the method of an interface.
type method uint8

const (
	noMethod      method = iota
	valueMethod          // the type implements the interface
	pointerMethod        // only the pointer type does: usable on an addressable value
)

// methodOf says how the type t (not a pointer type) has the method of the
// interface type iface.
func methodOf(t, iface reflect.Type) method {
	switch {
	case t.Implements(iface):
		return valueMethod
	case t.Kind() != reflect.Pointer && t.Kind() != reflect.Interface && reflect.PointerTo(t).Implements(iface):
		return pointerMethod
	}
	return noMethod
}

// typeInfo is what the walk needs to know about a type (other than a pointer
// type), worked out once per type.
type typeInfo struct {
	marshalJSON method        // how the type has MarshalJSON
	marshalText method        // how the type has MarshalText
	bytes       bool          // a slice type sent as []byte
	keysOK      bool          // a map type whose key type encoding/json accepts
	fields      []structField // the fields of a struct type that are encoded
	// Of a struct type with MarshalJSON or MarshalText (see promotedFrom):
	timeIndex []int   // the embedded time.Time that may have promoted them
	nilable   [][]int // the embedded pointers and interfaces they may be promoted through
}

var typeInfos sync.Map // reflect.Type → *typeInfo

func infoOf(t reflect.Type) *typeInfo {
	if ti, ok := typeInfos.Load(t); ok {
		return ti.(*typeInfo)
	}
	ti, _ := typeInfos.LoadOrStore(t, newTypeInfo(t))
	return ti.(*typeInfo)
}

func newTypeInfo(t reflect.Type) *typeInfo {
	ti := &typeInfo{
		marshalJSON: methodOf(t, marshalerType),
		marshalText: methodOf(t, textMarshalerType),
	}
	switch t.Kind() {
	case reflect.Slice:
		// A []byte, or a slice of a byte type without marshal methods.
		if t.Elem().Kind() == reflect.Uint8 {
			p := reflect.PointerTo(t.Elem())
			ti.bytes = !p.Implements(marshalerType) && !p.Implements(textMarshalerType)
		}
	case reflect.Map:
		switch t.Key().Kind() {
		case reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			ti.keysOK = true
		default:
			ti.keysOK = t.Key().Implements(textMarshalerType)
		}
	case reflect.Struct:
		ti.fields = structFields(t)
		if ti.marshalJSON != noMethod || ti.marshalText != noMethod {
			ti.timeIndex, ti.nilable = promotedFrom(t)
		}
	}
	return ti
}

// promotedFrom works out where the struct type t may have its MarshalJSON
// and MarshalText methods from, by Go's rules for promoted methods. A method
// is promoted from the shallowest depth at which t embeds a type that has
// it — directly, or through embedded structs — if only one type has it
// there; a method t declares itself comes first. reflect does not say where
// a method comes from, so the result is what can be told from the fields:
//
//   - timeIndex is the index sequence of the time.Time (or *time.Time) t
//     embeds at the shallowest depth it embeds one at, if it embeds only one
//     there: time.Time's methods are promoted to t from it unless t, or a
//     shallower embedded type, has its own.
//   - nilable are the index sequences of the embedded pointers and
//     interfaces (down to that depth) that have one of the methods: a method
//     promoted through one of them that is nil panics.
func promotedFrom(t reflect.Type) (timeIndex []int, nilable [][]int) {
	type embedded struct {
		typ   reflect.Type
		index []int
	}
	var (
		current, next    = []embedded{}, []embedded{{typ: t}}
		count, nextCount map[reflect.Type]int // how often each struct is embedded at the current and next depth
		visited          = map[reflect.Type]bool{}
		times            int // how often a time.Time is embedded at the current depth
	)
	for len(next) > 0 && times == 0 {
		current, next = next, current[:0]
		count, nextCount = nextCount, map[reflect.Type]int{}
		for _, s := range current {
			if visited[s.typ] {
				continue
			}
			visited[s.typ] = true
			for i := range s.typ.NumField() {
				sf := s.typ.Field(i)
				if !sf.Anonymous {
					continue
				}
				index := append(slices.Clip(s.index), i)
				ft := sf.Type
				switch ft.Kind() {
				case reflect.Pointer, reflect.Interface:
					if ft.Implements(marshalerType) || ft.Implements(textMarshalerType) {
						nilable = append(nilable, index)
					}
				}
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				switch {
				case ft == timeType:
					times += max(count[s.typ], 1)
					timeIndex = index
				case ft.Kind() == reflect.Struct:
					if nextCount[ft]++; nextCount[ft] == 1 {
						next = append(next, embedded{typ: ft, index: index})
					}
				}
			}
		}
	}
	if times != 1 {
		timeIndex = nil
	}
	return timeIndex, nilable
}

// structField is a struct field the walk encodes.
type structField struct {
	name      string
	tagged    bool  // the name comes from the json tag
	index     []int // the field's index sequence, through embedded structs
	omitEmpty bool
	omitZero  bool
	quoted    bool // the ",string" option, on a field whose kind takes it
	// isZero is the omitzero test through the field type's IsZero method,
	// nil for a type without one (reflect.Value.IsZero is used then).
	isZero func(reflect.Value) (bool, error)
}

// structFields returns the fields encoding/json encodes for the struct type
// t, in field order. It mirrors encoding/json's typeFields: the exported
// fields, and those of embedded structs (also through unexported embedded
// struct types and embedded pointers) unless the embedded field has a name
// in its tag, named by their json tag or else their Go name; fields tagged
// "-" are left out. Of several fields with the same name, the least nested
// one wins; at the same depth a tagged one wins over untagged ones; when
// that does not single one out, all of them are left out.
func structFields(t reflect.Type) []structField {
	type embedded struct {
		typ   reflect.Type
		index []int
	}
	var (
		fields           []structField
		current, next    = []embedded{}, []embedded{{typ: t}}
		count, nextCount map[reflect.Type]int // how often each struct is embedded at the current and next depth
		visited          = map[reflect.Type]bool{}
	)
	for len(next) > 0 {
		current, next = next, current[:0]
		count, nextCount = nextCount, map[reflect.Type]int{}
		for _, s := range current {
			if visited[s.typ] {
				continue
			}
			visited[s.typ] = true
			for i := range s.typ.NumField() {
				sf := s.typ.Field(i)
				if sf.Anonymous {
					et := sf.Type
					if et.Kind() == reflect.Pointer {
						et = et.Elem()
					}
					if !sf.IsExported() && et.Kind() != reflect.Struct {
						continue // nothing to promote from an unexported non-struct type
					}
				} else if !sf.IsExported() {
					continue
				}
				tag := sf.Tag.Get("json")
				if tag == "-" {
					continue
				}
				name, opts, _ := strings.Cut(tag, ",")
				if !isValidTag(name) {
					name = ""
				}
				index := append(slices.Clip(s.index), i)
				ft := sf.Type
				if ft.Name() == "" && ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if name == "" && sf.Anonymous && ft.Kind() == reflect.Struct {
					// An embedded struct: its fields are promoted, one depth down.
					if nextCount[ft]++; nextCount[ft] == 1 {
						next = append(next, embedded{typ: ft, index: index})
					}
					continue
				}
				f := structField{
					name:      name,
					tagged:    name != "",
					index:     index,
					omitEmpty: hasOption(opts, "omitempty"),
					omitZero:  hasOption(opts, "omitzero"),
					quoted:    hasOption(opts, "string") && quotable(ft.Kind()),
				}
				if f.name == "" {
					f.name = sf.Name
				}
				if f.omitZero {
					f.isZero = zeroFunc(sf.Type)
				}
				fields = append(fields, f)
				if count[s.typ] > 1 {
					// The struct is embedded more than once at this depth: a
					// second copy of the field makes it ambiguous below.
					fields = append(fields, f)
				}
			}
		}
	}
	// Sort by name, then least nested first, then tagged first.
	slices.SortFunc(fields, func(a, b structField) int {
		if c := strings.Compare(a.name, b.name); c != 0 {
			return c
		}
		if c := cmp.Compare(len(a.index), len(b.index)); c != 0 {
			return c
		}
		if a.tagged != b.tagged {
			if a.tagged {
				return -1
			}
			return 1
		}
		return slices.Compare(a.index, b.index)
	})
	out := fields[:0]
	for i := 0; i < len(fields); {
		j := i + 1
		for j < len(fields) && fields[j].name == fields[i].name {
			j++
		}
		// fields[i] wins its name unless fields[i+1] is as nested and as tagged.
		if j == i+1 || len(fields[i].index) != len(fields[i+1].index) || fields[i].tagged != fields[i+1].tagged {
			out = append(out, fields[i])
		}
		i = j
	}
	slices.SortFunc(out, func(a, b structField) int { return slices.Compare(a.index, b.index) })
	return out
}

// isValidTag reports whether s can name a field: encoding/json's rule.
func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", c):
			// Backslash and quote characters are reserved, but other
			// punctuation is allowed in a field name.
		case !unicode.IsLetter(c) && !unicode.IsDigit(c):
			return false
		}
	}
	return true
}

// hasOption reports whether the comma-separated tag options opts include name.
func hasOption(opts, name string) bool {
	for opts != "" {
		var o string
		o, opts, _ = strings.Cut(opts, ",")
		if o == name {
			return true
		}
	}
	return false
}

// quotable reports whether the ",string" option applies to a field of kind k.
func quotable(k reflect.Kind) bool {
	switch k {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	}
	return false
}

// isEmptyValue is encoding/json's omitempty test.
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

// zeroFunc returns encoding/json's omitzero test for a field of type t that
// has an IsZero method, or nil when t has none.
func zeroFunc(t reflect.Type) func(reflect.Value) (bool, error) {
	switch {
	case t.Kind() == reflect.Interface && t.Implements(isZeroerType):
		return func(v reflect.Value) (bool, error) {
			// Not called on a nil interface, or a nil pointer in one.
			if v.IsNil() || v.Elem().Kind() == reflect.Pointer && v.Elem().IsNil() {
				return true, nil
			}
			return callIsZero(v)
		}
	case t.Kind() == reflect.Pointer && t.Implements(isZeroerType):
		return func(v reflect.Value) (bool, error) {
			if v.IsNil() {
				return true, nil
			}
			return callIsZero(v)
		}
	case t.Implements(isZeroerType):
		return callIsZero
	case reflect.PointerTo(t).Implements(isZeroerType):
		return func(v reflect.Value) (bool, error) {
			if !v.CanAddr() {
				if !v.CanInterface() {
					return false, unreachableMethod(v.Type(), "IsZero")
				}
				// Copy the value to call the method on a pointer to it.
				c := reflect.New(v.Type()).Elem()
				c.Set(v)
				v = c
			}
			return callIsZero(v.Addr())
		}
	}
	return nil
}

func callIsZero(v reflect.Value) (bool, error) {
	if !v.CanInterface() {
		return false, unreachableMethod(v.Type(), "IsZero")
	}
	return v.Interface().(interface{ IsZero() bool }).IsZero(), nil
}

// payloadError reports a part of a payload value that cannot be sent, and
// where in the value it is.
type payloadError struct {
	path []string // field, key and index segments, innermost first
	err  error
}

// maxPathSegments bounds the path an error message shows: a value that
// contains itself fails a thousand levels down.
const maxPathSegments = 16

// Error implements error: "<path>: <problem>", e.g.
// `readings[2].callback: unsupported type func()`. A longer path than
// maxPathSegments is shortened in the middle.
func (e *payloadError) Error() string {
	n := len(e.path)
	if n == 0 {
		return e.err.Error()
	}
	var b strings.Builder
	for j := 0; j < n; j++ { // outermost segment first
		seg := e.path[n-1-j]
		if n > maxPathSegments && j == maxPathSegments/2 {
			j = n - maxPathSegments/2
			seg = "…" + strings.TrimPrefix(e.path[n-1-j], ".")
		}
		b.WriteString(seg)
	}
	return strings.TrimPrefix(b.String(), ".") + ": " + e.err.Error()
}

func (e *payloadError) Unwrap() error { return e.err }

func failf(format string, args ...any) error {
	return &payloadError{err: fmt.Errorf(format, args...)}
}

// at returns err located in the field, key or index seg of a value.
func at(err error, seg string) error {
	var pe *payloadError
	if !errors.As(err, &pe) {
		pe = &payloadError{err: err}
	}
	pe.path = append(pe.path, seg)
	return pe
}

// keySegment is the path segment of a field or map key: .name, or ["name"]
// when the name is not an ASCII identifier.
func keySegment(name string) string {
	if isIdentifier(name) {
		return "." + name
	}
	return "[" + strconv.Quote(name) + "]"
}

func isIdentifier(s string) bool {
	for i, c := range s {
		switch {
		case c == '_', 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z':
		case i > 0 && '0' <= c && c <= '9':
		default:
			return false
		}
	}
	return s != ""
}

func indexSegment(i int) string { return "[" + strconv.Itoa(i) + "]" }

// unreachableMethod is the error for a method encoding/json would call (or a
// time.Time it would read) on a value it can only reach through an
// unexported embedded field; reflection cannot do that, and encoding/json
// panics.
func unreachableMethod(t reflect.Type, name string) error {
	return failf("cannot call %s of %s: it is reached through an unexported embedded field", name, t)
}

// normalizeRows validates a bulk batch — a non-empty slice or array whose
// elements are rows (maps with string keys, or structs) — and returns it as
// []any of map[string]any, each row normalized as PublishToTable normalizes
// a row passed to it.
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
	var e encoder
	out := make([]any, rv.Len())
	for i := range out {
		// Each row is taken as a value, as PublishToTable takes one: not
		// addressable, so a MarshalJSON or MarshalText method with a pointer
		// receiver is not used for it (as for json.Marshal(row), not
		// json.Marshal(rows)), and a row is sent the same alone and in a
		// batch.
		row := rv.Index(i).Interface()
		n, err := e.anyValue(row)
		if err != nil {
			return nil, fmt.Errorf("row at index %d: %v", i, err)
		}
		m, ok := n.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("row at index %d must be a map or struct, got %s", i, describeRow(row, n))
		}
		out[i] = m
	}
	return out, nil
}

// describeRow says what row is, a bulk row that normalized to n, which is
// not a map.
func describeRow(row, n any) string {
	if row == nil {
		return "nil"
	}
	v := reflect.ValueOf(row)
	if name := marshalMethod(v); name != "" {
		return fmt.Sprintf("%s, which its %s method encodes as %s", v.Type(), name, shape(n))
	}
	if n == nil {
		return "a nil " + v.Type().String()
	}
	return v.Type().String()
}

// marshalMethod returns the name of the method normalize encodes v through,
// MarshalJSON or MarshalText, or "" if it uses none.
func marshalMethod(v reflect.Value) string {
	addressable := false
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v, addressable = v.Elem(), true
	}
	ti := infoOf(v.Type())
	switch {
	case ti.marshalJSON == valueMethod || ti.marshalJSON == pointerMethod && addressable:
		return "MarshalJSON"
	case ti.marshalText == valueMethod || ti.marshalText == pointerMethod && addressable:
		return "MarshalText"
	}
	return ""
}

// shape names what kind of value n, a normalized value, is.
func shape(n any) string {
	switch n.(type) {
	case nil:
		return "null"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case []any:
		return "a list"
	case []byte:
		return "binary data"
	}
	return "a number"
}

// DecodeRows converts rows (as returned by GetHistory and friends) into
// typed values through encoding/json, matching columns to json tags. The
// data backend sends timestamp columns, tsp included, as epoch milliseconds:
// decode them into an int64 (time.UnixMilli converts it).
//
// NaN and ±Inf (which the Python SDK sends as they are, e.g. for a failed
// sensor read) decode as JSON null does: the field keeps its zero value, or
// is nil when it is a pointer — declare it as *float64 to tell such a value
// from 0.
func DecodeRows[T any](rows []Row) ([]T, error) {
	data, err := json.Marshal(finite.OrNil(rows))
	if err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	var out []T
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	return out, nil
}

// Decode converts a payload value (an event argument, a call result, a row)
// into T through encoding/json, decoding NaN and ±Inf as DecodeRows does.
func Decode[T any](v any) (T, error) {
	var out T
	data, err := json.Marshal(finite.OrNil(v))
	if err != nil {
		return out, fmt.Errorf("decode: %w", err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("decode: %w", err)
	}
	return out, nil
}
