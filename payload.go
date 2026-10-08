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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/RecordEvolution/ironflock-go/internal/finite"
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
// value with a MarshalJSON or MarshalText method is encoded through it (the
// JSON a MarshalJSON method returns is decoded, a number written as an
// integer becoming int64, or uint64 above math.MaxInt64, any other float64);
// strings are coerced to valid UTF-8, each invalid byte becoming U+FFFD.
// Unlike encoding/json:
//
//   - Floats stay float64, NaN and ±Inf included, as the Python SDK sends
//     them; an integral float such as 2.0 stays a float. A float32 is widened
//     through its shortest decimal form, so float32(0.1) is sent as 0.1.
//     Only a field with the ",string" option, which asks for the JSON number
//     in a string, cannot hold NaN or ±Inf.
//   - []byte stays []byte (msgpack bin, like Python bytes) instead of
//     becoming a base64 string.
//   - A time.Time, wherever it is, becomes an RFC 3339 string in UTC
//     ("2026-01-02T03:04:05.123Z") instead of keeping its zone offset; the
//     Python and JavaScript SDKs send their timestamps in UTC too.
//
// Like encoding/json, normalize fails on channels, functions, complex
// numbers, maps with other key types, and values that contain themselves.
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
		return validUTF8(x), nil
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
		return marshalJSON(v, ti.marshalJSON)
	}
	if ti.marshalText == valueMethod || ti.marshalText == pointerMethod && v.CanAddr() {
		return marshalText(v, ti.marshalText)
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
		return validUTF8(v.String()), nil
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
	// (e.g. a field of type json.Marshaler): encoding/json calls that method
	// of the value inside, except that a time.Time inside is still sent in
	// UTC and a nil pointer inside as nil.
	elem := v.Elem()
	if elem.Kind() == reflect.Pointer {
		if elem.IsNil() {
			return nil, nil
		}
		if elem.Type().Elem() == timeType {
			return timeString(elem.Elem())
		}
	} else if elem.Type() == timeType {
		return timeString(elem)
	}
	if ti.marshalJSON != noMethod {
		return marshalJSON(v, valueMethod)
	}
	return marshalText(v, valueMethod)
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
		k, err := keyName(it.Key())
		if err != nil {
			return nil, err
		}
		val, err := e.value(it.Value(), false)
		if err != nil {
			return nil, at(err, keySegment(k))
		}
		obj.set(k, val)
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

// keyName returns the JSON object key encoding/json makes of the map key k.
func keyName(k reflect.Value) (string, error) {
	if k.Kind() == reflect.String {
		return k.String(), nil
	}
	if k.CanInterface() {
		if tm, ok := k.Interface().(encoding.TextMarshaler); ok {
			if k.Kind() == reflect.Pointer && k.IsNil() {
				return "", nil
			}
			b, err := tm.MarshalText()
			if err != nil {
				return "", failf("error calling MarshalText for a map key of type %s: %v", k.Type(), err)
			}
			return string(b), nil
		}
	} else if k.Type().Implements(textMarshalerType) {
		return "", unreachableMethod(k.Type(), "MarshalText")
	}
	switch k.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(k.Uint(), 10), nil
	}
	return "", failf("unsupported map key type %s", k.Type())
}

// object builds the map[string]any a map converts to. Its keys are coerced
// to valid UTF-8 as strings are. When two keys coerce to the same string, the
// entry encoding/json would keep wins — it writes the keys in sorted order,
// and a decoder keeps the last of duplicate keys — so the result does not
// depend on the order of map iteration.
type object struct {
	m    map[string]any
	orig map[string]string // the original key of each entry whose key was coerced
}

func newObject(n int) object { return object{m: make(map[string]any, n)} }

// set stores val under key.
func (o *object) set(key string, val any) {
	k := validUTF8(key)
	if k == key && o.orig == nil {
		// No key was coerced, so no two keys can be the same.
		o.m[k] = val
		return
	}
	if _, dup := o.m[k]; dup {
		prev := k
		if p, ok := o.orig[k]; ok {
			prev = p
		}
		if key < prev {
			return
		}
	}
	o.m[k] = val
	if k != key {
		if o.orig == nil {
			o.orig = make(map[string]string)
		}
		o.orig[k] = key
	} else {
		delete(o.orig, k)
	}
}

// validUTF8 returns s with each byte that is not part of a valid UTF-8
// sequence replaced by U+FFFD, as encoding/json does. Python's msgpack
// decoder refuses a str that is not valid UTF-8, losing the whole message.
func validUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	b := make([]byte, 0, len(s)+2*utf8.UTFMax)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = utf8.AppendRune(b, utf8.RuneError)
		} else {
			b = append(b, s[i:i+size]...)
		}
		i += size
	}
	return string(b)
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

// marshalJSON encodes v through its MarshalJSON method (how says whether
// through v's pointer) and decodes the JSON it returns.
func marshalJSON(v reflect.Value, how method) (any, error) {
	if !v.CanInterface() {
		return nil, unreachableMethod(v.Type(), "MarshalJSON")
	}
	rcv := v
	if how == pointerMethod {
		rcv = v.Addr()
	}
	m, ok := rcv.Interface().(json.Marshaler)
	if !ok {
		return nil, nil
	}
	b, err := m.MarshalJSON()
	if err == nil {
		var out any
		if out, err = fromJSONText(b); err == nil {
			return out, nil
		}
	}
	return nil, failf("error calling MarshalJSON for type %s: %v", v.Type(), err)
}

// marshalText encodes v through its MarshalText method (how says whether
// through v's pointer) as a string.
func marshalText(v reflect.Value, how method) (any, error) {
	if !v.CanInterface() {
		return nil, unreachableMethod(v.Type(), "MarshalText")
	}
	rcv := v
	if how == pointerMethod {
		rcv = v.Addr()
	}
	m, ok := rcv.Interface().(encoding.TextMarshaler)
	if !ok {
		return nil, nil
	}
	b, err := m.MarshalText()
	if err != nil {
		return nil, failf("error calling MarshalText for type %s: %v", v.Type(), err)
	}
	return validUTF8(string(b)), nil
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
	}
	return ti
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

// normalizeRows validates a bulk batch — a non-empty slice whose elements
// are rows (maps with string keys, or structs) — and returns it as []any of
// map[string]any, each row normalized as json.Marshal(rows) would encode it.
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
		elem := rv.Index(i)
		n, err := e.value(elem, false)
		if err != nil {
			return nil, fmt.Errorf("row at index %d: %v", i, err)
		}
		row, ok := n.(map[string]any)
		if !ok {
			if elem.Kind() == reflect.Interface {
				elem = elem.Elem()
			}
			got := "nil"
			if elem.IsValid() {
				got = elem.Type().String()
				if n == nil {
					got = "a nil " + got
				}
			}
			return nil, fmt.Errorf("row at index %d must be a map or struct, got %s", i, got)
		}
		out[i] = row
	}
	return out, nil
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
