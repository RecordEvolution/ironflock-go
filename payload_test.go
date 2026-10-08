package ironflock

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/gammazero/nexus/v3/transport/serialize"
)

func TestNormalizeIntegers(t *testing.T) {
	cases := []struct {
		in   any
		want any
	}{
		{-7, int64(-7)},
		{int8(-3), int64(-3)},
		{int16(-3), int64(-3)},
		{int32(-3), int64(-3)},
		{uint8(5), int64(5)},
		{uint16(5), int64(5)},
		{uint32(5), int64(5)},
		{uint(5), int64(5)},
		{uint64(5), int64(5)},
		{uintptr(5), int64(5)},
		{pjInt(-9), int64(-9)},
		{pjUint(math.MaxUint64), uint64(math.MaxUint64)},
		{uint64(math.MaxInt64), int64(math.MaxInt64)},
		{uint64(math.MaxUint64), uint64(math.MaxUint64)}, // only above MaxInt64
		{float32(1.5), 1.5},
		{json.Number("12"), int64(12)},
		{json.Number("18446744073709551615"), uint64(math.MaxUint64)},
		{json.Number("1.25"), 1.25},
		{json.Number("2.0"), 2.0}, // a number with a fraction is a float
		{json.Number(""), int64(0)},
	}
	for _, tc := range cases {
		got, err := normalize(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("normalize(%T %v) = %T %v, %v; want %T %v", tc.in, tc.in, got, got, err, tc.want, tc.want)
		}
	}
}

func TestNormalizeKeepsLargeIntegers(t *testing.T) {
	type counter struct {
		Big   uint64 `json:"big"`
		Small uint8  `json:"small"`
		Neg   int64  `json:"neg"`
	}
	got, err := normalize(counter{Big: math.MaxUint64, Small: 7, Neg: math.MinInt64})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"big": uint64(math.MaxUint64), "small": int64(7), "neg": int64(math.MinInt64)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestNormalizeNestedValues(t *testing.T) {
	type inner struct {
		N int `json:"n"`
	}
	var nilPtr *inner
	got, err := normalize(map[string]any{
		"list":   []any{inner{N: 1}, nilPtr, []string{"a"}},
		"kwargs": Kwargs{"k": uint8(1)},
		"bytes":  []byte("raw"),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"list":   []any{map[string]any{"n": int64(1)}, nil, []any{"a"}},
		"kwargs": map[string]any{"k": int64(1)},
		"bytes":  []byte("raw"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v\nwant %#v", got, want)
	}
	for _, bad := range []any{func() {}, make(chan int), complex(1, 2), map[string]any{"x": func() {}}} {
		if _, err := normalize(bad); err == nil {
			t.Errorf("normalize(%T) succeeded", bad)
		}
	}
}

func TestSplitArgsErrorsAreInvalidArguments(t *testing.T) {
	_, _, _, err := splitArgs([]any{Kwargs{"cb": func() {}}})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), `kwarg "cb"`) {
		t.Errorf("kwarg: %v", err)
	}
	_, _, _, err = splitArgs([]any{1, make(chan int)})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "argument 1") {
		t.Errorf("argument: %v", err)
	}
	if got := invalidParams("publish", err).Error(); !strings.HasPrefix(got, "ironflock: invalid argument: Invalid publish parameters: argument 1") {
		t.Errorf("invalidParams: %q", got)
	}
	// Several Kwargs merge, later keys win; a *CallOptions is taken as is.
	opts := &CallOptions{DiscloseMe: true}
	pos, kw, co, err := splitArgs([]any{Kwargs{"a": 1, "b": 1}, Kwargs{"b": 2}, opts})
	if err != nil || pos != nil || co != opts || !reflect.DeepEqual(kw, map[string]any{"a": int64(1), "b": int64(2)}) {
		t.Errorf("pos %#v kw %#v opts %v err %v", pos, kw, co, err)
	}
}

func TestDecodeHelpers(t *testing.T) {
	type reading struct {
		Temp float64 `json:"temp"`
	}
	rows, err := DecodeRows[reading]([]Row{{"temp": 21.5}, {"temp": int64(3)}})
	if err != nil || !reflect.DeepEqual(rows, []reading{{21.5}, {3}}) {
		t.Errorf("DecodeRows: %#v %v", rows, err)
	}
	if _, err := DecodeRows[reading]([]Row{{"temp": "hot"}}); err == nil {
		t.Error("DecodeRows accepted a mistyped column")
	}
	r, err := Decode[reading](map[string]any{"temp": 1.5})
	if err != nil || r.Temp != 1.5 {
		t.Errorf("Decode: %#v %v", r, err)
	}
	// What normalize sends decodes back into the same struct.
	type full struct {
		F  float64           `json:"f"`
		I  int64             `json:"i"`
		U  uint64            `json:"u"`
		B  []byte            `json:"b"`
		T  time.Time         `json:"t"`
		S  []string          `json:"s"`
		M  map[string]string `json:"m"`
		P  *float64          `json:"p"`
		Ok bool              `json:"ok"`
	}
	half := 0.5
	in := full{F: 2, I: -3, U: math.MaxUint64, B: []byte{0, 255}, T: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
		S: []string{"x"}, M: map[string]string{"k": "v"}, P: &half, Ok: true}
	sent, err := normalize(in)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Decode[full](sent)
	if err != nil || !reflect.DeepEqual(back, in) {
		t.Errorf("round trip: %+v, %v\nwant %+v", back, err, in)
	}
}

// ---- findings 11 and 30, and the other differences from encoding/json ----

// sameFloat reports whether got is a float64 equal to want, NaN matching NaN.
func sameFloat(got any, want float64) bool {
	f, ok := got.(float64)
	if !ok {
		return false
	}
	if math.IsNaN(want) {
		return math.IsNaN(f)
	}
	return f == want
}

// Finding 11: NaN and ±Inf travel as float64 however the value is built — a
// Row, a struct, a typed map or slice — as the Python SDK sends them.
func TestNormalizeNonFiniteFloatsEverywhere(t *testing.T) {
	type reading struct {
		T float64 `json:"t"`
	}
	type reading32 struct {
		T float32 `json:"t"`
	}
	type readingPtr struct {
		T *float64 `json:"t"`
	}
	type readingAny struct {
		T any `json:"t"`
	}
	for _, x := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		x := x
		objects := map[string]any{
			"Row":                Row{"t": x},
			"Kwargs":             Kwargs{"t": x},
			"map[string]float64": map[string]float64{"t": x},
			"map[string]float32": map[string]float32{"t": float32(x)},
			"struct":             reading{T: x},
			"*struct":            &reading{T: x},
			"float32 field":      reading32{T: float32(x)},
			"*float64 field":     readingPtr{T: &x},
			"any field":          readingAny{T: x},
		}
		for name, in := range objects {
			got, err := normalize(in)
			if err != nil {
				t.Errorf("%v %s: %v", x, name, err)
				continue
			}
			if m, ok := got.(map[string]any); !ok || !sameFloat(m["t"], x) {
				t.Errorf("%v %s: got %#v", x, name, got)
			}
		}
		lists := map[string]any{
			"[]any":      []any{x},
			"[]float64":  []float64{x},
			"[1]float64": [1]float64{x},
			"[]float32":  []float32{float32(x)},
		}
		for name, in := range lists {
			got, err := normalize(in)
			if err != nil {
				t.Errorf("%v %s: %v", x, name, err)
				continue
			}
			if l, ok := got.([]any); !ok || len(l) != 1 || !sameFloat(l[0], x) {
				t.Errorf("%v %s: got %#v", x, name, got)
			}
		}
		// A batch with one non-finite reading is sent whole.
		rows, err := normalizeRows([]reading{{T: 1}, {T: x}})
		if err != nil || len(rows) != 2 || !sameFloat(rows[1].(map[string]any)["t"], x) {
			t.Errorf("%v normalizeRows: %#v, %v", x, rows, err)
		}
		pos, kw, _, err := splitArgs([]any{reading{T: x}, Kwargs{"r": reading{T: x}}})
		if err != nil || !sameFloat(pos[0].(map[string]any)["t"], x) || !sameFloat(kw["r"].(map[string]any)["t"], x) {
			t.Errorf("%v splitArgs: %#v %#v, %v", x, pos, kw, err)
		}
	}
}

// Finding 11 through the public API: a struct batch with a NaN reading is
// published whole, as the equivalent []Row batch is.
func TestBulkWritesSendNonFiniteReadings(t *testing.T) {
	type reading struct {
		Temperature float64 `json:"temperature"`
	}
	f := flock(t)
	if err := f.PublishRowsToTable(bg, "sensordata", []reading{{Temperature: 1}, {Temperature: math.NaN()}}); err != nil {
		t.Fatal(err)
	}
	if err := f.PublishToTable(bg, "sensordata", reading{Temperature: math.Inf(1)}); err != nil {
		t.Fatal(err)
	}
	pubs := f.own.allPublishes()
	if len(pubs) != 2 {
		t.Fatalf("%d publications, want 2", len(pubs))
	}
	batch := pubs[0].Args[0].([]any)
	if len(batch) != 2 || !sameFloat(batch[0].(map[string]any)["temperature"], 1) ||
		!sameFloat(batch[1].(map[string]any)["temperature"], math.NaN()) {
		t.Errorf("batch %#v", batch)
	}
	if !sameFloat(pubs[1].Args[0].(map[string]any)["temperature"], math.Inf(1)) {
		t.Errorf("row %#v", pubs[1].Args)
	}
}

// Finding 11, receiving side: a non-finite value (as a Python app sends it)
// decodes as JSON null instead of failing the whole result.
func TestDecodeNonFiniteFloatsAsNull(t *testing.T) {
	type reading struct {
		Temperature float64  `json:"temperature"`
		Humidity    *float64 `json:"humidity"`
	}
	rows := []Row{
		{"temperature": 21.5, "humidity": 40.0},
		{"temperature": math.NaN(), "humidity": math.Inf(1)},
		{"temperature": math.Inf(-1), "humidity": 41.0},
	}
	got, err := DecodeRows[reading](rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Temperature != 21.5 || got[0].Humidity == nil || *got[0].Humidity != 40 ||
		got[1].Temperature != 0 || got[1].Humidity != nil ||
		got[2].Temperature != 0 || got[2].Humidity == nil || *got[2].Humidity != 41 {
		t.Errorf("DecodeRows: %+v", got)
	}
	if !math.IsNaN(rows[1]["temperature"].(float64)) {
		t.Error("DecodeRows modified its input")
	}
	r, err := Decode[reading](map[string]any{"temperature": math.NaN(), "humidity": 3.0})
	if err != nil || r.Temperature != 0 || r.Humidity == nil || *r.Humidity != 3 {
		t.Errorf("Decode: %+v, %v", r, err)
	}
	list, err := Decode[[]*float64]([]any{1.0, math.NaN()})
	if err != nil || len(list) != 2 || list[0] == nil || *list[0] != 1 || list[1] != nil {
		t.Errorf("Decode list: %v, %v", list, err)
	}
}

// strings returns every string in a normalized value, in no particular order.
func stringsIn(v any) []string {
	switch x := v.(type) {
	case string:
		return []string{x}
	case []any:
		var out []string
		for _, e := range x {
			out = append(out, stringsIn(e)...)
		}
		return out
	case map[string]any:
		var out []string
		for _, e := range x {
			out = append(out, stringsIn(e)...)
		}
		return out
	}
	return nil
}

// Finding 30: a time.Time is sent as an RFC 3339 string in UTC wherever it
// is, not only at the top level.
func TestNormalizeTimeIsUTCEverywhere(t *testing.T) {
	at := time.Date(2026, 10, 8, 14, 0, 0, 123e6, time.FixedZone("CEST", 2*3600))
	const want = "2026-10-08T12:00:00.123Z"
	type withTime struct {
		Tsp time.Time `json:"tsp"`
	}
	type withPtr struct {
		Tsp *time.Time `json:"tsp"`
	}
	type nested struct {
		Inner withTime `json:"inner"`
	}
	type withAny struct {
		Tsp any `json:"tsp"`
	}
	type withMarshaler struct {
		Tsp json.Marshaler `json:"tsp"`
	}
	type withText struct {
		Tsp encoding.TextMarshaler `json:"tsp"`
	}
	cases := map[string]any{
		"time.Time":            at,
		"*time.Time":           &at,
		"Row":                  Row{"tsp": at},
		"struct":               withTime{Tsp: at},
		"*struct":              &withTime{Tsp: at},
		"*time.Time field":     withPtr{Tsp: &at},
		"nested struct":        nested{Inner: withTime{Tsp: at}},
		"any field":            withAny{Tsp: at},
		"json.Marshaler field": withMarshaler{Tsp: at},
		"TextMarshaler field":  withText{Tsp: &at},
		"map[string]time.Time": map[string]time.Time{"tsp": at},
		"[]time.Time":          []time.Time{at},
		"[1]time.Time":         [1]time.Time{at},
		"map holding a struct": map[string]any{"r": withTime{Tsp: at}},
		"[]*time.Time":         []*time.Time{&at},
	}
	for name, in := range cases {
		got, err := normalize(in)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if s := stringsIn(got); len(s) != 1 || s[0] != want {
			t.Errorf("%s: %#v, want %q", name, got, want)
		}
	}
	rows, err := normalizeRows([]withTime{{Tsp: at}})
	if err != nil || !reflect.DeepEqual(rows, []any{map[string]any{"tsp": want}}) {
		t.Errorf("normalizeRows: %#v, %v", rows, err)
	}
	pos, kw, _, err := splitArgs([]any{withTime{Tsp: at}, Kwargs{"r": withTime{Tsp: at}}})
	if err != nil || !reflect.DeepEqual(pos, []any{map[string]any{"tsp": want}}) ||
		!reflect.DeepEqual(kw, map[string]any{"r": map[string]any{"tsp": want}}) {
		t.Errorf("splitArgs: %#v %#v, %v", pos, kw, err)
	}
	got, err := normalize(withPtr{})
	if err != nil || !reflect.DeepEqual(got, map[string]any{"tsp": nil}) {
		t.Errorf("nil *time.Time: %#v, %v", got, err)
	}
}

// Floats stay floats: a float field holding 2.0 is sent as the float 2.0,
// as a Row holding it is and as Python sends it, not as the integer 2.
func TestNormalizeFloatsStayFloats(t *testing.T) {
	type mixed struct {
		F   float64 `json:"f"`
		G   float32 `json:"g"`
		I   int     `json:"i"`
		Neg float64 `json:"neg"`
	}
	got, err := normalize(mixed{F: 2, G: 3, I: 4, Neg: -5})
	want := map[string]any{"f": float64(2), "g": float64(3), "i": int64(4), "neg": float64(-5)}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("struct: %#v, %v", got, err)
	}
	for _, in := range []any{[]float64{2}, [1]float64{2}, []any{2.0}, []float32{2}} {
		if got, err := normalize(in); err != nil || !reflect.DeepEqual(got, []any{float64(2)}) {
			t.Errorf("%T: %#v, %v", in, got, err)
		}
	}
	if got, err := normalize(map[string]float64{"f": 2}); err != nil || !reflect.DeepEqual(got, map[string]any{"f": float64(2)}) {
		t.Errorf("typed map: %#v, %v", got, err)
	}
	// A float32 is widened through its shortest decimal form, the value
	// encoding/json and fmt show for it.
	if got, err := normalize(float32(0.1)); err != nil || got != 0.1 {
		t.Errorf("float32(0.1): %#v, %v", got, err)
	}
	if got, err := normalize(map[string]float32{"g": 22.3}); err != nil || !reflect.DeepEqual(got, map[string]any{"g": 22.3}) {
		t.Errorf("float32 map: %#v, %v", got, err)
	}
}

// []byte stays []byte (msgpack bin, like Python bytes) wherever it is,
// instead of encoding/json's base64 string.
func TestNormalizeBytesStayBytes(t *testing.T) {
	type blob []byte
	type withBytes struct {
		Data  []byte `json:"data"`
		Named blob   `json:"named"`
		None  []byte `json:"none"`
	}
	data := []byte("raw\x00\xff")
	got, err := normalize(withBytes{Data: data, Named: blob("b")})
	want := map[string]any{"data": []byte("raw\x00\xff"), "named": []byte("b"), "none": nil}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, %v", got, err)
	}
	// The result shares no memory with the caller's slice.
	data[0] = 'X'
	if got.(map[string]any)["data"].([]byte)[0] != 'r' {
		t.Error("normalize returned the caller's byte slice")
	}
	for _, in := range []any{Row{"data": []byte("b")}, map[string][]byte{"data": []byte("b")}} {
		if got, err := normalize(in); err != nil || !reflect.DeepEqual(got, map[string]any{"data": []byte("b")}) {
			t.Errorf("%T: %#v, %v", in, got, err)
		}
	}
}

// Strings are sent as valid UTF-8, as encoding/json makes them: each invalid
// byte becomes U+FFFD. Python's msgpack decoder refuses a str that is not
// UTF-8 and drops the whole message.
func TestNormalizeCoercesStringsToUTF8(t *testing.T) {
	type text struct {
		S string `json:"s"`
	}
	for _, in := range []any{Row{"s": "a\xffb"}, text{S: "a\xffb"}, map[string]string{"s": "a\xffb"}} {
		if got, err := normalize(in); err != nil || !reflect.DeepEqual(got, map[string]any{"s": "a�b"}) {
			t.Errorf("%T: %#v, %v", in, got, err)
		}
	}
	if got, err := normalize(Row{"k\xfe": 1}); err != nil || !reflect.DeepEqual(got, map[string]any{"k�": int64(1)}) {
		t.Errorf("key: %#v, %v", got, err)
	}
	_, kw, _, err := splitArgs([]any{Kwargs{"k\xfe": "v\xfe"}})
	if err != nil || !reflect.DeepEqual(kw, map[string]any{"k�": "v�"}) {
		t.Errorf("kwargs: %#v, %v", kw, err)
	}
}

// A cyclic value is an error, as it is for encoding/json — never a stack
// overflow, which would kill the process.
func TestNormalizeRejectsCycles(t *testing.T) {
	type node struct {
		Next *node `json:"next"`
	}
	n := &node{}
	n.Next = n
	m := map[string]any{}
	m["self"] = m
	s := []any{nil}
	s[0] = s
	type holder struct {
		M map[string]any `json:"m"`
	}
	for name, in := range map[string]any{
		"pointer":          n,
		"map[string]any":   m,
		"[]any":            s,
		"struct with map":  holder{M: m},
		"Row inside []any": []any{Row{"m": m}},
	} {
		_, err := normalize(in)
		if err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Errorf("%s: %v, want a cycle error", name, err)
		}
	}
}

// Errors say where in the value the problem is.
func TestNormalizeErrorsSayWhere(t *testing.T) {
	type leaf struct {
		Callback func() `json:"callback"`
	}
	type mid struct {
		Readings []leaf `json:"readings"`
	}
	type quoted struct {
		F float64 `json:"f,string"`
	}
	type node struct {
		Next *node `json:"next"`
	}
	loop := &node{}
	loop.Next = loop
	cases := []struct {
		in   any
		want string
	}{
		{mid{Readings: []leaf{{}}}, "readings[0].callback: unsupported type func()"},
		{Row{"a b": []any{1, make(chan int)}}, `["a b"][1]: unsupported type chan int`},
		{[]any{Row{"ok": 1, "bad": complex(1, 2)}}, "[0].bad: unsupported type complex128"},
		{map[string]pjFailJSON{"x": {}}, "x: error calling MarshalJSON for type ironflock.pjFailJSON: boom"},
		{pjRawJSON("{"), "error calling MarshalJSON for type ironflock.pjRawJSON: unexpected end of JSON input"},
		{pjRawJSON("1 2"), "error calling MarshalJSON for type ironflock.pjRawJSON: invalid character '2' after top-level value"},
		{[]pjFailText{{}}, "[0]: error calling MarshalText for type ironflock.pjFailText: text boom"},
		{map[pjFailKey]int{1: 1}, "error calling MarshalText for a map key of type ironflock.pjFailKey: key boom"},
		{map[bool]int{true: 1}, "unsupported type map[bool]int"},
		{quoted{F: math.NaN()}, `f: NaN cannot be sent in a field with the ",string" option`},
		{quoted{F: math.Inf(-1)}, `f: -Inf cannot be sent in a field with the ",string" option`},
		{json.Number("abc"), `invalid number literal "abc"`},
		{map[string]any{"n": json.Number("1e400")}, `n: invalid number "1e400"`},
		{unsafe.Pointer(nil), "unsupported type unsafe.Pointer"},
		{loop, "next.next.next.next.next.next.next.next…next.next.next.next.next.next.next.next: encountered a cycle via *ironflock.node"},
	}
	for _, c := range cases {
		_, err := normalize(c.in)
		if err == nil || err.Error() != c.want {
			t.Errorf("normalize(%T): %v\nwant %s", c.in, err, c.want)
		}
	}
	_, _, _, err := splitArgs([]any{mid{Readings: []leaf{{}}}})
	if err == nil || err.Error() != "ironflock: invalid argument: argument 0: readings[0].callback: unsupported type func()" {
		t.Errorf("splitArgs: %v", err)
	}
}

// pjAmbiguousJSON embeds two unexported structs with a MarshalJSON method
// under tag names: neither method is promoted (they conflict at the same
// depth), so each field is encoded through its own method — which
// reflection cannot call on a value reached through an unexported embedded
// field. encoding/json panics on such types; normalize reports an error.
type pjAmbiguousJSON struct {
	pjHiddenJSON1 `json:"a"`
	pjHiddenJSON2 `json:"b"`
}

type pjHiddenJSON1 struct{ N int }

func (pjHiddenJSON1) MarshalJSON() ([]byte, error) { return []byte(`"hidden"`), nil }

type pjHiddenJSON2 struct{ N int }

func (pjHiddenJSON2) MarshalJSON() ([]byte, error) { return []byte(`"hidden"`), nil }

// pjAmbiguousText is pjAmbiguousJSON with MarshalText.
type pjAmbiguousText struct {
	pjHiddenText1 `json:"a"`
	pjHiddenText2 `json:"b"`
}

type pjHiddenText1 struct{ N int }

func (pjHiddenText1) MarshalText() ([]byte, error) { return []byte("hidden"), nil }

type pjHiddenText2 struct{ N int }

func (pjHiddenText2) MarshalText() ([]byte, error) { return []byte("hidden"), nil }

// pjAmbiguousZero is pjAmbiguousJSON with IsZero, for omitzero.
type pjAmbiguousZero struct {
	pjHiddenZero1 `json:"a,omitzero"`
	pjHiddenZero2 `json:"b,omitzero"`
}

type pjHiddenZero1 struct{ N int }

func (pjHiddenZero1) IsZero() bool { return false }

type pjHiddenZero2 struct{ N int }

func (pjHiddenZero2) IsZero() bool { return false }

// normalize does not panic where encoding/json does, and returns nil for a
// nil pointer inside a json.Marshaler interface, where encoding/json calls
// the method on the nil pointer.
func TestNormalizeWhereEncodingJSONPanics(t *testing.T) {
	type marshalerField struct {
		M json.Marshaler `json:"m"`
	}
	panics := func(v any) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		_, _ = json.Marshal(v)
		return false
	}
	for _, v := range []any{pjAmbiguousJSON{}, &pjAmbiguousJSON{}, pjAmbiguousText{}, pjAmbiguousZero{}} {
		if !panics(v) {
			t.Errorf("%T: encoding/json no longer panics; add it to the differential corpus", v)
		}
		_, err := normalize(v)
		if err == nil || !strings.Contains(err.Error(), "reached through an unexported embedded field") {
			t.Errorf("%T: %v", v, err)
		}
	}
	nilInside := marshalerField{M: (*pjJSONVal)(nil)}
	if !panics(nilInside) {
		t.Error("encoding/json no longer panics on a nil pointer in a json.Marshaler field")
	}
	if got, err := normalize(nilInside); err != nil || !reflect.DeepEqual(got, map[string]any{"m": nil}) {
		t.Errorf("nil pointer in a json.Marshaler: %#v, %v", got, err)
	}
}

// ---- normalize against encoding/json ----

// viaJSON is the reference normalize is checked against: the JSON
// encoding/json makes of v, decoded with integral numbers as int64 (uint64
// above math.MaxInt64) and other numbers as float64.
func viaJSON(v any) (any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return refNumbers(out)
}

func refNumbers(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
			return i, nil
		}
		if u, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return u, nil
		}
		f, err := strconv.ParseFloat(x.String(), 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case []any:
		for i, e := range x {
			n, err := refNumbers(e)
			if err != nil {
				return nil, err
			}
			x[i] = n
		}
	case map[string]any:
		for k, e := range x {
			n, err := refNumbers(e)
			if err != nil {
				return nil, err
			}
			x[k] = n
		}
	}
	return v, nil
}

// diffJSON describes how got, normalize's result for a value, differs from
// want, viaJSON's for the same value, allowing for normalize's documented
// differences: []byte stays bytes instead of a base64 string; a time.Time
// is sent in UTC; and a float stays a float — where encoding/json wrote an
// integral float as an integer, lenient accepts the float, otherwise the
// caller must use non-integral floats only. It returns "" when they agree.
func diffJSON(got, want any, lenient bool) string {
	return diffAt("", got, want, lenient)
}

func diffAt(path string, got, want any, lenient bool) string {
	bad := fmt.Sprintf("at %q: got %#v (%T), want %#v (%T)", path, got, got, want, want)
	switch w := want.(type) {
	case nil:
		if got != nil {
			return bad
		}
	case bool:
		if g, ok := got.(bool); !ok || g != w {
			return bad
		}
	case string:
		switch g := got.(type) {
		case string:
			if g != w {
				// A time.Time: normalize sends the same instant in UTC.
				t, err := time.Parse(time.RFC3339Nano, w)
				if err != nil || g != t.UTC().Format(time.RFC3339Nano) {
					return bad
				}
			}
		case []byte:
			if base64.StdEncoding.EncodeToString(g) != w {
				return bad
			}
		default:
			return bad
		}
	case int64:
		switch g := got.(type) {
		case int64:
			if g != w {
				return bad
			}
		case float64:
			// encoding/json writes an integral float as its shortest decimal
			// form: the float nearest to that integer is the float itself.
			if !lenient || float64(w) != g {
				return bad
			}
		default:
			return bad
		}
	case uint64:
		switch g := got.(type) {
		case uint64:
			if g != w {
				return bad
			}
		case float64:
			if !lenient || float64(w) != g {
				return bad
			}
		default:
			return bad
		}
	case float64:
		if g, ok := got.(float64); !ok || g != w {
			return bad
		}
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return bad
		}
		for i := range w {
			if d := diffAt(fmt.Sprintf("%s[%d]", path, i), g[i], w[i], lenient); d != "" {
				return d
			}
		}
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok || len(g) != len(w) {
			return bad
		}
		for k, wv := range w {
			gv, ok := g[k]
			if !ok {
				return fmt.Sprintf("at %q: key %q missing from %#v", path, k, got)
			}
			if d := diffAt(path+"."+k, gv, wv, lenient); d != "" {
				return d
			}
		}
	default:
		return fmt.Sprintf("at %q: unexpected reference value %#v", path, want)
	}
	return ""
}

// checkLikeJSON fails the test unless normalize(v) and viaJSON(v) both fail,
// or agree (see diffJSON). It also checks the shape of normalize's result.
func checkLikeJSON(t *testing.T, name string, v any, lenient bool) {
	t.Helper()
	got, err := normalize(v)
	want, wantErr := viaJSON(v)
	switch {
	case err != nil && wantErr != nil:
	case wantErr != nil:
		t.Errorf("%s: normalize gave %#v; encoding/json fails: %v", name, got, wantErr)
	case err != nil:
		t.Errorf("%s: normalize failed: %v; encoding/json gives %#v", name, err, want)
	default:
		if d := diffJSON(got, want, lenient); d != "" {
			t.Errorf("%s: %s\n got  %#v\n want %#v", name, d, got, want)
		}
		if d := checkShape(got); d != "" {
			t.Errorf("%s: %s", name, d)
		}
	}
}

// checkShape describes where v is not made of the types normalize promises:
// nil, bool, valid UTF-8 strings, int64, uint64 above math.MaxInt64,
// float64, non-nil []byte, []any and map[string]any with valid UTF-8 keys.
func checkShape(v any) string {
	switch x := v.(type) {
	case nil, bool, int64, float64:
	case string:
		if !utf8.ValidString(x) {
			return fmt.Sprintf("invalid UTF-8 string %q", x)
		}
	case uint64:
		if x <= math.MaxInt64 {
			return fmt.Sprintf("uint64 %d fits an int64", x)
		}
	case []byte:
		if x == nil {
			return "typed nil []byte"
		}
	case []any:
		if x == nil {
			return "typed nil []any"
		}
		for _, e := range x {
			if d := checkShape(e); d != "" {
				return d
			}
		}
	case map[string]any:
		if x == nil {
			return "typed nil map"
		}
		for k, e := range x {
			if !utf8.ValidString(k) {
				return fmt.Sprintf("invalid UTF-8 key %q", k)
			}
			if d := checkShape(e); d != "" {
				return d
			}
		}
	default:
		return fmt.Sprintf("value of type %T", v)
	}
	return ""
}

// Types of the differential corpus.
type (
	pjInt     int
	pjString  string
	pjFloat   float64
	pjFloat32 float32
	pjBool    bool
	pjByte    byte
	pjBytes   []byte
	pjUint    uint64
	pjMap     map[string]int
	pjSlice   []string
	pjPtr     *int
	PjEmbInt  int
)

// pjJSONVal encodes itself (value receiver) as padded JSON holding numbers
// of every kind.
type pjJSONVal struct{ N int }

func (v pjJSONVal) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, ` {"n": %d, "two": 2.0, "half": 1.5, "exp": 1e3, "neg": -0,
		"list": [1, -2.5e-3, "s<>&", null, true, {}, []],
		"big": 18446744073709551615, "huge": 123456789012345678901234567890} `, v.N), nil
}

// pjJSONPtr encodes itself with a pointer receiver: encoding/json calls the
// method only on addressable values.
type pjJSONPtr struct{ N int }

func (v *pjJSONPtr) MarshalJSON() ([]byte, error) { return fmt.Appendf(nil, `"ptr-%d"`, v.N), nil }

type pjTextVal struct{ S string }

func (v pjTextVal) MarshalText() ([]byte, error) { return []byte("text:" + v.S), nil }

type pjTextPtr struct{ S string }

func (v *pjTextPtr) MarshalText() ([]byte, error) { return []byte("ptext:" + v.S), nil }

// pjBoth has both methods: MarshalJSON wins.
type pjBoth struct{ N int }

func (pjBoth) MarshalJSON() ([]byte, error) { return []byte(`"json"`), nil }
func (pjBoth) MarshalText() ([]byte, error) { return []byte("text"), nil }

// pjMixed has MarshalJSON on its pointer and MarshalText on its value: an
// addressable value uses the first, any other the second.
type pjMixed struct{ N int }

func (*pjMixed) MarshalJSON() ([]byte, error) { return []byte(`"mixed-json"`), nil }
func (pjMixed) MarshalText() ([]byte, error)  { return []byte("mixed-text"), nil }

// pjList is a slice type that encodes itself.
type pjList []int

func (l pjList) MarshalJSON() ([]byte, error) { return fmt.Appendf(nil, `{"len": %d}`, len(l)), nil }

// pjByteText is a byte type whose pointer has MarshalText: a []pjByteText is
// encoded element by element, not as bytes.
type pjByteText byte

func (b *pjByteText) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "b%d", *b), nil }

// pjRawJSON returns itself as its JSON, valid or not.
type pjRawJSON string

func (r pjRawJSON) MarshalJSON() ([]byte, error) { return []byte(r), nil }

// pjKey is a struct map key with MarshalText.
type pjKey struct{ A, B uint8 }

func (k pjKey) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "%d-%d", k.A, k.B), nil }

// pjIntKey is an integer map key with MarshalText, which wins over the number.
type pjIntKey int

func (k pjIntKey) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "k%d", int(k)), nil }

// pjStringKey is a string map key with MarshalText, which is not used:
// string keys are taken as they are.
type pjStringKey string

func (pjStringKey) MarshalText() ([]byte, error) { return []byte("never"), nil }

// pjPtrKey is a pointer map key with MarshalText; a nil key becomes "".
type pjPtrKey struct{ N int }

func (k *pjPtrKey) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "p%d", k.N), nil }

// pjZeroVal and pjZeroPtr are zero when N <= 0: omitzero asks their IsZero.
type pjZeroVal struct{ N int }

func (z pjZeroVal) IsZero() bool { return z.N <= 0 }

type pjZeroPtr struct{ N int }

func (z *pjZeroPtr) IsZero() bool { return z.N <= 0 }

type pjFailJSON struct{}

func (pjFailJSON) MarshalJSON() ([]byte, error) { return nil, errors.New("boom") }

type pjFailText struct{}

func (pjFailText) MarshalText() ([]byte, error) { return nil, errors.New("text boom") }

type pjFailKey int

func (pjFailKey) MarshalText() ([]byte, error) { return nil, errors.New("key boom") }

// pjTags has every kind of tag, malformed ones included.
//
//nolint:staticcheck // SA5008: the odd tags are what is tested.
type pjTags struct {
	Plain       int
	Named       int    `json:"named"`
	Dash        int    `json:"-"`
	DashComma   int    `json:"-,"`
	OnlyOptions int    `json:",omitempty"`
	Punct       string `json:"a-b.c:d#e"`
	Unicode     string `json:"ünï"`
	Invalid     string `json:"in\"valid"`
	Space       string `json:"with space"`
	TrailComma  string `json:"tc,"`
	Upper       int    `json:"KEY"`
	Lower       int    `json:"key"`
	Unknown     int    `json:"unknown,nosuchoption"`
	unexported  int
}

type pjOmitEmpty struct {
	Bool    bool           `json:",omitempty"`
	Int     int            `json:",omitempty"`
	Int8    int8           `json:",omitempty"`
	Uint    uint           `json:",omitempty"`
	Uintptr uintptr        `json:",omitempty"`
	Float   float64        `json:",omitempty"`
	Float32 float32        `json:",omitempty"`
	String  string         `json:",omitempty"`
	Bytes   []byte         `json:",omitempty"`
	Slice   []int          `json:",omitempty"`
	Map     map[string]int `json:",omitempty"`
	Array   [2]int         `json:",omitempty"`
	Array0  [0]int         `json:",omitempty"`
	Ptr     *int           `json:",omitempty"`
	Iface   any            `json:",omitempty"`
	Struct  struct{}       `json:",omitempty"`
	Time    time.Time      `json:",omitempty"`
	Number  json.Number    `json:",omitempty"`
	Named   pjInt          `json:",omitempty"`
	Marsh   pjJSONVal      `json:",omitempty"`
	MarshP  *pjJSONVal     `json:",omitempty"`
	Text    pjTextVal      `json:",omitempty"`
}

type pjOmitZero struct {
	Int     int                        `json:",omitzero"`
	Float   float64                    `json:",omitzero"`
	String  string                     `json:",omitzero"`
	Slice   []int                      `json:",omitzero"`
	Map     map[string]int             `json:",omitzero"`
	Array   [2]int                     `json:",omitzero"`
	Struct  struct{ A int }            `json:",omitzero"`
	Time    time.Time                  `json:",omitzero"`
	TimePtr *time.Time                 `json:",omitzero"`
	ZeroVal pjZeroVal                  `json:",omitzero"`
	ZeroPtr pjZeroPtr                  `json:",omitzero"`
	ZeroPP  *pjZeroPtr                 `json:",omitzero"`
	ZeroIf  interface{ IsZero() bool } `json:",omitzero"`
	Both    []int                      `json:",omitempty,omitzero"`
	Iface   any                        `json:",omitzero"`
}

// pjQuoted has the ",string" option on fields of every kind, also those it
// does not apply to.
//
//nolint:staticcheck // SA5008: the option on other kinds is what is tested.
type pjQuoted struct {
	Int     int         `json:",string"`
	Int8    int8        `json:",string"`
	Uint    uint64      `json:",string"`
	Float   float64     `json:",string"`
	Float32 float32     `json:",string"`
	Bool    bool        `json:",string"`
	String  string      `json:",string"`
	Ptr     *int        `json:",string"`
	NilPtr  *int        `json:",string"`
	PtrPtr  **int       `json:",string"`
	Named   pjInt       `json:",string"`
	NamedS  pjString    `json:",string"`
	NamedP  pjPtr       `json:",string"`
	Number  json.Number `json:",string"`
	Slice   []int       `json:",string"`
	Iface   any         `json:",string"`
	Marsh   pjJSONVal   `json:",string"`
	Text    pjTextVal   `json:",string"`
	Both    int         `json:"both,omitempty,string"`
}

type PjEmbA struct {
	A      int
	Shared string
}

type PjEmbB struct {
	B      int
	Shared string
}

// Shared is untagged at the same depth in both: it is left out.
type pjTwoEmbedded struct {
	PjEmbA
	PjEmbB
}

type PjTagged struct {
	X int `json:"Shared"`
}

// A tagged field wins over an untagged one at the same depth.
type pjTaggedWins struct {
	PjEmbA
	PjTagged
}

// A less nested field wins over embedded ones.
type pjOuterWins struct {
	PjEmbA
	PjEmbB
	Shared float64
}

// Embedded pointers, nil or not; the tagged one is a named field.
type pjEmbPointers struct {
	*PjEmbA
	*PjEmbB `json:"b"`
}

type pjhidden struct {
	Visible int
	hidden  int
}

type pjhiddenPtr struct{ ViaPointer int }

// Fields promoted through an unexported embedded struct and a pointer to one.
type pjUnexportedEmbedded struct {
	pjhidden
	*pjhiddenPtr
	Own int
}

// An unexported embedded struct named by its tag is a field of that name.
type pjUnexportedTagged struct {
	pjhidden `json:"hidden"`
}

// An embedded exported non-struct type is a field named after the type; an
// unexported one is left out; an embedded interface is a field.
type pjEmbeddedNonStruct struct {
	PjEmbInt
	pjInt
	fmt.Stringer
}

// PjCommon is embedded twice at the same depth (through PjLevelA and
// PjLevelB): its fields are ambiguous and left out.
type PjCommon struct {
	C int
	D int
}

type PjLevelA struct {
	PjCommon
	LA int
}

type PjLevelB struct {
	PjCommon
	LB int
}

type pjDiamond struct {
	PjLevelA
	PjLevelB
}

type PjDeep3 struct {
	X int `json:"x"`
	Y int
	Z int
}

type PjDeep2 struct {
	PjDeep3
	Y int `json:"Y"`
	W int
}

// Fields at three depths: of each name, the least nested one wins.
type pjDeep1 struct {
	PjDeep2
	Z string
}

type pjNode struct {
	Name     string    `json:"name"`
	Children []*pjNode `json:"children,omitempty"`
	Parent   *pjNode   `json:"-"`
}

// pjEmbedsJSON gets pjJSONVal's MarshalJSON promoted: it is encoded through it.
type pjEmbedsJSON struct {
	pjJSONVal
	Other int
}

// pjEmbedsTime gets time.Time's MarshalJSON promoted: it is encoded through
// it, zone and all — it is not a time.Time.
type pjEmbedsTime struct {
	time.Time
	Note string
}

// pjEmbedsPtrJSON gets MarshalJSON promoted to its pointer type only.
type pjEmbedsPtrJSON struct {
	pjJSONPtr
	Other int
}

// pjAddressable holds types whose methods have pointer receivers:
// encoding/json uses them only where the value is addressable.
type pjAddressable struct {
	J  pjJSONPtr
	T  pjTextPtr
	M  pjMixed
	A  [2]pjJSONPtr
	S  []pjJSONPtr
	MV map[string]pjJSONPtr
	Z  pjZeroPtr `json:",omitzero"`
	B  big.Int
	BP *big.Int
}

type pjInterfaces struct {
	M json.Marshaler
	T encoding.TextMarshaler
	S fmt.Stringer
	E error
	A any
}

type pjSpecial struct {
	Raw    json.RawMessage
	RawNil json.RawMessage
	RawPtr *json.RawMessage
	Num    json.Number
	Nums   []json.Number
	Time   time.Time
	TimeP  *time.Time
	Times  map[string]time.Time
	Dur    time.Duration
	Month  time.Month
	Big    *big.Int
	BigF   *big.Float
}

// pjCorpus returns the values normalize must encode as encoding/json does
// (up to the documented differences): every struct-tag rule, embedding,
// map keys, nil values, marshalers, special types, and values both refuse.
func pjCorpus() []struct {
	name string
	v    any
} {
	n, m := 7, 0
	pn := &n
	at := time.Date(2026, 10, 8, 14, 0, 0, 123456789, time.FixedZone("CEST", 2*3600))
	atUTC := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tree := &pjNode{Name: "root", Children: []*pjNode{{Name: "a"}, {Name: "b", Children: []*pjNode{{Name: "c"}}}}}
	tree.Children[0].Parent = tree
	addressable := pjAddressable{
		J: pjJSONPtr{1}, T: pjTextPtr{"t"}, M: pjMixed{2}, A: [2]pjJSONPtr{{3}, {4}},
		S: []pjJSONPtr{{5}}, MV: map[string]pjJSONPtr{"k": {6}}, Z: pjZeroPtr{1}, BP: big.NewInt(-42),
	}
	addressable.B.SetString("123456789012345678901234567890", 10)
	raw := json.RawMessage(`{"z": [1, 2.0, "s"]}`)
	bigF, _ := new(big.Float).SetString("1.5e100")
	cyclicMap := map[string]any{}
	cyclicMap["self"] = cyclicMap
	cyclicSlice := []any{nil}
	cyclicSlice[0] = cyclicSlice
	type pjCycle struct {
		Next *pjCycle
	}
	cyclicPtr := &pjCycle{}
	cyclicPtr.Next = cyclicPtr

	type c = struct {
		name string
		v    any
	}
	return []c{
		// Scalars.
		{"nil", nil},
		{"bools", []any{true, false, pjBool(true)}},
		{"strings", []string{"", "plain", "html <a href='x'>&amp;</a>", "  ", "quote \" backslash \\",
			"ctl \x00\x01\x1f\n\t", "héllo 世界 🎉", "a\xffb\xc0", "\xed\xa0\x80", "�", "2026-01-01T00:00:00+01:00"}},
		{"ints", []any{0, -1, int64(math.MinInt64), int64(math.MaxInt64), int8(-128), int16(1000), int32(-7), uint8(255),
			uint16(65535), uint32(math.MaxUint32), uint64(math.MaxUint64), uint64(1 << 63), uintptr(42), pjInt(3), pjUint(9)}},
		{"floats", []any{0.0, math.Copysign(0, -1), 1.5, -2.25, 2.0, 1e20, 1e21, 1e-7, 123456789.125, math.MaxFloat64,
			math.SmallestNonzeroFloat64, float64(1 << 63), float64(1 << 64), -float64(1 << 63), float64(1<<53 + 1), pjFloat(4)}},
		{"float32s", []any{float32(0.1), float32(1.1), float32(16777217), float32(3.4e38), float32(1e-7),
			float32(math.SmallestNonzeroFloat32), pjFloat32(22.3), float32(math.Copysign(0, -1))}},
		{"numbers", []json.Number{"12", "-7", "1.5", "1e3", "18446744073709551615", "", "0.000001", "-0", "2.0"}},
		{"times", []any{atUTC, at, time.Time{}, &at, (*time.Time)(nil), time.Unix(0, 1).In(time.FixedZone("x", -5*3600-1800))}},
		// Named and composite types.
		{"named", []any{pjString("s"), pjBytes("b"), pjMap{"a": 1}, pjSlice{"x"}, pjPtr(pn), pjPtr(nil), PjEmbInt(5)}},
		{"slices", []any{[]int{}, []int(nil), []string{"a"}, [][]int{nil, {}, {1}}, []*int{nil, pn}, []any{1, "a", nil, 2.5, []any{}, map[string]any{}}}},
		{"arrays", []any{[3]int{1, 2, 3}, [0]int{}, [2][]int{{1}, nil}, [4]byte{1, 2, 3, 255}, [2]*int{nil, pn}}},
		{"bytes", []any{[]byte(nil), []byte{}, []byte("bin\x00\xff"), [][]byte{[]byte("a"), nil}, []pjByte{1, 2}, pjBytes(nil)}},
		{"byte elements with marshal methods", []pjByteText{1, 2}},
		{"pointers", []any{pn, &pn, (*int)(nil), &[]int{1}, &map[string]int{"a": 1}, &PjEmbA{A: 1}}},
		{"maps", []any{
			map[string]int{"b": 2, "a": 1},
			map[string]int(nil),
			map[int]string{-5: "x", 10: "y"},
			map[uint8]bool{1: true},
			map[int64]float64{math.MinInt64: 1.5},
			map[uint64]int{math.MaxUint64: 1},
			map[uintptr]int{7: 1},
			map[pjString]int{"s": 1},
			map[pjKey]string{{1, 2}: "a", {3, 4}: "b"},
			map[pjIntKey]int{3: 1, -4: 2},
			map[pjStringKey]int{"s": 1},
			map[*pjPtrKey]int{nil: 1, {N: 2}: 2},
			map[string][]byte{"b": []byte("x")},
			map[string]map[string]any{"m": {"n": nil}},
			map[string]func(){},
		}},
		{"map keys that coerce to the same string", []any{
			map[string]int{"\xff": 1, "�": 2, "\xfe": 3, "ok": 4},
			map[string]int{"�": 2, "\xef": 5},
			Row{"a\xffb": "first", "a�b": "second"},
		}},
		{"rows", []any{
			Row{"rows": []Row{{"a": 1}, {"b": []any{Row{"c": pjTags{Plain: 1}}, nil}}}, "deep": Row{"x": Row{"y": []Row{nil, {}}}}},
			[]Row{{"temperature": 21.5, "tsp": at}, {"temperature": 2.0, "ok": true}},
			map[string]Row{"r": {"k": Kwargs{"x": 1}}},
			Kwargs{"kw": []Kwargs{{"a": 1}}},
		}},
		// Struct tags.
		{"tags", pjTags{Plain: 1, Named: 2, Dash: 3, DashComma: 4, OnlyOptions: 5, Punct: "p", Unicode: "u",
			Invalid: "i", Space: "s", TrailComma: "t", Upper: 6, Lower: 7, Unknown: 8, unexported: 9}},
		{"tags, zero", pjTags{}},
		{"omitempty, zero", pjOmitEmpty{}},
		{"omitempty, empty but not nil", pjOmitEmpty{Bytes: []byte{}, Slice: []int{}, Map: map[string]int{}, Iface: (*int)(nil)}},
		{"omitempty, set", pjOmitEmpty{Bool: true, Int: -1, Int8: 1, Uint: 1, Uintptr: 1, Float: 0.5, Float32: 1.5,
			String: "s", Bytes: []byte("b"), Slice: []int{1}, Map: map[string]int{"a": 1}, Array: [2]int{1, 0},
			Ptr: &m, Iface: 0, Time: at, Number: "1", Named: 1, MarshP: &pjJSONVal{1}}},
		{"omitzero, zero", pjOmitZero{}},
		{"omitzero, zero by IsZero", pjOmitZero{ZeroVal: pjZeroVal{-1}, ZeroPtr: pjZeroPtr{-1}, ZeroPP: &pjZeroPtr{}, ZeroIf: pjZeroVal{}}},
		{"omitzero, set", pjOmitZero{Int: 1, Float: 0.5, String: "s", Slice: []int{}, Map: map[string]int{}, Array: [2]int{0, 1},
			Struct: struct{ A int }{1}, Time: at, TimePtr: &time.Time{}, ZeroVal: pjZeroVal{1}, ZeroPtr: pjZeroPtr{1},
			ZeroPP: &pjZeroPtr{1}, ZeroIf: &pjZeroPtr{1}, Both: []int{}, Iface: false}},
		{"omitzero, addressable", &pjOmitZero{ZeroPtr: pjZeroPtr{-1}}},
		{"omitzero, nil pointer in an IsZero interface", pjOmitZero{ZeroIf: (*pjZeroPtr)(nil)}},
		{"string option", pjQuoted{Int: -1, Int8: 2, Uint: math.MaxUint64, Float: 1e21, Float32: 0.1, Bool: true,
			String: "a<b>&\"c\xff", Ptr: &n, PtrPtr: &pn, Named: 3, NamedS: "n", NamedP: pn, Number: "12",
			Slice: []int{1}, Iface: 1, Both: 4}},
		{"string option, zero", pjQuoted{}},
		{"string option, small float", pjQuoted{Float: 1e-7, Float32: 1e-7}},
		// Embedding.
		{"conflict at the same depth", pjTwoEmbedded{PjEmbA{1, "a"}, PjEmbB{2, "b"}}},
		{"tagged wins", pjTaggedWins{PjEmbA{1, "a"}, PjTagged{2}}},
		{"less nested wins", pjOuterWins{PjEmbA{1, "a"}, PjEmbB{2, "b"}, 3.5}},
		{"embedded pointers", pjEmbPointers{&PjEmbA{1, "a"}, &PjEmbB{2, "b"}}},
		{"nil embedded pointers", pjEmbPointers{}},
		{"through unexported embedded structs", pjUnexportedEmbedded{pjhidden{1, 2}, &pjhiddenPtr{3}, 4}},
		{"through a nil unexported embedded pointer", pjUnexportedEmbedded{pjhidden: pjhidden{1, 2}}},
		{"unexported embedded struct with a tag", pjUnexportedTagged{pjhidden{1, 2}}},
		{"embedded non-structs", pjEmbeddedNonStruct{PjEmbInt(1), pjInt(2), time.Duration(3)}},
		{"embedded twice", pjDiamond{PjLevelA{PjCommon{1, 2}, 3}, PjLevelB{PjCommon{4, 5}, 6}}},
		{"three depths", pjDeep1{PjDeep2{PjDeep3{1, 2, 3}, 4, 5}, "z"}},
		{"recursive type", tree},
		// Marshalers.
		{"MarshalJSON", []any{pjJSONVal{1}, &pjJSONVal{2}, (*pjJSONVal)(nil), pjList{1, 2}, pjList(nil), pjBoth{}}},
		{"MarshalJSON output", []pjRawJSON{` [1, 2.0] `, `"a<b"`, "\"a\xffb\"", `null`, `{"a": {"b": [true]}}`, `-0.0`, `1E2`}},
		{"MarshalText", []any{pjTextVal{"v"}, &pjTextVal{"p"}, &pjTextPtr{"q"}, pjMixed{1}, &pjMixed{2}, pjTextVal{"\xff"}}},
		{"pointer receivers, not addressable", addressable},
		{"pointer receivers, addressable", &addressable},
		{"pointer receivers in a slice", []pjAddressable{addressable}},
		{"pointer receivers in a map", map[string]pjAddressable{"a": addressable}},
		{"pointer receivers in an array", [1]pjAddressable{addressable}},
		{"pointer receivers in an addressable array", &[1]pjAddressable{addressable}},
		{"embedded MarshalJSON", pjEmbedsJSON{pjJSONVal{1}, 2}},
		{"embedded time.Time", pjEmbedsTime{at, "note"}},
		{"embedded pointer MarshalJSON", []any{pjEmbedsPtrJSON{pjJSONPtr{1}, 2}, &pjEmbedsPtrJSON{pjJSONPtr{3}, 4}}},
		{"interfaces", pjInterfaces{M: pjJSONVal{1}, T: pjBoth{}, S: time.Duration(1500), E: errors.New("x"), A: &pjJSONPtr{2}}},
		{"interfaces holding pointers", pjInterfaces{M: &pjMixed{}, T: &pjTextPtr{"p"}, S: tree.Children[1].Name2(), A: pjMixed{}}},
		{"nil interfaces", pjInterfaces{}},
		{"special types", pjSpecial{Raw: raw, RawPtr: &raw, Num: "-1.5e3", Nums: []json.Number{"1", "2.5"}, Time: at,
			TimeP: &at, Times: map[string]time.Time{"t": at}, Dur: time.Second, Month: time.March, Big: big.NewInt(7), BigF: bigF}},
		{"special types, zero", pjSpecial{}},
		{"json.RawMessage", []any{raw, json.RawMessage(nil), json.RawMessage("null"), []json.RawMessage{raw, nil}}},
		{"big numbers", []any{big.NewInt(-5), new(big.Int).Lsh(big.NewInt(1), 80), *big.NewInt(3), big.NewFloat(0.25)}},
		// Values both refuse.
		{"channel", make(chan int)},
		{"nil channel field", struct{ C chan int }{}},
		{"function", func() {}},
		{"complex", complex64(1)},
		{"unsafe pointer", unsafe.Pointer(&n)},
		{"bool map key", map[bool]int{true: 1}},
		{"float map key", map[float64]int{1: 1}},
		{"interface map key", map[any]int{"a": 1}},
		{"struct map key", map[[2]int]int{}},
		{"func in a slice", []any{1, func() {}}},
		{"empty slice of funcs", []func(){}},
		{"cyclic map", cyclicMap},
		{"cyclic slice", cyclicSlice},
		{"cyclic pointer", cyclicPtr},
		{"cyclic map in a struct", struct{ M map[string]any }{cyclicMap}},
		{"MarshalJSON fails", pjFailJSON{}},
		{"MarshalJSON returns invalid JSON", []pjRawJSON{"{"}},
		{"MarshalJSON returns trailing data", pjRawJSON("1 2")},
		{"MarshalJSON returns nothing", pjRawJSON("")},
		{"MarshalText fails", pjFailText{}},
		{"MarshalText of a key fails", map[pjFailKey]int{1: 1}},
		{"invalid number", json.Number("abc")},
		{"number with a space", json.Number(" 1")},
		{"number with a leading zero", json.Number("01")},
		{"number out of range", json.Number("1e400")},
		{"string option on NaN", pjQuoted{Float: math.NaN()}},
	}
}

// Name2 returns the name, as an fmt.Stringer.
func (n *pjNode) Name2() fmt.Stringer { return pjStringer(n.Name) }

type pjStringer string

func (s pjStringer) String() string { return string(s) }

// normalize encodes values as encoding/json does — every struct-tag rule,
// embedding, map keys, marshalers — except for its documented differences.
func TestNormalizeMatchesEncodingJSON(t *testing.T) {
	for _, c := range pjCorpus() {
		checkLikeJSON(t, c.name, c.v, true)
		// The same value at the top level and inside other values: in an
		// interface, a struct field, a map, a slice (where it is
		// addressable) and through a pointer.
		holder := struct{ V any }{c.v}
		checkLikeJSON(t, c.name+" in a struct", holder, true)
		checkLikeJSON(t, c.name+" in a Row", Row{"v": c.v}, true)
		checkLikeJSON(t, c.name+" in a list", []any{c.v}, true)
		checkLikeJSON(t, c.name+" through a pointer", &holder, true)
		if c.v != nil {
			slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(c.v)), 1, 1)
			slice.Index(0).Set(reflect.ValueOf(c.v))
			checkLikeJSON(t, c.name+" in a typed slice", slice.Interface(), true)
		}
	}
}

// ---- properties over random types and values ----

// pjRandom builds random types — struct types made with reflect.StructOf,
// with random field names, tags and embedding, inside slices, arrays, maps
// and pointers — and random values of them.
type pjRandom struct {
	types, values *rand.Rand
	// strict makes every float non-integral, so that comparing with
	// encoding/json needs no leniency to tell floats from integers.
	strict bool
	// quoted allows the ",string" option in tags.
	quoted bool
	// special is put in about every fourth float.
	special float64
}

// pjSentinel is the special float of finite values: exact in float32 and
// float64, not integral, and in no float pool.
const pjSentinel = 1234.5

var (
	pjLeafTypes = []reflect.Type{
		reflect.TypeFor[bool](), reflect.TypeFor[int](), reflect.TypeFor[int8](), reflect.TypeFor[int16](),
		reflect.TypeFor[int32](), reflect.TypeFor[int64](), reflect.TypeFor[uint](), reflect.TypeFor[uint8](),
		reflect.TypeFor[uint16](), reflect.TypeFor[uint32](), reflect.TypeFor[uint64](), reflect.TypeFor[uintptr](),
		reflect.TypeFor[float32](), reflect.TypeFor[float64](), reflect.TypeFor[string](), reflect.TypeFor[[]byte](),
		reflect.TypeFor[pjInt](), reflect.TypeFor[pjString](), reflect.TypeFor[pjFloat](), reflect.TypeFor[pjFloat32](),
		reflect.TypeFor[pjBytes](), reflect.TypeFor[pjByte](), reflect.TypeFor[time.Time](), reflect.TypeFor[json.Number](),
		reflect.TypeFor[any](), reflect.TypeFor[json.RawMessage](), reflect.TypeFor[pjJSONVal](), reflect.TypeFor[pjJSONPtr](),
		reflect.TypeFor[pjTextVal](), reflect.TypeFor[pjTextPtr](), reflect.TypeFor[pjMixed](), reflect.TypeFor[pjBoth](),
		reflect.TypeFor[pjZeroVal](), reflect.TypeFor[pjZeroPtr](), reflect.TypeFor[pjList](), reflect.TypeFor[pjByteText](),
	}
	pjKeyTypes = []reflect.Type{
		reflect.TypeFor[string](), reflect.TypeFor[pjString](), reflect.TypeFor[int](), reflect.TypeFor[uint8](),
		reflect.TypeFor[pjKey](), reflect.TypeFor[pjIntKey](),
	}
	// pjAnyTypes are the types of the values put in an interface.
	pjAnyTypes = []reflect.Type{
		reflect.TypeFor[int](), reflect.TypeFor[float64](), reflect.TypeFor[float32](), reflect.TypeFor[string](),
		reflect.TypeFor[[]any](), reflect.TypeFor[map[string]any](), reflect.TypeFor[[]int](), reflect.TypeFor[[]byte](),
		reflect.TypeFor[time.Time](), reflect.TypeFor[*time.Time](), reflect.TypeFor[pjJSONVal](), reflect.TypeFor[*pjJSONPtr](),
		reflect.TypeFor[pjJSONPtr](), reflect.TypeFor[pjTextVal](), reflect.TypeFor[pjMixed](), reflect.TypeFor[*pjMixed](),
		reflect.TypeFor[pjTags](), reflect.TypeFor[pjOmitEmpty](), reflect.TypeFor[json.Number](), reflect.TypeFor[pjByteText](),
	}
	pjFieldNames = []string{"A", "B", "Shared", "X", "Y"}
	pjTagNames   = []string{"a", "b", "B", "Shared", "shared", "x", "A", "-", "a-b", "ü", `in\"valid`}
	pjInts       = []int64{0, 1, -1, 42, -128, 127, 255, 1 << 31, math.MaxInt64, math.MinInt64, 1<<53 + 1}
	pjUints      = []uint64{0, 1, 255, 1 << 16, math.MaxUint32, 1 << 63, math.MaxUint64}
	pjFloats     = []float64{0, math.Copysign(0, -1), 1, -1, 2, 0.5, -2.25, 0.1, 1.1, 22.3, 1e20, 1e21, 1.5e300,
		1e-7, 5e-324, 123456789.125, 1 << 63, 1 << 64, -(1 << 63), 1<<53 + 1}
	pjFractions = []float64{0.5, -2.25, 0.1, 1.1, 22.3, 1e-7, 123.375, -0.001, 3.4e-30, 6.02e-5} // also as float32
	pjStrings   = []string{"", "a", "Shared", "<html> & co", "\u2028", "quote\"back\\slash", "ctl\x00\x1f\n",
		"ünï", "🎉", "a\xffb", "\xfe", "\uFFFD", "2026-01-01T00:00:00+01:00"}
	pjNumbers = []string{"0", "-1", "12", "1.5", "2.0", "1e3", "18446744073709551615", "-9223372036854775808", ""}
	pjZones   = []*time.Location{time.UTC, time.FixedZone("CEST", 2*3600), time.FixedZone("x", -9*3600-1800)}
	pjTimes   = []time.Time{{}, time.Date(2026, 10, 8, 14, 0, 0, 123456789, time.UTC), time.Date(1999, 12, 31, 23, 59, 59, 999999999, time.UTC)}
)

// pjRandomValue returns a random value of a random type, both chosen by
// seed. Values of the same seed and options differ only where nan is set:
// NaN is where the sentinel would be.
func pjRandomValue(seed uint64, strict, quoted, nan bool) any {
	g := &pjRandom{
		types:   rand.New(rand.NewPCG(seed, 1)),
		values:  rand.New(rand.NewPCG(seed, 2)),
		strict:  strict,
		quoted:  quoted,
		special: pjSentinel,
	}
	if nan {
		g.special = math.NaN()
	}
	t := g.typ(0)
	if seed%4 != 0 {
		t = g.structType(1) // mostly a struct at the top
	}
	v := reflect.New(t).Elem()
	g.fill(v, 0)
	if seed%3 == 0 {
		return v.Addr().Interface() // addressable inside
	}
	return v.Interface()
}

func (g *pjRandom) typ(depth int) reflect.Type {
	r := g.types
	if depth < 4 {
		switch r.IntN(12) {
		case 0:
			return reflect.SliceOf(g.typ(depth + 1))
		case 1:
			return reflect.ArrayOf(r.IntN(3), g.typ(depth+1))
		case 2:
			return reflect.MapOf(pjKeyTypes[r.IntN(len(pjKeyTypes))], g.typ(depth+1))
		case 3:
			return reflect.PointerTo(g.typ(depth + 1))
		case 4, 5, 6:
			return g.structType(depth + 1)
		}
	}
	return pjLeafTypes[r.IntN(len(pjLeafTypes))]
}

func (g *pjRandom) structType(depth int) reflect.Type {
	r := g.types
	n := r.IntN(6)
	fields := make([]reflect.StructField, 0, n)
	used := map[string]bool{}
	for i := range n {
		var f reflect.StructField
		switch k := r.IntN(10); {
		case k < 2 && depth < 4: // an embedded struct, or a pointer to one
			et := g.structType(depth + 1)
			if r.IntN(3) == 0 {
				et = reflect.PointerTo(et)
			}
			f = reflect.StructField{Name: fmt.Sprintf("E%d", i), Type: et, Anonymous: true}
		case k == 2: // unexported
			f = reflect.StructField{Name: fmt.Sprintf("u%d", i), PkgPath: "github.com/RecordEvolution/ironflock-go", Type: g.typ(depth + 1)}
		default:
			name := pjFieldNames[r.IntN(len(pjFieldNames))]
			if used[name] {
				name = fmt.Sprintf("%s%d", name, i)
			}
			f = reflect.StructField{Name: name, Type: g.typ(depth + 1)}
		}
		used[f.Name] = true
		f.Tag = g.tag(f.Anonymous)
		fields = append(fields, f)
	}
	return reflect.StructOf(fields)
}

func (g *pjRandom) tag(embedded bool) reflect.StructTag {
	r := g.types
	name := pjTagNames[r.IntN(len(pjTagNames))]
	var options []string
	if embedded {
		options = []string{"", "", "", name, "-", ",omitempty"}
	} else {
		options = []string{"", "", "", "", name, "-", "-,", ",omitempty", ",omitzero", name + ",omitempty", name + ",omitempty,omitzero"}
		if g.quoted {
			options = append(options, ",string", name+",string", name+",omitzero,string")
		}
	}
	tag := options[r.IntN(len(options))]
	if tag == "" {
		return ""
	}
	return reflect.StructTag(`json:"` + tag + `"`)
}

func (g *pjRandom) fill(v reflect.Value, depth int) {
	r := g.values
	t := v.Type()
	switch t {
	case timeType:
		v.Set(reflect.ValueOf(pjTimes[r.IntN(len(pjTimes))].In(pjZones[r.IntN(len(pjZones))])))
		return
	case numberType:
		v.SetString(pjNumbers[r.IntN(len(pjNumbers))])
		return
	case reflect.TypeFor[json.RawMessage]():
		if raw := []string{"", "null", `{"a": [1, 2.5]}`, `"s"`, "7"}[r.IntN(5)]; raw != "" {
			v.SetBytes([]byte(raw))
		}
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(r.IntN(2) == 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		for {
			if x := pjInts[r.IntN(len(pjInts))]; !v.OverflowInt(x) {
				v.SetInt(x)
				return
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		for {
			if x := pjUints[r.IntN(len(pjUints))]; !v.OverflowUint(x) {
				v.SetUint(x)
				return
			}
		}
	case reflect.Float32, reflect.Float64:
		if r.IntN(4) == 0 {
			v.SetFloat(g.special)
			return
		}
		pool := pjFloats
		if g.strict {
			pool = pjFractions
		}
		for {
			if x := pool[r.IntN(len(pool))]; !v.OverflowFloat(x) {
				v.SetFloat(x)
				return
			}
		}
	case reflect.String:
		v.SetString(pjStrings[r.IntN(len(pjStrings))])
	case reflect.Slice:
		switch k := r.IntN(5); k {
		case 0: // nil
		case 1:
			v.Set(reflect.MakeSlice(t, 0, 0))
		default:
			s := reflect.MakeSlice(t, k-1, k-1)
			for i := range s.Len() {
				g.fill(s.Index(i), depth+1)
			}
			v.Set(s)
		}
	case reflect.Array:
		for i := range v.Len() {
			g.fill(v.Index(i), depth+1)
		}
	case reflect.Map:
		if k := r.IntN(5); k > 0 {
			m := reflect.MakeMap(t)
			for range k - 1 {
				key := reflect.New(t.Key()).Elem()
				g.fill(key, depth+1)
				val := reflect.New(t.Elem()).Elem()
				g.fill(val, depth+1)
				m.SetMapIndex(key, val)
			}
			v.Set(m)
		}
	case reflect.Pointer:
		if r.IntN(3) > 0 {
			p := reflect.New(t.Elem())
			g.fill(p.Elem(), depth+1)
			v.Set(p)
		}
	case reflect.Interface:
		if r.IntN(3) > 0 && depth < 6 {
			e := reflect.New(pjAnyTypes[r.IntN(len(pjAnyTypes))]).Elem()
			g.fill(e, depth+1)
			v.Set(e)
		}
	case reflect.Struct:
		for i := range v.NumField() {
			if f := v.Field(i); f.CanSet() {
				g.fill(f, depth+1)
			}
		}
	}
}

// pjRuns is how many random values each property is checked on.
func pjRuns() int {
	if testing.Short() {
		return 300
	}
	return 3000
}

// Random struct types (made with reflect.StructOf: conflicting names, every
// tag option, embedding at several depths, unexported fields) and random
// values of them encode as encoding/json encodes them.
func TestNormalizeMatchesEncodingJSONOnRandomTypes(t *testing.T) {
	for seed := range uint64(pjRuns()) {
		strict := seed%2 == 0
		v := pjRandomValue(seed, strict, true, false)
		checkLikeJSON(t, fmt.Sprintf("seed %d, %T", seed, v), v, !strict)
		if t.Failed() {
			t.Fatalf("seed %d: value %#v", seed, v)
		}
	}
}

// FuzzNormalizeMatchesEncodingJSON is TestNormalizeMatchesEncodingJSONOnRandomTypes
// for go test -fuzz.
func FuzzNormalizeMatchesEncodingJSON(f *testing.F) {
	for _, seed := range []uint64{0, 1, 2, 3, 1 << 40} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed uint64) {
		strict := seed%2 == 0
		v := pjRandomValue(seed, strict, true, false)
		checkLikeJSON(t, fmt.Sprintf("seed %d, %T", seed, v), v, !strict)
	})
}

// NaN is encoded like any other float: a value holding NaN where an
// otherwise equal value holds a finite float normalizes the same, with NaN
// in that place.
func TestNormalizeNonFiniteFloatsLikeFinite(t *testing.T) {
	for seed := range uint64(pjRuns()) {
		withNaN := pjRandomValue(seed, false, false, true)
		finite := pjRandomValue(seed, false, false, false)
		got, err := normalize(withNaN)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		want, err := normalize(finite)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !pjEqual(got, want, true) {
			t.Fatalf("seed %d:\n got  %#v\n want %#v (NaN for %v)", seed, got, want, pjSentinel)
		}
	}
}

// pjEqual reports whether a and b, normalized values, are equal; NaN equals
// NaN. With sentinel, a NaN in a also equals pjSentinel in b.
func pjEqual(a, b any, sentinel bool) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		switch {
		case !ok:
			return false
		case math.IsNaN(x):
			return math.IsNaN(y) || sentinel && y == pjSentinel
		}
		return x == y && math.Signbit(x) == math.Signbit(y)
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for i := range x {
			if !pjEqual(x[i], y[i], sentinel) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for k, e := range x {
			if f, ok := y[k]; !ok || !pjEqual(e, f, sentinel) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

// normalize's result is made of the promised types, and normalizing it
// again changes nothing.
func TestNormalizeIsIdempotent(t *testing.T) {
	for seed := range uint64(pjRuns()) {
		v := pjRandomValue(seed, false, true, seed%2 == 0)
		once, err := normalize(v)
		if err != nil {
			continue // a ",string" field holding NaN
		}
		if d := checkShape(once); d != "" {
			t.Fatalf("seed %d: %s in %#v", seed, d, once)
		}
		twice, err := normalize(once)
		if err != nil || !pjEqual(twice, once, false) {
			t.Fatalf("seed %d: normalized again: %#v, %v\nonce: %#v", seed, twice, err, once)
		}
	}
}

// A float is sent the same however it is held: in a struct field, through a
// pointer, in an interface, a Row, a typed map or slice — NaN, ±Inf, -0 and
// subnormals included.
func TestNormalizeRowAndStructAgree(t *testing.T) {
	type reading struct {
		F   float64  `json:"f"`
		G   float32  `json:"g"`
		P   *float64 `json:"p"`
		Any any      `json:"any"`
	}
	r := rand.New(rand.NewPCG(11, 12))
	specials := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1), math.MaxFloat64,
		math.SmallestNonzeroFloat64, 2, 0.1, math.Float64frombits(0xfff8000000000000)}
	for i := range pjRuns() {
		x := math.Float64frombits(r.Uint64()) // any bit pattern, NaNs included
		if i < len(specials) {
			x = specials[i]
		}
		x32 := float32(x)
		want, err := normalize(reading{F: x, G: x32, P: &x, Any: x})
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range []any{
			&reading{F: x, G: x32, P: &x, Any: x},
			Row{"f": x, "g": x32, "p": &x, "any": x},
			Kwargs{"f": x, "g": x32, "p": &x, "any": x},
			map[string]any{"f": pjFloat(x), "g": pjFloat32(x32), "p": &x, "any": any(x)},
		} {
			if got, err := normalize(in); err != nil || !pjEqual(got, want, false) {
				t.Fatalf("%v: %T gives %#v, %v; the struct gives %#v", x, in, got, err, want)
			}
		}
		f, g := want.(map[string]any)["f"], want.(map[string]any)["g"]
		for _, in := range []any{map[string]float64{"f": x}, []float64{x}, [1]float64{x}, []any{x}} {
			got, err := normalize(in)
			var got0 any
			switch v := got.(type) {
			case map[string]any:
				got0 = v["f"]
			case []any:
				got0 = v[0]
			}
			if err != nil || !pjEqual(got0, f, false) {
				t.Fatalf("%v: %T gives %#v, %v", x, in, got, err)
			}
		}
		if got, err := normalize([]float32{x32}); err != nil || !pjEqual(got.([]any)[0], g, false) {
			t.Fatalf("%v: []float32 gives %#v, %v", x32, got, err)
		}
	}
}

// normalize is safe for concurrent use: its only shared state is the cache
// of type information, filled here from many goroutines at once.
func TestNormalizeConcurrently(t *testing.T) {
	const goroutines = 8
	runs := pjRuns() / 10
	want := make([]any, runs)
	for i := range runs {
		var err error
		if want[i], err = normalize(pjRandomValue(uint64(i), false, true, false)); err != nil {
			want[i] = err.Error()
		}
	}
	typeInfos.Clear()
	var wg sync.WaitGroup
	errs := make(chan string, goroutines)
	for g := range goroutines {
		wg.Go(func() {
			for j := range runs {
				i := (j + g*runs/goroutines) % runs // start at different values
				got, err := normalize(pjRandomValue(uint64(i), false, true, false))
				if err != nil {
					got = err.Error()
				}
				if !pjEqual(got, want[i], false) {
					errs <- fmt.Sprintf("seed %d: %#v, want %#v", i, got, want[i])
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// What normalize returns goes on the wire as autobahn-python's msgpack
// serializer sends the equivalent Python values: a float as a msgpack float
// (also when integral), []byte as bin, NaN and ±Inf as floats, text as
// valid UTF-8.
func TestNormalizedPayloadOnTheWire(t *testing.T) {
	type (
		withF struct {
			F float64 `json:"f"`
		}
		withI struct {
			I int `json:"i"`
		}
		withB struct {
			B []byte `json:"b"`
		}
		withT struct {
			T float64 `json:"t"`
		}
		withU struct {
			U uint64 `json:"u"`
		}
		withS struct {
			S string `json:"s"`
		}
	)
	cases := []struct {
		in  any
		hex string // MsgPackObjectSerializer().serialize(python) of autobahn 25.12.2
	}{
		{withF{2}, "81a166cb4000000000000000"},                                              // {'f': 2.0}
		{withI{3}, "81a16903"},                                                              // {'i': 3}
		{withB{[]byte("raw")}, "81a162c403726177"},                                          // {'b': b'raw'}
		{withT{math.Float64frombits(0x7ff8000000000000)}, "81a174cb7ff8000000000000"},       // {'t': float('nan')}
		{withT{math.Inf(1)}, "81a174cb7ff0000000000000"},                                    // {'t': float('inf')}
		{withT{math.Inf(-1)}, "81a174cbfff0000000000000"},                                   // {'t': float('-inf')}
		{withU{math.MaxUint64}, "81a175cfffffffffffffffff"},                                 // {'u': 2**64 - 1}
		{withS{"a\xffb"}, "81a173a561efbfbd62"},                                             // {'s': 'a\ufffdb'}
		{[]any{float32(1.5), 2.0, nil, true}, "94cb3ff8000000000000cb4000000000000000c0c3"}, // [1.5, 2.0, None, True]
	}
	var ser serialize.MessagePackSerializer
	for _, c := range cases {
		v, err := normalize(c.in)
		if err != nil {
			t.Fatal(err)
		}
		data, err := ser.SerializeDataItem(v)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(data); got != c.hex {
			t.Errorf("%#v on the wire: %s\nwant %s", c.in, got, c.hex)
		}
	}
}

func BenchmarkNormalize(b *testing.B) {
	type reading struct {
		Temperature float64   `json:"temperature"`
		Humidity    float64   `json:"humidity"`
		Device      string    `json:"device"`
		Count       int       `json:"count"`
		Tsp         time.Time `json:"tsp"`
		Tags        []string  `json:"tags,omitempty"`
	}
	now := time.Now()
	r := reading{21.5, 40, "dev-1", 3, now, []string{"a", "b"}}
	row := Row{"temperature": 21.5, "humidity": 40.0, "device": "dev-1", "count": 3, "tsp": now, "tags": []any{"a", "b"}}
	batch := make([]reading, 100)
	for i := range batch {
		batch[i] = r
	}
	b.Run("struct", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalize(r)
		}
	})
	b.Run("Row", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalize(row)
		}
	})
	rows := make([]Row, 100)
	for i := range rows {
		rows[i] = row
	}
	b.Run("100 Rows", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalizeRows(rows)
		}
	})
	b.Run("100 struct rows", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalizeRows(batch)
		}
	})
	b.Run("struct via encoding/json (before)", func(b *testing.B) {
		for b.Loop() {
			_, _ = viaJSON(r)
		}
	})
}
