package finite

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

var (
	nan    = math.NaN()
	posInf = math.Inf(1)
	negInf = math.Inf(-1)
)

func TestOrNilReplacesNonFiniteFloats(t *testing.T) {
	cases := []struct {
		name    string
		in, out any
	}{
		{"nil", nil, nil},
		{"finite float64", 1.5, 1.5},
		{"NaN", nan, nil},
		{"+Inf", posInf, nil},
		{"-Inf", negInf, nil},
		{"float32 NaN", float32(nan), nil},
		{"float32 Inf", float32(posInf), nil},
		{"finite float32", float32(2.5), float32(2.5)},
		{"scalars", []any{"s", int64(1), uint64(math.MaxUint64), true, []byte("b"), nil},
			[]any{"s", int64(1), uint64(math.MaxUint64), true, []byte("b"), nil}},
		{"list", []any{1.0, nan, posInf, "x"}, []any{1.0, nil, nil, "x"}},
		{"map", map[string]any{"a": nan, "b": 2.0}, map[string]any{"a": nil, "b": 2.0}},
		{"nested", map[string]any{"rows": []any{map[string]any{"t": negInf, "u": []any{nan}}}},
			map[string]any{"rows": []any{map[string]any{"t": nil, "u": []any{nil}}}}},
		{"rows", []map[string]any{{"t": 1.0}, {"t": nan}}, []map[string]any{{"t": 1.0}, {"t": nil}}},
		{"nil rows", []map[string]any(nil), []map[string]any(nil)},
		// Other types are not payload shapes: they are returned as they are.
		{"typed slice", []float64{1}, []float64{1}},
		{"string", "NaN", "NaN"},
	}
	for _, c := range cases {
		got := OrNil(c.in)
		if !reflect.DeepEqual(got, c.out) {
			t.Errorf("%s: OrNil(%#v) = %#v, want %#v", c.name, c.in, got, c.out)
		}
		if _, err := json.Marshal(got); err != nil && c.name != "typed slice" {
			t.Errorf("%s: the result does not encode: %v", c.name, err)
		}
	}
}

// OrNil never modifies its argument: values (an event's Args) may be shared
// with other handlers.
func TestOrNilDoesNotModifyItsArgument(t *testing.T) {
	inner := []any{nan, 1.0}
	row := map[string]any{"t": posInf, "list": inner, "ok": "x"}
	in := []any{row, 2.0}
	out := OrNil(in).([]any)
	if !math.IsNaN(inner[0].(float64)) || !math.IsInf(row["t"].(float64), 1) || len(row) != 3 {
		t.Fatalf("the argument was modified: %#v", in)
	}
	want := []any{map[string]any{"t": nil, "list": []any{nil, 1.0}, "ok": "x"}, 2.0}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("got %#v, want %#v", out, want)
	}
}

// A value without non-finite floats is returned as it is, without copying.
func TestOrNilSharesCleanValues(t *testing.T) {
	clean := map[string]any{"a": []any{1.0, "x", map[string]any{"b": int64(2)}}}
	if got := OrNil(clean); reflect.ValueOf(got).UnsafePointer() != reflect.ValueOf(clean).UnsafePointer() {
		t.Error("a clean map was copied")
	}
	// Only the containers on the path to a replaced value are copied.
	shared := []any{1.0}
	in := map[string]any{"bad": nan, "shared": shared}
	out := OrNil(in).(map[string]any)
	if reflect.ValueOf(out["shared"]).UnsafePointer() != reflect.ValueOf(shared).UnsafePointer() {
		t.Error("a clean list next to a replaced value was copied")
	}
	allocs := testing.AllocsPerRun(100, func() { _ = OrNil(clean) })
	if allocs != 0 {
		t.Errorf("OrNil allocated %v times for a clean value", allocs)
	}
}

// A cyclic value (not a decoded payload, but OrNil must not overflow the
// stack on one) is returned without descending forever; encoding/json then
// reports the cycle.
func TestOrNilTerminatesOnCycles(t *testing.T) {
	m := map[string]any{"x": 1.0}
	m["self"] = m
	if got := OrNil(m); reflect.ValueOf(got).UnsafePointer() != reflect.ValueOf(m).UnsafePointer() {
		t.Error("a clean cyclic map was copied")
	}
	bad := map[string]any{"x": nan}
	bad["self"] = bad
	out := OrNil(bad)
	if _, err := json.Marshal(out); err == nil {
		t.Error("encoding/json accepted a cyclic value")
	}
	s := []any{nan}
	s = append(s, s)
	_ = OrNil(s)
}

// Several references back into a cycle must not make OrNil branch at every
// level (2^depth work): it gives up at the first container it meets again.
func TestOrNilGivesUpOnCyclesQuickly(t *testing.T) {
	m := map[string]any{"x": nan}
	m["a"], m["b"], m["c"] = m, m, m
	s := []any{nan, nil, nil}
	s[1], s[2] = s, s
	rows := []map[string]any{{"x": nan}, nil}
	rows[1] = map[string]any{"rows": rows, "again": rows}
	for _, in := range []any{m, s, rows, []any{m, s}} {
		out := OrNil(in)
		if reflect.ValueOf(out).UnsafePointer() != reflect.ValueOf(in).UnsafePointer() {
			t.Errorf("%T: a cyclic value was not returned as it is", in)
		}
	}
	// A deep value without a cycle is walked to the bottom.
	var deep any = nan
	for i := 0; i < 3*trackDepth; i++ {
		deep = []any{deep}
	}
	out := OrNil(deep)
	for i := 0; i < 3*trackDepth; i++ {
		out = out.([]any)[0]
	}
	if out != nil {
		t.Errorf("the NaN at the bottom of a deep value became %v", out)
	}
}
