package ironflock

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeIntegers(t *testing.T) {
	cases := []struct {
		in   any
		want any
	}{
		{int8(-3), int64(-3)},
		{uint8(5), int64(5)},
		{uint16(5), int64(5)},
		{uint32(5), int64(5)},
		{uint(5), int64(5)},
		{uint64(5), int64(5)},
		{uint64(math.MaxInt64), int64(math.MaxInt64)},
		{uint64(math.MaxUint64), uint64(math.MaxUint64)}, // only above MaxInt64
		{float32(1.5), 1.5},
		{json.Number("12"), int64(12)},
		{json.Number("18446744073709551615"), uint64(math.MaxUint64)},
		{json.Number("1.25"), 1.25},
	}
	for _, tc := range cases {
		got, err := normalize(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("normalize(%T %v) = %T %v, %v; want %T %v", tc.in, tc.in, got, got, err, tc.want, tc.want)
		}
	}
}

func TestNormalizeKeepsLargeIntegersThroughJSON(t *testing.T) {
	type counter struct {
		Big   uint64 `json:"big"`
		Small uint8  `json:"small"`
	}
	got, err := normalize(counter{Big: math.MaxUint64, Small: 7})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"big": uint64(math.MaxUint64), "small": int64(7)}
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
}
