package wamp

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

// Error renders its arguments as compact JSON the way the Python and
// JavaScript SDKs do: without escaping <, > and & for HTML (finding 21).
func TestErrorTextIsCompactJSON(t *testing.T) {
	tests := []struct {
		err  *Error
		want string
	}{
		{&Error{URI: "wamp.error.invalid_argument", Args: []any{"a<b && c>d"}},
			`wamp.error.invalid_argument: ["a<b && c>d"]`},
		{&Error{URI: "app.error.query", Args: []any{map[string]any{"q": "x<1"}, int64(2)}},
			`app.error.query: [{"q":"x<1"},2]`},
		{&Error{URI: "app.error.bare"}, "app.error.bare"},
		{&Error{URI: "app.error.kwargs_only", Kwargs: map[string]any{"k": 1}}, "app.error.kwargs_only"},
		// JSON cannot encode NaN: the arguments are printed as Go values.
		{&Error{URI: "app.error.nan", Args: []any{math.NaN()}}, "app.error.nan: [NaN]"},
		{nil, "<nil>"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
}

// Rows gives the rows of a table event (one row map) and of a transform's
// event (the whole view as a list of row maps) alike (finding 4).
func TestEventRows(t *testing.T) {
	row1 := map[string]any{"tsp": "2026-10-02T00:00:00Z", "temp": 21.5}
	row2 := map[string]any{"tsp": "2026-10-02T01:00:00Z", "temp": 22.0}
	tests := []struct {
		name string
		ev   *Event
		want []map[string]any
	}{
		{"a table row", &Event{Args: []any{row1}}, []map[string]any{row1}},
		{"a transform snapshot", &Event{Args: []any{[]any{row1, row2}}}, []map[string]any{row1, row2}},
		{"an empty snapshot", &Event{Args: []any{[]any{}}}, []map[string]any{}},
		{"later arguments are ignored", &Event{Args: []any{[]any{row2}, "x"}}, []map[string]any{row2}},
		{"a list that is not all rows", &Event{Args: []any{[]any{row1, "x"}}}, nil},
		{"a scalar", &Event{Args: []any{int64(1)}}, nil},
		{"a nil first argument", &Event{Args: []any{nil}}, nil},
		{"no arguments", &Event{Kwargs: map[string]any{"k": 1}}, nil},
		{"a nil event", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ev.Rows()
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Rows() = %#v, want %#v", got, tt.want)
			}
			if (got == nil) != (tt.want == nil) {
				t.Fatalf("Rows() = %#v, want %#v (nil and empty differ)", got, tt.want)
			}
		})
	}
	// Row still means the single-row shape: nil for a snapshot.
	if row := (&Event{Args: []any{[]any{row1}}}).Row(); row != nil {
		t.Fatalf("Row() of a snapshot = %v, want nil", row)
	}
}

// IsNotServedYet is the one definition of a call refused because its
// procedure is not served yet: Call's retry window retries exactly these
// (finding 15).
func TestIsNotServedYet(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"router: no callee", &Error{URI: URINoSuchProcedure}, true},
		{"router: no callee, with a message", &Error{URI: URINoSuchProcedure, Args: []any{"no callee"}}, true},
		{"wrapped", fmt.Errorf("op: %w", &Error{URI: URINoSuchProcedure}), true},
		{"callee: no handler yet", &Error{URI: "wamp.error.invalid_argument",
			Args: []any{"client has no handler for registration 4711"}}, true},
		{"callee: other invalid argument", &Error{URI: "wamp.error.invalid_argument", Args: []any{"bad input"}}, false},
		{"invalid argument without a message", &Error{URI: "wamp.error.invalid_argument"}, false},
		{"invalid argument with a non-string message", &Error{URI: "wamp.error.invalid_argument", Args: []any{int64(7)}}, false},
		{"another refusal", &Error{URI: URINotAuthorized, Args: []any{"client has no handler for registration 1"}}, false},
		{"not a WAMP error", errors.New(URINoSuchProcedure), false},
		{"a typed nil *Error", (*Error)(nil), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNotServedYet(tt.err); got != tt.want {
				t.Fatalf("IsNotServedYet(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// NaN and ±Inf, which the Python SDK sends as they are (a failed sensor
// read), decode as JSON null instead of failing the whole result.
func TestResultDecodeToleratesNonFiniteFloats(t *testing.T) {
	type reading struct {
		Temperature *float64 `json:"temperature"`
		Humidity    float64  `json:"humidity"`
		Pressure    float64  `json:"pressure"`
		Device      string   `json:"device"`
	}
	res := &Result{Args: []any{map[string]any{
		"temperature": math.NaN(),
		"humidity":    math.Inf(1),
		"pressure":    1013.25,
		"device":      "d1",
	}}}
	var got reading
	if err := res.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Temperature != nil || got.Humidity != 0 || got.Pressure != 1013.25 || got.Device != "d1" {
		t.Fatalf("decoded %+v", got)
	}
	// The result itself is left as it is.
	if v := res.Value().(map[string]any)["temperature"].(float64); !math.IsNaN(v) {
		t.Fatalf("Decode changed the result: %v", v)
	}

	var rows []reading
	res = &Result{Args: []any{[]any{
		map[string]any{"temperature": 21.5, "device": "a"},
		map[string]any{"temperature": math.Inf(-1), "device": "b"},
	}}}
	if err := res.Decode(&rows); err != nil {
		t.Fatalf("Decode rows: %v", err)
	}
	if len(rows) != 2 || rows[0].Temperature == nil || *rows[0].Temperature != 21.5 || rows[1].Temperature != nil {
		t.Fatalf("decoded rows %+v", rows)
	}

	// Values JSON cannot represent at all still fail.
	err := (&Result{Args: []any{func() {}}}).Decode(&got)
	if err == nil || !strings.HasPrefix(err.Error(), "decode result: ") {
		t.Fatalf("Decode of a func = %v", err)
	}
}
