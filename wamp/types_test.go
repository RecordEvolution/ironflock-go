package wamp

import (
	"math"
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
