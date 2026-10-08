package jsontext

import (
	"math"
	"testing"
)

func TestCompact(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"app <weather> & co", `"app <weather> & co"`},
		{[]any{"a<b && c>d"}, `["a<b && c>d"]`},
		{map[string]any{"q": "x<1"}, `{"q":"x<1"}`},
		{"line\nbreak", `"line\nbreak"`},
		{"trailing space ", `"trailing space "`},
		{int64(42), `42`},
		{nil, `null`},
		{math.NaN(), `NaN`},
	}
	for _, c := range cases {
		if got := Compact(c.in); got != c.want {
			t.Errorf("Compact(%#v) = %s, want %s", c.in, got, c.want)
		}
	}
}
