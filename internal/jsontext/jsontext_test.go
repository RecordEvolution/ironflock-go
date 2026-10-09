package jsontext

import (
	"encoding/json"
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

func TestValidUTF8(t *testing.T) {
	for in, want := range map[string]string{
		"":                   "",
		"plain":              "plain",
		"ümlaut ✓":           "ümlaut ✓",
		"bad \xff\xfe frame": "bad �� frame", // each invalid byte, as encoding/json
		"cut \xe2\x9c":       "cut ��",       // a truncated sequence
		"\xed\xa0\x80":       "���",          // a surrogate half is no valid UTF-8
	} {
		if got := ValidUTF8(in); got != want {
			t.Errorf("ValidUTF8(%+q) = %+q, want %+q", in, got, want)
		}
		var viaJSON string
		if b, err := json.Marshal(in); err != nil || json.Unmarshal(b, &viaJSON) != nil || viaJSON != ValidUTF8(in) {
			t.Errorf("ValidUTF8(%+q) = %+q, but encoding/json makes it %+q", in, ValidUTF8(in), viaJSON)
		}
	}
}
