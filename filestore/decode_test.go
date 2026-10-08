package filestore

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestToInt64(t *testing.T) {
	cases := []struct {
		in   any
		want int64
	}{
		{nil, 0}, {int64(5), 5}, {int(-3), -3}, {int32(7), 7}, {uint8(8), 8}, {uint64(9), 9},
		{uint64(math.MaxUint64), math.MaxInt64}, {12.9, 12}, {-12.9, -12}, {math.NaN(), 0},
		{math.Inf(1), math.MaxInt64}, {math.Inf(-1), math.MinInt64}, {float32(2.5), 2},
		{"42", 42}, {" 42 ", 42}, {"1e3", 1000}, {"4.7", 4}, {"", 0}, {"abc", 0},
		{true, 1}, {false, 0}, {json.Number("17"), 17}, {[]any{1}, 0}, {map[string]any{}, 0},
	}
	for _, tc := range cases {
		if got := toInt64(tc.in); got != tc.want {
			t.Errorf("toInt64(%#v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestToBool(t *testing.T) {
	truthy := []any{true, int64(1), -1, 0.5, uint64(2), "true", "TRUE", " yes ", "1", "on", "t", "y"}
	falsy := []any{nil, false, int64(0), 0.0, math.NaN(), "", "false", "False", "0", "no", "off", "nonsense", []any{}, map[string]any{}}
	for _, v := range truthy {
		if !toBool(v) {
			t.Errorf("toBool(%#v) = false, want true", v)
		}
	}
	for _, v := range falsy {
		if toBool(v) {
			t.Errorf("toBool(%#v) = true, want false", v)
		}
	}
}

func TestToString(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, ""}, {"s", "s"}, {[]byte("b"), "b"}, {int64(3), "3"}, {1.5, "1.5"}, {true, "true"},
		{json.Number("7"), "7"}, {time.Second, "1s"},
	}
	for _, tc := range cases {
		if got := toString(tc.in); got != tc.want {
			t.Errorf("toString(%#v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEscaping(t *testing.T) {
	cases := []struct {
		in, key, component string
	}{
		{"a/b c.jpg", "a/b%20c.jpg", "a%2Fb%20c.jpg"},
		{"AZaz09-._~", "AZaz09-._~", "AZaz09-._~"},
		{"!*'()", "%21%2A%27%28%29", "!*'()"},
		{"ä€", "%C3%A4%E2%82%AC", "%C3%A4%E2%82%AC"},
		{"%2F", "%252F", "%252F"},
		{"", "", ""},
	}
	for _, tc := range cases {
		if got := escapeKey(tc.in); got != tc.key {
			t.Errorf("escapeKey(%q) = %q, want %q", tc.in, got, tc.key)
		}
		if got := escapeComponent(tc.in); got != tc.component {
			t.Errorf("escapeComponent(%q) = %q, want %q", tc.in, got, tc.component)
		}
	}
}

func TestTTLSeconds(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want int64
	}{
		{0, 900}, {-time.Second, 900}, {time.Second, 1}, {time.Millisecond, 1}, {1500 * time.Millisecond, 2},
		{2 * time.Minute, 120}, {time.Hour, 3600},
	}
	for _, tc := range cases {
		if got := ttlSeconds(tc.in, DefaultShareTTL); got != tc.want {
			t.Errorf("ttlSeconds(%v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestTruncateChars(t *testing.T) {
	if got := truncateChars("héllo", 2); got != "hé" {
		t.Errorf("truncateChars = %q", got)
	}
	if got := truncateChars("abc", 5); got != "abc" {
		t.Errorf("truncateChars = %q", got)
	}
}
