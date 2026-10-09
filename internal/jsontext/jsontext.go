// Package jsontext renders payload values as compact JSON for error messages,
// the way JavaScript's JSON.stringify does: without escaping <, > and & for
// HTML, which encoding/json does by default.
package jsontext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// Compact returns v as compact JSON without HTML escaping. A value JSON cannot
// encode (a NaN, a channel) is rendered with fmt's %v instead.
func Compact(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprintf("%v", v)
	}
	// Encode terminates its output with exactly one newline; a newline inside
	// a JSON string is always escaped, so trimming one byte is safe.
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
}

// ValidUTF8 returns s with each byte that is not part of a valid UTF-8
// sequence replaced by U+FFFD, as encoding/json does. Python's msgpack
// decoder refuses a str that is not valid UTF-8, losing the whole message.
func ValidUTF8(s string) string {
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
