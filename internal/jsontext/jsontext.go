// Package jsontext renders payload values as compact JSON for error messages,
// the way JavaScript's JSON.stringify does: without escaping <, > and & for
// HTML, which encoding/json does by default.
package jsontext

import (
	"bytes"
	"encoding/json"
	"fmt"
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
