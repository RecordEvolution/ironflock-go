package filestore

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
)

// Decoding of service payloads.
//
// Decoding is lenient, like the JavaScript SDK: a field of the wrong type
// reads as its zero value instead of failing the whole call, numbers are
// accepted in any numeric representation (including numeric strings), and
// unknown fields are ignored. Payload values normally arrive normalized by
// the crossbar package (nil, bool, string, int64, uint64, float64, []byte,
// []any, map[string]any), but other Go types are tolerated as well.

// asMap returns v as a JSON-like object.
func asMap(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, val := range m {
			out[fmt.Sprint(k)] = val
		}
		return out, true
	case map[string]string:
		out := make(map[string]any, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out, true
	}
	return nil, false
}

// asList returns v as a JSON-like array.
func asList(v any) ([]any, bool) {
	switch l := v.(type) {
	case []any:
		return l, true
	case []string:
		out := make([]any, len(l))
		for i, s := range l {
			out[i] = s
		}
		return out, true
	case []map[string]any:
		out := make([]any, len(l))
		for i, m := range l {
			out[i] = m
		}
		return out, true
	}
	return nil, false
}

// toString reads v as a string: nil is "", other scalars are formatted.
func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		return strconv.FormatBool(x)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case json.Number:
		return x.String()
	case fmt.Stringer:
		return x.String()
	}
	return fmt.Sprint(v)
}

// toInt64 reads v as an integer. Floats are truncated toward zero and
// clamped to the int64 range; numeric strings are parsed; true is 1; anything
// unreadable (nil, NaN, a non-numeric string, an object) is 0.
func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int16:
		return int64(x)
	case int8:
		return int64(x)
	case uint64:
		if x > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(x)
	case uint:
		if uint64(x) > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(x)
	case uint32:
		return int64(x)
	case uint16:
		return int64(x)
	case uint8:
		return int64(x)
	case float64:
		return floatToInt64(x)
	case float32:
		return floatToInt64(float64(x))
	case bool:
		if x {
			return 1
		}
		return 0
	case json.Number:
		return parseInt64(string(x))
	case string:
		return parseInt64(x)
	}
	return 0
}

func parseInt64(s string) int64 {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return floatToInt64(f)
	}
	return 0
}

func floatToInt64(f float64) int64 {
	switch {
	case math.IsNaN(f):
		return 0
	case f >= math.MaxInt64:
		return math.MaxInt64
	case f <= math.MinInt64:
		return math.MinInt64
	}
	return int64(f)
}

// toInt reads v as an int (see toInt64), clamped to the int range.
func toInt(v any) int {
	n := toInt64(v)
	if n > math.MaxInt {
		return math.MaxInt
	}
	if n < math.MinInt {
		return math.MinInt
	}
	return int(n)
}

// toBool reads v as a boolean: numbers are true when non-zero, and strings
// when they spell true ("true", "t", "1", "yes", "y", "on", any case). Every
// other value — including the string "false" — is false.
func toBool(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "t", "1", "yes", "y", "on":
			return true
		}
		return false
	case float64:
		return x != 0 && !math.IsNaN(x)
	case float32:
		return x != 0 && !math.IsNaN(float64(x))
	case json.Number:
		f, err := x.Float64()
		return err == nil && f != 0
	}
	return toInt64(v) != 0
}

// toStrings reads v as a list of strings; nil when v is not a list.
func toStrings(v any) []string {
	l, ok := asList(v)
	if !ok {
		return nil
	}
	out := make([]string, len(l))
	for i, s := range l {
		out[i] = toString(s)
	}
	return out
}

// decodeObjectInfo reads an object descriptor. URL is not read from the wire:
// the SDK composes it (see FileStore.withURL).
func decodeObjectInfo(p map[string]any) *ObjectInfo {
	return &ObjectInfo{
		Namespace:      toString(p["namespace"]),
		Key:            toString(p["key"]),
		Size:           toInt64(p["size"]),
		ETag:           toString(p["etag"]),
		ContentType:    toString(p["content_type"]),
		LastModified:   toString(p["last_modified"]),
		ChecksumSHA256: toString(p["checksum_sha256"]),
	}
}

// decodeNamespaceInfo reads a namespace declaration. An absent private flag
// reads as false: namespaces are shared unless the template says otherwise.
// A namespace carries no quota of its own, so a quota_bytes field is ignored.
func decodeNamespaceInfo(p map[string]any) NamespaceInfo {
	return NamespaceInfo{
		Name:           toString(p["name"]),
		Description:    toString(p["description"]),
		Private:        toBool(p["private"]),
		ContentTypes:   toStrings(p["content_types"]),
		MaxObjectBytes: toInt64(p["max_object_bytes"]),
	}
}

// decodeCatalog reads the files.read.namespaces payload. A list_max_limit of
// 0 or none reads as 1000; every other absent field reads as its zero value.
func decodeCatalog(p map[string]any) *Catalog {
	c := &Catalog{
		Namespaces:          []NamespaceInfo{},
		InlineMaxBytes:      toInt64(p["inline_max_bytes"]),
		ListMaxLimit:        toInt(p["list_max_limit"]),
		QuotaBytes:          toInt64(p["quota_bytes"]),
		SuggestedQuotaBytes: toInt64(p["suggested_quota_bytes"]),
		SadKey:              toInt64(p["sad_key"]),
		PublicBaseURL:       toString(p["public_base_url"]),
		CloudBaseURL:        toString(p["cloud_base_url"]),
		PresignAvailable:    toBool(p["presign_available"]),
		PresignMaxBytes:     toInt64(p["presign_max_bytes"]),
	}
	if c.ListMaxLimit == 0 {
		c.ListMaxLimit = 1000
	}
	if l, ok := asList(p["namespaces"]); ok {
		for _, item := range l {
			if m, ok := asMap(item); ok {
				c.Namespaces = append(c.Namespaces, decodeNamespaceInfo(m))
			}
		}
	}
	return c
}

// decodeUsage reads the files.read.usage payload. An absent (or null)
// free_bytes reads as -1, "unlimited": 0 would read as "full".
func decodeUsage(p map[string]any) *StorageUsage {
	u := &StorageUsage{
		SizeBytes:   toInt64(p["size_bytes"]),
		ObjectCount: toInt64(p["object_count"]),
		QuotaBytes:  toInt64(p["quota_bytes"]),
		FreeBytes:   -1,
	}
	if v, ok := p["free_bytes"]; ok && v != nil {
		u.FreeBytes = toInt64(v)
	}
	if m, ok := asMap(p["per_namespace"]); ok {
		u.PerNamespace = make(map[string]int64, len(m))
		for name, n := range m {
			u.PerNamespace[name] = toInt64(n)
		}
	}
	return u
}

// decodeUploadTarget reads the files.write.url payload in the JavaScript
// SDK's shape: Method defaults to "PUT" and Headers to an empty map.
func decodeUploadTarget(p map[string]any) *UploadTarget {
	t := &UploadTarget{
		URL:       toString(p["url"]),
		Method:    toString(p["method"]),
		Headers:   map[string]string{},
		ExpiresIn: toInt(p["expires_in"]),
	}
	if t.Method == "" {
		t.Method = http.MethodPut
	}
	if m, ok := asMap(p["headers"]); ok {
		for name, v := range m {
			t.Headers[name] = toString(v)
		}
	}
	return t
}

// jsonType names the JSON type of a payload value, for error messages.
func jsonType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []byte:
		return "binary"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return "number"
	}
	if _, ok := asList(v); ok {
		return "array"
	}
	if _, ok := asMap(v); ok {
		return "object"
	}
	return fmt.Sprintf("%T", v)
}
