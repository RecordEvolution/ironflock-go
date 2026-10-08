package filestore

import (
	"strconv"
	"strings"
)

// objectURL composes the permanent URL of an object on base:
//
//	<base without trailing "/">/f/<sad key>/<namespace>/<escaped key>[?v=<escaped version>]
//
// It returns "" when base is empty (no HTTP edge, or no cloud tunnel). The
// namespace is not escaped (namespace names are URL-safe by construction).
func objectURL(base string, sadKey int64, namespace, key, version string) string {
	if base == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(strings.TrimRight(base, "/"))
	b.WriteString("/f/")
	b.WriteString(strconv.FormatInt(sadKey, 10))
	b.WriteByte('/')
	b.WriteString(namespace)
	b.WriteByte('/')
	b.WriteString(escapeKey(key))
	if version != "" {
		b.WriteString("?v=")
		b.WriteString(escapeComponent(version))
	}
	return b.String()
}

// escapeKey percent-encodes an object key for a URL path, keeping "/" as the
// folder separator (escaping it would yield a URL that no longer addresses
// the object). Only RFC 3986 unreserved characters (A-Z a-z 0-9 - . _ ~) and
// "/" are kept, as Python's urllib.parse.quote(key, safe="/") does.
func escapeKey(key string) string {
	return escape(key, func(c byte) bool { return isUnreserved(c) || c == '/' })
}

// escapeComponent percent-encodes s like JavaScript's encodeURIComponent:
// unreserved characters and !*'() are kept, everything else is encoded.
func escapeComponent(s string) string {
	return escape(s, func(c byte) bool {
		return isUnreserved(c) || c == '!' || c == '*' || c == '\'' || c == '(' || c == ')'
	})
}

func isUnreserved(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' ||
		c == '-' || c == '.' || c == '_' || c == '~'
}

// escape percent-encodes (upper-case hex) every byte of s's UTF-8 encoding
// that keep rejects.
func escape(s string, keep func(byte) bool) string {
	const hex = "0123456789ABCDEF"
	n := 0
	for i := 0; i < len(s); i++ {
		if !keep(s[i]) {
			n++
		}
	}
	if n == 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 2*n)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if keep(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0f])
	}
	return b.String()
}
