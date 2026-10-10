package filestore

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// fleetfiles models the read side of fleetfiles-service v0.2.0 (af6d814) on
// an in-memory store, for the calls whose contract the SDK depends on:
// internal/dataplane/handlers.go (wrap, doList, doStat, doGet, readRange,
// getRange, doReadURL, CodeFor), internal/dataplane/ops.go (List) and
// internal/naming/naming.go (NormalizeKey), with S3's ListObjectsV2 paging
// (internal/objectstore/s3.go). Its replies go through the package's fake
// caller like any other scripted reply; install it with serve.
type fleetfiles struct {
	mu sync.Mutex
	// objects holds the stored objects by namespace, then key (in NFC).
	objects map[string]map[string]fleetObject
	// inlineMax is InlineMaxBytes: a plain get of a larger object is
	// refused with TOO_LARGE, and a range carries at most this many bytes.
	inlineMax int64
	// presign is whether files.read.url mints (else NOT_SUPPORTED, as
	// presignReady answers on a deployment that cannot presign).
	presign bool
	// readURL is the URL files.read.url answers with.
	readURL string
	// noRanges models a file service from before ranged reads (fleetfiles
	// v0.1.33): offset and length are ignored, so a ranged get is a plain
	// get.
	noRanges bool
	// beforeRange, when set, runs before a ranged get reads its range (after
	// its stat), with the requested offset; it may change the store.
	beforeRange func(f *fleetfiles, offset int64)
	// afterRange, when set, runs once a ranged get has answered.
	afterRange func(f *fleetfiles, offset int64)
}

type fleetObject struct {
	data []byte
	etag string
}

func newFleetfiles(inlineMax int64) *fleetfiles {
	return &fleetfiles{objects: map[string]map[string]fleetObject{"default": {}, "frames": {}}, inlineMax: inlineMax}
}

// put stores data under ns/key, unlocked: tests call it from the hooks too.
func (f *fleetfiles) put(ns, key string, data []byte) {
	sum := md5.Sum(data)
	f.objects[ns][key] = fleetObject{data: data, etag: hex.EncodeToString(sum[:])}
}

// serve answers the read procedures on fc.
func (f *fleetfiles) serve(fc *fakeCaller) {
	for uri, op := range map[string]func(map[string]any) (map[string]any, error){
		URIList: f.list, URIStat: f.stat, URIGet: f.get, URIReadURL: f.readURLOp,
	} {
		fc.set(uri, replyFunc(func(args []any) (any, error) {
			a := map[string]any{}
			if len(args) > 0 {
				a = args[0].(map[string]any)
			}
			f.mu.Lock()
			p, err := f.wrap(a, op)
			after := f.afterRange
			f.mu.Unlock()
			if err != nil {
				return fail(codeFor(err), err.Error()), nil
			}
			if after != nil && uri == URIGet {
				if off, ranged := a["offset"]; ranged {
					f.mu.Lock()
					after(f, off.(int64))
					f.mu.Unlock()
				}
			}
			return ok(p), nil
		}))
	}
}

// The errors CodeFor classifies (handlers.go): a naming error is a plain
// error, so it is INTERNAL.
var (
	errNoSuchNamespace = errors.New("dataplane: no such namespace")
	errNoSuchObject    = errors.New("objectstore: no such object")
	errTooLarge        = errors.New("dataplane: object too large for a single call")
	errInvalidRange    = errors.New("dataplane: invalid byte range")
	errNotImplemented  = errors.New("objectstore: not implemented by this driver")
)

func codeFor(err error) string {
	switch {
	case errors.Is(err, errNoSuchNamespace):
		return CodeNoSuchNamespace
	case errors.Is(err, errNoSuchObject):
		return CodeNoSuchObject
	case errors.Is(err, errTooLarge):
		return CodeTooLarge
	case errors.Is(err, errInvalidRange):
		return CodeInvalidRange
	case errors.Is(err, errNotImplemented):
		return CodeNotSupported
	}
	return CodeInternal
}

// wrap is handlers.go wrap: "" is the default namespace, an unknown one is
// refused before anything else.
func (f *fleetfiles) wrap(a map[string]any, op func(map[string]any) (map[string]any, error)) (map[string]any, error) {
	ns, _ := a["namespace"].(string)
	if ns == "" {
		ns = "default"
	}
	if _, known := f.objects[ns]; !known {
		return nil, errNoSuchNamespace
	}
	b := map[string]any{}
	for k, v := range a {
		b[k] = v
	}
	b["namespace"] = ns
	return op(b)
}

// nfc stands in for Unicode normalization form C (golang.org/x/text is no
// dependency of this module) for the strings these tests use: "e" with a
// combining acute accent composes to "é".
func nfc(s string) string { return strings.ReplaceAll(s, "é", "é") }

// normalizeKey is naming.NormalizeKey, without the byte and segment limits.
func normalizeKey(key string) (string, error) {
	if key == "" {
		return "", errors.New("object key must not be empty")
	}
	key = nfc(key)
	switch {
	case strings.HasPrefix(key, "/"):
		return "", errors.New("object key must not start with '/'")
	case strings.HasSuffix(key, "/"):
		return "", errors.New("object key must not end with '/'")
	case strings.HasPrefix(key, "_sys/"):
		return "", errors.New(`object key must not start with "_sys/", which is reserved`)
	}
	for _, seg := range strings.Split(key, "/") {
		switch {
		case seg == "":
			return "", errors.New("object key must not contain an empty path segment ('//')")
		case seg == "." || seg == "..":
			return "", fmt.Errorf("object key must not contain a '%s' path segment", seg)
		case strings.HasPrefix(seg, " ") || strings.HasSuffix(seg, " "):
			return "", fmt.Errorf("path segment %q must not start or end with a space", seg)
		case strings.HasSuffix(seg, "."):
			return "", fmt.Errorf("path segment %q must not end with '.'", seg)
		case strings.ContainsAny(seg, `\:*?"<>|^~`):
			return "", fmt.Errorf("path segment %q must not contain any of \\ : * ? \" < > | ^ ~", seg)
		}
	}
	return key, nil
}

func objectDict(ns, key string, o fleetObject) map[string]any {
	return map[string]any{"namespace": ns, "key": key, "size": int64(len(o.data)), "etag": o.etag,
		"content_type": "application/octet-stream"}
}

// list is doList's cursor listing and ops.go List over S3 ListObjectsV2: the
// prefix is validated as a key; delimiter "/" unless the call sends a string
// (an explicit "" lists flat); objects and common prefixes share the page
// budget (limit 200 by default, at most 1000); the continuation token is
// the last entry returned.
func (f *fleetfiles) list(a map[string]any) (map[string]any, error) {
	ns := a["namespace"].(string)
	prefix, _ := a["prefix"].(string)
	token, _ := a["cursor"].(string)
	delimiter := "/"
	if v, present := a["delimiter"]; present {
		if s, isString := v.(string); isString {
			delimiter = s
		}
	}
	limit := 200
	if v, isInt := a["limit"].(int64); isInt && v > 0 {
		limit = int(min(v, 1000))
	}
	if prefix != "" {
		normalized, err := normalizeKey(prefix)
		if err != nil {
			return nil, err
		}
		prefix = normalized
	}
	keys := make([]string, 0, len(f.objects[ns]))
	for k := range f.objects[ns] {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	type entry struct {
		name   string
		folder bool
	}
	var entries []entry
	for _, k := range keys {
		rest := k[len(prefix):]
		if i := strings.Index(rest, delimiter); delimiter != "" && i >= 0 {
			common := prefix + rest[:i+len(delimiter)]
			if len(entries) == 0 || entries[len(entries)-1] != (entry{common, true}) {
				entries = append(entries, entry{common, true})
			}
			continue
		}
		entries = append(entries, entry{k, false})
	}
	if token != "" {
		i := slices.IndexFunc(entries, func(e entry) bool { return e.name > token })
		if i < 0 {
			i = len(entries)
		}
		entries = entries[i:]
	}
	truncated := len(entries) > limit
	if truncated {
		entries = entries[:limit]
	}
	objects, prefixes := []any{}, []any{}
	for _, e := range entries {
		if e.folder {
			prefixes = append(prefixes, e.name)
		} else {
			objects = append(objects, objectDict(ns, e.name, f.objects[ns][e.name]))
		}
	}
	next := ""
	if truncated {
		next = entries[len(entries)-1].name
	}
	return map[string]any{"objects": objects, "prefixes": prefixes, "is_truncated": truncated, "cursor": next}, nil
}

// lookup is ops.go Stat: the key is validated, then looked up.
func (f *fleetfiles) lookup(ns string, a map[string]any) (string, fleetObject, error) {
	key, _ := a["key"].(string)
	normalized, err := normalizeKey(key)
	if err != nil {
		return "", fleetObject{}, err
	}
	o, found := f.objects[ns][normalized]
	if !found {
		return "", fleetObject{}, errNoSuchObject
	}
	return normalized, o, nil
}

func (f *fleetfiles) stat(a map[string]any) (map[string]any, error) {
	ns := a["namespace"].(string)
	key, o, err := f.lookup(ns, a)
	if err != nil {
		return nil, err
	}
	return objectDict(ns, key, o), nil
}

// get is doGet with readRange and getRange: either offset or length makes
// the read ranged; length defaults to and is clamped to inlineMax; an
// offset at the end is an empty range, one past it INVALID_RANGE. Every
// range carries the object's size and etag as its stat read them.
func (f *fleetfiles) get(a map[string]any) (map[string]any, error) {
	ns := a["namespace"].(string)
	rawOffset, hasOffset := a["offset"]
	rawLength, hasLength := a["length"]
	ranged := (hasOffset || hasLength) && !f.noRanges
	offset, length := int64(0), f.inlineMax
	if ranged {
		if hasOffset {
			v, isInt := rawOffset.(int64)
			if !isInt || v < 0 {
				return nil, fmt.Errorf("%w: offset must be a non-negative integer", errInvalidRange)
			}
			offset = v
		}
		if hasLength {
			v, isInt := rawLength.(int64)
			if !isInt || v <= 0 {
				return nil, fmt.Errorf("%w: length must be a positive integer", errInvalidRange)
			}
			length = min(length, v)
		}
	}
	key, o, err := f.lookup(ns, a)
	if err != nil {
		return nil, err
	}
	out := objectDict(ns, key, o)
	size := int64(len(o.data))
	if ranged {
		if offset > size {
			return nil, fmt.Errorf("%w: offset %d is past the end of a %d-byte object", errInvalidRange, offset, size)
		}
		if f.beforeRange != nil {
			// The object as the range's own read finds it: getRange stats and
			// reads in two steps, so what it reads may be a newer object.
			f.beforeRange(f, offset)
			if cur, found := f.objects[ns][key]; found {
				o = cur
			}
		}
		n := min(size-offset, length)
		if offset+n > int64(len(o.data)) {
			return nil, fmt.Errorf("dataplane: range %d-%d of %q returned %d bytes", offset, offset+n-1, key, max(int64(len(o.data))-offset, 0))
		}
		out["data"] = base64.StdEncoding.EncodeToString(o.data[offset : offset+n])
		out["encoding"] = "base64"
		out["offset"] = offset
		out["length"] = n
		return out, nil
	}
	if size > f.inlineMax {
		return nil, fmt.Errorf("%w: %d bytes, inline limit %d - read it in ranges with offset and length", errTooLarge, size, f.inlineMax)
	}
	out["data"] = base64.StdEncoding.EncodeToString(o.data)
	out["encoding"] = "base64"
	return out, nil
}

// readURLOp is doReadURL: NOT_SUPPORTED where the deployment cannot presign,
// and only for an object that exists.
func (f *fleetfiles) readURLOp(a map[string]any) (map[string]any, error) {
	if !f.presign {
		return nil, fmt.Errorf("%w: presigned URLs are not available on this deployment", errNotImplemented)
	}
	ns := a["namespace"].(string)
	_, o, err := f.lookup(ns, a)
	if err != nil {
		return nil, err
	}
	return map[string]any{"url": f.readURL, "method": "GET", "expires_in": int64(900), "size": int64(len(o.data)), "etag": o.etag}, nil
}
