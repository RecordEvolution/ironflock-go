package filestore

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The object the ranged tests read: over the model's 4-byte inline limit,
// so it travels in three ranges (4, 4 and 3 bytes).
const rangedObject = "hello world"

// rangedTriggers are the ways the presigned path of a read can be
// unavailable, each of which makes the read fall back to ranges over the
// router.
var rangedTriggers = []struct {
	name string
	// setup prepares the model and the store; it reports whether the
	// catalog is cached (so the ranges ask for its inline limit).
	setup func(t *testing.T, fs *FileStore, ff *fleetfiles) (cached bool)
	// mints is how many URLs the read mints.
	mints int
}{
	{"catalog says no presign", func(t *testing.T, fs *FileStore, ff *fleetfiles) bool {
		if _, err := fs.Catalog(context.Background()); err != nil {
			t.Fatal(err)
		}
		return true
	}, 0},
	{"minting not supported", func(t *testing.T, fs *FileStore, ff *fleetfiles) bool {
		return false
	}, 1},
	{"object store unreachable", func(t *testing.T, fs *FileStore, ff *fleetfiles) bool {
		ff.presign, ff.readURL = true, unreachableURL(t)
		return false
	}, 1},
}

// rangedStore returns a FileStore reading from the fleetfiles model with a
// 4-byte inline limit and presigning off, holding "big.bin" and the
// catalog of a deployment that cannot presign.
func rangedStore(t *testing.T, data string) (*FileStore, *fakeCaller, *fleetfiles) {
	t.Helper()
	ff := newFleetfiles(4)
	ff.put("default", "big.bin", []byte(data))
	fs, fc := newStore(t, map[string]any{URINamespaces: ok(catalogPayload(map[string]any{
		"inline_max_bytes": int64(4), "presign_available": false,
	}))})
	ff.serve(fc)
	return fs, fc, ff
}

// objectReaders are the three ways to read an object. read returns the
// bytes it read (a file's content after GetToFile) and the descriptor (nil
// for Get).
var objectReaders = []struct {
	name string
	read func(t *testing.T, ctx context.Context, fs *FileStore, path string) ([]byte, *ObjectInfo, error)
}{
	{"Get", func(t *testing.T, ctx context.Context, fs *FileStore, _ string) ([]byte, *ObjectInfo, error) {
		data, err := fs.Get(ctx, "big.bin")
		return data, nil, err
	}},
	{"GetTo", func(t *testing.T, ctx context.Context, fs *FileStore, _ string) ([]byte, *ObjectInfo, error) {
		var buf bytes.Buffer
		info, err := fs.GetTo(ctx, "big.bin", &buf)
		return buf.Bytes(), info, err
	}},
	{"GetToFile", func(t *testing.T, ctx context.Context, fs *FileStore, path string) ([]byte, *ObjectInfo, error) {
		info, err := fs.GetToFile(ctx, "big.bin", path)
		data, rerr := os.ReadFile(path)
		if rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			t.Fatal(rerr)
		}
		return data, info, err
	}},
}

// rangeCalls returns the offsets (and lengths, -1 when not sent) of the
// ranged gets fc received.
func rangeCalls(fc *fakeCaller) (offsets, lengths []int64) {
	for _, c := range fc.callsTo(URIGet) {
		a := c.Args[0].(map[string]any)
		off, ranged := a["offset"]
		if !ranged {
			continue
		}
		offsets = append(offsets, off.(int64))
		if l, sent := a["length"]; sent {
			lengths = append(lengths, l.(int64))
		} else {
			lengths = append(lengths, -1)
		}
	}
	return offsets, lengths
}

// An object over the inline limit leaves a deployment that cannot presign,
// or reaches a device that cannot reach the object store, only in ranges
// over the router (files.read.get with offset and length, fleetfiles
// v0.1.33 and later). The read advances by what each range carried (the
// service clamps a range to its inline limit) and confirms with a final
// stat that the object did not change while the last range was read.
func TestReadFallsBackToRanges(t *testing.T) {
	for _, tr := range rangedTriggers {
		for _, rd := range objectReaders {
			t.Run(tr.name+"/"+rd.name, func(t *testing.T) {
				ctx := context.Background()
				fs, fc, ff := rangedStore(t, rangedObject)
				cached := tr.setup(t, fs, ff)
				path := filepath.Join(t.TempDir(), "out", "big.bin")
				data, info, err := rd.read(t, ctx, fs, path)
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != rangedObject {
					t.Errorf("read %q, want %q", data, rangedObject)
				}
				offsets, lengths := rangeCalls(fc)
				if want := []int64{0, 4, 8}; !reflect.DeepEqual(offsets, want) {
					t.Errorf("range offsets %v, want %v", offsets, want)
				}
				wantLength := int64(-1) // the service's own limit
				if cached {
					wantLength = 4 // the catalog's inline limit
				}
				for _, l := range lengths {
					if l != wantLength {
						t.Errorf("range lengths %v, want %d each", lengths, wantLength)
						break
					}
				}
				if n := fc.count(URIReadURL); n != tr.mints {
					t.Errorf("minted %d read URLs, want %d", n, tr.mints)
				}
				uris := fc.uris()
				if last := uris[len(uris)-1]; rd.name == "Get" && last != URIStat {
					t.Errorf("calls %v: want a final stat", uris)
				}
				if n := fc.count(URIStat); n != 1 {
					t.Errorf("%d stats, want 1 (after the last range)", n)
				}
				if rd.name != "Get" {
					if info == nil || info.Size != int64(len(rangedObject)) || info.Key != "big.bin" ||
						info.URL != "https://files.ironflock.com/f/3317/default/big.bin" {
						t.Errorf("info = %+v", info)
					}
				}
				if rd.name == "GetToFile" {
					if names := dirEntries(t, filepath.Dir(path)); !reflect.DeepEqual(names, []string{"big.bin"}) {
						t.Errorf("directory holds %v, want only the file", names)
					}
				}
			})
		}
	}
}

// Every range is a read of its own, so the object can be replaced between
// two of them, or while one is read. The read fails rather than splice two
// versions; GetToFile keeps the previous file.
func TestRangedReadOfAChangingObject(t *testing.T) {
	const old = "previous content"
	cases := []struct {
		name string
		hook func(ff *fleetfiles)
	}{
		{"replaced between ranges", func(ff *fleetfiles) {
			ff.afterRange = func(f *fleetfiles, off int64) {
				if off == 0 {
					f.put("default", "big.bin", []byte("HELLO WORLD"))
				}
			}
		}},
		{"replaced while the last range is read", func(ff *fleetfiles) {
			// The range's stat sees the old object, its read the new one:
			// only the final stat can tell.
			ff.beforeRange = func(f *fleetfiles, off int64) {
				if off == 8 {
					f.put("default", "big.bin", []byte("hello WORLD"))
				}
			}
		}},
		{"shrunk below the next range", func(ff *fleetfiles) {
			// The next range starts past the new end: INVALID_RANGE.
			ff.afterRange = func(f *fleetfiles, off int64) {
				if off == 0 {
					f.put("default", "big.bin", []byte("hi"))
				}
			}
		}},
		{"shrunk", func(ff *fleetfiles) {
			ff.afterRange = func(f *fleetfiles, off int64) {
				if off == 0 {
					f.put("default", "big.bin", []byte("hello wo"))
				}
			}
		}},
		{"grown", func(ff *fleetfiles) {
			ff.afterRange = func(f *fleetfiles, off int64) {
				if off == 4 {
					f.put("default", "big.bin", []byte("hello world, again"))
				}
			}
		}},
	}
	for _, tc := range cases {
		for _, rd := range objectReaders {
			t.Run(tc.name+"/"+rd.name, func(t *testing.T) {
				ctx := context.Background()
				fs, fc, ff := rangedStore(t, rangedObject)
				tc.hook(ff)
				path := filepath.Join(t.TempDir(), "big.bin")
				if err := os.WriteFile(path, []byte(old), 0o644); err != nil {
					t.Fatal(err)
				}
				data, info, err := rd.read(t, ctx, fs, path)
				wantError(t, err, CodeInternal, "")
				if fe := fileError(t, err); !strings.HasPrefix(fe.Reason, "object changed while it was read") {
					t.Errorf("reason = %q", fe.Reason)
				}
				if info != nil {
					t.Errorf("info = %+v, want none", info)
				}
				if rd.name == "Get" && data != nil {
					t.Errorf("Get returned %q with the error", data)
				}
				if rd.name == "GetToFile" {
					if string(data) != old {
						t.Errorf("file = %q, want the previous content", data)
					}
					if names := dirEntries(t, filepath.Dir(path)); !reflect.DeepEqual(names, []string{"big.bin"}) {
						t.Errorf("directory holds %v", names)
					}
				}
				if offsets, _ := rangeCalls(fc); len(offsets) > 3 {
					t.Errorf("range offsets %v: the read went on after the change", offsets)
				}
			})
		}
	}
}

// A file service that answers ranges wrongly cannot make the read loop,
// splice, or overrun: each malformed answer fails it with CodeInternal at
// once.
func TestRangedReadOfMalformedRanges(t *testing.T) {
	type m = map[string]any
	// answer builds a range of the 11-byte object.
	answer := func(fields m) m {
		p := m{"namespace": "default", "key": "big.bin", "size": int64(11), "etag": "e1", "encoding": "base64"}
		for k, v := range fields {
			p[k] = v
		}
		return ok(p)
	}
	cases := []struct {
		name   string
		reply  m
		reason string
	}{
		{"empty range before the end", answer(m{"offset": int64(0), "length": int64(0), "data": ""}),
			"the file service returned an empty range at offset 0 of an object of 11 bytes"},
		{"wrong offset", answer(m{"offset": int64(4), "length": int64(4), "data": "b28g"}),
			"asked for the range at offset 0, the file service answered offset 4"},
		{"no range in the answer", answer(m{"data": "aGVsbG8gd29ybGQ="}),
			"the file service answered a ranged read without a range"},
		{"length not what it carried", answer(m{"offset": int64(0), "length": int64(4), "data": "aGU="}),
			"the range at offset 0 reports 4 bytes and carries 2"},
		{"past the end", answer(m{"offset": int64(0), "length": int64(12), "data": "aGVsbG8gd29ybGQh"}),
			"the range at offset 0 carries 12 bytes, past the end of an object of 11 bytes"},
		{"not base64", answer(m{"offset": int64(0), "length": int64(4), "data": "!!!!"}),
			"object payload was not valid base64"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, fc := newStore(t, map[string]any{
				URINamespaces: ok(catalogPayload(map[string]any{"presign_available": false})),
				URIGet: replyFunc(func(args []any) (any, error) {
					if _, ranged := args[0].(map[string]any)["offset"]; !ranged {
						return fail(CodeTooLarge, "too big"), nil
					}
					return tc.reply, nil
				}),
			})
			if _, err := fs.Catalog(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err := fs.Get(context.Background(), "big.bin")
			wantError(t, err, CodeInternal, tc.reason)
			if n := fc.count(URIGet); n != 2 {
				t.Errorf("%d gets, want 2 (one plain, one ranged)", n)
			}
		})
	}
}

// A file service from before ranged reads (fleetfiles v0.1.33) ignores
// offset and length and refuses the range like the plain get, TOO_LARGE:
// the read stops after that one call, with the presigned path's own error
// when it had one. GetToFile creates nothing.
func TestRangedReadAgainstAServiceWithoutRanges(t *testing.T) {
	wantCodes := map[string]string{
		"catalog says no presign":  CodeTooLarge,
		"minting not supported":    CodeNotSupported,
		"object store unreachable": CodePresignUnreachable,
	}
	for _, tr := range rangedTriggers {
		for _, rd := range objectReaders {
			t.Run(tr.name+"/"+rd.name, func(t *testing.T) {
				ctx := context.Background()
				fs, fc, ff := rangedStore(t, rangedObject)
				ff.noRanges = true
				tr.setup(t, fs, ff)
				dir := t.TempDir()
				_, info, err := rd.read(t, ctx, fs, filepath.Join(dir, "sub", "big.bin"))
				wantError(t, err, wantCodes[tr.name], "")
				if info != nil {
					t.Errorf("info = %+v", info)
				}
				if n := fc.count(URIGet); n != 2 {
					t.Errorf("%d gets, want 2 (one plain, one ranged)", n)
				}
				if names := dirEntries(t, dir); len(names) != 0 {
					t.Errorf("directory holds %v, want nothing", names)
				}
			})
		}
	}
}

// Only an unusable presigned path falls back to ranges: any other failure
// of the read (a refusal, an object store error, a missing object) is
// returned, and an object that fits inline is read in one call.
func TestRangedReadOnlyAsAFallback(t *testing.T) {
	ctx := context.Background()
	t.Run("inline object", func(t *testing.T) {
		fs, fc, _ := rangedStore(t, "tiny")
		if _, err := fs.Catalog(ctx); err != nil {
			t.Fatal(err)
		}
		if data, err := fs.Get(ctx, "big.bin"); err != nil || string(data) != "tiny" {
			t.Fatalf("Get = %q, %v", data, err)
		}
		if offsets, _ := rangeCalls(fc); len(offsets) != 0 || fc.count(URIGet) != 1 {
			t.Errorf("ranges %v, gets %d", offsets, fc.count(URIGet))
		}
	})
	cases := []struct {
		name    string
		replies map[string]any
		status  int
		code    string
	}{
		{"mint refused", map[string]any{URIReadURL: fail(CodeNotAuthorized, "no")}, 0, CodeNotAuthorized},
		{"mint finds no object", map[string]any{URIReadURL: fail(CodeNoSuchObject, "gone")}, 0, CodeNoSuchObject},
		{"object store refuses", nil, http.StatusForbidden, CodeNotAuthorized},
		{"object store has no object", nil, http.StatusNotFound, CodeNoSuchObject},
		{"object store fails", nil, http.StatusInternalServerError, CodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, fc, _ := directStore(t, 4, respondWith(tc.status, "<Error/>"), tc.replies)
			_, err := fs.Get(ctx, "big.bin")
			wantError(t, err, tc.code, "")
			if n := fc.count(URIGet); n != 1 {
				t.Errorf("%d gets, want 1: the read fell back to ranges", n)
			}
		})
	}
}

// A ranged read ends with its context, between ranges as well, and
// GetToFile then leaves no file.
func TestRangedReadCancelled(t *testing.T) {
	for _, rd := range objectReaders {
		t.Run(rd.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fs, fc, ff := rangedStore(t, rangedObject)
			ff.afterRange = func(f *fleetfiles, off int64) {
				if off == 4 {
					cancel()
				}
			}
			dir := t.TempDir()
			_, _, err := rd.read(t, ctx, fs, filepath.Join(dir, "big.bin"))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
			if codeOf(err) != "" {
				t.Errorf("cancellation reported as %s", codeOf(err))
			}
			if offsets, _ := rangeCalls(fc); !reflect.DeepEqual(offsets, []int64{0, 4}) {
				t.Errorf("range offsets %v, want [0 4]", offsets)
			}
			if names := dirEntries(t, dir); len(names) != 0 {
				t.Errorf("directory holds %v, want nothing", names)
			}
		})
	}
}

// A zero-length read target, an object deleted mid-read, and a writer that
// fails: each ends the ranged read with its own error.
func TestRangedReadFailures(t *testing.T) {
	ctx := context.Background()
	t.Run("deleted mid-read", func(t *testing.T) {
		fs, _, ff := rangedStore(t, rangedObject)
		ff.afterRange = func(f *fleetfiles, off int64) { delete(f.objects["default"], "big.bin") }
		_, err := fs.Get(ctx, "big.bin")
		wantError(t, err, CodeNoSuchObject, "objectstore: no such object")
	})
	t.Run("writer fails", func(t *testing.T) {
		boom := errors.New("disk full")
		fs, fc, _ := rangedStore(t, rangedObject)
		if _, err := fs.GetTo(ctx, "big.bin", &errWriter{boom}); !errors.Is(err, boom) || codeOf(err) != "" {
			t.Fatalf("err = %v, want the writer's error", err)
		}
		if offsets, _ := rangeCalls(fc); len(offsets) != 1 {
			t.Errorf("range offsets %v, want only the first", offsets)
		}
	})
}
