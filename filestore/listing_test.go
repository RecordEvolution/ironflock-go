package filestore

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// fleetfilesStore returns a FileStore whose reads the fleetfiles model
// answers, holding keys in the default namespace (each object holds its own
// key).
func fleetfilesStore(t *testing.T, keys ...string) (*FileStore, *fakeCaller, *fleetfiles) {
	t.Helper()
	ff := newFleetfiles(6291456)
	for _, k := range keys {
		ff.put("default", k, []byte(k))
	}
	fs, fc := newStore(t, nil)
	ff.serve(fc)
	return fs, fc, ff
}

// The keys of the reproduction against fleetfiles v0.2.0: objects in a
// folder, in a subfolder, at the top, and next to the folder's name.
var listingKeys = []string{"2026/a.txt", "2026/b.txt", "2026/sub/c.txt", "top.txt", "2026-notes.txt", "inspections/part-1.txt"}

func objectKeys(objects []ObjectInfo) []string {
	keys := []string{}
	for _, o := range objects {
		keys = append(keys, o.Key)
	}
	return keys
}

// Iter yields every object under the prefix, in subfolders too. fleetfiles
// lists folder-style unless asked for a flat listing, and refuses a prefix
// that ends in "/": Iter asks for the flat listing, and for a folder prefix
// lists its name and keeps the keys under it.
func TestIterYieldsEveryObjectUnderAPrefix(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		prefix string
		want   []string
	}{
		{"", []string{"2026-notes.txt", "2026/a.txt", "2026/b.txt", "2026/sub/c.txt", "inspections/part-1.txt", "top.txt"}},
		{"2026/", []string{"2026/a.txt", "2026/b.txt", "2026/sub/c.txt"}},
		{"2026", []string{"2026-notes.txt", "2026/a.txt", "2026/b.txt", "2026/sub/c.txt"}},
		{"2026/sub/", []string{"2026/sub/c.txt"}},
		{"2026/sub", []string{"2026/sub/c.txt"}},
		{"2026/a", []string{"2026/a.txt"}},
		{"inspections/", []string{"inspections/part-1.txt"}},
		{"nothing/", []string{}},
	}
	for _, tc := range cases {
		for _, limit := range []int{0, 1, 2} {
			t.Run(tc.prefix+"/limit "+string(rune('0'+limit)), func(t *testing.T) {
				fs, fc, _ := fleetfilesStore(t, listingKeys...)
				keys, err := collectKeys(t, fs, ctx, Prefix(tc.prefix), Limit(limit))
				if err != nil {
					t.Fatal(err)
				}
				if keys == nil {
					keys = []string{}
				}
				if !reflect.DeepEqual(keys, tc.want) {
					t.Errorf("Iter(Prefix(%q)) = %q, want %q", tc.prefix, keys, tc.want)
				}
				for _, c := range fc.callsTo(URIList) {
					if d, sent := c.Args[0].(map[string]any)["delimiter"]; !sent || d != "" {
						t.Fatalf("page args %v: want delimiter \"\" (a flat listing)", c.Args[0])
					}
				}
			})
		}
	}
}

// A folder prefix is listed by its name, so the pages also carry the keys
// next to the folder ("2026-notes.txt" sorts before "2026/a.txt"): pages
// whose keys are all skipped do not end the iteration.
func TestIterPagesPastSkippedKeys(t *testing.T) {
	ctx := context.Background()
	fs, fc, _ := fleetfilesStore(t, "d-1", "d-2", "d-3", "d/x", "d.txt")
	keys, err := collectKeys(t, fs, ctx, Prefix("d/"), Limit(1))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"d/x"}) {
		t.Errorf("keys = %q, want [d/x]", keys)
	}
	var sent []string
	for _, c := range fc.callsTo(URIList) {
		a := c.Args[0].(map[string]any)
		sent = append(sent, a["prefix"].(string)+"|"+a["cursor"].(string))
	}
	if want := []string{"d|", "d|d-1", "d|d-2", "d|d-3", "d|d.txt"}; !reflect.DeepEqual(sent, want) {
		t.Errorf("pages (prefix|cursor) = %q, want %q", sent, want)
	}
}

// fleetfiles stores keys in Unicode NFC and matches a prefix after
// normalizing it. A folder prefix that is not in NFC cannot be matched
// against the returned keys byte for byte, which would skip every key
// silently: the iteration fails instead.
func TestIterFolderPrefixNotInNFC(t *testing.T) {
	ctx := context.Background()
	fs, _, _ := fleetfilesStore(t, "café/x.txt", "café-notes.txt")
	keys, err := collectKeys(t, fs, ctx, Prefix("café/"))
	if err != nil || !reflect.DeepEqual(keys, []string{"café/x.txt"}) {
		t.Errorf("NFC prefix: keys = %q, err = %v", keys, err)
	}
	keys, err = collectKeys(t, fs, ctx, Prefix("café/"))
	if err == nil || !strings.Contains(err.Error(), "Unicode normalization form C") {
		t.Errorf("decomposed prefix: keys = %q, err = %v; want the NFC error", keys, err)
	}
	if len(keys) != 0 {
		t.Errorf("decomposed prefix yielded %q before the error", keys)
	}
	// A prefix without the trailing "/" is matched by the service alone.
	keys, err = collectKeys(t, fs, ctx, Prefix("café"))
	if err != nil || len(keys) != 2 {
		t.Errorf("decomposed prefix without the slash: keys = %q, err = %v", keys, err)
	}
}

// The file service is the authority on prefixes: Iter takes off one
// trailing "/" and passes anything else on, so a malformed prefix is
// refused by the service, not by the SDK.
func TestIterMalformedPrefixes(t *testing.T) {
	ctx := context.Background()
	cases := []struct{ prefix, sent, reason string }{
		{"/", "/", "object key must not start with '/'"},
		{"a//", "a/", "object key must not end with '/'"},
		{"report./", "report.", `path segment "report." must not end with '.'`},
		{"a:b", "a:b", `path segment "a:b" must not contain any of \ : * ? " < > | ^ ~`},
	}
	for _, tc := range cases {
		t.Run(tc.prefix, func(t *testing.T) {
			fs, fc, _ := fleetfilesStore(t, listingKeys...)
			_, err := collectKeys(t, fs, ctx, Prefix(tc.prefix))
			wantError(t, err, CodeInternal, tc.reason)
			if got := listArg(t, fc.callsTo(URIList)[0], "prefix"); got != tc.sent {
				t.Errorf("sent prefix %q, want %q", got, tc.sent)
			}
		})
	}
}

// List is one page of the service's listing as it gives it: folder-style by
// default (subfolders in Prefixes), flat with Delimiter(""), and the prefix
// as given — fleetfiles v0.2.0 refuses a prefix ending in "/".
func TestListIsTheServicesListing(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name         string
		opts         []Option
		objects      []string
		prefixes     []string
		wantErrorMsg string
	}{
		{"folder view by default", nil,
			[]string{"2026-notes.txt", "top.txt"}, []string{"2026/", "inspections/"}, ""},
		{"prefix", []Option{Prefix("2026")},
			[]string{"2026-notes.txt"}, []string{"2026/"}, ""},
		{"flat", []Option{Delimiter("")},
			[]string{"2026-notes.txt", "2026/a.txt", "2026/b.txt", "2026/sub/c.txt", "inspections/part-1.txt", "top.txt"}, []string{}, ""},
		{"flat with a prefix", []Option{Prefix("2026"), Delimiter("")},
			[]string{"2026-notes.txt", "2026/a.txt", "2026/b.txt", "2026/sub/c.txt"}, []string{}, ""},
		{"explicit folder delimiter", []Option{Delimiter("/")},
			[]string{"2026-notes.txt", "top.txt"}, []string{"2026/", "inspections/"}, ""},
		{"folder prefix refused today", []Option{Prefix("2026/")}, nil, nil, "object key must not end with '/'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, _ := fleetfilesStore(t, listingKeys...)
			res, err := fs.List(ctx, tc.opts...)
			if tc.wantErrorMsg != "" {
				wantError(t, err, CodeInternal, tc.wantErrorMsg)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := objectKeys(res.Objects); !reflect.DeepEqual(got, tc.objects) {
				t.Errorf("objects = %q, want %q", got, tc.objects)
			}
			if !reflect.DeepEqual(res.Prefixes, tc.prefixes) {
				t.Errorf("prefixes = %q, want %q", res.Prefixes, tc.prefixes)
			}
		})
	}
	t.Run("objects and prefixes share the page", func(t *testing.T) {
		fs, _, _ := fleetfilesStore(t, listingKeys...)
		res, err := fs.List(ctx, Limit(3))
		if err != nil {
			t.Fatal(err)
		}
		if got := objectKeys(res.Objects); !reflect.DeepEqual(got, []string{"2026-notes.txt"}) ||
			!reflect.DeepEqual(res.Prefixes, []string{"2026/", "inspections/"}) || !res.IsTruncated {
			t.Fatalf("page 1 = %q %q truncated %v", got, res.Prefixes, res.IsTruncated)
		}
		res, err = fs.List(ctx, Limit(3), Cursor(res.Cursor))
		if err != nil {
			t.Fatal(err)
		}
		if got := objectKeys(res.Objects); !reflect.DeepEqual(got, []string{"top.txt"}) || len(res.Prefixes) != 0 || res.IsTruncated {
			t.Errorf("page 2 = %q %q truncated %v", got, res.Prefixes, res.IsTruncated)
		}
	})
}
