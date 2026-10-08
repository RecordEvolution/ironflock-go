package filestore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/RecordEvolution/ironflock-go/wamp"
)

// Failures arrive inside a successful WAMP result, so a call that ignored
// the envelope would treat every failure as success.
func TestEnvelopeFailuresAreTypedCodes(t *testing.T) {
	ctx := context.Background()
	fs, _ := newStore(t, map[string]any{URIStat: fail(CodeNoSuchObject, "dataplane: no such object")})

	_, err := fs.Stat(ctx, "nope", Namespace("frames"))
	fe := wantError(t, err, CodeNoSuchObject, "dataplane: no such object")
	if got, want := fe.Error(), "NO_SUCH_OBJECT: dataplane: no such object"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, &Error{Code: CodeNoSuchObject}) {
		t.Error("errors.Is does not match the code")
	}
	if errors.Unwrap(err) != nil {
		t.Errorf("a service failure has no cause, got %v", errors.Unwrap(err))
	}
}

func TestEnvelopeDefaults(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		reply      any
		wantCode   string
		wantReason string
	}{
		{"unknown code passes through", fail("SOMETHING_NEW", "later"), "SOMETHING_NEW", "later"},
		{"missing code is INTERNAL", map[string]any{"success": false, "reason": "boom"}, CodeInternal, "boom"},
		{"missing reason stays empty", map[string]any{"success": false, "code": CodeQuotaExceeded}, CodeQuotaExceeded, ""},
		{"null code is INTERNAL", map[string]any{"success": false, "code": nil}, CodeInternal, ""},
		{"absent success is a failure", map[string]any{"payload": map[string]any{}}, CodeInternal, ""},
		{"success 0 is a failure", map[string]any{"success": int64(0), "code": "X"}, "X", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, _ := newStore(t, map[string]any{URIUsage: tc.reply})
			_, err := fs.Usage(ctx)
			fe := wantError(t, err, tc.wantCode, "")
			if fe.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", fe.Reason, tc.wantReason)
			}
			if tc.wantReason == "" && fe.Error() != tc.wantCode {
				t.Errorf("Error() = %q, want just the code", fe.Error())
			}
		})
	}
}

func TestNonObjectResultIsInternal(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		reply any
		typ   string
	}{
		{&wamp.Result{}, "null"},
		{&wamp.Result{Args: []any{nil}}, "null"},
		{[]any{map[string]any{"success": true}}, "array"},
		{"ok", "string"},
		{int64(1), "number"},
		{1.5, "number"},
		{true, "boolean"},
		{[]byte("x"), "binary"},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			fs, _ := newStore(t, map[string]any{URIStat: tc.reply})
			_, err := fs.Stat(ctx, "a")
			wantError(t, err, CodeInternal, fmt.Sprintf("files.read.stat returned %s, expected an object", tc.typ))
		})
	}
}

func TestEnvelopePayload(t *testing.T) {
	ctx := context.Background()
	t.Run("non-object payload reads as empty", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIUsage: map[string]any{"success": true, "payload": "nonsense"}})
		u, err := fs.Usage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if want := (&StorageUsage{FreeBytes: -1}); !reflect.DeepEqual(u, want) {
			t.Errorf("usage = %+v, want %+v", u, want)
		}
	})
	t.Run("no payload", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIDelete: map[string]any{"success": true}})
		if err := fs.Delete(ctx, "a"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("map[any]any envelope", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIUsage: map[any]any{"success": true, "payload": map[any]any{"size_bytes": int64(7)}}})
		u, err := fs.Usage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if u.SizeBytes != 7 {
			t.Errorf("SizeBytes = %d, want 7", u.SizeBytes)
		}
	})
}

// Router-level rejections DO arrive as WAMP errors. A deployment with no
// file service must be distinguishable from "you may not do this".
func TestWampErrorMapping(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		uri        string
		args       []any
		wantCode   string
		wantReason string
	}{
		{wamp.URINoSuchProcedure, nil, CodeNotAvailable,
			"the file service is not available on this deployment"},
		{wamp.URINoSuchProcedure, []any{"no callee for files.read.usage"}, CodeNotAvailable,
			`the file service is not available on this deployment: "no callee for files.read.usage"`},
		{wamp.URINotAuthorized, nil, CodeNotAuthorized, "wamp.error.not_authorized"},
		{wamp.URINotAuthorized, []any{map[string]any{"why": "<role> & more"}}, CodeNotAuthorized,
			`wamp.error.not_authorized: {"why":"<role> & more"}`},
		{wamp.URIAuthorizationFailed, []any{"first", "second"}, CodeNotAuthorized,
			`wamp.error.authorization_failed: "first"`},
		{wamp.URIAuthenticationFailed, []any{int64(3)}, CodeNotAuthorized,
			"wamp.error.authentication_failed: 3"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%d", tc.uri, len(tc.args)), func(t *testing.T) {
			we := &wamp.Error{URI: tc.uri, Args: tc.args}
			fs, _ := newStore(t, map[string]any{URIUsage: we})
			_, err := fs.Usage(ctx)
			wantError(t, err, tc.wantCode, tc.wantReason)
			var got *wamp.Error
			if !errors.As(err, &got) || got != we {
				t.Errorf("the WAMP error is not reachable through the mapped error")
			}
			if !errors.Is(err, &Error{Code: tc.wantCode}) {
				t.Errorf("errors.Is does not match %s", tc.wantCode)
			}
		})
	}

	t.Run("wrapped WAMP error", func(t *testing.T) {
		we := &wamp.Error{URI: wamp.URINotAuthorized}
		fs, _ := newStore(t, map[string]any{URIPut: fmt.Errorf("call failed: %w", we)})
		_, err := fs.Put(ctx, "a.jpg", []byte("x"), Namespace("frames"))
		wantError(t, err, CodeNotAuthorized, "wamp.error.not_authorized")
	})

	t.Run("unmapped WAMP error passes through", func(t *testing.T) {
		we := &wamp.Error{URI: wamp.URIRuntimeError, Args: []any{"boom"}}
		fs, _ := newStore(t, map[string]any{URIUsage: we})
		_, err := fs.Usage(ctx)
		if err != we {
			t.Fatalf("err = %v (%T), want the WAMP error itself", err, err)
		}
		var fe *Error
		if errors.As(err, &fe) {
			t.Error("an unmapped WAMP error must not become a *filestore.Error")
		}
	})

	t.Run("other errors pass through", func(t *testing.T) {
		notConnected := errors.New("not connected")
		fs, _ := newStore(t, map[string]any{URIUsage: notConnected})
		if _, err := fs.Usage(ctx); err != notConnected {
			t.Fatalf("err = %v, want the caller's error", err)
		}
	})

	t.Run("file errors pass through unchanged", func(t *testing.T) {
		orig := wrapError(CodeNotAvailable, "x", &wamp.Error{URI: wamp.URINoSuchProcedure})
		fs, _ := newStore(t, map[string]any{URIUsage: orig})
		if _, err := fs.Usage(ctx); err != orig {
			t.Fatalf("err = %v, want the original *Error", err)
		}
	})

	t.Run("the catalog fetch is mapped too", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URINamespaces: &wamp.Error{URI: wamp.URINoSuchProcedure}})
		_, err := fs.Put(ctx, "a", []byte("x"))
		wantError(t, err, CodeNotAvailable, "the file service is not available on this deployment")
	})
}

// Every method's URI and exact positional args. No kwargs, no call options
// and no retry window are ever passed.
func TestWireShapes(t *testing.T) {
	ctx := context.Background()
	obj := ok(map[string]any{"namespace": "default", "key": "k"})
	replies := map[string]any{
		URIUsage:    ok(map[string]any{}),
		URIList:     ok(map[string]any{"objects": []any{}, "is_truncated": false, "cursor": ""}),
		URIStat:     obj,
		URIGet:      ok(map[string]any{"namespace": "default", "key": "k", "data": "", "encoding": "base64"}),
		URIPut:      obj,
		URIDelete:   ok(map[string]any{"deleted": true}),
		URICopy:     obj,
		URIReadURL:  ok(map[string]any{"url": "https://s3.example/get", "method": "GET"}),
		URIWriteURL: ok(map[string]any{"url": "https://s3.example/put", "method": "PUT", "headers": map[string]any{}}),
	}
	type m = map[string]any
	cases := []struct {
		name string
		run  func(fs *FileStore) error
		uri  string
		args []any
	}{
		{"Catalog", func(fs *FileStore) error { _, err := fs.Catalog(ctx); return err },
			URINamespaces, nil},
		{"Usage", func(fs *FileStore) error { _, err := fs.Usage(ctx); return err },
			URIUsage, nil},
		{"Usage detail", func(fs *FileStore) error { _, err := fs.Usage(ctx, Detail()); return err },
			URIUsage, []any{m{"detail": true}}},
		{"List defaults", func(fs *FileStore) error { _, err := fs.List(ctx); return err },
			URIList, []any{m{"namespace": "default", "prefix": "", "limit": int64(200), "cursor": ""}}},
		{"List options", func(fs *FileStore) error {
			_, err := fs.List(ctx, Namespace("frames"), Prefix("a/"), Limit(50), Cursor("c1"))
			return err
		}, URIList, []any{m{"namespace": "frames", "prefix": "a/", "limit": int64(50), "cursor": "c1"}}},
		{"List explicit empty namespace", func(fs *FileStore) error { _, err := fs.List(ctx, Namespace("")); return err },
			URIList, []any{m{"namespace": "", "prefix": "", "limit": int64(200), "cursor": ""}}},
		{"List zero limit is the default", func(fs *FileStore) error { _, err := fs.List(ctx, Limit(0)); return err },
			URIList, []any{m{"namespace": "default", "prefix": "", "limit": int64(200), "cursor": ""}}},
		{"Stat", func(fs *FileStore) error { _, err := fs.Stat(ctx, "a.jpg", Namespace("frames")); return err },
			URIStat, []any{m{"namespace": "frames", "key": "a.jpg"}}},
		{"Exists", func(fs *FileStore) error { _, err := fs.Exists(ctx, "a.jpg"); return err },
			URIStat, []any{m{"namespace": "default", "key": "a.jpg"}}},
		{"Get", func(fs *FileStore) error { _, err := fs.Get(ctx, "a.jpg", Namespace("frames")); return err },
			URIGet, []any{m{"namespace": "frames", "key": "a.jpg"}}},
		{"Delete", func(fs *FileStore) error { return fs.Delete(ctx, "a.jpg") },
			URIDelete, []any{m{"namespace": "default", "key": "a.jpg"}}},
		{"Copy", func(fs *FileStore) error { _, err := fs.Copy(ctx, "a", "b", Namespace("frames")); return err },
			URICopy, []any{m{"namespace": "frames", "key": "a", "to": "b"}}},
		{"Copy to namespace", func(fs *FileStore) error {
			_, err := fs.Copy(ctx, "a", "b", ToNamespace("archive"))
			return err
		}, URICopy, []any{m{"namespace": "default", "key": "a", "to": "b", "to_namespace": "archive"}}},
		{"Copy empty to-namespace omitted", func(fs *FileStore) error { _, err := fs.Copy(ctx, "a", "b", ToNamespace("")); return err },
			URICopy, []any{m{"namespace": "default", "key": "a", "to": "b"}}},
		{"ShareURL", func(fs *FileStore) error { _, err := fs.ShareURL(ctx, "a b.txt"); return err },
			URIReadURL, []any{m{"namespace": "default", "key": "a b.txt", "expires_in": int64(900)}}},
		{"ShareURL TTL", func(fs *FileStore) error {
			_, err := fs.ShareURL(ctx, "a", Namespace("frames"), TTL(120*time.Second))
			return err
		}, URIReadURL, []any{m{"namespace": "frames", "key": "a", "expires_in": int64(120)}}},
		{"ShareURL TTL rounds up", func(fs *FileStore) error { _, err := fs.ShareURL(ctx, "a", TTL(1500*time.Millisecond)); return err },
			URIReadURL, []any{m{"namespace": "default", "key": "a", "expires_in": int64(2)}}},
		{"ShareURL zero TTL is the default", func(fs *FileStore) error { _, err := fs.ShareURL(ctx, "a", TTL(0)); return err },
			URIReadURL, []any{m{"namespace": "default", "key": "a", "expires_in": int64(900)}}},
		{"UploadURL", func(fs *FileStore) error { _, err := fs.UploadURL(ctx, "u.bin"); return err },
			URIWriteURL, []any{m{"namespace": "default", "key": "u.bin", "expires_in": int64(3600)}}},
		{"UploadURL options", func(fs *FileStore) error {
			_, err := fs.UploadURL(ctx, "u.bin", TTL(time.Minute), ContentType("application/octet-stream"), Size(10), Namespace("frames"))
			return err
		}, URIWriteURL, []any{m{"namespace": "frames", "key": "u.bin", "expires_in": int64(60),
			"content_type": "application/octet-stream", "size": int64(10)}}},
		{"Put", func(fs *FileStore) error {
			_, err := fs.Put(ctx, "part-1.jpg", []byte("hey"), Namespace("frames"), ContentType("image/jpeg"))
			return err
		}, URIPut, []any{m{"namespace": "frames", "key": "part-1.jpg", "data": "aGV5", "content_type": "image/jpeg"}}},
		{"Put without content type", func(fs *FileStore) error { _, err := fs.Put(ctx, "k", []byte{1, 2, 3}); return err },
			URIPut, []any{m{"namespace": "default", "key": "k", "data": "AQID"}}},
		{"Put empty", func(fs *FileStore) error { _, err := fs.Put(ctx, "k", nil); return err },
			URIPut, []any{m{"namespace": "default", "key": "k", "data": ""}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, fc := newStore(t, replies)
			if err := tc.run(fs); err != nil {
				t.Fatal(err)
			}
			calls := fc.callsTo(tc.uri)
			if len(calls) != 1 {
				t.Fatalf("%s called %d times, want 1 (calls: %v)", tc.uri, len(calls), fc.uris())
			}
			if !reflect.DeepEqual(calls[0].Args, tc.args) {
				t.Errorf("args = %#v\nwant   %#v", calls[0].Args, tc.args)
			}
			if tc.args == nil && len(calls[0].Args) != 0 {
				t.Errorf("args = %#v, want none", calls[0].Args)
			}
			for _, c := range fc.allCalls() {
				if c.Kwargs != nil || c.Opts != nil || c.Retry != 0 {
					t.Errorf("%s: kwargs %v, options %v, retry window %v; want none", c.URI, c.Kwargs, c.Opts, c.Retry)
				}
				if c.URI != tc.uri && c.URI != URINamespaces {
					t.Errorf("unexpected call to %s", c.URI)
				}
			}
		})
	}
}

func TestNilCaller(t *testing.T) {
	fs := New(nil, nil)
	if _, err := fs.Stat(context.Background(), "a"); !errors.Is(err, errNoCaller) {
		t.Fatalf("err = %v, want errNoCaller", err)
	}
}

func TestErrorType(t *testing.T) {
	cause := errors.New("dial refused")
	e := wrapError(CodePresignUnreachable, "could not reach", cause)
	if e.Error() != "PRESIGN_UNREACHABLE: could not reach" {
		t.Errorf("Error() = %q", e.Error())
	}
	if !errors.Is(e, cause) {
		t.Error("the cause is not reachable with errors.Is")
	}
	if !errors.Is(e, &Error{Code: CodePresignUnreachable}) {
		t.Error("errors.Is does not match the code alone")
	}
	if !errors.Is(e, &Error{Code: CodePresignUnreachable, Reason: "could not reach"}) {
		t.Error("errors.Is does not match code and reason")
	}
	if errors.Is(e, &Error{Code: CodePresignUnreachable, Reason: "other"}) {
		t.Error("errors.Is matches a different reason")
	}
	if errors.Is(e, &Error{Code: CodeInternal}) {
		t.Error("errors.Is matches a different code")
	}
	if (&Error{Code: CodeInternal}).Error() != "INTERNAL" {
		t.Error("an error without reason must read as its code")
	}
}

// ErrorCodes hands out a copy: a caller cannot change the list for others.
func TestErrorCodesIsACopy(t *testing.T) {
	want := []string{
		CodeNotAuthorized, CodeNoSuchNamespace, CodeNoSuchObject, CodeTooLarge,
		CodeObjectTooLarge, CodeQuotaExceeded, CodeContentTypeNotAllowed,
		CodeNotSupported, CodeInternal, CodeNotAvailable, CodePresignUnreachable,
		CodeClockSkew,
	}
	codes := ErrorCodes()
	if !reflect.DeepEqual(codes, want) {
		t.Fatalf("ErrorCodes() = %q, want %q", codes, want)
	}
	codes[0] = "CHANGED"
	if got := ErrorCodes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the list changed through a returned slice: %q", got)
	}
}
