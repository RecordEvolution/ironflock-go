package filestore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// directStore returns a FileStore whose catalog has the given inline cap and
// the direct path available, with the mint calls pointing at an object
// store answering with respond.
func directStore(t *testing.T, inlineMax int64, respond func(w http.ResponseWriter, r *http.Request), replies map[string]any) (*FileStore, *fakeCaller, *objectStore) {
	t.Helper()
	store := newObjectStore(t, respond)
	all := map[string]any{
		URINamespaces: directCatalog(inlineMax),
		URIWriteURL:   ok(map[string]any{"url": store.url("/bucket/put"), "method": "PUT", "headers": map[string]any{}}),
		URIReadURL:    ok(map[string]any{"url": store.url("/bucket/get"), "method": "GET"}),
		URIGet:        fail(CodeTooLarge, "too big"),
		URIStat:       ok(map[string]any{"namespace": "frames", "key": "big.bin", "size": int64(100), "etag": "e"}),
	}
	maps.Copy(all, replies)
	fs, fc := newStore(t, all)
	return fs, fc, store
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// onlyReader hides every method of a reader but Read.
type onlyReader struct{ r io.Reader }

func (o onlyReader) Read(p []byte) (int, error) { return o.r.Read(p) }

// One round trip beats two: below the cap a put must not pay for a mint.
// Above it the object cannot cross the router at all.
func TestPutPathSelection(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		inlineMax int64
		size      int
		direct    bool
	}{
		{"small object inline", 6291456, 3, false},
		{"equal to the cap inline", 10, 10, false},
		{"over the cap direct", 10, 11, true},
		{"no cap known: always inline", 0, 100, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, fc, store := directStore(t, tc.inlineMax, nil, map[string]any{
				URIPut: ok(map[string]any{"namespace": "frames", "key": "k", "size": int64(tc.size)}),
			})
			if _, err := fs.Put(ctx, "k", make([]byte, tc.size), Namespace("frames")); err != nil {
				t.Fatal(err)
			}
			wantPut, wantMint, wantHTTP := 1, 0, 0
			if tc.direct {
				wantPut, wantMint, wantHTTP = 0, 1, 1
			}
			if fc.count(URIPut) != wantPut || fc.count(URIWriteURL) != wantMint || len(store.requests()) != wantHTTP {
				t.Errorf("put %d, mint %d, HTTP %d; want %d, %d, %d",
					fc.count(URIPut), fc.count(URIWriteURL), len(store.requests()), wantPut, wantMint, wantHTTP)
			}
		})
	}
}

func TestPutDirect(t *testing.T) {
	ctx := context.Background()
	data := randomBytes(t, 100)
	var store *objectStore
	fs, fc, store := directStore(t, 10, nil, map[string]any{
		URIWriteURL: replyFunc(func([]any) (any, error) {
			return ok(map[string]any{
				"url":    store.url("/bucket/frames/big.jpg"),
				"method": "PUT",
				"headers": map[string]any{
					"Content-Type":      "image/jpeg",
					"x-amz-meta-origin": "device",
					"Content-Length":    "999", // overridden by the real size
				},
				"expires_in": int64(3600),
			}), nil
		}),
		URIStat: replyFunc(func([]any) (any, error) {
			// The descriptor is read back after the upload: S3 does not return one.
			if n := len(store.requests()); n != 1 {
				return nil, fmt.Errorf("stat before the upload finished (%d requests)", n)
			}
			return ok(map[string]any{"namespace": "frames", "key": "big.jpg", "size": int64(100), "etag": "e"}), nil
		}),
	})
	info, err := fs.Put(ctx, "big.jpg", data, Namespace("frames"), ContentType("image/jpeg"))
	if err != nil {
		t.Fatal(err)
	}

	wantMint := []any{map[string]any{"namespace": "frames", "key": "big.jpg", "expires_in": int64(3600),
		"content_type": "image/jpeg", "size": int64(100)}}
	if got := fc.callsTo(URIWriteURL); len(got) != 1 || !reflect.DeepEqual(got[0].Args, wantMint) {
		t.Errorf("mint calls = %+v, want args %v", got, wantMint)
	}
	if fc.count(URIPut) != 0 {
		t.Error("a large object went inline")
	}
	if want := []string{URINamespaces, URIWriteURL, URIStat}; !reflect.DeepEqual(fc.uris(), want) {
		t.Errorf("calls = %v, want %v", fc.uris(), want)
	}
	reqs := store.requests()
	if len(reqs) != 1 {
		t.Fatalf("%d HTTP requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Method != http.MethodPut || r.Path != "/bucket/frames/big.jpg" || r.RawQuery != "X-Amz-Signature=secret-signature" {
		t.Errorf("request = %s %s?%s", r.Method, r.Path, r.RawQuery)
	}
	if r.ContentLength != 100 || len(r.TransferEncoding) != 0 {
		t.Errorf("Content-Length %d, Transfer-Encoding %v; want 100, none (no chunked upload)", r.ContentLength, r.TransferEncoding)
	}
	if r.Header.Get("Content-Type") != "image/jpeg" || r.Header.Get("X-Amz-Meta-Origin") != "device" {
		t.Errorf("minted headers not sent: %v", r.Header)
	}
	if !bytes.Equal(r.Body, data) {
		t.Error("the uploaded body differs from the data")
	}
	if info.Size != 100 || info.Key != "big.jpg" || info.URL != "https://files.ironflock.com/f/3317/frames/big.jpg" {
		t.Errorf("info = %+v", info)
	}
}

// Exactly the minted headers are sent (no default Content-Type), and the
// minted method is ignored: a presigned upload is always a PUT.
func TestPutDirectSendsOnlyTheMintedHeaders(t *testing.T) {
	ctx := context.Background()
	var store *objectStore
	fs, _, store := directStore(t, 10, nil, map[string]any{
		URIWriteURL: replyFunc(func([]any) (any, error) {
			return ok(map[string]any{"url": store.url("/p"), "method": "POST"}), nil
		}),
	})
	if _, err := fs.Put(ctx, "big.bin", make([]byte, 50)); err != nil {
		t.Fatal(err)
	}
	r := store.requests()[0]
	if r.Method != http.MethodPut {
		t.Errorf("method = %s, want PUT", r.Method)
	}
	if ct, has := r.Header["Content-Type"]; has {
		t.Errorf("Content-Type %v sent although none was minted", ct)
	}
}

func TestPutDirectRefusals(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		catalog map[string]any
		reason  string
	}{
		// On an air-gapped appliance the object is untransferable, not merely
		// oversized: no retry or smaller chunk can help.
		{"no direct endpoint", map[string]any{"inline_max_bytes": int64(10), "presign_available": false},
			"object is 100 bytes, over the 10-byte inline limit, and this deployment has no direct storage endpoint to upload it through"},
		// Above the single-PUT ceiling multipart is required, which does not
		// exist yet: better to say so than to upload for an hour.
		{"over the single-upload ceiling", map[string]any{"inline_max_bytes": int64(10), "presign_available": true, "presign_max_bytes": int64(50)},
			"object is 100 bytes, over the 50-byte single-upload limit; multipart upload is not implemented yet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, fc := newStore(t, map[string]any{URINamespaces: ok(catalogPayload(tc.catalog))})
			_, err := fs.Put(ctx, "big.jpg", make([]byte, 100), Namespace("frames"))
			wantError(t, err, CodeTooLarge, tc.reason)
			_, err = fs.PutReader(ctx, "big.jpg", bytes.NewReader(make([]byte, 100)), 100)
			wantError(t, err, CodeTooLarge, tc.reason)
			if fc.count(URIWriteURL) != 0 || fc.count(URIPut) != 0 {
				t.Error("a refused object was sent")
			}
		})
	}
	t.Run("presign cap 0 means no cap", func(t *testing.T) {
		fs, _, store := directStore(t, 10, nil, map[string]any{
			URINamespaces: ok(catalogPayload(map[string]any{"inline_max_bytes": int64(10), "presign_available": true})),
		})
		if _, err := fs.Put(ctx, "big.jpg", make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
		if len(store.requests()) != 1 {
			t.Error("not uploaded")
		}
	})
	t.Run("no upload URL", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{
			URINamespaces: directCatalog(10),
			URIWriteURL:   ok(map[string]any{"headers": map[string]any{}}),
		})
		_, err := fs.Put(ctx, "big.jpg", make([]byte, 100))
		wantError(t, err, CodeInternal, "the service returned no upload URL")
		if fc.count(URIStat) != 0 {
			t.Error("stat after a failed upload")
		}
	})
}

func TestUploadAcceptsAny2xx(t *testing.T) {
	for _, status := range []int{200, 201, 202, 204, 299} {
		fs, _, _ := directStore(t, 10, respondWith(status, ""), nil)
		if _, err := fs.Put(context.Background(), "big.bin", make([]byte, 20)); err != nil {
			t.Errorf("HTTP %d: %v", status, err)
		}
	}
}

// Classification of object store failures, the same for both directions.
func TestHTTPErrorClassification(t *testing.T) {
	const skewed = "<Error><Code>RequestTimeTooSkewed</Code><Message>The difference between the request time and the current time is too large.</Message></Error>"
	const ntp = "the device clock is too far out of step for the object store to accept the request; check NTP"
	cases := []struct {
		name   string
		status int
		body   string
		code   string
		reason string
	}{
		// SigV4 rejects requests more than 15 minutes out of step; on a device
		// without NTP that would otherwise read as a credential problem.
		{"clock skew before status mapping", 403, skewed, CodeClockSkew, ntp},
		{"clock skew on any status", 400, skewed, CodeClockSkew, ntp},
		{"401", 401, "", CodeNotAuthorized, "the object store rejected the signed URL (HTTP 401)"},
		{"403", 403, "<Error><Code>SignatureDoesNotMatch</Code></Error>", CodeNotAuthorized, "the object store rejected the signed URL (HTTP 403)"},
		{"404", 404, "<Error><Code>NoSuchKey</Code></Error>", CodeNoSuchObject, "the object store has no such object"},
		{"500", 500, "boom", CodeInternal, "object store returned HTTP 500: boom"},
		{"body truncated to 400 characters", 503, strings.Repeat("x", 1000), CodeInternal,
			"object store returned HTTP 503: " + strings.Repeat("x", 400)},
		{"truncation counts characters", 500, strings.Repeat("é", 1000), CodeInternal,
			"object store returned HTTP 500: " + strings.Repeat("é", 400)},
		{"skew beyond 400 characters is not seen", 500, strings.Repeat("x", 400) + "RequestTimeTooSkewed", CodeInternal,
			"object store returned HTTP 500: " + strings.Repeat("x", 400)},
		{"invalid UTF-8", 500, "a\xffb", CodeInternal, "object store returned HTTP 500: a�b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, fc, _ := directStore(t, 10, respondWith(tc.status, tc.body), nil)
			_, err := fs.Put(context.Background(), "big.bin", make([]byte, 100))
			wantError(t, err, tc.code, tc.reason)
			if fc.count(URIStat) != 0 {
				t.Error("stat after a failed upload")
			}
			_, err = fs.Get(context.Background(), "big.bin")
			wantError(t, err, tc.code, tc.reason)
		})
	}
}

// A corporate proxy allowing only the router is a real deployment shape; it
// must not look like a permissions failure.
func TestPresignUnreachable(t *testing.T) {
	ctx := context.Background()
	check := func(t *testing.T, err error) {
		t.Helper()
		wantError(t, err, CodePresignUnreachable, "")
		fe := fileError(t, err)
		if !strings.HasPrefix(fe.Reason, "could not reach the object store directly (") ||
			!strings.HasSuffix(fe.Reason, "); a proxy may allow only the router") {
			t.Errorf("reason = %q", fe.Reason)
		}
		if strings.Contains(err.Error(), "secret-signature") || strings.Contains(fmt.Sprintf("%v", errors.Unwrap(err)), "secret-signature") {
			t.Errorf("the presigned URL leaked into the error: %v / %v", err, errors.Unwrap(err))
		}
		var opErr *net.OpError
		if !errors.As(err, &opErr) {
			t.Errorf("the network error is not reachable: %v", errors.Unwrap(err))
		}
	}
	url := unreachableURL(t)
	fs, fc := newStore(t, map[string]any{
		URINamespaces: directCatalog(10),
		URIWriteURL:   ok(map[string]any{"url": url, "headers": map[string]any{}}),
		URIGet:        fail(CodeTooLarge, "too big"),
		URIReadURL:    ok(map[string]any{"url": url}),
	})
	_, err := fs.Put(ctx, "big.jpg", make([]byte, 100), Namespace("frames"))
	check(t, err)
	_, err = fs.Get(ctx, "big.jpg")
	check(t, err)
	if fc.count(URIStat) != 0 {
		t.Error("stat after a failed transfer")
	}
}

// A cancelled context ends a transfer with the context's error, not a file
// error.
func TestTransferCancellation(t *testing.T) {
	block := func(arrived chan<- struct{}) func(w http.ResponseWriter, r *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			arrived <- struct{}{}
			<-r.Context().Done()
		}
	}
	run := func(t *testing.T, op func(ctx context.Context, fs *FileStore) error) {
		arrived := make(chan struct{}, 1)
		fs, fc, _ := directStore(t, 10, block(arrived), nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-arrived
			cancel()
		}()
		err := op(ctx, fs)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		var fe *Error
		if errors.As(err, &fe) {
			t.Errorf("cancellation reported as a file error: %v", fe)
		}
		if fc.count(URIStat) != 0 {
			t.Error("stat after a cancelled transfer")
		}
	}
	t.Run("upload", func(t *testing.T) {
		run(t, func(ctx context.Context, fs *FileStore) error {
			_, err := fs.Put(ctx, "big.bin", make([]byte, 100))
			return err
		})
	})
	t.Run("download", func(t *testing.T) {
		run(t, func(ctx context.Context, fs *FileStore) error {
			_, err := fs.Get(ctx, "big.bin")
			return err
		})
	})
	t.Run("deadline", func(t *testing.T) {
		arrived := make(chan struct{}, 1)
		fs, _, _ := directStore(t, 10, block(arrived), nil)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := fs.GetTo(ctx, "big.bin", io.Discard)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want deadline exceeded", err)
		}
	})
}

// The service refuses to inline an object over the cap; Get must not make
// the caller know that limit.
func TestGetFallsBackToTheDirectPath(t *testing.T) {
	ctx := context.Background()
	fs, fc, store := directStore(t, 10, respondWith(200, "big-payload"), nil)
	data, err := fs.Get(ctx, "big.bin", Namespace("frames"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "big-payload" {
		t.Errorf("data = %q", data)
	}
	wantMint := []any{map[string]any{"namespace": "frames", "key": "big.bin", "expires_in": int64(900)}}
	if got := fc.callsTo(URIReadURL); len(got) != 1 || !reflect.DeepEqual(got[0].Args, wantMint) {
		t.Errorf("read URL calls = %+v, want args %v", got, wantMint)
	}
	if fc.count(URINamespaces) != 0 {
		t.Error("the get fallback consulted the catalog")
	}
	reqs := store.requests()
	if len(reqs) != 1 || reqs[0].Method != http.MethodGet || reqs[0].Path != "/bucket/get" || reqs[0].RawQuery != "X-Amz-Signature=secret-signature" {
		t.Fatalf("requests = %+v", reqs)
	}

	t.Run("other codes do not fall back", func(t *testing.T) {
		fs, fc, _ := directStore(t, 10, nil, map[string]any{URIGet: fail(CodeNotAuthorized, "no")})
		_, err := fs.Get(ctx, "big.bin")
		wantError(t, err, CodeNotAuthorized, "no")
		if fc.count(URIReadURL) != 0 {
			t.Error("minted a URL for a refused get")
		}
	})
	t.Run("no download URL", func(t *testing.T) {
		fs, _, store := directStore(t, 10, nil, map[string]any{URIReadURL: ok(map[string]any{"url": ""})})
		_, err := fs.Get(ctx, "big.bin")
		wantError(t, err, CodeInternal, "the service returned no download URL")
		if len(store.requests()) != 0 {
			t.Error("downloaded without a URL")
		}
	})
	t.Run("stored bytes verbatim", func(t *testing.T) {
		// An object stored with Content-Encoding: gzip comes back as stored,
		// not transparently decompressed.
		raw := "\x1f\x8b raw stored bytes"
		fs, _, store := directStore(t, 10, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = io.WriteString(w, raw)
		}, nil)
		data, err := fs.Get(ctx, "big.bin")
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != raw {
			t.Errorf("data = %q, want %q", data, raw)
		}
		if ae := store.requests()[0].Header.Get("Accept-Encoding"); ae != "identity" {
			t.Errorf("Accept-Encoding = %q", ae)
		}
	})
}

func TestPutReader(t *testing.T) {
	ctx := context.Background()
	t.Run("streams on the direct path", func(t *testing.T) {
		data := randomBytes(t, 3<<20)
		fs, fc, store := directStore(t, 1024, nil, nil)
		if _, err := fs.PutReader(ctx, "big.bin", onlyReader{bytes.NewReader(data)}, int64(len(data)), ContentType("application/octet-stream")); err != nil {
			t.Fatal(err)
		}
		r := store.requests()[0]
		if r.ContentLength != int64(len(data)) || len(r.TransferEncoding) != 0 || !bytes.Equal(r.Body, data) {
			t.Errorf("Content-Length %d, Transfer-Encoding %v, body intact %v", r.ContentLength, r.TransferEncoding, bytes.Equal(r.Body, data))
		}
		mint := fc.callsTo(URIWriteURL)[0].Args[0].(map[string]any)
		if mint["size"] != int64(len(data)) || mint["content_type"] != "application/octet-stream" {
			t.Errorf("mint args = %v", mint)
		}
	})
	t.Run("reads exactly size bytes", func(t *testing.T) {
		data := randomBytes(t, 5000)
		src := bytes.NewReader(data)
		fs, _, store := directStore(t, 1024, nil, nil)
		if _, err := fs.PutReader(ctx, "big.bin", onlyReader{src}, 2000); err != nil {
			t.Fatal(err)
		}
		if body := store.requests()[0].Body; !bytes.Equal(body, data[:2000]) {
			t.Errorf("uploaded %d bytes, want the first 2000", len(body))
		}
		if src.Len() != 3000 {
			t.Errorf("%d bytes left unread, want 3000", src.Len())
		}
	})
	t.Run("short reader on the direct path", func(t *testing.T) {
		fs, fc, _ := directStore(t, 1024, nil, nil)
		_, err := fs.PutReader(ctx, "big.bin", onlyReader{bytes.NewReader(make([]byte, 1500))}, 2000)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want unexpected EOF", err)
		}
		if codeOf(err) != "" {
			t.Errorf("a short reader reported as %s", codeOf(err))
		}
		if fc.count(URIStat) != 0 {
			t.Error("stat after a failed upload")
		}
	})
	t.Run("failing reader on the direct path", func(t *testing.T) {
		boom := errors.New("disk on fire")
		fs, _, _ := directStore(t, 1024, nil, nil)
		r := io.MultiReader(bytes.NewReader(make([]byte, 100)), &errReader{boom})
		if _, err := fs.PutReader(ctx, "big.bin", r, 2000); !errors.Is(err, boom) || codeOf(err) != "" {
			t.Fatalf("err = %v, want the reader's error", err)
		}
	})
	t.Run("inline within the cap", func(t *testing.T) {
		src := strings.NewReader("hello world")
		fs, fc, store := directStore(t, 1024, nil, map[string]any{URIPut: ok(map[string]any{"namespace": "default", "key": "k"})})
		if _, err := fs.PutReader(ctx, "k", src, 5); err != nil {
			t.Fatal(err)
		}
		args := fc.callsTo(URIPut)[0].Args[0].(map[string]any)
		if args["data"] != "aGVsbG8=" || src.Len() != 6 || len(store.requests()) != 0 {
			t.Errorf("data = %v, unread = %d, HTTP = %d", args["data"], src.Len(), len(store.requests()))
		}
	})
	t.Run("short reader inline", func(t *testing.T) {
		fs, fc, _ := directStore(t, 1024, nil, map[string]any{URIPut: ok(nil)})
		if _, err := fs.PutReader(ctx, "k", strings.NewReader("abc"), 5); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want unexpected EOF", err)
		}
		if fc.count(URIPut) != 0 {
			t.Error("a short object was stored")
		}
	})
	t.Run("invalid arguments", func(t *testing.T) {
		fs, fc, _ := directStore(t, 1024, nil, nil)
		if _, err := fs.PutReader(ctx, "k", nil, 1); err == nil {
			t.Error("nil reader accepted")
		}
		if _, err := fs.PutReader(ctx, "k", strings.NewReader(""), -1); err == nil {
			t.Error("negative size accepted")
		}
		if len(fc.allCalls()) != 0 {
			t.Error("invalid arguments reached the service")
		}
	})
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

func TestPutFile(t *testing.T) {
	ctx := context.Background()
	writeTemp := func(t *testing.T, data []byte) string {
		t.Helper()
		path := t.TempDir() + "/src.bin"
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Run("streams on the direct path", func(t *testing.T) {
		data := randomBytes(t, 3<<20)
		fs, fc, store := directStore(t, 1024, nil, nil)
		info, err := fs.PutFile(ctx, "big.bin", writeTemp(t, data), Namespace("frames"))
		if err != nil {
			t.Fatal(err)
		}
		r := store.requests()[0]
		if r.ContentLength != int64(len(data)) || len(r.TransferEncoding) != 0 || !bytes.Equal(r.Body, data) {
			t.Errorf("Content-Length %d, Transfer-Encoding %v, body intact %v", r.ContentLength, r.TransferEncoding, bytes.Equal(r.Body, data))
		}
		if fc.callsTo(URIWriteURL)[0].Args[0].(map[string]any)["size"] != int64(len(data)) {
			t.Error("size not declared when minting")
		}
		if info.Key != "big.bin" {
			t.Errorf("info = %+v", info)
		}
	})
	t.Run("body is resent on a redirect", func(t *testing.T) {
		data := randomBytes(t, 4096)
		fs, _, store := directStore(t, 1024, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/bucket/put" {
				http.Redirect(w, r, "/bucket/moved?"+r.URL.RawQuery, http.StatusTemporaryRedirect)
			}
		}, nil)
		if _, err := fs.PutFile(ctx, "big.bin", writeTemp(t, data)); err != nil {
			t.Fatal(err)
		}
		reqs := store.requests()
		if len(reqs) != 2 || reqs[1].Path != "/bucket/moved" || reqs[1].Method != http.MethodPut ||
			reqs[1].ContentLength != int64(len(data)) || !bytes.Equal(reqs[1].Body, data) {
			t.Errorf("redirected upload: %d requests", len(reqs))
		}
	})
	t.Run("inline within the cap", func(t *testing.T) {
		fs, fc, _ := directStore(t, 1024, nil, map[string]any{URIPut: ok(map[string]any{"namespace": "default", "key": "k"})})
		if _, err := fs.PutFile(ctx, "k", writeTemp(t, []byte("hey")), ContentType("text/plain")); err != nil {
			t.Fatal(err)
		}
		want := []any{map[string]any{"namespace": "default", "key": "k", "data": "aGV5", "content_type": "text/plain"}}
		if got := fc.callsTo(URIPut)[0].Args; !reflect.DeepEqual(got, want) {
			t.Errorf("put args = %v, want %v", got, want)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		fs, fc, _ := directStore(t, 1024, nil, nil)
		_, err := fs.PutFile(ctx, "k", t.TempDir()+"/missing")
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err = %v, want not exist", err)
		}
		if len(fc.allCalls()) != 0 {
			t.Error("a missing file reached the service")
		}
	})
	t.Run("directory", func(t *testing.T) {
		fs, fc, _ := directStore(t, 1024, nil, nil)
		if _, err := fs.PutFile(ctx, "k", t.TempDir()); err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("err = %v", err)
		}
		if len(fc.allCalls()) != 0 {
			t.Error("a directory reached the service")
		}
	})
}

func TestGetTo(t *testing.T) {
	ctx := context.Background()
	t.Run("streams on the direct path", func(t *testing.T) {
		data := randomBytes(t, 3<<20)
		var store *objectStore
		fs, fc, store := directStore(t, 1024, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }, map[string]any{
			URIStat: replyFunc(func([]any) (any, error) {
				if len(store.requests()) != 1 {
					return nil, errors.New("stat before the download")
				}
				return ok(map[string]any{"namespace": "frames", "key": "big.bin", "size": int64(len(data))}), nil
			}),
		})
		var buf bytes.Buffer
		info, err := fs.GetTo(ctx, "big.bin", &buf, Namespace("frames"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), data) {
			t.Error("downloaded bytes differ")
		}
		if info.Size != int64(len(data)) || info.URL != "https://files.ironflock.com/f/3317/frames/big.bin" {
			t.Errorf("info = %+v", info)
		}
		if want := []any{map[string]any{"namespace": "frames", "key": "big.bin"}}; !reflect.DeepEqual(fc.callsTo(URIStat)[0].Args, want) {
			t.Errorf("stat args = %v", fc.callsTo(URIStat)[0].Args)
		}
	})
	t.Run("inline", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{URIGet: ok(map[string]any{
			"namespace": "frames", "key": "a.txt", "size": int64(3), "data": "aGV5", "encoding": "base64",
		})})
		var buf bytes.Buffer
		info, err := fs.GetTo(ctx, "a.txt", &buf, Namespace("frames"))
		if err != nil {
			t.Fatal(err)
		}
		if buf.String() != "hey" || info.Size != 3 || info.URL != "https://files.ironflock.com/f/3317/frames/a.txt" {
			t.Errorf("data %q, info %+v", buf.String(), info)
		}
		if fc.count(URIStat) != 0 {
			t.Error("an inline get read the descriptor back")
		}
	})
	t.Run("writer errors", func(t *testing.T) {
		boom := errors.New("disk full")
		fs, _, _ := directStore(t, 1024, respondWith(200, "payload"), nil)
		if _, err := fs.GetTo(ctx, "big.bin", &errWriter{boom}); !errors.Is(err, boom) {
			t.Errorf("direct: err = %v", err)
		}
		fs, _ = newStore(t, map[string]any{URIGet: ok(map[string]any{"key": "a", "data": "aGV5"})})
		if _, err := fs.GetTo(ctx, "a", &errWriter{boom}); !errors.Is(err, boom) {
			t.Errorf("inline: err = %v", err)
		}
	})
	t.Run("nil writer", func(t *testing.T) {
		fs, fc := newStore(t, nil)
		if _, err := fs.GetTo(ctx, "a", nil); err == nil || len(fc.allCalls()) != 0 {
			t.Errorf("err = %v, calls = %d", err, len(fc.allCalls()))
		}
	})
}

type errWriter struct{ err error }

func (e *errWriter) Write([]byte) (int, error) { return 0, e.err }

// TestConcurrentUse exercises one FileStore from many goroutines (run with
// -race).
func TestConcurrentUse(t *testing.T) {
	ctx := context.Background()
	big := randomBytes(t, 4096)
	fs, _, _ := directStore(t, 1024, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write(big)
		}
	}, map[string]any{
		URIPut:  ok(map[string]any{"namespace": "default", "key": "small"}),
		URIList: listPages(page(true, "c1", "a"), page(false, "", "b")),
	})
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			switch i % 5 {
			case 0:
				_, err = fs.Put(ctx, "small", []byte("x"))
			case 1:
				_, err = fs.Put(ctx, "big.bin", big)
			case 2:
				var data []byte
				if data, err = fs.Get(ctx, "big.bin"); err == nil && !bytes.Equal(data, big) {
					err = errors.New("corrupt download")
				}
			case 3:
				_, err = fs.URL(ctx, "a/b c")
			case 4:
				for _, e := range fs.Iter(ctx) {
					if e != nil {
						err = e
					}
				}
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestHTTPClient(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		fs := New(newFakeCaller(nil), nil)
		if fs.http == nil || fs.http.Timeout != 0 {
			t.Fatalf("client = %+v, want one without an overall timeout", fs.http)
		}
		tr, ok := fs.http.Transport.(*http.Transport)
		if !ok || tr.Proxy == nil || tr == http.DefaultTransport {
			t.Errorf("transport = %T, want a private *http.Transport honouring proxies", fs.http.Transport)
		}
	})
	t.Run("custom client is used", func(t *testing.T) {
		store := newObjectStore(t, respondWith(200, "via custom"))
		var used int
		var mu sync.Mutex
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			used++
			mu.Unlock()
			return http.DefaultTransport.RoundTrip(r)
		})}
		fc := newFakeCaller(map[string]any{
			URIGet:     fail(CodeTooLarge, "too big"),
			URIReadURL: ok(map[string]any{"url": store.url("/x")}),
		})
		data, err := New(fc, client).Get(context.Background(), "x")
		if err != nil || string(data) != "via custom" || used != 1 {
			t.Errorf("data %q, err %v, custom client used %d times", data, err, used)
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// persistConnGoroutines counts the goroutines net/http runs for its pooled
// client connections (a read and a write loop per connection).
func persistConnGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "net/http.(*persistConn)") {
			n++
		}
	}
	return n
}

// connCounter counts the connections an HTTP server accepts and closes.
type connCounter struct{ opened, closed atomic.Int32 }

func (c *connCounter) track(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		c.opened.Add(1)
	case http.StateClosed, http.StateHijacked:
		c.closed.Add(1)
	}
}

// idleCloseRecorder is a caller's transport that records being told to
// close its idle connections (and does so).
type idleCloseRecorder struct {
	*http.Transport
	calls atomic.Int32
}

func (r *idleCloseRecorder) CloseIdleConnections() {
	r.calls.Add(1)
	r.Transport.CloseIdleConnections()
}

// The store's own client pools keep-alive connections, each with two
// goroutines that would otherwise outlive the app by up to 90 s.
// CloseIdleConnections (which IronFlock.Stop calls) releases them, while
// the object store is still up; a client the caller passed in is not
// touched.
func TestCloseIdleConnections(t *testing.T) {
	// storeOn returns a store whose Get of "big.bin" downloads from srv.
	storeOn := func(srv *httptest.Server, client *http.Client) *FileStore {
		return New(newFakeCaller(map[string]any{
			URIGet:     fail(CodeTooLarge, "too big"),
			URIReadURL: ok(map[string]any{"url": srv.URL + "/big.bin"}),
		}), client)
	}
	// server starts an object store that counts its connections.
	server := func(t *testing.T) (*httptest.Server, *connCounter) {
		conns := &connCounter{}
		srv := httptest.NewUnstartedServer(http.HandlerFunc(respondWith(http.StatusOK, "payload")))
		srv.Config.ConnState = conns.track
		srv.Start()
		t.Cleanup(srv.Close)
		return srv, conns
	}
	get := func(t *testing.T, fs *FileStore) {
		t.Helper()
		if data, err := fs.Get(context.Background(), "big.bin"); err != nil || string(data) != "payload" {
			t.Fatalf("Get = %q, %v", data, err)
		}
	}

	t.Run("own client", func(t *testing.T) {
		if !eventually(func() bool { return persistConnGoroutines() == 0 }) {
			t.Fatalf("%d pooled-connection goroutines of earlier tests did not wind down", persistConnGoroutines())
		}
		srv, conns := server(t)
		fs := storeOn(srv, nil)
		get(t, fs)
		if persistConnGoroutines() == 0 {
			t.Fatal("no pooled connection after a direct transfer: nothing to release")
		}
		fs.CloseIdleConnections()
		if !eventually(func() bool { return persistConnGoroutines() == 0 }) {
			t.Errorf("%d pooled-connection goroutines still running after CloseIdleConnections", persistConnGoroutines())
		}
		if !eventually(func() bool { return conns.closed.Load() == conns.opened.Load() }) {
			t.Errorf("object store: %d connections opened, %d closed", conns.opened.Load(), conns.closed.Load())
		}
		// The store stays usable.
		get(t, fs)
		fs.CloseIdleConnections()
	})

	t.Run("caller's client is left alone", func(t *testing.T) {
		srv, conns := server(t)
		inner := &http.Transport{}
		t.Cleanup(inner.CloseIdleConnections)
		rec := &idleCloseRecorder{Transport: inner}
		fs := storeOn(srv, &http.Client{Transport: rec})
		get(t, fs)
		fs.CloseIdleConnections()
		if n := rec.calls.Load(); n != 0 {
			t.Errorf("the caller's transport was told %d times to close its idle connections", n)
		}
		get(t, fs)
		if n := conns.opened.Load(); n != 1 {
			t.Errorf("%d connections for two transfers, want 1: the caller's pooled connection was not reused", n)
		}
	})
}

// The source of an upload is not read once the upload has returned, even if
// the transport still holds the request body.
func TestUploadSourceIsReleased(t *testing.T) {
	state := &uploadState{}
	src := strings.NewReader("abcdef")
	r := &sizedReader{r: src, remain: 6, size: 6, state: state}
	buf := make([]byte, 2)
	if n, err := r.Read(buf); n != 2 || err != nil {
		t.Fatalf("Read = %d, %v", n, err)
	}
	state.done.Store(true)
	if n, err := r.Read(buf); n != 0 || !errors.Is(err, errUploadFinished) {
		t.Fatalf("Read after the upload returned = %d, %v", n, err)
	}
	if src.Len() != 4 {
		t.Errorf("source read after the upload returned: %d bytes left, want 4", src.Len())
	}
}
