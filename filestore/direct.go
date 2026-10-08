package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

// Direct (presigned) transfers: the bytes go straight to the object store
// over HTTP(S); the router carries only the small call that mints the URL.

// errorBodyChars is how much of an object store error response is read
// (in characters) to classify it and to quote it in CodeInternal reasons.
const errorBodyChars = 400

// drainLimit bounds how much of an unread response body is discarded so the
// connection can be reused; larger remainders close the connection instead.
const drainLimit = 64 << 10

// defaultHTTPClient returns the client used when New is given none: a
// private copy of http.DefaultTransport — proxies from the environment
// (HTTP_PROXY, HTTPS_PROXY, NO_PROXY), system TLS roots with verification —
// and no overall timeout, since a multi-gigabyte transfer may legitimately
// take hours. Transfers are bounded by the caller's context instead.
func defaultHTTPClient() *http.Client {
	var tr *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		tr = dt.Clone()
	} else {
		tr = &http.Transport{ForceAttemptHTTP2: true}
	}
	tr.Proxy = http.ProxyFromEnvironment
	return &http.Client{Transport: tr}
}

// upload PUTs size bytes to a presigned target with exactly the minted
// headers and an explicit Content-Length (object stores reject chunked
// presigned uploads). The minted method is ignored, as in the Python and
// JavaScript SDKs: a presigned upload is always a PUT. Any 2xx status is
// success.
//
// open returns the body positioned at its start. When rewindable is true it
// may be called again, which lets the transport retry on a stale pooled
// connection and follow a 307/308 redirect; otherwise it is called once.
// Only the first size bytes of the body are sent, and a body that ends
// early fails the upload.
func (s *FileStore) upload(ctx context.Context, t *UploadTarget, size int64, open func() io.Reader, rewindable bool) error {
	if t.URL == "" {
		return newError(CodeInternal, "the service returned no upload URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, t.URL, nil)
	if err != nil {
		// Not quoted: a presigned URL is a bearer credential.
		return wrapError(CodeInternal, "the service returned an invalid upload URL", stripURL(err))
	}
	state := &uploadState{}
	newBody := func() io.ReadCloser {
		if size == 0 {
			return http.NoBody
		}
		return io.NopCloser(&sizedReader{r: open(), remain: size, size: size, state: state})
	}
	req.Body = newBody()
	req.ContentLength = size
	if rewindable {
		req.GetBody = func() (io.ReadCloser, error) { return newBody(), nil }
	}
	for name, value := range t.Headers {
		if strings.EqualFold(name, "Host") {
			req.Host = value
			continue
		}
		req.Header.Set(name, value)
	}

	resp, err := s.http.Do(req)
	// The transport may hold on to a request body after Do returns; the
	// source belongs to the caller again once this returns.
	state.done.Store(true)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if berr := state.err.get(); berr != nil {
			return fmt.Errorf("filestore: read upload body: %w", berr)
		}
		return unreachable(err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return httpError(resp)
	}
	return nil
}

// download GETs a presigned URL and returns the response body, which the
// caller must close. Failures are classified by httpError; read errors on
// the returned body are reported by readError.
func (s *FileStore) download(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, wrapError(CodeInternal, "the service returned an invalid download URL", stripURL(err))
	}
	// The stored bytes, verbatim: left to itself the transport would ask for
	// gzip and transparently decompress an object stored with
	// Content-Encoding: gzip.
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := s.http.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, unreachable(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer drainClose(resp.Body)
		return nil, httpError(resp)
	}
	return resp.Body, nil
}

// readError reports a failure while reading a download body: the context's
// error when it ended the transfer, otherwise the read error, wrapped.
func readError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("filestore: download from the object store interrupted: %w", err)
}

// copyDownload streams a download body to w, telling read failures (see
// readError) from write failures.
func copyDownload(ctx context.Context, w io.Writer, body io.Reader) error {
	tr := &trackingReader{r: body}
	if _, err := io.Copy(w, tr); err != nil {
		if tr.err != nil {
			return readError(ctx, tr.err)
		}
		return fmt.Errorf("filestore: write object: %w", err)
	}
	return nil
}

// unreachable reports that the object store could not be reached at all
// (DNS, connection refused, TLS, proxy). The URL is stripped from the
// message and the cause: a presigned URL is a bearer credential.
func unreachable(err error) *Error {
	err = stripURL(err)
	return wrapError(CodePresignUnreachable,
		fmt.Sprintf("could not reach the object store directly (%v); a proxy may allow only the router", err), err)
}

// stripURL removes the *url.Error layer, which quotes the request URL.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err
	}
	return err
}

// httpError classifies an object store error response, in this order:
//
//   - a body mentioning RequestTimeTooSkewed (any status): CodeClockSkew —
//     SigV4 rejects requests more than 15 minutes out of step, which on a
//     device without NTP would otherwise read as a credential problem;
//   - 401 or 403: CodeNotAuthorized;
//   - 404: CodeNoSuchObject;
//   - anything else: CodeInternal, quoting the first 400 characters of the
//     body.
func httpError(resp *http.Response) *Error {
	// 4 bytes per character is enough for 400 characters of any UTF-8 text.
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4*errorBodyChars))
	body := truncateChars(strings.ToValidUTF8(string(raw), "�"), errorBodyChars)
	switch {
	case strings.Contains(body, "RequestTimeTooSkewed"):
		return newError(CodeClockSkew,
			"the device clock is too far out of step for the object store to accept the request; check NTP")
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return newError(CodeNotAuthorized, fmt.Sprintf("the object store rejected the signed URL (HTTP %d)", resp.StatusCode))
	case resp.StatusCode == http.StatusNotFound:
		return newError(CodeNoSuchObject, "the object store has no such object")
	}
	return newError(CodeInternal, fmt.Sprintf("object store returned HTTP %d: %s", resp.StatusCode, body))
}

// truncateChars returns the first n characters (runes) of s.
func truncateChars(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	i := 0
	for range n {
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
	}
	return s[:i]
}

// drainClose discards a bounded remainder of body, so the connection can be
// reused, and closes it.
func drainClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, drainLimit))
	_ = body.Close()
}

// firstError records the first error reported to it. It is safe for
// concurrent use: the transport may still read a request body after Do
// returns.
type firstError struct {
	mu  sync.Mutex
	err error
}

func (f *firstError) set(err error) {
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
}

func (f *firstError) get() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// errUploadFinished is returned to a transport that reads a request body
// after the upload has returned.
var errUploadFinished = errors.New("filestore: upload already finished")

// uploadState is shared by every body reader of one upload.
type uploadState struct {
	// done is set once the upload has returned: the source is not read
	// after that (a read already in progress may still complete).
	done atomic.Bool
	// err is the first error reading the source.
	err firstError
}

// sizedReader yields exactly size bytes of r: it stops after size bytes and
// fails with io.ErrUnexpectedEOF when r ends early. Read errors are recorded
// in the upload state, so the upload can report them rather than a network
// failure.
type sizedReader struct {
	r      io.Reader
	remain int64
	size   int64
	state  *uploadState
}

func (b *sizedReader) Read(p []byte) (int, error) {
	if b.state.done.Load() {
		return 0, errUploadFinished
	}
	if b.remain <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remain {
		p = p[:b.remain]
	}
	n, err := b.r.Read(p)
	b.remain -= int64(n)
	switch {
	case err == io.EOF && b.remain > 0:
		err = fmt.Errorf("body ended after %d of %d bytes: %w", b.size-b.remain, b.size, io.ErrUnexpectedEOF)
		b.state.err.set(err)
	case err == io.EOF:
		// Exactly size bytes: a clean end.
	case err != nil:
		b.state.err.set(err)
	}
	return n, err
}

// trackingReader records the error its reader returned, telling read
// failures from write failures in io.Copy.
type trackingReader struct {
	r   io.Reader
	err error
}

func (t *trackingReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}

// readExactly reads exactly size bytes from r; a reader that ends early
// fails with io.ErrUnexpectedEOF.
func readExactly(r io.Reader, size int64) ([]byte, error) {
	const preallocMax = 64 << 20
	var (
		data []byte
		n    int64
		err  error
	)
	if size <= preallocMax {
		data = make([]byte, size)
		var m int
		m, err = io.ReadFull(r, data)
		n = int64(m)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = nil // reported as a short read below
		}
	} else {
		// No cap known on the inline path: grow with the data rather than
		// trusting size for one huge allocation.
		var buf bytes.Buffer
		n, err = buf.ReadFrom(io.LimitReader(r, size))
		data = buf.Bytes()
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: read object body: %w", err)
	}
	if n < size {
		return nil, fmt.Errorf("filestore: read object body: ended after %d of %d bytes: %w", n, size, io.ErrUnexpectedEOF)
	}
	return data, nil
}

// writeFileAtomic writes a file through fill without ever leaving a partial
// file at path: the content goes to a temporary file in the same directory
// (created along with any missing parents), which is synced and renamed
// over path only when fill succeeds. An existing file's permissions are
// kept; a new file gets 0666 minus the umask.
func writeFileAtomic(path string, fill func(w io.Writer) error) (err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return err
	}
	tmp, err := createTemp(dir, filepath.Base(abs))
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	if fi, statErr := os.Stat(abs); statErr == nil && fi.Mode().IsRegular() {
		if err := tmp.Chmod(fi.Mode().Perm()); err != nil {
			return err
		}
	}
	if err := fill(tmp); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), abs)
}

// createTemp creates a new, hidden temporary file next to the target base
// name. Unlike os.CreateTemp it creates the file with mode 0666 (before the
// umask), because the file becomes the final download.
func createTemp(dir, base string) (*os.File, error) {
	for range 100 {
		name := filepath.Join(dir, "."+base+"."+strconv.FormatUint(rand.Uint64(), 36)+".tmp")
		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return f, err
	}
	return nil, fmt.Errorf("filestore: could not create a temporary file in %s", dir)
}
