// Package filestore is the app's managed object storage — the IronFlock
// "files" API.
//
// Every app data backend gets private object storage alongside its tables:
// one bucket, organised in namespaces (key prefixes that carry policy). Small
// objects travel inline over the WAMP router; objects above the server's
// inline limit go directly to the object store over HTTPS through presigned
// URLs, bypassing the router. Obtain a FileStore with ironflock.IronFlock.Files.
//
// Failures inside the file service arrive as an envelope
// {"success": false, "code": ..., "reason": ...} rather than as WAMP errors;
// every method returns them as *Error. Branch on Error.Code.
package filestore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/RecordEvolution/ironflock-go/wamp"
)

// WAMP procedures of the file service. The namespace is an argument, not a
// URI segment, so these are static.
const (
	URINamespaces = "files.read.namespaces"
	URIUsage      = "files.read.usage"
	URIList       = "files.read.list"
	URIStat       = "files.read.stat"
	URIGet        = "files.read.get"
	URIReadURL    = "files.read.url"
	URIPut        = "files.write.put"
	URIDelete     = "files.write.delete"
	URICopy       = "files.write.copy"
	URIWriteURL   = "files.write.url"
)

// Defaults of the URL-minting calls and of listing.
const (
	DefaultShareTTL  = 900 * time.Second
	DefaultUploadTTL = 3600 * time.Second
	DefaultListLimit = 200
)

// Caller performs WAMP calls; *wamp.Connection implements it.
type Caller interface {
	Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *wamp.CallOptions, retryWindow time.Duration) (*wamp.Result, error)
}

// FileStore is the app's managed object storage. It is safe for concurrent
// use. Construction does no I/O.
//
// Each file service call goes through the Caller without a retry window: it
// waits for a session only as long as the Caller does by default (for a
// *wamp.Connection, its session wait timeout, 10 s unless configured
// otherwise) and is not retried. Unlike table operations, file calls do not
// use the reconnect window, so they do not ride out a platform restart:
// until the file service has registered again, a call fails with
// CodeNotAvailable. How calls made before ironflock.IronFlock.Start behave
// is described at ironflock.IronFlock.Files.
//
// Create one with New (or obtain it from ironflock.IronFlock.Files); the
// zero value is not usable.
type FileStore struct {
	caller Caller
	http   *http.Client
	// ownsHTTP is set when New created http, which CloseIdleConnections may
	// then close.
	ownsHTTP bool

	// catalog caches the catalog once fetched; it is replaced, never
	// mutated.
	catalog atomic.Pointer[Catalog]
	// catalogSem (capacity 1) serializes catalog fetches, so concurrent
	// first calls share one fetch. A channel rather than a mutex lets
	// waiters give up when their context ends.
	catalogSem chan struct{}
}

// New returns a FileStore that calls the file service through caller.
//
// httpClient is used for direct (presigned) transfers. With nil the store
// creates a client of its own, which honours HTTP(S)_PROXY and has no
// overall timeout (transfers are bounded by the context instead); release
// its pooled connections with CloseIdleConnections once the store is no
// longer used.
func New(caller Caller, httpClient *http.Client) *FileStore {
	s := &FileStore{
		caller:     caller,
		http:       httpClient,
		catalogSem: make(chan struct{}, 1),
	}
	if httpClient == nil {
		s.http, s.ownsHTTP = defaultHTTPClient(), true
	}
	return s
}

// CloseIdleConnections closes the keep-alive connections that the store's
// own HTTP client (the one New creates when given none) holds idle after
// direct transfers, releasing their sockets and goroutines, which would
// otherwise linger for up to 90 s. Transfers in progress are not
// interrupted, but their connections return to the pool when they end, so
// call it once the store's calls have returned. The store stays usable: a
// later transfer opens a new connection.
//
// It does nothing for a client passed to New, which belongs to the caller
// and may be shared. ironflock.IronFlock.Stop calls it for the store that
// Files returns.
func (s *FileStore) CloseIdleConnections() {
	if s.ownsHTTP {
		s.http.CloseIdleConnections()
	}
}

// Option configures a single FileStore call. Options that do not apply to a
// call are ignored.
type Option func(*options)

type options struct {
	namespace    string
	namespaceSet bool
	toNamespace  string
	contentType  string
	version      string
	ttl          time.Duration
	size         int64
	prefix       string
	limit        int
	cursor       string
	detail       bool
}

// collect applies opts over the defaults.
func collect(opts []Option) *options {
	o := &options{}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return o
}

// ns returns the selected namespace. An explicit Namespace("") is sent as
// "", like the Python and JavaScript SDKs do.
func (o *options) ns() string {
	if o.namespaceSet {
		return o.namespace
	}
	return DefaultNamespace
}

// pageSize returns the List/Iter page size.
func (o *options) pageSize() int64 {
	if o.limit <= 0 {
		return DefaultListLimit
	}
	return int64(o.limit)
}

// ttlSeconds converts a requested lifetime to the whole seconds sent on the
// wire, rounding up; zero or less selects def.
func ttlSeconds(d, def time.Duration) int64 {
	if d <= 0 {
		d = def
	}
	secs := int64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	return secs
}

// Namespace selects the namespace (default DefaultNamespace).
func Namespace(name string) Option {
	return func(o *options) { o.namespace, o.namespaceSet = name, true }
}

// ToNamespace selects the destination namespace of Copy and Move (default:
// the source namespace).
func ToNamespace(name string) Option { return func(o *options) { o.toNamespace = name } }

// ContentType sets the MIME type of Put, PutReader, PutFile and UploadURL.
// The server defaults to application/octet-stream; a namespace may restrict
// which types it accepts.
func ContentType(ct string) Option { return func(o *options) { o.contentType = ct } }

// Version pins an ETag in URL and CloudURL, letting browsers cache the
// response immutably.
func Version(etag string) Option { return func(o *options) { o.version = etag } }

// TTL sets the requested lifetime of ShareURL (default 15 min) and UploadURL
// (default 1 h). The server clamps it. It is sent in whole seconds, rounded
// up; zero or less selects the default.
func TTL(d time.Duration) Option { return func(o *options) { o.ttl = d } }

// Size declares the intended size for UploadURL, so an impossible upload
// fails before the bytes cross the network.
func Size(n int64) Option { return func(o *options) { o.size = n } }

// Prefix filters List and Iter by key prefix.
func Prefix(p string) Option { return func(o *options) { o.prefix = p } }

// Limit sets the page size of List and Iter (default 200; the server caps
// it at Catalog.ListMaxLimit). Zero or less selects the default.
func Limit(n int) Option { return func(o *options) { o.limit = n } }

// Cursor continues List from a previous page's ListResult.Cursor.
func Cursor(c string) Option { return func(o *options) { o.cursor = c } }

// Detail makes Usage break the total down per namespace (one listing per
// namespace server-side, so it is off by default).
func Detail() Option { return func(o *options) { o.detail = true } }

// Catalog returns the file store's namespaces and server-issued limits. It is
// fetched once and cached; concurrent first calls share one fetch, and a
// failed fetch is not cached.
func (s *FileStore) Catalog(ctx context.Context) (*Catalog, error) {
	c, err := s.loadCatalog(ctx, false)
	if err != nil {
		return nil, err
	}
	return c.clone(), nil
}

// RefreshCatalog re-fetches the catalog (needed only after the app's data
// template changes) and replaces the cached one.
func (s *FileStore) RefreshCatalog(ctx context.Context) (*Catalog, error) {
	c, err := s.loadCatalog(ctx, true)
	if err != nil {
		return nil, err
	}
	return c.clone(), nil
}

// Namespaces returns the namespaces this app may use.
func (s *FileStore) Namespaces(ctx context.Context) ([]NamespaceInfo, error) {
	c, err := s.loadCatalog(ctx, false)
	if err != nil {
		return nil, err
	}
	return c.clone().Namespaces, nil
}

// loadCatalog returns the cached catalog, fetching it when there is none or
// refresh is set. The result is shared and must not be modified.
//
// Fetches are serialized: a caller that finds a fetch in progress waits for
// it and then uses its result (unless refreshing), so concurrent cold calls
// fetch once. A failed fetch leaves the cache as it was, and the next waiter
// tries again with its own context.
func (s *FileStore) loadCatalog(ctx context.Context, refresh bool) (*Catalog, error) {
	if c := s.catalog.Load(); c != nil && !refresh {
		return c, nil
	}
	select {
	case s.catalogSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.catalogSem }()
	if c := s.catalog.Load(); c != nil && !refresh {
		return c, nil
	}
	p, err := s.call(ctx, URINamespaces, nil)
	if err != nil {
		return nil, err
	}
	c := decodeCatalog(p)
	s.catalog.Store(c)
	return c, nil
}

// clone returns a deep copy of c, so callers cannot modify the cache.
func (c *Catalog) clone() *Catalog {
	out := *c
	out.Namespaces = make([]NamespaceInfo, len(c.Namespaces))
	for i, ns := range c.Namespaces {
		if ns.ContentTypes != nil {
			ns.ContentTypes = append([]string{}, ns.ContentTypes...)
		}
		out.Namespaces[i] = ns
	}
	return &out
}

// Usage returns the bytes stored and the budget they count against. With
// Detail() it adds the per-namespace breakdown.
func (s *FileStore) Usage(ctx context.Context, opts ...Option) (*StorageUsage, error) {
	var payload map[string]any
	if collect(opts).detail {
		payload = map[string]any{"detail": true}
	}
	p, err := s.call(ctx, URIUsage, payload)
	if err != nil {
		return nil, err
	}
	return decodeUsage(p), nil
}

// List returns one page of objects (options: Namespace, Prefix, Limit,
// Cursor).
func (s *FileStore) List(ctx context.Context, opts ...Option) (*ListResult, error) {
	return s.list(ctx, collect(opts))
}

func (s *FileStore) list(ctx context.Context, o *options) (*ListResult, error) {
	p, err := s.call(ctx, URIList, map[string]any{
		"namespace": o.ns(),
		"prefix":    o.prefix,
		"limit":     o.pageSize(),
		"cursor":    o.cursor,
	})
	if err != nil {
		return nil, err
	}
	res := &ListResult{
		Objects:     []ObjectInfo{},
		Prefixes:    []string{},
		IsTruncated: toBool(p["is_truncated"]),
		Cursor:      toString(p["cursor"]),
	}
	if objects, ok := asList(p["objects"]); ok {
		for _, item := range objects {
			m, ok := asMap(item)
			if !ok {
				continue
			}
			info, err := s.withURL(ctx, m)
			if err != nil {
				return nil, err
			}
			res.Objects = append(res.Objects, *info)
		}
	}
	if prefixes := toStrings(p["prefixes"]); prefixes != nil {
		res.Prefixes = prefixes
	}
	return res, nil
}

// Iter iterates over every object under a prefix, paginating automatically
// (options: Namespace, Prefix, Limit). Iteration stops at the first error,
// which is yielded with a nil object.
//
// Pagination ends when a page is not truncated or comes without a cursor
// (a server reporting truncation without one would otherwise loop forever
// on the first page). A Cursor option is ignored: iteration always starts
// at the beginning.
func (s *FileStore) Iter(ctx context.Context, opts ...Option) iter.Seq2[*ObjectInfo, error] {
	o := collect(opts)
	return func(yield func(*ObjectInfo, error) bool) {
		page := *o
		page.cursor = ""
		for {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			res, err := s.list(ctx, &page)
			if err != nil {
				yield(nil, err)
				return
			}
			for i := range res.Objects {
				if !yield(&res.Objects[i], nil) {
					return
				}
			}
			if !res.IsTruncated || res.Cursor == "" {
				return
			}
			page.cursor = res.Cursor
		}
	}
}

// Stat returns an object's metadata without transferring it. A missing
// object is an *Error with CodeNoSuchObject.
func (s *FileStore) Stat(ctx context.Context, key string, opts ...Option) (*ObjectInfo, error) {
	return s.stat(ctx, key, collect(opts).ns())
}

func (s *FileStore) stat(ctx context.Context, key, namespace string) (*ObjectInfo, error) {
	p, err := s.call(ctx, URIStat, map[string]any{"namespace": namespace, "key": key})
	if err != nil {
		return nil, err
	}
	return s.withURL(ctx, p)
}

// Exists reports whether an object exists. Only CodeNoSuchObject means
// false; every other failure (not authorized, no such namespace, ...) is
// returned as an error.
func (s *FileStore) Exists(ctx context.Context, key string, opts ...Option) (bool, error) {
	if _, err := s.Stat(ctx, key, opts...); err != nil {
		if codeOf(err) == CodeNoSuchObject {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Get returns an object's bytes. Objects above the inline limit are fetched
// directly from the object store, still into memory; use GetTo or GetToFile
// for large objects.
func (s *FileStore) Get(ctx context.Context, key string, opts ...Option) ([]byte, error) {
	_, data, body, err := s.fetch(ctx, key, collect(opts).ns())
	if err != nil {
		return nil, err
	}
	if body == nil {
		return data, nil
	}
	defer closeQuietly(body)
	data, err = io.ReadAll(body)
	if err != nil {
		return nil, readError(ctx, err)
	}
	return data, nil
}

// GetTo writes an object to w, streaming it on the direct path.
//
// On the direct path the returned metadata is read back with Stat once the
// transfer is complete. A failure part-way leaves whatever was already
// written in w.
func (s *FileStore) GetTo(ctx context.Context, key string, w io.Writer, opts ...Option) (*ObjectInfo, error) {
	if w == nil {
		return nil, errors.New("filestore: GetTo: nil writer")
	}
	namespace := collect(opts).ns()
	p, data, body, err := s.fetch(ctx, key, namespace)
	if err != nil {
		return nil, err
	}
	if body == nil {
		if _, err := w.Write(data); err != nil {
			return nil, fmt.Errorf("filestore: write object: %w", err)
		}
		return s.withURL(ctx, p)
	}
	defer closeQuietly(body)
	if err := copyDownload(ctx, w, body); err != nil {
		return nil, err
	}
	return s.stat(ctx, key, namespace)
}

// GetToFile writes an object to a local file (creating parent directories),
// streaming it on the direct path.
//
// The file is written under a temporary name in the same directory and
// renamed into place only once complete, so a failed or cancelled download
// never leaves a partial file at path (and keeps a previous file there
// intact). A replaced file's permission bits are kept. Because the file is
// replaced rather than rewritten, its directory must be writable (the file
// itself need not be), and other hard links to the old file keep the old
// content.
//
// Symbolic links are followed, as Python's open(path, "wb") follows them:
// the file at the end of the links is written (created when the last link
// dangles) and staged in its own directory, and the links stay. Targets a
// rename cannot replace are written in place instead:
//   - a FIFO or a device (/dev/stdout, for instance, when it is a terminal
//     or a pipe) is opened and written as the object streams in;
//   - a file that is a mount point (a single file bind-mounted into a
//     container, which rename refuses with EBUSY) is overwritten with the
//     staged download once it is complete; only a failure during that copy
//     can leave it partly written.
func (s *FileStore) GetToFile(ctx context.Context, key, path string, opts ...Option) (*ObjectInfo, error) {
	namespace := collect(opts).ns()
	p, data, body, err := s.fetch(ctx, key, namespace)
	if err != nil {
		return nil, err
	}
	if body == nil {
		err := writeFileAtomic(path, func(w io.Writer) error {
			_, err := w.Write(data)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("filestore: write %s: %w", path, err)
		}
		return s.withURL(ctx, p)
	}
	defer closeQuietly(body)
	var copyErr error
	err = writeFileAtomic(path, func(w io.Writer) error {
		copyErr = copyDownload(ctx, w, body)
		return copyErr
	})
	if copyErr != nil {
		return nil, copyErr
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: write %s: %w", path, err)
	}
	return s.stat(ctx, key, namespace)
}

// fetch reads an object. An object small enough to travel inline is
// returned as its descriptor payload and decoded bytes. For one the service
// refuses to inline (CodeTooLarge), fetch mints a read URL (DefaultShareTTL)
// and returns the open download body instead, which the caller must close.
func (s *FileStore) fetch(ctx context.Context, key, namespace string) (payload map[string]any, data []byte, body io.ReadCloser, err error) {
	p, err := s.call(ctx, URIGet, map[string]any{"namespace": namespace, "key": key})
	if err != nil {
		if codeOf(err) != CodeTooLarge {
			return nil, nil, nil, err
		}
		// Over the inline cap: fall back to the direct path rather than make
		// the caller know the limit.
		u, err := s.mintReadURL(ctx, key, namespace, DefaultShareTTL)
		if err != nil {
			return nil, nil, nil, err
		}
		body, err := s.download(ctx, u)
		if err != nil {
			return nil, nil, nil, err
		}
		return nil, nil, body, nil
	}
	data, err = decodeInlineData(p)
	if err != nil {
		return nil, nil, nil, err
	}
	return p, data, nil, nil
}

// decodeInlineData decodes the bytes of a files.read.get payload: standard
// base64 ("data") in the "base64" transfer encoding (or none).
func decodeInlineData(p map[string]any) ([]byte, error) {
	if enc := toString(p["encoding"]); enc != "" && enc != "base64" {
		return nil, newError(CodeInternal, fmt.Sprintf("unsupported transfer encoding '%s'", enc))
	}
	encoded := toString(p["data"])
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil && len(encoded)%4 != 0 {
		// Tolerate a sender that drops the padding.
		if raw, rawErr := base64.RawStdEncoding.DecodeString(encoded); rawErr == nil {
			data, err = raw, nil
		}
	}
	if err != nil {
		return nil, wrapError(CodeInternal, "object payload was not valid base64", err)
	}
	return data, nil
}

// Put stores data under key. The returned ObjectInfo.URL is permanent and
// safe to write into a table column right away. Objects above the inline
// limit are uploaded directly to the object store.
//
// Errors: CodeTooLarge (over the transport limit, or no direct path on this
// deployment), CodeObjectTooLarge (over the namespace's own cap),
// CodeQuotaExceeded, CodeContentTypeNotAllowed.
func (s *FileStore) Put(ctx context.Context, key string, data []byte, opts ...Option) (*ObjectInfo, error) {
	o := collect(opts)
	// The transport is chosen by size, and it is not a preference: above the
	// cap the object cannot cross the router at all.
	c, err := s.loadCatalog(ctx, false)
	if err != nil {
		return nil, err
	}
	size := int64(len(data))
	if c.InlineMaxBytes > 0 && size > c.InlineMaxBytes {
		return s.putDirect(ctx, c, key, size, func() io.Reader { return bytes.NewReader(data) }, true, o)
	}
	return s.putInline(ctx, key, data, o)
}

// PutReader stores size bytes read from r, streaming them on the direct path.
//
// Exactly size bytes are read: further data in r is left unread, and a
// reader that ends early fails the call. Objects within the inline limit
// are read into memory and sent inline. r is not read after PutReader
// returns (except that a read already in progress when a failed upload is
// abandoned may still complete).
func (s *FileStore) PutReader(ctx context.Context, key string, r io.Reader, size int64, opts ...Option) (*ObjectInfo, error) {
	if r == nil {
		return nil, errors.New("filestore: PutReader: nil reader")
	}
	if size < 0 {
		return nil, fmt.Errorf("filestore: PutReader: negative size %d", size)
	}
	return s.putStream(ctx, key, size, r, nil, collect(opts))
}

// PutFile stores a local file, streaming it from disk on the direct path.
//
// path must name a regular file (use PutReader for pipes and other
// streams); its size is taken when it is opened.
func (s *FileStore) PutFile(ctx context.Context, key, path string, opts ...Option) (*ObjectInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer closeQuietly(f)
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("filestore: PutFile: %s is not a regular file", path)
	}
	return s.putStream(ctx, key, fi.Size(), f, f, collect(opts))
}

// putStream stores size bytes of r. When ra is set (a file) the direct
// upload reads through it from offset 0, which makes the body rewindable.
func (s *FileStore) putStream(ctx context.Context, key string, size int64, r io.Reader, ra io.ReaderAt, o *options) (*ObjectInfo, error) {
	c, err := s.loadCatalog(ctx, false)
	if err != nil {
		return nil, err
	}
	if c.InlineMaxBytes > 0 && size > c.InlineMaxBytes {
		// Streamed rather than read into memory: the point of the direct path
		// is that a multi-gigabyte object never has to fit in RAM.
		if ra != nil {
			return s.putDirect(ctx, c, key, size, func() io.Reader { return io.NewSectionReader(ra, 0, size) }, true, o)
		}
		return s.putDirect(ctx, c, key, size, func() io.Reader { return r }, false, o)
	}
	data, err := readExactly(r, size)
	if err != nil {
		return nil, err
	}
	return s.putInline(ctx, key, data, o)
}

// putInline stores data with one files.write.put call.
func (s *FileStore) putInline(ctx context.Context, key string, data []byte, o *options) (*ObjectInfo, error) {
	payload := map[string]any{
		"namespace": o.ns(),
		"key":       key,
		"data":      base64.StdEncoding.EncodeToString(data),
	}
	if o.contentType != "" {
		payload["content_type"] = o.contentType
	}
	p, err := s.call(ctx, URIPut, payload)
	if err != nil {
		return nil, err
	}
	return s.withURL(ctx, p)
}

// putDirect uploads straight to the object store, bypassing the router: it
// mints an upload URL (DefaultUploadTTL), PUTs the body, and reads the
// descriptor back with Stat (the object store does not return one).
func (s *FileStore) putDirect(ctx context.Context, c *Catalog, key string, size int64, open func() io.Reader, rewindable bool, o *options) (*ObjectInfo, error) {
	if !c.PresignAvailable {
		// Untransferable on this deployment, not merely oversized: no retry or
		// smaller chunk will help.
		return nil, newError(CodeTooLarge, fmt.Sprintf(
			"object is %d bytes, over the %d-byte inline limit, and this deployment has no direct storage endpoint to upload it through",
			size, c.InlineMaxBytes))
	}
	if c.PresignMaxBytes > 0 && size > c.PresignMaxBytes {
		return nil, newError(CodeTooLarge, fmt.Sprintf(
			"object is %d bytes, over the %d-byte single-upload limit; multipart upload is not implemented yet",
			size, c.PresignMaxBytes))
	}
	namespace := o.ns()
	target, err := s.uploadURL(ctx, key, namespace, DefaultUploadTTL, o.contentType, size)
	if err != nil {
		return nil, err
	}
	if err := s.upload(ctx, target, size, open, rewindable); err != nil {
		return nil, err
	}
	return s.stat(ctx, key, namespace)
}

// Delete removes an object.
func (s *FileStore) Delete(ctx context.Context, key string, opts ...Option) error {
	return s.delete(ctx, key, collect(opts).ns())
}

func (s *FileStore) delete(ctx context.Context, key, namespace string) error {
	_, err := s.call(ctx, URIDelete, map[string]any{"namespace": namespace, "key": key})
	return err
}

// Copy copies an object, optionally into another namespace (ToNamespace),
// and returns the copy.
func (s *FileStore) Copy(ctx context.Context, key, to string, opts ...Option) (*ObjectInfo, error) {
	o := collect(opts)
	payload := map[string]any{"namespace": o.ns(), "key": key, "to": to}
	if o.toNamespace != "" {
		payload["to_namespace"] = o.toNamespace
	}
	p, err := s.call(ctx, URICopy, payload)
	if err != nil {
		return nil, err
	}
	return s.withURL(ctx, p)
}

// Move copies, then deletes the source. It is NOT atomic: the service has
// no move verb, so if the delete fails both objects exist and the delete's
// error is returned.
//
// The copy lands before the source is removed, so a failure never destroys
// the only copy; a failed copy deletes nothing.
func (s *FileStore) Move(ctx context.Context, key, to string, opts ...Option) (*ObjectInfo, error) {
	info, err := s.Copy(ctx, key, to, opts...)
	if err != nil {
		return nil, err
	}
	if err := s.delete(ctx, key, collect(opts).ns()); err != nil {
		return nil, err
	}
	return info, nil
}

// URL returns the permanent URL of an object (options: Namespace, Version).
// It never expires and stays readable only to authenticated requestors with
// READ on this data backend. It returns "" where the deployment has no HTTP
// edge — the signal to fall back to Get.
func (s *FileStore) URL(ctx context.Context, key string, opts ...Option) (string, error) {
	c, err := s.loadCatalog(ctx, false)
	if err != nil {
		return "", err
	}
	o := collect(opts)
	return objectURL(c.PublicBaseURL, c.SadKey, o.ns(), key, o.version), nil
}

// CloudURL returns the permanent URL of an object through an appliance's
// cloud files tunnel, or "" where no tunnel base is reported.
func (s *FileStore) CloudURL(ctx context.Context, key string, opts ...Option) (string, error) {
	c, err := s.loadCatalog(ctx, false)
	if err != nil {
		return "", err
	}
	o := collect(opts)
	return objectURL(c.CloudBaseURL, c.SadKey, o.ns(), key, o.version), nil
}

// ShareURL returns an expiring link anyone holding it can fetch (options:
// Namespace, TTL). It is a bearer capability: hand it to a person, do not
// store it in a column.
func (s *FileStore) ShareURL(ctx context.Context, key string, opts ...Option) (string, error) {
	o := collect(opts)
	return s.mintReadURL(ctx, key, o.ns(), o.ttl)
}

// mintReadURL mints a presigned download URL (files.read.url).
func (s *FileStore) mintReadURL(ctx context.Context, key, namespace string, ttl time.Duration) (string, error) {
	p, err := s.call(ctx, URIReadURL, map[string]any{
		"namespace":  namespace,
		"key":        key,
		"expires_in": ttlSeconds(ttl, DefaultShareTTL),
	})
	if err != nil {
		return "", err
	}
	u := toString(p["url"])
	if u == "" {
		return "", newError(CodeInternal, "the service returned no download URL")
	}
	return u, nil
}

// UploadURL returns an expiring URL that accepts a direct upload (options:
// Namespace, TTL, ContentType, Size).
func (s *FileStore) UploadURL(ctx context.Context, key string, opts ...Option) (*UploadTarget, error) {
	o := collect(opts)
	return s.uploadURL(ctx, key, o.ns(), o.ttl, o.contentType, o.size)
}

// uploadURL mints a presigned upload URL (files.write.url). The content
// type and size are sent only when set.
func (s *FileStore) uploadURL(ctx context.Context, key, namespace string, ttl time.Duration, contentType string, size int64) (*UploadTarget, error) {
	args := map[string]any{
		"namespace":  namespace,
		"key":        key,
		"expires_in": ttlSeconds(ttl, DefaultUploadTTL),
	}
	if contentType != "" {
		args["content_type"] = contentType
	}
	if size != 0 {
		args["size"] = size
	}
	p, err := s.call(ctx, URIWriteURL, args)
	if err != nil {
		return nil, err
	}
	return decodeUploadTarget(p), nil
}

// withURL builds an ObjectInfo from a descriptor payload and attaches its
// permanent URL (composed by the SDK, never read from the wire).
func (s *FileStore) withURL(ctx context.Context, p map[string]any) (*ObjectInfo, error) {
	info := decodeObjectInfo(p)
	if info.Key == "" {
		return info, nil
	}
	c, err := s.loadCatalog(ctx, false)
	if err != nil {
		return nil, err
	}
	namespace := info.Namespace
	if namespace == "" {
		namespace = DefaultNamespace
	}
	info.URL = objectURL(c.PublicBaseURL, c.SadKey, namespace, info.Key, "")
	return info, nil
}

// codeOf returns the Code of the *Error in err's chain, or "".
func codeOf(err error) string {
	var fe *Error
	if errors.As(err, &fe) {
		return fe.Code
	}
	return ""
}
