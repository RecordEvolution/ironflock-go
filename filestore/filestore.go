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
	"context"
	"io"
	"iter"
	"net/http"
	"time"

	"github.com/RecordEvolution/ironflock-go/crossbar"
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

// Caller performs WAMP calls; *crossbar.Connection implements it.
type Caller interface {
	Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *crossbar.CallOptions, retryWindow time.Duration) (*crossbar.Result, error)
}

// FileStore is the app's managed object storage. It is safe for concurrent
// use. Construction does no I/O; every call waits for the connection's
// session like the table API does.
type FileStore struct {
	caller Caller
	http   *http.Client
}

// New returns a FileStore that calls the file service through caller.
// httpClient is used for direct (presigned) transfers; nil uses a client
// that honours HTTP(S)_PROXY and has no overall timeout (transfers are
// bounded by the context instead).
func New(caller Caller, httpClient *http.Client) *FileStore {
	panic("TODO: implement")
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
// (default 1 h). The server clamps it.
func TTL(d time.Duration) Option { return func(o *options) { o.ttl = d } }

// Size declares the intended size for UploadURL, so an impossible upload
// fails before the bytes cross the network.
func Size(n int64) Option { return func(o *options) { o.size = n } }

// Prefix filters List and Iter by key prefix.
func Prefix(p string) Option { return func(o *options) { o.prefix = p } }

// Limit sets the page size of List and Iter (default 200; the server caps
// it at Catalog.ListMaxLimit).
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
	panic("TODO: implement")
}

// RefreshCatalog re-fetches the catalog (needed only after the app's data
// template changes) and replaces the cached one.
func (s *FileStore) RefreshCatalog(ctx context.Context) (*Catalog, error) {
	panic("TODO: implement")
}

// Namespaces returns the namespaces this app may use.
func (s *FileStore) Namespaces(ctx context.Context) ([]NamespaceInfo, error) {
	panic("TODO: implement")
}

// Usage returns the bytes stored and the budget they count against. With
// Detail() it adds the per-namespace breakdown.
func (s *FileStore) Usage(ctx context.Context, opts ...Option) (*StorageUsage, error) {
	panic("TODO: implement")
}

// List returns one page of objects (options: Namespace, Prefix, Limit,
// Cursor).
func (s *FileStore) List(ctx context.Context, opts ...Option) (*ListResult, error) {
	panic("TODO: implement")
}

// Iter iterates over every object under a prefix, paginating automatically
// (options: Namespace, Prefix, Limit). Iteration stops at the first error,
// which is yielded with a nil object.
func (s *FileStore) Iter(ctx context.Context, opts ...Option) iter.Seq2[*ObjectInfo, error] {
	panic("TODO: implement")
}

// Stat returns an object's metadata without transferring it. A missing
// object is an *Error with CodeNoSuchObject.
func (s *FileStore) Stat(ctx context.Context, key string, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// Exists reports whether an object exists. Only CodeNoSuchObject means
// false; every other failure (not authorized, no such namespace, ...) is
// returned as an error.
func (s *FileStore) Exists(ctx context.Context, key string, opts ...Option) (bool, error) {
	panic("TODO: implement")
}

// Get returns an object's bytes. Objects above the inline limit are fetched
// directly from the object store, still into memory; use GetTo or GetToFile
// for large objects.
func (s *FileStore) Get(ctx context.Context, key string, opts ...Option) ([]byte, error) {
	panic("TODO: implement")
}

// GetTo writes an object to w, streaming it on the direct path.
func (s *FileStore) GetTo(ctx context.Context, key string, w io.Writer, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// GetToFile writes an object to a local file (creating parent directories),
// streaming it on the direct path.
func (s *FileStore) GetToFile(ctx context.Context, key, path string, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// Put stores data under key. The returned ObjectInfo.URL is permanent and
// safe to write into a table column right away. Objects above the inline
// limit are uploaded directly to the object store.
//
// Errors: CodeTooLarge (over the transport limit, or no direct path on this
// deployment), CodeObjectTooLarge (over the namespace's own cap),
// CodeQuotaExceeded, CodeContentTypeNotAllowed.
func (s *FileStore) Put(ctx context.Context, key string, data []byte, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// PutReader stores size bytes read from r, streaming them on the direct path.
func (s *FileStore) PutReader(ctx context.Context, key string, r io.Reader, size int64, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// PutFile stores a local file, streaming it from disk on the direct path.
func (s *FileStore) PutFile(ctx context.Context, key, path string, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// Delete removes an object.
func (s *FileStore) Delete(ctx context.Context, key string, opts ...Option) error {
	panic("TODO: implement")
}

// Copy copies an object, optionally into another namespace (ToNamespace),
// and returns the copy.
func (s *FileStore) Copy(ctx context.Context, key, to string, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// Move copies, then deletes the source. It is NOT atomic: the service has
// no move verb, so if the delete fails both objects exist and the delete's
// error is returned.
func (s *FileStore) Move(ctx context.Context, key, to string, opts ...Option) (*ObjectInfo, error) {
	panic("TODO: implement")
}

// URL returns the permanent URL of an object (options: Namespace, Version).
// It never expires and stays readable only to authenticated requestors with
// READ on this data backend. It returns "" where the deployment has no HTTP
// edge — the signal to fall back to Get.
func (s *FileStore) URL(ctx context.Context, key string, opts ...Option) (string, error) {
	panic("TODO: implement")
}

// CloudURL returns the permanent URL of an object through an appliance's
// cloud files tunnel, or "" where no tunnel base is reported.
func (s *FileStore) CloudURL(ctx context.Context, key string, opts ...Option) (string, error) {
	panic("TODO: implement")
}

// ShareURL returns an expiring link anyone holding it can fetch (options:
// Namespace, TTL). It is a bearer capability: hand it to a person, do not
// store it in a column.
func (s *FileStore) ShareURL(ctx context.Context, key string, opts ...Option) (string, error) {
	panic("TODO: implement")
}

// UploadURL returns an expiring URL that accepts a direct upload (options:
// Namespace, TTL, ContentType, Size).
func (s *FileStore) UploadURL(ctx context.Context, key string, opts ...Option) (*UploadTarget, error) {
	panic("TODO: implement")
}
