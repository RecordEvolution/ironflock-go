package filestore

import (
	"fmt"
	"slices"
)

// DefaultNamespace is the namespace used when a call names none (or names
// ""). The file service creates it only when the app's data template
// declares no namespaces (no files: section, or files: without
// namespaces:): a template that declares namespaces replaces it, and must
// list one named "default" for calls without a Namespace option to work —
// otherwise they fail with CodeNoSuchNamespace.
const DefaultNamespace = "default"

// Error codes carried by Error. The set is open: a newer server may send
// codes this release does not know, and they are passed through verbatim.
// Branch on Code, never on Reason.
//
// The file service (fleetfiles) is the authority on object keys: the SDK
// sends keys and prefixes as given and does not check them itself. A
// malformed key or prefix (empty, not valid UTF-8, with a leading or
// trailing "/", an empty or "."/".." segment, a segment ending in "." or
// starting or ending with a space, one of \ : * ? " < > | ^ ~, a control or
// Unicode format character, over 900 bytes, 20 segments or 255 bytes per
// segment, or the reserved "_sys/" prefix) is refused with CodeInvalidKey by
// a service that classifies key errors; fleetfiles v0.2.0 reports it as
// CodeInternal.
const (
	CodeNotAuthorized         = "NOT_AUTHORIZED"
	CodeNoSuchNamespace       = "NO_SUCH_NAMESPACE"
	CodeNoSuchObject          = "NO_SUCH_OBJECT"
	CodeTooLarge              = "TOO_LARGE"
	CodeObjectTooLarge        = "OBJECT_TOO_LARGE"
	CodeQuotaExceeded         = "QUOTA_EXCEEDED"
	CodeContentTypeNotAllowed = "CONTENT_TYPE_NOT_ALLOWED"
	CodeInvalidKey            = "INVALID_KEY"   // a malformed object key or prefix (see above)
	CodeInvalidRange          = "INVALID_RANGE" // a range outside the object; the SDK never asks for one
	CodeNotSupported          = "NOT_SUPPORTED"
	CodeInternal              = "INTERNAL"
	CodeNotAvailable          = "NOT_AVAILABLE"       // client-side: the file service does not serve the call (yet)
	CodePresignUnreachable    = "PRESIGN_UNREACHABLE" // client-side: object store not reachable directly
	CodeClockSkew             = "CLOCK_SKEW"          // client-side: device clock too far off for S3
)

// errorCodes is the list ErrorCodes returns: fleetfiles' code table in its
// order, then the client-side codes.
var errorCodes = []string{
	CodeNotAuthorized, CodeNoSuchNamespace, CodeNoSuchObject, CodeTooLarge,
	CodeObjectTooLarge, CodeQuotaExceeded, CodeContentTypeNotAllowed,
	CodeInvalidKey, CodeInvalidRange, CodeNotSupported, CodeInternal,
	CodeNotAvailable, CodePresignUnreachable, CodeClockSkew,
}

// ErrorCodes returns the codes this release knows about. The slice is a
// copy.
func ErrorCodes() []string { return slices.Clone(errorCodes) }

// Error is returned when a file operation is declined or fails.
type Error struct {
	// Code is the stable, machine-readable failure code (see the Code*
	// constants). Unknown codes from a newer server pass through.
	Code string
	// Reason is human-readable detail for logs. Never branch on it. It is
	// kept as the service sent it, so it may be empty.
	Reason string

	// cause is the underlying failure, when there is one: the
	// *wamp.Error a router rejection was mapped from, the network
	// error behind CodePresignUnreachable, the decoding error behind a
	// malformed payload.
	cause error
}

// newError returns an *Error without an underlying cause.
func newError(code, reason string) *Error { return &Error{Code: code, Reason: reason} }

// wrapError returns an *Error caused by cause.
func wrapError(code, reason string, cause error) *Error {
	return &Error{Code: code, Reason: reason, cause: cause}
}

// Error implements error.
func (e *Error) Error() string {
	if e.Reason == "" {
		return e.Code
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Reason)
}

// Is makes errors.Is(err, &filestore.Error{Code: X}) match any *Error with
// the same code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code && (t.Reason == "" || t.Reason == e.Reason)
}

// Unwrap returns the underlying failure, if any — for example the
// *wamp.Error a CodeNotAvailable or CodeNotAuthorized was mapped
// from, or the network error behind CodePresignUnreachable — so errors.As
// can reach it. It returns nil for failures the service reported itself.
func (e *Error) Unwrap() error { return e.cause }

// ObjectInfo describes one stored object.
type ObjectInfo struct {
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	ETag        string `json:"etag"`
	ContentType string `json:"content_type"`
	// LastModified is an ISO-8601 UTC timestamp, "" when unknown.
	LastModified string `json:"last_modified,omitempty"`
	// ChecksumSHA256 is the base64 SHA-256, when the store computed one.
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	// URL is the permanent, authenticated URL of the object: safe to store in
	// a table column and to use in <img src>. "" where the deployment has no
	// HTTP edge (a plain-HTTP appliance). The edge answers it with 404 for a
	// namespace declared with share: none (see FileStore.URL).
	URL string `json:"url,omitempty"`
}

// NamespaceInfo is a namespace as declared in the app's data template.
type NamespaceInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Private is false when the catalog does not say otherwise.
	Private bool `json:"private"`
	// ContentTypes are the MIME type patterns the namespace accepts: an
	// exact type, "type/*" or "*/*". The file service reports ["*/*"] for a
	// namespace that accepts any type; an empty list accepts any type too.
	// An object of another type is refused with CodeContentTypeNotAllowed
	// when it is stored, or when UploadURL mints a URL for it.
	ContentTypes []string `json:"content_types,omitempty"`
	// MaxObjectBytes caps a single object: 104857600 (100 MiB) unless the
	// template's maxObjectBytes sets it (at most 5 GiB); 0 when the catalog
	// reports no cap. A larger object is refused with CodeObjectTooLarge when
	// it is stored, or when UploadURL mints a URL for a larger declared Size.
	MaxObjectBytes int64 `json:"max_object_bytes"`
}

// Catalog is what the server reports about the app's file store. Everything
// here is discovered at runtime, so limits and hosts change without an SDK
// release. A Catalog is a snapshot as of its fetch (see FileStore.Catalog).
type Catalog struct {
	Namespaces []NamespaceInfo `json:"namespaces"`
	// InlineMaxBytes is the largest object that crosses the router in one
	// call (currently 6 MiB). 0 means no cap is known: always inline.
	InlineMaxBytes int64 `json:"inline_max_bytes"`
	// ListMaxLimit is the server's page-size cap (1000 when not reported).
	ListMaxLimit int `json:"list_max_limit"`
	// QuotaBytes is the ENFORCED budget the user set, as of the fetch (the
	// user may change it at any time; FileStore.Usage reports the current
	// one); 0 means unlimited.
	QuotaBytes int64 `json:"quota_bytes"`
	// SuggestedQuotaBytes is what the app's data template suggested.
	SuggestedQuotaBytes int64 `json:"suggested_quota_bytes"`
	SadKey              int64 `json:"sad_key"`
	// PublicBaseURL is the base of permanent object URLs; "" where there is
	// no HTTP edge.
	PublicBaseURL string `json:"public_base_url"`
	// CloudBaseURL is the base of object URLs through an appliance's cloud
	// files tunnel, as of the fetch: "" on cloud-hosted deployments, where
	// there is no HTTP edge, and while the data backend's cloud-share toggle
	// is off — a user setting that changes at any time (FileStore.CloudURL
	// keeps it at most 30 s old).
	CloudBaseURL string `json:"cloud_base_url"`
	// PresignAvailable reports whether the direct-to-storage path is usable.
	// False on an air-gapped appliance, and by default on any appliance:
	// objects over the inline limit are then read in ranges over the router
	// (see FileStore.Get) and cannot be written.
	PresignAvailable bool `json:"presign_available"`
	// PresignMaxBytes is the largest single presigned upload (S3: 5 GiB).
	// 0 means no client-side cap.
	PresignMaxBytes int64 `json:"presign_max_bytes"`
}

// ListResult is one page of a namespace listing (see FileStore.List).
type ListResult struct {
	// Objects holds the page's objects, in the order the store lists them
	// (see FileStore.List).
	Objects []ObjectInfo `json:"objects"`
	// Prefixes holds the page's folders in a folder-style listing (the
	// default): the common prefixes, each ending in the delimiter ("/"
	// unless Delimiter sets another), of keys that continue below the listed
	// prefix; their objects are not in Objects. Empty in a flat listing
	// (Delimiter("")).
	Prefixes []string `json:"prefixes"`
	// IsTruncated reports that more entries follow; pass Cursor back to get
	// the next page.
	IsTruncated bool   `json:"is_truncated"`
	Cursor      string `json:"cursor"`
}

// StorageUsage is what the object store reports about the app's storage.
type StorageUsage struct {
	SizeBytes   int64 `json:"size_bytes"`
	ObjectCount int64 `json:"object_count"`
	// QuotaBytes is the enforced budget; 0 means unlimited.
	QuotaBytes int64 `json:"quota_bytes"`
	// FreeBytes is the remaining budget; -1 means unlimited (a quota of 0 is
	// "no quota", and 0 free would read as "full").
	FreeBytes int64 `json:"free_bytes"`
	// PerNamespace holds bytes per namespace; only with the Detail option.
	PerNamespace map[string]int64 `json:"per_namespace,omitempty"`
}

// UploadTarget is a presigned URL that accepts a direct upload (a PUT). Send
// Headers as given, so that the object is stored with the minted
// Content-Type.
//
// Do not rely on the URL to restrict what is uploaded: the namespace's
// accepted content types, its maximum object size and a declared Size are
// checked when the URL is minted, not when it is used (the file service
// signs it for the host only). Anyone holding the URL can write any content
// to its key, any number of times, until it expires: treat it as a bearer
// write capability.
type UploadTarget struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	// ExpiresIn is the URL's lifetime in seconds.
	ExpiresIn int `json:"expires_in"`
}
