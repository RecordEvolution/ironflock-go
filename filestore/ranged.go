package filestore

import (
	"context"
	"fmt"
	"io"
)

// Ranged reads: an object over the inline limit read over the router in
// ranges — files.read.get with offset and length (fleetfiles v0.1.33 and
// later), each range at most the service's inline limit. The read path of
// last resort, for a deployment that cannot presign or a device that cannot
// reach the object store (see FileStore.Get).

// objectStream is the body of an object that does not travel inline: a
// presigned download, or ranges over the router.
type objectStream interface {
	// copyTo writes the object to w. It returns the object's descriptor
	// when the transfer yields one, or nil when the caller must read it
	// back with Stat.
	copyTo(ctx context.Context, w io.Writer) (map[string]any, error)
	// close releases the stream; copyTo is not called after it.
	close()
}

// downloadStream is the open response body of a presigned download.
type downloadStream struct{ body io.ReadCloser }

func (d downloadStream) copyTo(ctx context.Context, w io.Writer) (map[string]any, error) {
	return nil, copyDownload(ctx, w, d.body)
}

func (d downloadStream) close() { closeQuietly(d.body) }

// rangeStream reads an object in ranges. Every range is a read of its own
// (the service stats the object, then reads the range), so the object may
// be replaced between two ranges, or within one; each answer carries the
// object's size and ETag as its stat found them. They are pinned from the
// first range, and a change fails the read with CodeInternal ("object
// changed while it was read") rather than splice two versions of the
// object; a final stat covers the last range. A range answer that does not
// advance the read, or claims more than the object holds, fails it as well,
// so a misbehaving service cannot make it loop.
type rangeStream struct {
	s              *FileStore
	key, namespace string
	// length is the range length asked for; 0 leaves it to the service,
	// which defaults it to (and clamps it at) its inline limit.
	length int64
	// off is where chunk starts in the object: the offset of the next
	// range once chunk is written.
	off int64
	// chunk is the range read and not yet written.
	chunk []byte
	// size and etag are the object's as the first range found it.
	size   int64
	etag   string
	pinned bool
}

// openRanges starts reading an object in ranges and reads the first range,
// so that a refusal of the read arrives before the caller has written
// anything (or created a file).
//
// why is the failure of the presigned path that made the read fall back,
// nil when the catalog said the deployment cannot presign. A file service
// from before ranged reads ignores offset and length and refuses the range
// like the whole object, CodeTooLarge: the read then fails with why, the
// reason the object could not travel directly (or with that refusal when
// there is none).
func (s *FileStore) openRanges(ctx context.Context, key, namespace string, why error) (objectStream, error) {
	r := &rangeStream{s: s, key: key, namespace: namespace}
	if c := s.peekCatalog(); c != nil {
		r.length = c.InlineMaxBytes
	}
	if err := r.next(ctx); err != nil {
		if why != nil && codeOf(err) == CodeTooLarge {
			return nil, why
		}
		return nil, err
	}
	return r, nil
}

// next reads the range starting at off into chunk.
func (r *rangeStream) next(ctx context.Context) error {
	args := map[string]any{"namespace": r.namespace, "key": r.key, "offset": r.off}
	if r.length > 0 {
		args["length"] = r.length
	}
	p, err := r.s.call(ctx, URIGet, args)
	if err != nil {
		if r.pinned && codeOf(err) == CodeInvalidRange {
			// off is within the object as pinned: it has shrunk since.
			return wrapError(CodeInternal, fmt.Sprintf(
				"object changed while it was read: it was %d bytes, and offset %d is past its end now", r.size, r.off), err)
		}
		return err
	}
	rawOff, hasOff := p["offset"]
	rawLen, hasLen := p["length"]
	if !hasOff || !hasLen {
		return newError(CodeInternal, "the file service answered a ranged read without a range")
	}
	if off := toInt64(rawOff); off != r.off {
		return newError(CodeInternal, fmt.Sprintf("asked for the range at offset %d, the file service answered offset %d", r.off, off))
	}
	data, err := decodeInlineData(p)
	if err != nil {
		return err
	}
	if n := toInt64(rawLen); n != int64(len(data)) {
		return newError(CodeInternal, fmt.Sprintf("the range at offset %d reports %d bytes and carries %d", r.off, n, len(data)))
	}
	size, etag := toInt64(p["size"]), toString(p["etag"])
	if !r.pinned {
		r.size, r.etag, r.pinned = size, etag, true
	} else if size != r.size || etag != r.etag {
		return r.changed(size, etag)
	}
	switch {
	case r.off+int64(len(data)) > r.size:
		return newError(CodeInternal, fmt.Sprintf("the range at offset %d carries %d bytes, past the end of an object of %d bytes",
			r.off, len(data), r.size))
	case len(data) == 0 && r.off < r.size:
		return newError(CodeInternal, fmt.Sprintf("the file service returned an empty range at offset %d of an object of %d bytes",
			r.off, r.size))
	}
	r.chunk = data
	return nil
}

// changed is the error of a read during which the object changed: its
// size or ETag is no longer the pinned one.
func (r *rangeStream) changed(size int64, etag string) *Error {
	return newError(CodeInternal, fmt.Sprintf("object changed while it was read: it was %d bytes with ETag %q, now %d bytes with ETag %q",
		r.size, r.etag, size, etag))
}

// copyTo writes the object to w, range by range, and returns its descriptor
// as the final stat reads it.
func (r *rangeStream) copyTo(ctx context.Context, w io.Writer) (map[string]any, error) {
	for {
		if len(r.chunk) > 0 {
			if _, err := w.Write(r.chunk); err != nil {
				return nil, fmt.Errorf("filestore: write object: %w", err)
			}
			r.off += int64(len(r.chunk))
			r.chunk = nil
		}
		if r.off >= r.size {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := r.next(ctx); err != nil {
			return nil, err
		}
	}
	// The last range's stat and read are two steps on the service: only a
	// stat after it shows whether the object was replaced in between.
	p, err := r.s.call(ctx, URIStat, map[string]any{"namespace": r.namespace, "key": r.key})
	if err != nil {
		return nil, err
	}
	if size, etag := toInt64(p["size"]), toString(p["etag"]); size != r.size || etag != r.etag {
		return nil, r.changed(size, etag)
	}
	return p, nil
}

func (r *rangeStream) close() { r.chunk = nil }
