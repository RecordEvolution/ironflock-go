"""Model of fleetfiles-service for the integration harness.

Pinned to fleetfiles-service v0.2.0 (commit af6d814): internal/dataplane
handlers.go, ops.go, authz.go and internal/naming/naming.go. Every rule names
the place it mirrors; when fleetfiles changes, re-read those places and update
this file and contracts/fleetfiles_keys.json (see test_fake_platform.py).

Pure Python, standard library only. fake_platform.py binds FileService.call
to the files.* procedures and serves the S3-compatible store the presigned
URLs point at (FileService.s3_put / s3_get).

Modeled: the {success, payload} / {success: false, code, reason} envelope;
the namespace check before anything else (NO_SUCH_NAMESPACE); every key and
non-empty list prefix through naming.NormalizeKey (a refusal is INTERNAL, as
CodeFor maps the plain error); folder-style files.read.list (delimiter "/"
unless the call sends a string delimiter, "" for a flat listing; objects and
common prefixes share the page budget like S3's MaxKeys; limit absent or <= 0
-> 200, above 1000 -> 1000; listing rows carry no Content-Type, so it is taken
from the key's extension as Go's mime package does without a mime.types file);
the offset/search/"*" scan listing; ranged files.read.get (offset/length,
INVALID_RANGE, TOO_LARGE for a plain get over the inline cap); put's answer
without last_modified; copy answering the destination's stat; idempotent
delete; presigned URLs (TTL clamped to 60..3600 s, 300 s for app_reader,
NOT_SUPPORTED when presigning is unavailable); the role matrix of authz.go;
files.events after put, delete and copy.

Harness choices (deliberate, see the README): the catalog reports
inline_max_bytes 1024 instead of 6291456 (so 5000-byte objects take the
presigned path), the store has no versions or multipart uploads, a presigned
PUT is checked only for its URL signature and expiry (as S3 checks one that
signs only `host`).
"""

import base64
import datetime
import hashlib
import hmac
import threading
import time
import unicodedata
import urllib.parse

FLEETFILES_VERSION = "fleetfiles-service v0.2.0 (af6d814)"

# handlers.go
INLINE_MAX_BYTES = (8 << 20) * 3 // 4
PRESIGN_MAX_BYTES = 5 << 30
DEFAULT_PRESIGN_TTL, MIN_PRESIGN_TTL, MAX_PRESIGN_TTL, CROSS_APP_PRESIGN_TTL = 900, 60, 3600, 300
LIST_DEFAULT_LIMIT, LIST_MAX_LIMIT = 200, 1000
SCAN_PAGE_CAP = 50 * 1000  # ops.go scanPageCap pages of 1000

# naming.go
MAX_KEY_BYTES, MAX_KEY_SEGMENTS, MAX_KEY_SEGMENT_BYTES = 900, 20, 255
RESERVED_KEY_PREFIX = "_sys/"

# template.go defaults
DEFAULT_MAX_OBJECT_BYTES = 100 << 20
DEFAULT_QUOTA_BYTES_DEV = 1 << 30

CODE_NOT_AUTHORIZED = "NOT_AUTHORIZED"
CODE_NO_SUCH_NAMESPACE = "NO_SUCH_NAMESPACE"
CODE_NO_SUCH_OBJECT = "NO_SUCH_OBJECT"
CODE_TOO_LARGE = "TOO_LARGE"
CODE_OBJECT_TOO_LARGE = "OBJECT_TOO_LARGE"
CODE_QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
CODE_CONTENT_TYPE = "CONTENT_TYPE_NOT_ALLOWED"
CODE_INVALID_KEY = "INVALID_KEY"  # declared, never sent by v0.2.0 (key errors are INTERNAL)
CODE_INVALID_RANGE = "INVALID_RANGE"
CODE_NOT_SUPPORTED = "NOT_SUPPORTED"
CODE_INTERNAL = "INTERNAL"

# Go's mime package without a system mime.types (the alpine image has none).
_MIME = {".avif": "image/avif", ".css": "text/css", ".gif": "image/gif", ".htm": "text/html",
         ".html": "text/html", ".jpeg": "image/jpeg", ".jpg": "image/jpeg", ".js": "text/javascript",
         ".json": "application/json", ".mjs": "text/javascript", ".pdf": "application/pdf",
         ".png": "image/png", ".svg": "image/svg+xml", ".wasm": "application/wasm",
         ".webp": "image/webp", ".xml": "text/xml"}

URIS = ("files.read.namespaces", "files.read.usage", "files.read.list", "files.read.stat", "files.read.get",
        "files.write.put", "files.write.delete", "files.write.copy", "files.read.url", "files.write.url")


class FileError(Exception):
    def __init__(self, code, reason):
        super().__init__(reason)
        self.code, self.reason = code, reason


def go_quote(s):
    """strconv.Quote (what %q prints)."""
    out = ['"']
    for ch in s:
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\t":
            out.append("\\t")
        elif ch == "\r":
            out.append("\\r")
        elif ch in "\a\b\f\v":
            out.append({"\a": "\\a", "\b": "\\b", "\f": "\\f", "\v": "\\v"}[ch])
        elif ch.isprintable() or ch == " ":
            out.append(ch)
        elif ord(ch) < 0x10000:
            out.append("\\u%04x" % ord(ch) if ord(ch) > 0x7f else "\\x%02x" % ord(ch))
        else:
            out.append("\\U%08x" % ord(ch))
    out.append('"')
    return "".join(out)


def _validate_segment(seg):
    if seg == "":
        return "object key must not contain an empty path segment ('//')"
    if seg in (".", ".."):
        return "object key must not contain a '%s' path segment" % seg
    if len(seg.encode()) > MAX_KEY_SEGMENT_BYTES:
        return "path segment %s is longer than %d bytes" % (go_quote(seg), MAX_KEY_SEGMENT_BYTES)
    if seg.startswith(" ") or seg.endswith(" "):
        return "path segment %s must not start or end with a space" % go_quote(seg)
    if seg.endswith("."):
        return "path segment %s must not end with '.'" % go_quote(seg)
    for ch in seg:
        if ord(ch) < 0x20 or ord(ch) == 0x7f:
            return "path segment %s must not contain control characters" % go_quote(seg)
        if unicodedata.category(ch) == "Cf":
            return "path segment %s must not contain Unicode format characters" % go_quote(seg)
        if ch in '\\:*?"<>|^~':
            return "path segment %s must not contain any of \\ : * ? \" < > | ^ ~" % go_quote(seg)
    return None


def normalize_key(key):
    """naming.NormalizeKey: the canonical (NFC) key, or FileError INTERNAL
    with fleetfiles' message (a plain fmt error, so CodeFor says INTERNAL)."""
    def bad(msg):
        return FileError(CODE_INTERNAL, msg)
    if isinstance(key, (bytes, bytearray)):
        try:
            key = bytes(key).decode("utf-8")
        except UnicodeDecodeError:
            raise bad("object key must be valid UTF-8")
    if key == "":
        raise bad("object key must not be empty")
    try:
        key.encode("utf-8")
    except UnicodeEncodeError:
        raise bad("object key must be valid UTF-8")
    key = unicodedata.normalize("NFC", key)
    n = len(key.encode())
    if n > MAX_KEY_BYTES:
        raise bad("object key is %d bytes, the maximum is %d" % (n, MAX_KEY_BYTES))
    if key.startswith("/"):
        raise bad("object key must not start with '/'")
    if key.endswith("/"):
        raise bad("object key must not end with '/'")
    if key.startswith(RESERVED_KEY_PREFIX):
        raise bad("object key must not start with %s, which is reserved" % go_quote(RESERVED_KEY_PREFIX))
    segments = key.split("/")
    if len(segments) > MAX_KEY_SEGMENTS:
        raise bad("object key has %d segments, the maximum is %d" % (len(segments), MAX_KEY_SEGMENTS))
    for seg in segments:
        msg = _validate_segment(seg)
        if msg:
            raise bad(msg)
    return key


def content_type_for(key):
    """handlers.go contentTypeFor."""
    base = key.rsplit("/", 1)[-1]
    dot = base.rfind(".")
    ext = base[dot:].lower() if dot >= 0 else ""
    return _MIME.get(ext, "application/octet-stream")


def _as_int64(v):
    """nexus wamp.AsInt64: integers, and floats truncated; never bool."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v == v and abs(v) != float("inf"):
        return int(v)
    return None


def _as_string(v):
    if isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", "replace")
    return None


class Namespace:
    def __init__(self, d):
        self.name = d["name"]
        self.description = d.get("description", "")
        self.private = d.get("private") is True
        self.content_types = list(d.get("contentTypes") or ["*/*"])
        self.max_object_bytes = int(d.get("maxObjectBytes", DEFAULT_MAX_OBJECT_BYTES))
        self.render_inline = d.get("renderInline") is True


def _content_type_allowed(allowed, ct):
    if not allowed:
        return True
    for pattern in allowed:
        if pattern == "*/*" or pattern == ct:
            return True
        if len(pattern) > 2 and pattern.endswith("/*"):
            prefix = pattern[:-1]
            if len(ct) > len(prefix) and ct.startswith(prefix):
                return True
    return False


def _authorize(role, op, private):
    """dataplane/authz.go authorizeRole (WAMP callers)."""
    write = op in ("put", "presign_put", "copy", "move", "delete")
    if role == "app":
        return None
    if role == "app_reader":
        if write:
            return "cross-app access is read-only"
        if private:
            return "namespace is private and not readable cross-app"
        return None
    if role == "board_reader":
        return "board_reader is read-only" if write else None
    if role == "board_writer":
        return "board sessions may not delete or move objects" if op in ("delete", "move") else None
    if not role:
        return "undisclosed caller"
    return "role %s may not access files" % go_quote(role)


class FileService:
    """One databackend's file store.

    publish(topic, args, exclude_authrole) carries files.events; s3_base()
    returns the base URL of the presigned endpoint (the fake S3 server).
    """

    def __init__(self, swarm, sad_key, namespaces, public_base_url, s3_base, publish,
                 signing_secret="fake-presign-secret", now=time.time):
        self.swarm, self.sad_key = swarm, sad_key
        self.namespaces = {n.name: n for n in (Namespace(d) for d in namespaces)}
        self.public_base_url = public_base_url
        self.default_s3_base = s3_base
        self.publish = publish
        self.secret = signing_secret.encode()
        self.now = now
        self.lock = threading.RLock()
        self.reset()

    @property
    def bucket(self):
        """naming.PhysicalBucket."""
        return "if-%d-%d" % (self.swarm, self.sad_key)

    def reset(self):
        with self.lock:
            self.objects = {}  # (namespace, key) -> {data, content_type, etag, modified}
            self.quota_bytes = DEFAULT_QUOTA_BYTES_DEV
            self.inline_max_bytes = 1024
            self.presign_available = True
            self.presign_base = None

    def configure(self, presign_available=None, presign_base_url=None, inline_max_bytes=None):
        """Test hook: presign_available False makes the catalog say so and
        the URL verbs answer NOT_SUPPORTED; presign_base_url points minted
        URLs elsewhere (e.g. a closed port); inline_max_bytes changes the
        inline cap of get/put and the catalog."""
        with self.lock:
            if presign_available is not None:
                self.presign_available = bool(presign_available)
            if presign_base_url is not None:
                self.presign_base = presign_base_url or None
            if inline_max_bytes is not None:
                self.inline_max_bytes = int(inline_max_bytes)
        return self.settings()

    def settings(self):
        return {"presign_available": self.presign_available, "inline_max_bytes": self.inline_max_bytes,
                "presign_base_url": self.presign_base or self.default_s3_base()}

    # ----------------------------------------------------------- dispatch

    def call(self, uri, args, kwargs, details):
        """One files.* procedure: the envelope fleetfiles answers with."""
        role = (details or {}).get("caller_authrole")
        try:
            if uri == "files.read.namespaces":
                self._confirm(role, "list", False)
                return self._ok(self._catalog(role))
            if uri == "files.read.usage":
                self._confirm(role, "list", False)
                return self._ok(self._usage(self._args(args, lenient=True)))
            op, fn = {
                "files.read.list": ("list", self._list), "files.read.stat": ("stat", self._stat),
                "files.read.get": ("get", self._get), "files.write.put": ("put", self._put),
                "files.write.delete": ("delete", self._delete), "files.write.copy": ("copy", self._copy),
                "files.read.url": ("presign_get", self._read_url),
                "files.write.url": ("presign_put", self._write_url),
            }[uri]
            a = self._args(args)
            ns = _as_string(a.get("namespace")) or ""
            if ns == "":
                ns = "default"
                a["namespace"] = ns
            private = False
            if not (ns == "*" and op == "list"):
                spec = self.namespaces.get(ns)
                if spec is None:
                    raise FileError(CODE_NO_SUCH_NAMESPACE, "dataplane: no such namespace")
                private = spec.private
            self._confirm(role, op, private)
            with self.lock:
                return self._ok(fn(a, role))
        except FileError as e:
            return {"success": False, "code": e.code, "reason": e.reason}

    @staticmethod
    def _ok(payload):
        return {"success": True, "payload": payload}

    @staticmethod
    def _args(args, lenient=False):
        if not args or args[0] is None:
            return {}
        if not isinstance(args[0], dict):
            if lenient:
                return {}
            raise FileError(CODE_INTERNAL, "expected an argument object")
        return dict(args[0])

    @staticmethod
    def _confirm(role, op, private):
        reason = _authorize(role, op, private)
        if reason:
            raise FileError(CODE_NOT_AUTHORIZED, "dataplane: not authorized: " + reason)

    # ------------------------------------------------------------ helpers

    def _resolve(self, ns, key):
        """ops.resolve: the namespace spec and the normalized key."""
        spec = self.namespaces.get(ns)
        if spec is None:
            raise FileError(CODE_NO_SUCH_NAMESPACE, "dataplane: no such namespace")
        return spec, normalize_key(key if key is not None else "")

    def _object_dict(self, ns, key, obj, stat=True):
        """handlers.go objectDict; a put's answer has no last_modified."""
        d = {"namespace": ns, "key": key, "size": len(obj["data"]), "etag": obj["etag"],
             "content_type": obj["content_type"] or content_type_for(key)}
        if stat:
            d["last_modified"] = obj["modified"].strftime("%Y-%m-%dT%H:%M:%S.") + \
                "%03dZ" % (obj["modified"].microsecond // 1000)
        return d

    def _stat_obj(self, ns, key):
        spec, nkey = self._resolve(ns, key)
        obj = self.objects.get((ns, nkey))
        if obj is None:
            raise FileError(CODE_NO_SUCH_OBJECT, "objectstore: no such object")
        return spec, nkey, obj

    def _check_writable(self, spec, size, ct, replacing=0):
        if spec.max_object_bytes > 0 and size > spec.max_object_bytes:
            raise FileError(CODE_OBJECT_TOO_LARGE, "dataplane: object exceeds the namespace's maximum object "
                            "size: %d bytes, limit %d" % (size, spec.max_object_bytes))
        if not _content_type_allowed(spec.content_types, ct):
            raise FileError(CODE_CONTENT_TYPE, "dataplane: content type not allowed by the namespace: %s" % go_quote(ct))
        used = sum(len(o["data"]) for o in self.objects.values())
        if self.quota_bytes > 0 and used - replacing + size > self.quota_bytes:
            raise FileError(CODE_QUOTA_EXCEEDED, "objectstore: quota exceeded")

    def _store(self, ns, key, data, ct):
        now = datetime.datetime.fromtimestamp(self.now(), tz=datetime.timezone.utc)
        obj = {"data": bytes(data), "content_type": ct, "etag": hashlib.md5(data).hexdigest(), "modified": now}
        self.objects[(ns, key)] = obj
        return obj

    def _event(self, action, ns, obj_dict):
        spec = self.namespaces.get(ns)
        exclude = ["app_reader"] if (spec is None or spec.private) else None
        self.publish("files.events", [{"action": action, "object": obj_dict}], exclude)

    def _presign_ready(self):
        if not self.presign_available:
            raise FileError(CODE_NOT_SUPPORTED, "objectstore: not implemented by this driver: presigned URLs "
                            "are not available on this deployment")

    def _ttl(self, a, role):
        """handlers.go clampPresignTTL."""
        ttl = DEFAULT_PRESIGN_TTL
        secs = _as_int64(a.get("expires_in"))
        if secs is not None and secs > 0:
            ttl = secs
        ttl = min(max(ttl, MIN_PRESIGN_TTL), MAX_PRESIGN_TTL)
        if role == "app_reader" and ttl > CROSS_APP_PRESIGN_TTL:
            ttl = CROSS_APP_PRESIGN_TTL
        return ttl

    # -------------------------------------------------------- operations

    def _catalog(self, role):
        """handlers.go handleBuckets."""
        namespaces = []
        for name in sorted(self.namespaces):
            spec = self.namespaces[name]
            if role == "app_reader" and spec.private:
                continue
            namespaces.append({"name": spec.name, "description": spec.description, "private": spec.private,
                               "content_types": spec.content_types, "max_object_bytes": spec.max_object_bytes})
        return {"namespaces": namespaces, "inline_max_bytes": self.inline_max_bytes,
                "list_max_limit": LIST_MAX_LIMIT, "quota_bytes": self.quota_bytes,
                "suggested_quota_bytes": DEFAULT_QUOTA_BYTES_DEV, "sad_key": self.sad_key,
                "presign_available": self.presign_available, "presign_max_bytes": PRESIGN_MAX_BYTES,
                "public_base_url": self.public_base_url, "cloud_base_url": ""}

    def _usage(self, a):
        """handlers.go handleUsage."""
        with self.lock:
            size = sum(len(o["data"]) for o in self.objects.values())
            out = {"size_bytes": size, "object_count": len(self.objects), "quota_bytes": self.quota_bytes,
                   "free_bytes": -1 if self.quota_bytes <= 0 else max(self.quota_bytes - size, 0)}
            if a.get("detail") is True:
                out["per_namespace"] = {name: sum(len(o["data"]) for (n, _), o in self.objects.items() if n == name)
                                        for name in sorted(self.namespaces)}
        return out

    def _list(self, a, role):
        """handlers.go doList / ops.go List and Scan."""
        ns = a["namespace"]
        prefix = _as_string(a.get("prefix")) or ""
        token = _as_string(a.get("cursor")) or ""
        search = _as_string(a.get("search")) or ""
        offset = _as_int64(a.get("offset"))
        delimiter = "/"
        if "delimiter" in a:
            s = _as_string(a["delimiter"])
            if s is not None:
                delimiter = s
        limit = LIST_DEFAULT_LIMIT
        v = _as_int64(a.get("limit"))
        if v is not None and v > 0:
            limit = min(v, LIST_MAX_LIMIT)
        if ns == "*" or search != "" or offset is not None:
            return self._scan(ns, role, search, _as_string(a.get("sort")) or "", _as_string(a.get("dir")) == "desc",
                              max(offset or 0, 0), limit)
        if prefix != "":
            prefix = normalize_key(prefix)
        keys = sorted(k for (n, k) in self.objects if n == ns and k.startswith(prefix))
        entries = []  # (name, is_prefix)
        for key in keys:
            rest = key[len(prefix):]
            i = rest.find(delimiter) if delimiter else -1
            if i >= 0:
                common = prefix + rest[:i + len(delimiter)]
                if not entries or entries[-1] != (common, True):
                    entries.append((common, True))
            else:
                entries.append((key, False))
        if token:
            try:
                after = base64.urlsafe_b64decode(token.encode()).decode()
            except (ValueError, UnicodeDecodeError):
                raise FileError(CODE_INTERNAL, "operation error S3: ListObjectsV2, https response error "
                                "StatusCode: 400, api error InvalidArgument: The continuation token provided is incorrect")
            entries = [e for e in entries if e[0] > after]
        page, more = entries[:limit], len(entries) > limit
        return {"objects": [self._object_dict(ns, k, self.objects[(ns, k)], stat=True) | {
                    "content_type": content_type_for(k)} for k, is_p in page if not is_p],
                "prefixes": [k for k, is_p in page if is_p],
                "is_truncated": more,
                "cursor": base64.urlsafe_b64encode(page[-1][0].encode()).decode() if more else ""}

    def _scan(self, ns, role, search, sort_key, desc, offset, limit):
        if ns == "*":
            visible = [n for n, spec in self.namespaces.items() if not (role == "app_reader" and spec.private)]
        else:
            visible = [ns]
        tokens = search.lower().split()
        needle = search.lower()
        rows = []
        for (n, k), obj in self.objects.items():
            if n not in visible:
                continue
            low = (n + "/" + k).lower()
            if len(tokens) >= 2:
                if not all(t in low for t in tokens):
                    continue
            elif needle and needle not in low:
                continue
            rows.append((n, k, obj))

        def keyfn(r):
            n, k, obj = r
            return {"size": len(obj["data"]), "last_modified": obj["modified"],
                    "content_type": content_type_for(k), "namespace": n}.get(sort_key, k)
        # The native order is the store's: (namespace, key). Any other order
        # sorts by its field with (namespace, key) breaking ties (scanLess).
        rows.sort(key=lambda r: (r[0], r[1]))
        if sort_key not in ("", "key") or desc:
            rows.sort(key=keyfn, reverse=desc)
        page = rows[offset:offset + limit]
        return {"objects": [self._object_dict(n, k, o) | {"content_type": content_type_for(k)} for n, k, o in page],
                "prefixes": [], "is_truncated": offset + limit < len(rows), "cursor": ""}

    def _stat(self, a, role):
        ns, key = a["namespace"], _as_string(a.get("key")) or ""
        _, _, obj = self._stat_obj(ns, key)
        return self._object_dict(ns, key, obj)

    def _get(self, a, role):
        """handlers.go doGet / readRange / getRange."""
        ns, key = a["namespace"], _as_string(a.get("key")) or ""
        ranged = "offset" in a or "length" in a
        offset, length = 0, self.inline_max_bytes
        if ranged:
            if "offset" in a:
                v = _as_int64(a["offset"])
                if v is None or v < 0:
                    raise FileError(CODE_INVALID_RANGE, "dataplane: invalid byte range: offset must be a non-negative integer")
                offset = v
            if "length" in a:
                v = _as_int64(a["length"])
                if v is None or v <= 0:
                    raise FileError(CODE_INVALID_RANGE, "dataplane: invalid byte range: length must be a positive integer")
                length = min(v, length)
        _, _, obj = self._stat_obj(ns, key)
        size = len(obj["data"])
        out = self._object_dict(ns, key, obj)
        if ranged:
            if offset > size:
                raise FileError(CODE_INVALID_RANGE, "dataplane: invalid byte range: offset %d is past the end of a "
                                "%d-byte object" % (offset, size))
            raw = obj["data"][offset:offset + min(length, size - offset)]
            out.update({"data": base64.b64encode(raw).decode(), "encoding": "base64", "offset": offset,
                        "length": len(raw)})
            return out
        if size > self.inline_max_bytes:
            raise FileError(CODE_TOO_LARGE, "dataplane: object too large for a single call: %d bytes, inline limit "
                            "%d - read it in ranges with offset and length" % (size, self.inline_max_bytes))
        out.update({"data": base64.b64encode(obj["data"]).decode(), "encoding": "base64"})
        return out

    def _put(self, a, role):
        """handlers.go doPut / ops.go Put."""
        ns, key = a["namespace"], _as_string(a.get("key")) or ""
        ct = _as_string(a.get("content_type")) or "application/octet-stream"
        encoded = _as_string(a.get("data")) or ""
        if len(encoded) > 4 * ((self.inline_max_bytes + 2) // 3) + 4:
            raise FileError(CODE_TOO_LARGE, "dataplane: object too large for a single call: inline limit %d bytes"
                            % self.inline_max_bytes)
        try:
            raw = base64.b64decode(encoded.replace("\r", "").replace("\n", ""), validate=True)
        except ValueError as e:
            raise FileError(CODE_INTERNAL, "data must be base64: %s" % e)
        spec, nkey = self._resolve(ns, key)
        old = self.objects.get((ns, nkey))
        self._check_writable(spec, len(raw), ct, len(old["data"]) if old else 0)
        obj = self._store(ns, nkey, raw, ct)
        out = self._object_dict(ns, key, obj, stat=False)
        self._event("put", ns, out)
        return out

    def _delete(self, a, role):
        ns, key = a["namespace"], _as_string(a.get("key")) or ""
        _, nkey = self._resolve(ns, key)
        self.objects.pop((ns, nkey), None)  # S3 DeleteObject is idempotent
        self._event("delete", ns, {"namespace": ns, "key": key})
        return {"deleted": True}

    def _copy(self, a, role):
        """handlers.go doCopy / ops.go Copy: answers the destination's stat."""
        ns, src = a["namespace"], _as_string(a.get("key")) or ""
        dst = _as_string(a.get("to")) or ""
        dst_ns = _as_string(a.get("to_namespace")) or ns
        self._resolve(ns, src)
        dst_spec, dst_key = self._resolve(dst_ns, dst)
        _, _, obj = self._stat_obj(ns, src)
        old = self.objects.get((dst_ns, dst_key))
        self._check_writable(dst_spec, len(obj["data"]), obj["content_type"], len(old["data"]) if old else 0)
        copied = self._store(dst_ns, dst_key, obj["data"], obj["content_type"])
        out = self._object_dict(dst_ns, dst, copied)
        self._event("put", dst_ns, out)
        return out

    def _read_url(self, a, role):
        """handlers.go doReadURL."""
        ns, key = a["namespace"], _as_string(a.get("key")) or ""
        self._presign_ready()
        spec, nkey = self._resolve(ns, key)
        _, _, obj = self._stat_obj(ns, key)
        ttl = self._ttl(a, role)
        name = key.rsplit("/", 1)[-1]
        disposition = '%s; filename=%s' % ("inline" if spec.render_inline else "attachment", go_quote(name))
        return {"url": self.presign("GET", ns, nkey, ttl, {"response-content-disposition": disposition}),
                "method": "GET", "expires_in": ttl, "size": len(obj["data"]), "etag": obj["etag"]}

    def _write_url(self, a, role):
        """handlers.go doWriteURL."""
        ns, key = a["namespace"], _as_string(a.get("key")) or ""
        ct = _as_string(a.get("content_type")) or ""
        self._presign_ready()
        spec, nkey = self._resolve(ns, key)
        ct = ct or "application/octet-stream"
        if not _content_type_allowed(spec.content_types, ct):
            raise FileError(CODE_CONTENT_TYPE, "dataplane: content type not allowed by the namespace: %s" % go_quote(ct))
        declared = _as_int64(a.get("size"))
        if declared is not None and declared > 0:
            if declared > PRESIGN_MAX_BYTES:
                raise FileError(CODE_TOO_LARGE, "dataplane: object too large for a single call: %d bytes exceeds the "
                                "%d-byte single-upload limit; multipart is required" % (declared, PRESIGN_MAX_BYTES))
            if spec.max_object_bytes > 0 and declared > spec.max_object_bytes:
                raise FileError(CODE_OBJECT_TOO_LARGE, "dataplane: object exceeds the namespace's maximum object size: "
                                "%d bytes exceeds the namespace's %d-byte limit" % (declared, spec.max_object_bytes))
        ttl = self._ttl(a, role)
        return {"url": self.presign("PUT", ns, nkey, ttl), "method": "PUT", "headers": {"Content-Type": ct},
                "expires_in": ttl, "max_bytes": PRESIGN_MAX_BYTES}

    # ------------------------------------------------- presigned transfers

    def object_path(self, ns, key):
        return "/%s/%s" % (self.bucket, urllib.parse.quote(ns + "/" + key, safe="/-_.~"))

    def _signature(self, method, path, amz_date, expires):
        msg = "%s\n%s\n%s\n%s" % (method, path, amz_date, expires)
        return hmac.new(self.secret, msg.encode(), hashlib.sha256).hexdigest()

    def presign(self, method, ns, key, ttl, extra=None):
        """A path-style SigV4-shaped query-signed URL (the signature is the
        harness's own: HMAC over method, path, date and expiry)."""
        amz_date = datetime.datetime.fromtimestamp(self.now(), tz=datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.object_path(ns, key)
        query = {"X-Amz-Algorithm": "AWS4-HMAC-SHA256",
                 "X-Amz-Credential": "if-%d-%d-rw/%s/us-east-1/s3/aws4_request" % (self.swarm, self.sad_key, amz_date[:8]),
                 "X-Amz-Date": amz_date, "X-Amz-Expires": str(ttl), "X-Amz-SignedHeaders": "host"}
        query.update(extra or {})
        query["X-Amz-Signature"] = self._signature(method, path, amz_date, ttl)
        base = (self.presign_base or self.default_s3_base()).rstrip("/")
        return base + path + "?" + urllib.parse.urlencode(query, quote_via=urllib.parse.quote)

    def _check_signed(self, method, raw_path, query):
        """(namespace, key) of a presigned request, or (status, S3 code,
        message) when S3 would refuse it."""
        q = urllib.parse.parse_qs(query, keep_blank_values=True)
        get = lambda k: (q.get(k) or [""])[0]
        amz_date, expires, sig = get("X-Amz-Date"), get("X-Amz-Expires"), get("X-Amz-Signature")
        if not sig or not hmac.compare_digest(sig, self._signature(method, raw_path, amz_date, expires)):
            return 403, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided."
        try:
            signed = datetime.datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=datetime.timezone.utc)
            if self.now() > signed.timestamp() + int(expires):
                return 403, "AccessDenied", "Request has expired"
        except ValueError:
            return 403, "AuthorizationQueryParametersError", "X-Amz-Date must be in the ISO8601 Long Format"
        path = urllib.parse.unquote(raw_path)
        prefix = "/" + self.bucket + "/"
        if not path.startswith(prefix) or "/" not in path[len(prefix):]:
            return 404, "NoSuchBucket", "The specified bucket does not exist"
        ns, key = path[len(prefix):].split("/", 1)
        return ns, key

    def s3_put(self, raw_path, query, data, content_type):
        """A presigned PUT: (status, headers, body)."""
        checked = self._check_signed("PUT", raw_path, query)
        if isinstance(checked[0], int):
            return _s3_error(*checked)
        ns, key = checked
        with self.lock:
            obj = self._store(ns, key, data, content_type or "binary/octet-stream")
        return 200, {"ETag": '"%s"' % obj["etag"]}, b""

    def s3_get(self, raw_path, query, range_header):
        """A presigned GET: (status, headers, body)."""
        if urllib.parse.unquote(raw_path).rsplit("/", 1)[-1] == "skewed":
            return _s3_error(403, "RequestTimeTooSkewed",
                             "The difference between the request time and the current time is too large.")
        checked = self._check_signed("GET", raw_path, query)
        if isinstance(checked[0], int):
            return _s3_error(*checked)
        ns, key = checked
        with self.lock:
            obj = self.objects.get((ns, key))
        if obj is None:
            return _s3_error(404, "NoSuchKey", "The specified key does not exist.")
        data, status = obj["data"], 200
        headers = {"Content-Type": obj["content_type"], "ETag": '"%s"' % obj["etag"],
                   "Last-Modified": obj["modified"].strftime("%a, %d %b %Y %H:%M:%S GMT"), "Accept-Ranges": "bytes"}
        disposition = (urllib.parse.parse_qs(query).get("response-content-disposition") or [None])[0]
        if disposition:
            headers["Content-Disposition"] = disposition
        if range_header and range_header.startswith("bytes="):
            first, _, last = range_header[6:].partition("-")
            try:
                start = int(first) if first else max(len(data) - int(last), 0)
                end = int(last) if (first and last) else len(data) - 1
            except ValueError:
                start, end = 0, len(data) - 1
            if start >= len(data):
                return _s3_error(416, "InvalidRange", "The requested range is not satisfiable")
            end = min(end, len(data) - 1)
            headers["Content-Range"] = "bytes %d-%d/%d" % (start, end, len(data))
            data, status = data[start:end + 1], 206
        return status, headers, data


def _s3_error(status, code, message):
    body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>%s</Code><Message>%s</Message></Error>"
            % (code, message)).encode()
    return status, {"Content-Type": "application/xml"}, body
