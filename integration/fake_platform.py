"""Fake IronFlock platform services for end-to-end SDK tests.

Joins the local Crossbar node as the app's data backend (realm-2-26-dev) and
as a provider app's backend (realm-2-77-dev), emulates the procedures and
topics the real platform serves, and records every payload it receives so
tests can compare what different SDKs put on the wire.

Test hooks (on realm-2-26-dev):
  test.recorded()            -> list of {"kind", "uri", "args", "kwargs"}
  test.reset()               -> clears recordings and stored rows/files
  test.provider.publish(t, row, bulk=False) -> publishes on the provider realm
"""

import asyncio
import base64
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from autobahn.asyncio.component import Component, run
from autobahn.wamp import auth
from autobahn.wamp.exception import ApplicationError
from autobahn.wamp.types import RegisterOptions, SubscribeOptions, PublishOptions

URL = os.environ.get("FAKE_PLATFORM_ROUTER", "ws://localhost:18081/ws-ua-usr")
S3_PORT = int(os.environ.get("FAKE_PLATFORM_S3_PORT", "18090"))
AUTHID, SECRET = "fake-backend", "fake-backend-secret"
SWARM, APP = 2, 26

recorded = []
rows = {}      # table -> list of rows
files = {}     # (ns, key) -> {"data": bytes, "content_type": str}
blobs = {}     # s3 path -> bytes
provider_session = {"s": None}


def record(kind, uri, args, kwargs):
    recorded.append({"kind": kind, "uri": uri, "args": list(args), "kwargs": dict(kwargs)})


# ----------------------------------------------------------------- S3 server

class S3(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_PUT(self):
        n = int(self.headers.get("Content-Length", "-1"))
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked" or n < 0:
            self.send_response(411); self.end_headers(); return
        if self.headers.get("x-amz-meta-test") != "minted":
            self.send_response(403); self.end_headers()
            self.wfile.write(b"<Error><Code>SignatureDoesNotMatch</Code></Error>"); return
        body = self.rfile.read(n)
        path = self.path.split("?")[0]
        blobs[path] = body
        ns, key = path.lstrip("/").split("/", 1)
        files[(ns, key)] = {"data": body, "content_type": self.headers.get("Content-Type", "application/octet-stream")}
        recorded.append({"kind": "http", "uri": "PUT " + path, "args": [len(body)],
                         "kwargs": {"content_type": self.headers.get("Content-Type")}})
        self.send_response(200); self.send_header("ETag", '"x"'); self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path.endswith("/skewed"):
            self.send_response(403); self.end_headers()
            self.wfile.write(b"<Error><Code>RequestTimeTooSkewed</Code></Error>"); return
        body = blobs.get(path)
        if body is None:
            self.send_response(404); self.end_headers(); return
        self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers()
        self.wfile.write(body)


def start_s3():
    srv = ThreadingHTTPServer(("127.0.0.1", S3_PORT), S3)
    threading.Thread(target=srv.serve_forever, daemon=True).start()


# --------------------------------------------------------------- own realm

def catalog():
    return {
        "namespaces": [{"name": "default"},
                       {"name": "frames", "description": "Raw frames", "content_types": ["image/jpeg"],
                        "max_object_bytes": 1048576}],
        "inline_max_bytes": 1024, "list_max_limit": 1000, "quota_bytes": 0,
        "suggested_quota_bytes": 0, "sad_key": 3317, "public_base_url": "https://files.example.test",
        "presign_available": True, "presign_max_bytes": 5368709120,
    }


def ok(payload=None):
    return {"success": True, "payload": payload or {}}


def fail(code, reason):
    return {"success": False, "code": code, "reason": reason}


def info(ns, key):
    f = files[(ns, key)]
    return {"namespace": ns, "key": key, "size": len(f["data"]), "etag": "etag-%d" % len(f["data"]),
            "content_type": f["content_type"], "last_modified": "2026-10-08T00:00:00Z"}


def file_call(uri, p):
    p = p or {}
    ns, key = p.get("namespace", "default"), p.get("key", "")
    if uri == "files.read.namespaces":
        return ok(catalog())
    if uri == "files.read.usage":
        size = sum(len(f["data"]) for f in files.values())
        out = {"size_bytes": size, "object_count": len(files), "quota_bytes": 0}
        if p.get("detail"):
            out["per_namespace"] = {"default": size}
        return ok(out)
    if uri == "files.read.list":
        keys = sorted(k for (n, k) in files if n == ns and k.startswith(p.get("prefix", "")))
        limit = p.get("limit", 200)
        start = int(p.get("cursor") or 0)
        page = keys[start:start + limit]
        more = start + limit < len(keys)
        return ok({"objects": [info(ns, k) for k in page], "prefixes": [], "is_truncated": more,
                   "cursor": str(start + limit) if more else ""})
    if uri == "files.read.stat":
        if (ns, key) not in files:
            return fail("NO_SUCH_OBJECT", "no such object")
        return ok(info(ns, key))
    if uri == "files.read.get":
        if (ns, key) not in files:
            return fail("NO_SUCH_OBJECT", "no such object")
        f = files[(ns, key)]
        if len(f["data"]) > catalog()["inline_max_bytes"]:
            return fail("TOO_LARGE", "over the inline limit")
        out = info(ns, key)
        out.update({"data": base64.b64encode(f["data"]).decode(), "encoding": "base64"})
        return ok(out)
    if uri == "files.write.put":
        if ns == "frames" and p.get("content_type") != "image/jpeg":
            return fail("CONTENT_TYPE_NOT_ALLOWED", "frames accepts image/jpeg only")
        files[(ns, key)] = {"data": base64.b64decode(p.get("data", "")),
                            "content_type": p.get("content_type") or "application/octet-stream"}
        blobs["/%s/%s" % (ns, key)] = files[(ns, key)]["data"]
        return ok(info(ns, key))
    if uri == "files.write.delete":
        files.pop((ns, key), None)
        return ok({"deleted": True})
    if uri == "files.write.copy":
        if (ns, key) not in files:
            return fail("NO_SUCH_OBJECT", "no such object")
        to_ns = p.get("to_namespace") or ns
        files[(to_ns, p["to"])] = dict(files[(ns, key)])
        blobs["/%s/%s" % (to_ns, p["to"])] = files[(ns, key)]["data"]
        return ok(info(to_ns, p["to"]))
    if uri == "files.read.url":
        return ok({"url": "http://127.0.0.1:%d/%s/%s?sig=read" % (S3_PORT, ns, key), "method": "GET",
                   "expires_in": p.get("expires_in")})
    if uri == "files.write.url":
        return ok({"url": "http://127.0.0.1:%d/%s/%s?sig=write" % (S3_PORT, ns, key), "method": "PUT",
                   "headers": {"x-amz-meta-test": "minted",
                               **({"Content-Type": p["content_type"]} if p.get("content_type") else {})},
                   "expires_in": p.get("expires_in")})
    raise ApplicationError("wamp.error.no_such_procedure", uri)


WEATHER = {
    "app": "weather", "provider_app_key": 77,
    "stages": {"dev": {
        "tables": [{"tablename": "readings", "description": "raw readings",
                    "columns": [{"id": "temp", "dataType": "numeric"},
                                {"id": "api_key", "dataType": "string", "secret": True}]}],
        "transforms": [{"tablename": "hourly", "columns": []}]}},
}


def own_component():
    comp = Component(transports=[{"type": "websocket", "url": URL, "serializers": ["msgpack"],
                                  "max_retries": -1, "max_retry_delay": 2}],
                     realm="realm-%d-%d-dev" % (SWARM, APP),
                     authentication={"wampcra": {"authid": AUTHID, "secret": SECRET}})

    @comp.on_join
    async def joined(session, details):
        async def republish(table, row_list, bulk):
            if bulk:
                session.publish("transformed.bulk." + table, row_list)
            else:
                for r in row_list:
                    session.publish("transformed." + table, r)

        def on_table(*args, details=None, **kwargs):
            topic = details.topic
            record("publish", topic, args, kwargs)
            bulk = topic.startswith("bulk.")
            table = topic.split(".")[-1]
            row_list = list(args[0]) if bulk and args else [a for a in args[:1]]
            rows.setdefault(table, []).extend(row_list)
            asyncio.ensure_future(republish(table, row_list, bulk))

        await session.subscribe(on_table, "%d.%d." % (SWARM, APP), options=SubscribeOptions(match="prefix", details_arg="details"))
        await session.subscribe(on_table, "bulk.%d.%d." % (SWARM, APP), options=SubscribeOptions(match="prefix", details_arg="details"))

        def append(*args, details=None, **kwargs):
            uri = details.procedure
            record("call", uri, args, kwargs)
            table = uri.split(".")[-1]
            bulk = uri.startswith("appendBulk.")
            row_list = list(args[0]) if bulk else [a for a in args[:1]]
            rows.setdefault(table, []).extend(row_list)
            asyncio.ensure_future(republish(table, row_list, bulk))
            return {"success": True, "count": len(row_list)} if bulk else {"success": True}

        await session.register(append, "append.%d.%d." % (SWARM, APP), options=RegisterOptions(match="prefix", details_arg="details"))
        await session.register(append, "appendBulk.%d.%d." % (SWARM, APP), options=RegisterOptions(match="prefix", details_arg="details"))

        def history(*args, details=None, **kwargs):
            uri = details.procedure
            record("call", uri, args, kwargs)
            if uri.startswith("history.transformed.series."):
                return [{"tsp": "2026-01-01T00:00:00Z", "temperature": 21.5}]
            table = uri.split(".")[-1]
            q = args[0] if args else {}
            data = rows.get(table, [])
            return list(reversed(data))[: q.get("limit", 10)]

        await session.register(history, "history.transformed.", options=RegisterOptions(match="prefix", details_arg="details"))

        def reveal(*args, details=None, **kwargs):
            record("call", details.procedure, args, kwargs)
            return [{"api_key": "plaintext"}]

        def verify(*args, details=None, **kwargs):
            record("call", details.procedure, args, kwargs)
            q = args[0] if args else {}
            return {"match": q.get("candidate") == "right", "checked": 1}

        await session.register(reveal, "secret.reveal.", options=RegisterOptions(match="prefix", details_arg="details"))
        await session.register(verify, "secret.verify.", options=RegisterOptions(match="prefix", details_arg="details"))

        def resolve(*args, **kwargs):
            record("call", "sys.appaccess.resolve", args, kwargs)
            app = (args[0] or {}).get("app", "").lower() if args else ""
            if app == "weather":
                return WEATHER
            if app == "nogrant":
                raise ApplicationError("sys.appaccess.error.no_grant", "denied")
            raise ApplicationError("sys.appaccess.error.unknown_app", app)

        def list_apps(*args, **kwargs):
            record("call", "sys.appaccess.list", args, kwargs)
            return [WEATHER]

        await session.register(resolve, "sys.appaccess.resolve")
        await session.register(list_apps, "sys.appaccess.list")

        for uri in ("files.read.namespaces", "files.read.usage", "files.read.list", "files.read.stat",
                    "files.read.get", "files.write.put", "files.write.delete", "files.write.copy",
                    "files.read.url", "files.write.url"):
            def make(u):
                def h(*args, **kwargs):
                    record("call", u, args, kwargs)
                    return file_call(u, args[0] if args else None)
                return h
            await session.register(make(uri), uri)

        def get_recorded():
            return recorded

        def reset():
            recorded.clear(); rows.clear(); files.clear(); blobs.clear()
            return True

        def provider_publish(table, row, bulk=False):
            s = provider_session["s"]
            if s is None:
                raise ApplicationError("test.error.no_provider", "provider session not joined")
            s.publish(("transformed.bulk." if bulk else "transformed.") + table, row)
            return True

        def clear_recorded():
            recorded.clear()
            return True

        await session.register(clear_recorded, "test.clear_recorded")
        await session.register(get_recorded, "test.recorded")
        await session.register(reset, "test.reset")
        await session.register(provider_publish, "test.provider.publish")
        print("fake platform: own realm ready", flush=True)

    return comp


def provider_component():
    comp = Component(transports=[{"type": "websocket", "url": URL, "serializers": ["msgpack"],
                                  "max_retries": -1, "max_retry_delay": 2}],
                     realm="realm-%d-77-dev" % SWARM,
                     authentication={"wampcra": {"authid": AUTHID, "secret": SECRET}})

    @comp.on_join
    async def joined(session, details):
        provider_session["s"] = session

        def history(*args, details=None, **kwargs):
            record("provider-call", details.procedure, args, kwargs)
            if details.procedure.startswith("history.transformed.series."):
                return [{"tsp": "2026-01-01T00:00:00Z", "temp": 20.0}]
            return [{"tsp": "2026-01-01T00:00:00Z", "temp": 19.5, "api_key": "__secret__"}]

        await session.register(history, "history.transformed.", options=RegisterOptions(match="prefix", details_arg="details"))
        print("fake platform: provider realm ready", flush=True)

    @comp.on_leave
    async def left(session, details):
        provider_session["s"] = None

    return comp


if __name__ == "__main__":
    start_s3()
    run([own_component(), provider_component()], log_level="warn")
