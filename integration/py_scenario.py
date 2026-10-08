"""Runs the cross-SDK wire scenario with the Python SDK and prints, per step,
what the fake platform recorded plus the SDK's return value, as JSON."""

import asyncio
import json
import os
import sys

from ironflock import IronFlock

URL = os.environ.get("IRONFLOCK_TEST_PLATFORM_URL", "ws://localhost:18081/ws-ua-usr")


def jsonable(v):
    if isinstance(v, bytes):
        return {"$bytes": v.decode("latin-1")}
    if hasattr(v, "model_dump"):
        return jsonable(v.model_dump())
    if isinstance(v, dict):
        return {k: jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    if hasattr(v, "app") and hasattr(v, "tables"):
        return {"app": v.app, "stage": v.stage, "tables": v.tables, "transforms": v.transforms}
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return repr(type(v).__name__)


async def main():
    ifl = IronFlock(ironFlockUrl=URL, reconnect_window=5)
    await ifl.start()
    out = {}

    async def step(name, coro_fn, settle=0.4):
        await ifl.call("test.reset")
        try:
            result = await coro_fn()
            err = None
        except Exception as e:  # noqa: BLE001
            result, err = None, "%s: %s" % (type(e).__name__, getattr(e, "code", ""))
        await asyncio.sleep(settle)
        rec = await ifl.call("test.recorded")
        out[name] = {"recorded": rec, "result": jsonable(result), "error": err}

    f = ifl.files
    await step("publish_to_table_row", lambda: ifl.publish_to_table("sensordata", {"temperature": 22.5, "n": 3}))
    await step("publish_to_table_kwargs", lambda: ifl.publish_to_table("sensordata", temperature=1.5))
    await step("append_to_table", lambda: ifl.append_to_table("sensordata", {"temperature": 23}))
    await step("publish_rows_to_table", lambda: ifl.publish_rows_to_table("sensordata", [{"temperature": 1}, {"temperature": 2}], batch="b1"))
    await step("append_rows_to_table", lambda: ifl.append_rows_to_table("sensordata", [{"temperature": 3}]))
    await step("report_error_publish", lambda: ifl.report_error("Sensor timed out", level="warn", tsp="2026-01-01T00:00:00Z"))
    await step("report_error_append", lambda: ifl.report_error("Calibration failed", append=True, tsp="2026-01-01T00:00:00Z", user_message="Please recalibrate"))
    await step("get_history_full", lambda: ifl.getHistory("sensordata", {
        "limit": 5, "offset": 2, "timeRange": ["2026-01-01T00:00:00Z", None],
        "filterAnd": [{"column": "temperature", "operator": ">", "value": 20}, {"latest": True}],
        "columns": ["temperature"]}))
    await step("get_history_default", lambda: ifl.getHistory("sensordata"))
    await step("get_series_history", lambda: ifl.get_series_history("sensordata", {
        "metrics": ["temperature"], "method": "AVG", "limit": 100,
        "timeRange": ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"], "groupBy": ["device_key"]}))
    await step("reveal_secrets", lambda: ifl.reveal_secrets("credentials", {"limit": 1, "filterAnd": [{"latest": True}]}))
    await step("verify_secret_match", lambda: ifl.verify_secret("credentials", "api_key", "right"))
    await step("verify_secret_limit", lambda: ifl.verify_secret("credentials", "api_key", "wrong", {"limit": 3}))
    await step("files_put_inline", lambda: f.put("a/b c.txt", b"hello", content_type="text/plain"))

    async def put_then(fn):
        await f.put("a/b c.txt", b"hello", content_type="text/plain")
        await ifl.call("test.clear_recorded")
        return await fn()

    async def keep_files(fn):
        # test.reset clears the store; re-create the object first, then reset only recordings.
        return await put_then(fn)

    await step("files_get_inline", lambda: keep_files(lambda: f.get("a/b c.txt")))
    await step("files_list", lambda: keep_files(lambda: f.list(prefix="a/")))
    await step("files_stat", lambda: keep_files(lambda: f.stat("a/b c.txt")))
    await step("files_exists_missing", lambda: f.exists("missing.txt"))
    await step("files_copy", lambda: keep_files(lambda: f.copy("a/b c.txt", "copy.txt", to_namespace="default")))
    await step("files_move", lambda: keep_files(lambda: f.move("a/b c.txt", "moved.txt")))
    await step("files_put_direct", lambda: f.put("big.bin", b"x" * 5000, content_type="application/octet-stream"))

    async def get_big():
        await f.put("big.bin", b"y" * 5000)
        await ifl.call("test.clear_recorded")
        return await f.get("big.bin")

    await step("files_get_direct", get_big)
    await step("files_usage_detail", lambda: f.usage(detail=True))
    await step("files_share_url", lambda: f.share_url("a/b c.txt", ttl=120))
    await step("files_upload_url", lambda: f.upload_url("u.bin", ttl=60, content_type="application/octet-stream", size=10))
    await step("files_delete", lambda: keep_files(lambda: f.delete("a/b c.txt")))
    await step("files_catalog", lambda: f.catalog(refresh=True))

    async def consumed_history():
        app = await ifl.connect_to_app("weather")
        return await app.get_history("readings", {"limit": 2, "filterAnd": [{"latest": True}]})

    async def consumed_series():
        app = await ifl.connect_to_app("weather")
        return await app.get_series_history("readings", {
            "metrics": ["temp"], "method": "MAX", "limit": 10, "timeRange": [1767225600000, None]})

    await step("connect_to_app", lambda: ifl.connect_to_app("weather"))
    await step("consumed_get_history", consumed_history)
    await step("consumed_get_series", consumed_series)
    await step("connect_to_app_no_grant", lambda: ifl.connect_to_app("nogrant"))
    await step("list_consumable_apps", lambda: ifl.list_consumable_apps())

    await ifl.stop()
    json.dump(out, open(sys.argv[1], "w"), indent=1, sort_keys=True)


asyncio.run(main())
