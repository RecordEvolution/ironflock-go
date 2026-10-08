// Runs the cross-SDK wire scenario with the JavaScript SDK and writes, per
// step, what the fake platform recorded plus the SDK's return value.
import { writeFileSync } from "node:fs";
// The SDK under test: IRONFLOCK_JS_SDK may point at a built dist/index.mjs.
const { IronFlock } = await import(process.env.IRONFLOCK_JS_SDK ?? "ironflock");

const URL = process.env.IRONFLOCK_TEST_PLATFORM_URL ?? "ws://localhost:18081/ws-ua-usr";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function jsonable(v) {
  if (v instanceof Uint8Array) return { $bytes: Buffer.from(v).toString("latin1") };
  if (Array.isArray(v)) return v.map(jsonable);
  if (v && typeof v === "object") {
    if ("app" in v && "tables" in v && "transforms" in v) {
      return { app: v.app, stage: v.stage, tables: v.tables, transforms: v.transforms };
    }
    const o = {};
    for (const [k, x] of Object.entries(v)) o[k] = jsonable(x);
    return o;
  }
  return v === undefined ? null : v;
}

const ifl = new IronFlock({ ironFlockUrl: URL, reconnectWindowMs: 5000 });
await ifl.start();
const out = {};
const f = ifl.files;
const enc = (s) => new TextEncoder().encode(s);

async function step(name, fn, settle = 400) {
  await ifl.call("test.reset");
  let result = null, error = null;
  try {
    result = await fn();
  } catch (e) {
    error = `${e?.name ?? "Error"}: ${e?.code ?? ""}`;
  }
  await sleep(settle);
  const rec = await ifl.call("test.recorded");
  out[name] = { recorded: rec, result: jsonable(result), error };
}

async function keepFiles(fn) {
  await f.put("a/b c.txt", enc("hello"), { contentType: "text/plain" });
  await ifl.call("test.clear_recorded");
  return fn();
}

await step("publish_to_table_row", () => ifl.publishToTable("sensordata", [{ temperature: 22.5, n: 3 }]));
await step("publish_to_table_kwargs", () => ifl.publishToTable("sensordata", [], { temperature: 1.5 }));
await step("append_to_table", () => ifl.appendToTable("sensordata", [{ temperature: 23 }]));
await step("publish_rows_to_table", () => ifl.publishRowsToTable("sensordata", [{ temperature: 1 }, { temperature: 2 }], { batch: "b1" }));
await step("append_rows_to_table", () => ifl.appendRowsToTable("sensordata", [{ temperature: 3 }]));
await step("report_error_publish", () => ifl.reportError("Sensor timed out", { level: "warn", tsp: "2026-01-01T00:00:00Z" }));
await step("report_error_append", () => ifl.reportError("Calibration failed", { append: true, tsp: "2026-01-01T00:00:00Z" }));
await step("get_history_full", () => ifl.getHistory("sensordata", {
  limit: 5, offset: 2, timeRange: ["2026-01-01T00:00:00Z", null],
  filterAnd: [{ column: "temperature", operator: ">", value: 20 }, { latest: true }],
  columns: ["temperature"],
}));
await step("get_history_default", () => ifl.getHistory("sensordata"));
await step("get_series_history", () => ifl.getSeriesHistory("sensordata", {
  metrics: ["temperature"], method: "AVG", limit: 100,
  timeRange: ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"], groupBy: ["device_key"],
}));
await step("reveal_secrets", () => ifl.revealSecrets("credentials", { limit: 1, filterAnd: [{ latest: true }] }));
await step("verify_secret_match", () => ifl.verifySecret("credentials", "api_key", "right"));
await step("verify_secret_limit", () => ifl.verifySecret("credentials", "api_key", "wrong", { limit: 3 }));
await step("files_put_inline", () => f.put("a/b c.txt", enc("hello"), { contentType: "text/plain" }));
await step("files_get_inline", () => keepFiles(() => f.get("a/b c.txt")));
await step("files_list", () => keepFiles(() => f.list({ prefix: "a/" })));
await step("files_stat", () => keepFiles(() => f.stat("a/b c.txt")));
await step("files_exists_missing", () => f.exists("missing.txt"));
await step("files_copy", () => keepFiles(() => f.copy("a/b c.txt", "copy.txt", "default", "default")));
await step("files_move", () => keepFiles(() => f.move("a/b c.txt", "moved.txt")));
await step("files_put_direct", () => f.put("big.bin", enc("x".repeat(5000)), { contentType: "application/octet-stream" }));
await step("files_get_direct", async () => {
  await f.put("big.bin", enc("y".repeat(5000)));
  await ifl.call("test.clear_recorded");
  return f.get("big.bin");
});
await step("files_usage_detail", () => f.usage({ detail: true }));
await step("files_share_url", () => f.shareUrl("a/b c.txt", "default", 120));
await step("files_upload_url", () => f.uploadUrl("u.bin", { ttl: 60, contentType: "application/octet-stream", size: 10 }));
await step("files_delete", () => keepFiles(() => f.delete("a/b c.txt")));
await step("files_catalog", () => f.catalog(true));
await step("connect_to_app", () => ifl.connectToApp("weather"));
await step("consumed_get_history", async () => {
  const app = await ifl.connectToApp("weather");
  return app.getHistory("readings", { limit: 2, filterAnd: [{ latest: true }] });
});
await step("consumed_get_series", async () => {
  const app = await ifl.connectToApp("weather");
  return app.getSeriesHistory("readings", { metrics: ["temp"], method: "MAX", limit: 10, timeRange: [1767225600000, null] });
});
await step("connect_to_app_no_grant", () => ifl.connectToApp("nogrant"));
await step("list_consumable_apps", () => ifl.listConsumableApps());

await ifl.stop();
writeFileSync(process.argv[2], JSON.stringify(out, null, 1));
process.exit(0);
