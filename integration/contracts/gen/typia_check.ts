// Runs fleetdb's typia.assert request checks (src/types/rpc.ts) over a corpus
// of payloads: [{name, type, args}] -> [{name, ok} | {name, ok, error}], where
// error is what autobahn-js puts on the wire: JSON.stringify of the
// TypeGuardError. regen.sh compiles it inside a copy of fleetdb's src/ with
// fleetdb's own tsc and typia transform (see contracts/README.md).
import typia from "typia";
import { readFileSync } from "fs";
import {
  AppAccessResolveArgs,
  SecretRevealArgs,
  SecretVerifyArgs,
  SeriesQueryArgs,
  TableQueryArgs,
} from "./types/rpc";

const checks: Record<string, (x: unknown) => unknown> = {
  table: (x) => typia.assert<TableQueryArgs>(x),
  series: (x) => typia.assert<SeriesQueryArgs>(x),
  reveal: (x) => typia.assert<SecretRevealArgs>(x),
  verify: (x) => typia.assert<SecretVerifyArgs>(x),
  resolve: (x) => typia.assert<AppAccessResolveArgs>(x),
};

const corpus = JSON.parse(readFileSync(process.argv[2], "utf8"));
const out: any[] = [];
for (const c of corpus) {
  try {
    checks[c.type](c.args);
    out.push({ name: c.name, ok: true });
  } catch (e: any) {
    out.push({ name: c.name, ok: false, error: JSON.parse(JSON.stringify(e)) });
  }
}
console.log(JSON.stringify(out, null, 1));
