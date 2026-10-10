"""Writes the corpus of RPC argument payloads typia_check.ts runs fleetdb's
validators over (usage: typia_corpus.py OUT.json)."""
import json, sys

cases = []
def add(kind, name, args):
    cases.append({"name": "%s/%s" % (kind, name), "type": kind, "args": args})

BAD_SCALARS = {"null": None, "str": "x", "neg": -1, "frac": 1.5, "bool": True, "obj": {}, "list": [1], "big": 4294967296, "float_int": 10.0}

def filter_variants():
    v = {
        "none": None, "str": "x", "list_num": [1], "list_str": ["x"], "list_null": [None], "empty_obj": [{}],
        "col_only": [{"column": "a"}], "lower_op": [{"column": "a", "operator": "like", "value": "%x%"}],
        "val_null": [{"column": "a", "operator": "=", "value": None}],
        "col_num": [{"column": 1, "operator": "="}],
        "ok_eq": [{"column": "a", "operator": "=", "value": 1}],
        "ok_in": [{"column": "a", "operator": "IN", "value": [1, "a", True]}],
        "in_nested": [{"column": "a", "operator": "IN", "value": [[1]]}],
        "in_null_elem": [{"column": "a", "operator": "IN", "value": [1, None]}],
        "val_obj": [{"column": "a", "operator": "=", "value": {"a": 1}}],
        "datatype_num": [{"column": "a", "operator": "=", "value": 1, "dataType": 1}],
        "latest_str": [{"column": "a", "operator": "=", "value": 1, "latest": "true"}],
        "latest_false": [{"column": "a", "operator": "=", "value": 1, "latest": False}],
        "marker": [{"latest": True}],
        "marker_full": [{"latest": True, "column": "a", "operator": "=", "value": "x", "dataType": "string"}],
        "marker_badop": [{"latest": True, "operator": "eq"}],
        "marker_val_null": [{"latest": True, "value": None}],
        "marker_val_list": [{"latest": True, "value": [1]}],
        "marker_col_num": [{"latest": True, "column": 1}],
        "group_ok": [{"combinator": "OR", "filters": [{"column": "a", "operator": "=", "value": 1}, {"latest": True}]}],
        "group_nofilters": [{"combinator": "AND"}],
        "group_filters_str": [{"combinator": "AND", "filters": "x"}],
        "group_filters_num": [{"combinator": "AND", "filters": [1]}],
        "group_nested_bad": [{"combinator": "AND", "filters": [{"combinator": "OR", "filters": [{"column": "a", "operator": "~"}]}]}],
        "group_with_col": [{"combinator": "AND", "filters": [], "column": "a"}],
        "group_with_col_null": [{"combinator": "AND", "filters": [], "column": None}],
        "group_with_op": [{"combinator": "AND", "filters": [], "operator": "="}],
        "group_latest_true": [{"combinator": "AND", "filters": [], "latest": True}],
        "group_latest_str": [{"combinator": "AND", "filters": [], "latest": "x"}],
        "group_lower": [{"combinator": "and", "filters": []}],
        "pred_with_filters": [{"column": "a", "operator": "=", "value": 1, "filters": []}],
        "pred_with_comb_null": [{"column": "a", "operator": "=", "value": 1, "combinator": None}],
        "pred_with_filters_null": [{"column": "a", "operator": "=", "value": 1, "filters": None}],
        "pred_isnull": [{"column": "a", "operator": "IS NULL"}],
        "list_in_list": [[1]],
        "second_bad": [{"column": "a", "operator": "=", "value": 1}, {"column": "b", "operator": "?"}],
    }
    return v

def base(kind):
    if kind == "table": return {"limit": 10}
    if kind == "series": return {"metrics": [{"ref": "t", "method": "AVG"}], "limit": 10, "timeRange": [1, None]}
    if kind == "reveal": return {"limit": 1}
    if kind == "verify": return {"limit": 1, "column": "api_key", "candidate": "x"}

for kind in ("table", "series", "reveal", "verify"):
    b = base(kind)
    add(kind, "ok", [b])
    for k, v in BAD_SCALARS.items():
        add(kind, "limit_" + k, [dict(b, limit=v)])
    nolimit = dict(b); del nolimit["limit"]
    add(kind, "limit_missing", [nolimit])
    for lim in (0, 1, 100, 101, 10000, 10001):
        add(kind, "limit_%d" % lim, [dict(b, limit=lim)])
    for k, v in BAD_SCALARS.items():
        add(kind, "offset_" + k, [dict(b, offset=v)])
    add(kind, "offset_3", [dict(b, offset=3)])
    trs = {"null": None, "iso": ["2026-01-01T00:00:00Z", None], "nulls": [None, None], "nums": [1, 2],
           "mixed": [1, "a"], "one": [1], "three": [1, 2, 3], "str": "x", "obj": {}, "bool": [True, None], "empty": []}
    for k, v in trs.items():
        add(kind, "timeRange_" + k, [dict(b, timeRange=v)])
    for k, v in filter_variants().items():
        add(kind, "filterAnd_" + k, [dict(b, filterAnd=v)])
    for key in ("columns", "columnPaths", "groupBy"):
        for k, v in {"null": None, "str": "a", "num_elem": ["a", 1], "ok": ["a", "b"], "obj": {}}.items():
            add(kind, "%s_%s" % (key, k), [dict(b, **{key: v})])
    for k, v in {"obj": {}, "str": "x", "empty": [], "two": [b, b], "null": [None], "str_elem": ["x"], "list_elem": [[1]]}.items():
        add(kind, "args_" + k, v)
    add(kind, "args_kwargs_like", [b, {}])

# series specifics
b = base("series")
for k, v in {"missing": "__del__", "empty": [], "str": "temperature", "old_shape": ["temperature"],
             "no_method": [{"ref": "t"}], "lower_method": [{"ref": "t", "method": "avg"}],
             "ref_num": [{"ref": 1, "method": "AVG"}], "two": [{"ref": "t", "method": "AVG"}, {"ref": "u", "method": "COUNT"}],
             "second_bad": [{"ref": "t", "method": "AVG"}, {"ref": "u", "method": "MEDIAN"}], "null_elem": [None], "list_elem": [[1]]}.items():
    d = dict(b)
    if v == "__del__": del d["metrics"]
    else: d["metrics"] = v
    add("series", "metrics_" + k, [d])
d = dict(b); del d["timeRange"]; add("series", "timeRange_missing", [d])
for k, v in {"999": 999, "1000": 1000, "null": None, "str": "x", "neg": -5, "frac": 1500.5}.items():
    add("series", "bucketMs_" + k, [dict(b, bucketMs=v)])
add("series", "old_payload", [{"metrics": ["temperature"], "method": "AVG", "limit": 100,
    "timeRange": ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"], "groupBy": ["device_key"]}])
add("series", "new_payload", [{"metrics": [{"ref": "temperature", "method": "AVG"}], "limit": 100,
    "timeRange": ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"], "groupBy": ["device_key"], "bucketMs": 60000}])
# verify specifics
b = base("verify")
for k, v in {"empty": "", "num": 1, "null": None, "missing": "__del__"}.items():
    d = dict(b)
    if v == "__del__": del d["column"]
    else: d["column"] = v
    add("verify", "column_" + k, [d])
for k, v in {"num": 1, "null": None, "missing": "__del__", "empty": ""}.items():
    d = dict(b)
    if v == "__del__": del d["candidate"]
    else: d["candidate"] = v
    add("verify", "candidate_" + k, [d])
# resolve
for k, v in {"ok": [{"app": "weather"}], "num": [{"app": 1}], "missing": [{}], "empty": [], "obj": {},
             "two": [{"app": "x"}, 1], "null": [None], "str": ["x"], "list": [[1]]}.items():
    add("resolve", k, v)
json.dump(cases, open(sys.argv[1], "w"), indent=0)
print(len(cases))
