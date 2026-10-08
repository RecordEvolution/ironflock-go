"""Writes config.yaml from config.template.yaml, copying role blocks verbatim
from the router's reference config.

  #@ROLE <name>   -> the whole `- name: <name>` block (name, description,
                     permissions), byte-for-byte
  #@RULES <name>  -> only that role's permission lines, byte-for-byte

  python3 gen_config.py            # write config.yaml
  python3 gen_config.py --check    # also verify: the copied roles parse to
                                   # exactly the reference roles
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
# The router's reference config (ironflock-router/examples/config.yaml), the
# source of the production role blocks.
REF = os.environ.get("ROUTER_REF_CONFIG") or sys.exit("set ROUTER_REF_CONFIG to ironflock-router/examples/config.yaml")
TEMPLATE = os.path.join(HERE, "config.template.yaml")
OUT = os.path.join(os.environ.get("STATE_DIR", os.path.join(HERE, ".run")), "config.yaml")


def role_block(lines, name):
    """Lines of the `  - name: <name>` block, up to the next 2-space item or
    comment; trailing blank lines dropped."""
    head = "  - name: %s\n" % name
    try:
        i = lines.index(head)
    except ValueError:
        sys.exit("role %r not found in %s" % (name, REF))
    out = [lines[i]]
    for ln in lines[i + 1:]:
        if ln.startswith("    ") or ln.strip() == "":
            out.append(ln)
        else:
            break
    while out and out[-1].strip() == "":
        out.pop()
    return out


def rule_lines(lines, name):
    block = role_block(lines, name)
    return [ln for ln in block if ln.startswith("      - {")]


def generate():
    ref = open(REF).read().splitlines(keepends=True)
    out = []
    for ln in open(TEMPLATE).read().splitlines(keepends=True):
        s = ln.strip()
        if s.startswith("#@ROLE "):
            out.extend(role_block(ref, s.split()[1]))
        elif s.startswith("#@RULES "):
            out.extend(rule_lines(ref, s.split()[1]))
        else:
            out.append(ln)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT, "w").write("".join(out))
    print("wrote", OUT)


def check():
    import re
    import yaml  # system python3 has PyYAML

    def load(path):  # the router's ${VAR:-default} interpolation, env-free
        raw = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}", lambda m: m.group(2) or "", open(path).read())
        return {r["name"]: r for r in yaml.safe_load(raw)["roles"]}

    ref, got = load(REF), load(OUT)
    ok = True
    for name in ("app", "app_reader", "svc_auth"):
        same = ref[name] == got[name]
        ok &= same
        print("%-18s verbatim: %s" % (name, same))
    union = ref["svc_fleetdb"]["permissions"] + ref["svc_fleetfiles"]["permissions"]
    extra = [p for p in got["svc_fake_platform"]["permissions"] if p not in union]
    missing = [p for p in union if p not in got["svc_fake_platform"]["permissions"]]
    print("svc_fake_platform  = svc_fleetdb + svc_fleetfiles: missing=%d extra=%s" % (len(missing), extra))
    ok &= not missing and extra == [{"uri": "test.", "match": "prefix", "allow_register": True}]
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    generate()
    if "--check" in sys.argv:
        check()
