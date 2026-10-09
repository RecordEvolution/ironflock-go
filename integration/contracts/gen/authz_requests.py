"""Writes the table of auth.authorize callouts (usage: authz_requests.py
OUT.json) that authz_contract_test.go.tmpl runs the real Policy.Decide over:
the shapes the SDKs produce, the patterns and spellings ironflock-auth treats
specially, and other roles, realms and authids."""
import json
import sys

SER = "06a0bf96-a539-4d6a-8471-ac7adc67616e"
reqs = []


def add(action, uri, match="exact", authid=SER, role="app", realm="realm-2-26-dev"):
    reqs.append({"session": {"session": 1, "authid": authid, "authrole": role, "realm": realm},
                 "uri": uri, "action": action, "options": {"match": match}})


ROWS = [
    ("register", "2.42.26.DEV.fn"), ("register", "2.42.26.DEV.a.b"), ("call", "2.43.26.DEV.fn"),
    ("call", "2.42.26.DEV.fn"), ("publish", "2.26.sensordata"), ("register", "2.43.26.DEV.fn"),
    ("register", "2.42.26.PROD.fn"), ("register", "2.42.27.DEV.fn"), ("register", "3.42.26.DEV.fn"),
    ("register", "2..26.DEV.fn", "wildcard"), ("publish", "2.42.26.DEV.fn"), ("publish", "3.26.sensordata"),
    ("call", "3.43.26.DEV.fn"), ("register", "2.42.26.dev.fn"), ("register", "2.42.26.DEV.fn", "prefix"),
    ("register", "2.42.26.DEV.", "prefix"), ("register", "2.42.26.DEV..x", "wildcard"),
    ("publish", "2.43.26.DEV.fn"), ("publish", "2.43.99.PROD.fn"), ("publish", "2.99.sensordata"),
    ("publish", "2.26.a.b"), ("call", "2.43.99.PROD.fn"), ("call", "2.26.sensordata"),
    ("register", "2.42.26", "prefix"), ("register", "2.42.26.", "prefix"), ("register", "2.42", "prefix"),
    ("register", "2.", "prefix"), ("register", "2", "prefix"), ("register", "2.42.26.D", "prefix"),
    ("register", "2.42..DEV.x", "wildcard"), ("register", "2.42.26..x", "wildcard"),
    ("register", "2.42.26.DEV.fn", "regex"), ("subscribe", "2.26.sensordata"), ("register", "2.42.26.DeV.fn"),
    ("register", "02.42.26.DEV.fn"), ("register", "2.042.26.DEV.fn"), ("register", "2.42.026.DEV.fn"),
    ("register", "2.42.26.DEV"), ("call", "2"), ("publish", "2.", "prefix"), ("call", "", "prefix"),
    ("register", "com.example.x"), ("call", "re.mgmt.SER.x"), ("register", "٢.42.26.DEV.fn"),
]
for r in ROWS:
    add(*r)
for role in ("swarm_device", "device", "app_reader", "board_writer", ""):
    add("register", "2.42.26.DEV.fn", role=role)
    add("publish", "2.26.sensordata", role=role)
for realm in ("realm-2-26-prod", "realm-3-26-dev", "realm1", "realm-2-26-DEV", "realm-2-26-dev-x"):
    add("register", "2.42.26.DEV.fn", realm=realm)
    add("register", "2.42.26.PROD.fn", realm=realm)
    add("call", "2.43.26.DEV.fn", realm=realm)
add("register", "2.42.26.PROD.fn", realm="realm-2-26-prod")
for aid in ("unknown-serial", "app-26-dev-e1@" + SER, ""):
    add("register", "2.42.26.DEV.fn", authid=aid)
    add("call", "2.43.26.DEV.fn", authid=aid)
json.dump(reqs, open(sys.argv[1], "w"), indent=0, ensure_ascii=False)
