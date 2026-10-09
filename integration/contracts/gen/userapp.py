"""Runs REaccounting's device.f_authenticate_userapp in a throwaway Postgres
over the harness facts of fake_auth.PlatformDB and writes the verdicts per
state (usage: userapp.py SQL_FILE CONTAINER OUT.json). regen.sh starts and
removes the container."""
import json
import subprocess
import sys

SQL_FILE, CONTAINER, OUT = sys.argv[1:4]
SER = "06a0bf96-a539-4d6a-8471-ac7adc67616e"
KEY = "aXJvbmZsb2NrLWhhcm5lc3MtYXBwLWNyZWQta2V5NDI="  # fake_auth.APP_CRED_KEY
SCHEMA = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE ROLE acct_manager;
CREATE SCHEMA device; CREATE SCHEMA app; CREATE SCHEMA swarm;
CREATE TABLE device.t_device (device_key bigint, swarm_key bigint, serial_number text, exists boolean, valid_range tstzrange);
CREATE TABLE device.t_app_credential (device_key bigint, app_key bigint, stage text, epoch int, rotated_at timestamptz);
CREATE TABLE device.t_device_app_key (device_key bigint, app_cred_key text);
CREATE TABLE device.t_device_to_app (device_key bigint, app_key bigint, stage text, exists boolean, valid_range tstzrange);
CREATE TABLE app.t_app (app_key bigint, name text, app_type text, exists boolean, valid_range tstzrange);
CREATE TABLE swarm.t_swarm_to_app (swarm_key bigint, app_key bigint, data_access jsonb, exists boolean, valid_range tstzrange);
INSERT INTO device.t_device VALUES (42, 2, '%(ser)s', true, tstzrange('2020-01-01', 'infinity'));
INSERT INTO device.t_device_to_app VALUES (42, 26, 'DEV', true, tstzrange('2020-01-01', 'infinity'));
INSERT INTO app.t_app VALUES (26, 'interop', 'APP', true, tstzrange('2020-01-01', 'infinity')),
  (77, 'weather', 'APP', true, tstzrange('2020-01-01', 'infinity')),
  (3, 'boardstudio', 'PLATFORM', true, tstzrange('2020-01-01', 'infinity'));
INSERT INTO swarm.t_swarm_to_app VALUES (2, 26, '["weather"]', true, tstzrange('2020-01-01', 'infinity'));
""" % {"ser": SER}
CASES = [
    ("realm-2-26-dev", "app-26-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-77-dev", "app-26-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-3-dev", "app-26-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-99-dev", "app-26-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-26-prod", "app-26-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-26-prod", "app-26-prod-e1@" + SER, "own_realm_only"),
    ("realm-2-26-dev", "app-27-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-26-dev", "app-26-dev-e2@" + SER, "own_realm_only"),
    ("realm-2-26-dev", "app-26-dev-e0@" + SER, "own_realm_only"),
    ("realm-2-26-dev", "app-026-dev-e01@" + SER, "own_realm_only"),
    ("realm-2-26-dev", "app-26-dev-e1@unknown", "own_realm_only"),
    ("realm-3-26-dev", "app-26-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-26-dev", SER, "own_realm_only"),
    ("realm-2-77-dev", SER, "own_realm_only"),
    ("realm-2-26-dev", SER, "strict"),
    ("realm-2-77-dev", SER, "dual"),
    ("realm-2-3-dev", SER, "dual"),
    ("realm-2-26-prod", SER, "own_realm_only"),
    ("realm1", SER, "own_realm_only"),
    ("realm-2-26-dev", "app-26-dev-e1@" + SER + "@x", "own_realm_only"),
    ("realm-2-26-dev", "APP-26-dev-e1@" + SER, "own_realm_only"),
    ("realm-2-26-dev", "app-26-DEV-e1@" + SER, "own_realm_only"),
    ("realm-2-26-dev", "app-26-dev-e3@" + SER, "own_realm_only"),
    ("realm-2-77-dev", "app-26-dev-e2@" + SER, "own_realm_only"),
]
STATES = [
    {"name": "defaults", "grants": ["weather"], "epochs": [], "app_cred_key": True},
    {"name": "wildcard grant", "grants": ["*"], "epochs": [], "app_cred_key": True},
    {"name": "rotated to epoch 2 just now", "grants": ["weather"],
     "epochs": [{"app": 26, "stage": "DEV", "epoch": 2, "rotated_seconds_ago": 0}], "app_cred_key": True},
    {"name": "rotated to epoch 2 16 minutes ago", "grants": ["weather"],
     "epochs": [{"app": 26, "stage": "DEV", "epoch": 2, "rotated_seconds_ago": 960}], "app_cred_key": True},
    {"name": "no app credential key", "grants": ["weather"], "epochs": [], "app_cred_key": False},
]


def psql(sql):
    out = subprocess.run(["docker", "exec", "-i", CONTAINER, "psql", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres"],
                         input=sql, capture_output=True, text=True)
    if out.returncode:
        raise SystemExit("psql: " + out.stderr.strip())
    return out.stdout.strip()


psql(SCHEMA)
psql(open(SQL_FILE).read())
result = {"source": "REaccounting database/function/device/f_authenticate_userapp.sql, run in postgres:16-alpine "
                    "with pgcrypto over the harness facts of fake_auth.PlatformDB", "states": []}
for st in STATES:
    sql = ["DELETE FROM device.t_app_credential;", "DELETE FROM device.t_device_app_key;",
           "UPDATE swarm.t_swarm_to_app SET data_access = '%s';" % json.dumps(st["grants"])]
    if st["app_cred_key"]:
        sql.append("INSERT INTO device.t_device_app_key VALUES (42, '%s');" % KEY)
    for e in st["epochs"]:
        sql.append("INSERT INTO device.t_app_credential VALUES (42, %d, '%s', %d, now() - interval '%d seconds');"
                   % (e["app"], e["stage"], e["epoch"], e["rotated_seconds_ago"]))
    psql("\n".join(sql))
    cases = []
    for realm, authid, mode in CASES:
        arg = json.dumps({"realm": realm, "authid": authid, "legacy_mode": mode}).replace("'", "''")
        cases.append({"realm": realm, "authid": authid, "legacy_mode": mode,
                      "verdict": json.loads(psql("SELECT device.f_authenticate_userapp('%s'::jsonb);" % arg))})
    result["states"].append(dict(st, cases=cases))
json.dump(result, open(OUT, "w"), indent=1, sort_keys=True)
