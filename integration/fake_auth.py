"""Model of the platform's identity services for the integration harness.

  - PlatformDB: the facts these services read from the platform database
    (REaccounting): the harness's one device, its per-device app credential
    key, the installed apps, app types, data-access grants, credential epochs
  - authenticate_userapp: REaccounting device.f_authenticate_userapp
    (database/function/device/f_authenticate_userapp.sql, reacct-managers
    v1.5.30), the SQL behind ironflock-auth's app-container authenticator
  - userapp_authenticate: ironflock-auth v0.4.0 (c379332)
    internal/authn/userapp.go Userapp.Authenticate, the procedure the router
    calls as auth.userapp.authenticate
  - authorize: ironflock-auth v0.4.0 internal/authz/policy.go Policy.Decide,
    the router's `identity` authorizer callout (auth.authorize)
  - PlatformDB.appaccess_resolve / appaccess_list: RESWARM reswarm-backend
    v1.4.1 backend/api/appaccess/appaccess.ts, which fleetdb's
    sys.appaccess.resolve / sys.appaccess.list forward to
  - app_credential: the per-app credential the device agent mints
    (DeviceManagementAgent v1.2.1 src/apps/app_credential.go), byte for byte

Pure Python, standard library only. contracts/ironflock_auth.json holds the
verdicts of the real Policy.Decide for a table of requests;
test_fake_platform.py checks authorize against it.
"""

import base64
import hashlib
import hmac
import re
import threading
import time

IRONFLOCK_AUTH_VERSION = "ironflock-auth v0.4.0 (c379332)"

SERIAL = "06a0bf96-a539-4d6a-8471-ac7adc67616e"
SWARM = 2
DEVICE_KEY = 42
OWN_APP = 26
PROVIDER_APP = 77
# device.t_device_app_key.app_cred_key: a stored base64 string; the HMAC key
# is the UTF-8 bytes of that string itself (no base64 decoding).
APP_CRED_KEY = "aXJvbmZsb2NrLWhhcm5lc3MtYXBwLWNyZWQta2V5NDI="
ROTATION_GRACE_SECONDS = 15 * 60

APP_CRED_MESSAGE_PREFIX = "ironflock/app-cred/v1"
DATA_REALM = re.compile(r"^realm-(\d+)-(\d+)-(dev|prod)$")
APP_AUTHID = re.compile(r"^app-(\d+)-(dev|prod)-e(\d+)@(.+)$", re.S)
HEALTH_PROBE_REALM = "__healthcheck__"


def app_credential(serial, app_key, stage, epoch, app_cred_key=APP_CRED_KEY):
    """(authid, secret) as the device agent writes APP_AUTH_ID.txt and
    APP_AUTH_SECRET.txt (app_credential.go appCredentialAuthID/Secret)."""
    authid = "app-%d-%s-e%d@%s" % (app_key, stage.lower(), epoch, serial)
    msg = "%s|%s|%d|%s|%d" % (APP_CRED_MESSAGE_PREFIX, serial, app_key, stage.upper(), epoch)
    secret = base64.b64encode(hmac.new(app_cred_key.encode(), msg.encode(), hashlib.sha256).digest()).decode()
    return authid, secret


class PlatformDB:
    """The platform facts of the harness: one device (key 42, serial SERIAL)
    in swarm 2, running app 26 ("interop") in DEV; app 77 ("weather") has a
    DEV databackend in the same swarm; app 26 holds the data-access grant
    ["weather"]; app 3 is a PLATFORM app without a databackend here."""

    def __init__(self, now=time.time):
        self.now = now
        self.lock = threading.RLock()
        self.devices = {SERIAL: {"device_key": DEVICE_KEY, "swarm": SWARM}}
        self.app_cred_keys = {DEVICE_KEY: APP_CRED_KEY}
        self.apps = {OWN_APP: {"name": "interop", "type": "APP"},
                     PROVIDER_APP: {"name": "weather", "type": "APP"},
                     3: {"name": "boardstudio", "type": "PLATFORM"}}
        self.installed = {(DEVICE_KEY, OWN_APP, "DEV")}  # device.t_device_to_app
        self.databackends = {}  # (swarm, app) -> {stage: catalog()}
        self.reset()
        self.reset_credentials()

    def reset(self):
        """Grants back to the defaults (test.reset)."""
        with self.lock:
            self.grants = {(SWARM, OWN_APP): ["weather"]}  # swarm.t_swarm_to_app.data_access

    def reset_credentials(self):
        """Every credential epoch back to 1 (test.auth.reset)."""
        with self.lock:
            self.epochs = {}  # (device, app, STAGE) -> (epoch, rotated_at)

    def set_grants(self, data_access, app_key=OWN_APP):
        with self.lock:
            self.grants[(SWARM, app_key)] = [str(x) for x in data_access]

    def rotate(self, app_key=OWN_APP, stage="DEV", grace_expired=False):
        """Moves (device 42, app, stage) to the next epoch, as the platform
        does on a credential rotation; returns the new credential."""
        with self.lock:
            key = (DEVICE_KEY, app_key, stage.upper())
            epoch, _ = self.epochs.get(key, (1, None))
            rotated_at = self.now() - (ROTATION_GRACE_SECONDS + 1 if grace_expired else 0)
            self.epochs[key] = (epoch + 1, rotated_at)
        authid, secret = app_credential(SERIAL, app_key, stage, epoch + 1)
        return {"authid": authid, "secret": secret, "epoch": epoch + 1}

    def add_databackend(self, swarm, app_key, stage, catalog):
        """Registers an installed databackend; catalog() returns its
        non-private {tables, transforms}."""
        self.databackends.setdefault((swarm, app_key), {})[stage.lower()] = catalog

    def device_by_serial(self, serial):
        return self.devices.get(serial)

    def _grant(self, swarm, app_key):
        return self.grants.get((swarm, app_key), [])

    # ------------------------------------------- f_authenticate_userapp

    def authenticate_userapp(self, realm, authid, legacy_mode="own_realm_only"):
        """device.f_authenticate_userapp: the jsonb verdict."""
        with self.lock:
            return self._authenticate_userapp(realm, authid, legacy_mode)

    def _authenticate_userapp(self, realm, authid, legacy_mode):
        if realm is None or authid is None:
            return {"success": False, "reason": "missing_args"}
        m = DATA_REALM.match(realm)
        if not m:
            return {"success": False, "reason": "bad_realm"}
        swarm, realm_app, realm_stage = int(m.group(1)), int(m.group(2)), m.group(3).upper()
        a = APP_AUTHID.match(authid)
        if a:
            cred, app_key, auth_stage, serial = "APP", int(a.group(1)), a.group(2).upper(), a.group(4)
            epoch = int(a.group(3))
            if epoch > 2 ** 31 - 1 or app_key > 2 ** 63 - 1:
                raise OverflowError("integer out of range")  # the SQL cast fails: ironflock-auth denies
        else:
            cred, app_key, auth_stage, serial, epoch = "LEGACY", None, None, authid, None
        device = self.devices.get(serial)
        if device is None or device["swarm"] != swarm:
            return {"success": False, "reason": "unknown_device", "cred": cred, "authid": serial}
        device_key = device["device_key"]
        if cred == "APP":
            fail = lambda reason, stage=auth_stage: {"success": False, "reason": reason, "cred": cred,
                                                     "device_key": device_key, "app_key": app_key, "stage": stage}
            cur, rotated_at = self.epochs.get((device_key, app_key, auth_stage), (1, None))
            if not (epoch == cur or (epoch == cur - 1 and rotated_at is not None
                                     and rotated_at > self.now() - ROTATION_GRACE_SECONDS)):
                return fail("stale_epoch")
            key = self.app_cred_keys.get(device_key)
            if key is None:
                return fail("no_app_cred_key")
            _, secret = app_credential(serial, app_key, auth_stage, epoch, key)
            if (device_key, app_key, auth_stage) not in self.installed:
                return fail("not_installed")
            if app_key == realm_app:
                if (device_key, realm_app, realm_stage) not in self.installed:
                    return fail("not_installed_realm_stage", realm_stage)
                role = "swarm_device"
            else:
                provider = self.apps.get(realm_app)
                if provider is None:
                    return fail("unknown_provider")
                name = provider["name"].lower()
                grant = self._grant(swarm, app_key)
                if name in grant or ("*" in grant and provider["type"] != "PLATFORM"):
                    role = "app_reader"
                else:
                    return fail("no_grant")
        else:
            if legacy_mode == "strict":
                return {"success": False, "reason": "legacy_denied", "cred": cred, "device_key": device_key}
            secret = serial
            if (device_key, realm_app, realm_stage) in self.installed:
                role = "swarm_device"
            elif legacy_mode == "dual":
                provider = self.apps.get(realm_app)
                covering = provider is not None and any(
                    (provider["name"].lower() in self._grant(swarm, app)
                     or ("*" in self._grant(swarm, app) and provider["type"] != "PLATFORM"))
                    for (dev, app, _stage) in self.installed if dev == device_key)
                if not covering:
                    return {"success": False, "reason": "no_grant", "cred": cred, "device_key": device_key}
                role = "app_reader"
            else:
                return {"success": False, "reason": "legacy_foreign", "cred": cred, "device_key": device_key}
        return {"success": True, "role": role, "secret": secret, "authid": serial, "cred": cred,
                "device_key": device_key, "app_key": app_key if app_key is not None else realm_app,
                "stage": auth_stage or realm_stage}

    # ------------------------------------------------- RESWARM appaccess

    def appaccess_resolve(self, swarm, consumer_app, provider_name):
        """appaccess.ts resolve; raises AppAccessError."""
        with self.lock:
            name = provider_name.lower() if isinstance(provider_name, str) else provider_name
            if not isinstance(name, str) or not name:
                raise AppAccessError("wamp.error.invalid_argument",
                                     "swarm_key, consumer_app_key and provider_name are required")
            grant = self._grant(swarm, consumer_app)
            explicit, wildcard = name in grant, "*" in grant
            if not explicit and not wildcard:
                raise AppAccessError("sys.appaccess.error.no_grant", "no data access grant for provider '%s'" % name)
            provider = next(((k, a) for k, a in sorted(self.apps.items()) if a["name"].lower() == name), None)
            if provider is None:
                raise AppAccessError("sys.appaccess.error.unknown_app", "no app named '%s' exists" % name)
            provider_key, app = provider
            if not explicit and app["type"] == "PLATFORM":
                raise AppAccessError("sys.appaccess.error.provider_not_installed",
                                     "provider '%s' has no databackend in this swarm" % name)
            stages = self.databackends.get((swarm, provider_key))
            if not stages:
                raise AppAccessError("sys.appaccess.error.provider_not_installed",
                                     "provider '%s' has no databackend in this swarm" % name)
            return {"app": name, "provider_app_key": provider_key,
                    "stages": {stage: catalog() for stage, catalog in stages.items()}}

    def appaccess_list(self, swarm, consumer_app):
        """appaccess.ts list (wildcard consumers only)."""
        with self.lock:
            if "*" not in self._grant(swarm, consumer_app):
                raise AppAccessError("sys.appaccess.error.no_grant", "consumer holds no wildcard data access grant")
            out = []
            for (s, app_key), stages in sorted(self.databackends.items()):
                app = self.apps.get(app_key)
                if s != swarm or app_key == consumer_app or app is None or app["type"] == "PLATFORM":
                    continue
                entry = {"app": app["name"].lower(), "provider_app_key": app_key,
                         "stages": {stage: catalog() for stage, catalog in stages.items()}}
                if any(c["tables"] or c["transforms"] for c in entry["stages"].values()):
                    out.append(entry)
            return out


class AppAccessError(Exception):
    """An autobahn.Error RESWARM throws: URI and one message argument;
    fleetdb re-throws it with the same arguments."""

    def __init__(self, error, message):
        super().__init__(message)
        self.error, self.message = error, message


# ------------------------------------------------------ ironflock-auth authn

def userapp_authenticate(db, realm, authid, legacy_mode="own_realm_only"):
    """internal/authn/userapp.go Userapp.Authenticate: what the router's
    dynamic WAMP-CRA authenticator receives. On success the authid is the
    device serial (the router rewrites the session authid to it); a denial is
    {"success": False}, which the client sees as authentication_failed."""
    denied = {"success": False}
    if realm == HEALTH_PROBE_REALM or not isinstance(realm, str) or not DATA_REALM.match(realm):
        return denied, {"reason": "not a data realm"}
    try:
        res = db.authenticate_userapp(realm, authid, legacy_mode or "own_realm_only")
    except (OverflowError, ValueError) as e:
        return denied, {"reason": "f_authenticate_userapp failed: %s" % e}
    if not res.get("success"):
        return denied, res
    role = "app" if res["role"] == "swarm_device" else res["role"]
    return {"secret": res["secret"], "role": role, "authid": res["authid"], "success": True}, res


# ------------------------------------------------------ ironflock-auth authz

def scope_segments(uri, match):
    """policy.go scopeSegments: the segments fixed for every URI the request
    can touch (a prefix that stops inside a segment leaves it empty)."""
    segs = uri.split(".")
    if match in ("", "exact", "wildcard"):
        return segs
    if match == "prefix":
        if not uri.endswith("."):
            segs[-1] = ""
        else:
            segs = segs[:-1]
        return segs
    return None


def _segment(segs, i):
    return segs[i] if i < len(segs) and segs[i] != "" else None


def _is_device_role(role):
    return role in ("swarm_device", "device")


def authorize(db, session, uri, action, options=None, static_mode=False):
    """policy.go Policy.Decide for the router's callout args
    [session, uri, action, options] -> ({"allow", "cache_prefix"?}, why).

    Only the data-realm family (digit-led URIs, appFunction) can reach the
    callout in this harness: the re.mgmt, reswarm.logs and instance families
    need device-agent, appliance or account sessions, which it has none of,
    and are denied here.

    static_mode: AUTH_MODE=static users are not rewritten to the serial, so a
    per-app authid's serial is taken from the authid itself (production never
    needs this: the authenticator rewrites every app session to its serial).
    """
    options = options or {}
    match = options.get("match") or "exact"
    realm = session.get("realm", "") or ""
    authid, role = session.get("authid"), session.get("authrole")
    segs = scope_segments(uri, match)
    if segs is None or len(segs) < 1:
        return {"allow": False}, "unknown match"
    if not (segs[0].isdigit() and segs[0].isascii()):
        return {"allow": False}, "no policy for this family in the harness"
    m = DATA_REALM.match(realm)
    if m is None or (not _is_device_role(role) and role != "app"):
        return {"allow": False}, "not an app session on a data realm"
    swarm, app, stage = m.group(1), m.group(2), m.group(3)
    if segs[0] != swarm:
        return {"allow": False}, "other swarm"
    if action != "register":
        # Writes and calls stay inside the realm's own swarm.
        return {"allow": True, "cache_prefix": swarm + "."}, "same swarm"
    device_key, app_seg, stage_seg = _segment(segs, 1), _segment(segs, 2), _segment(segs, 3)
    if not device_key or not app_seg or not stage_seg or app_seg != app or stage_seg.casefold() != stage.casefold():
        return {"allow": False}, "not a function URI of this app and stage"
    device = db.device_by_serial(authid)
    if device is None and static_mode:
        a = APP_AUTHID.match(authid or "")
        device = db.device_by_serial(a.group(4)) if a else None
    if device is None:
        return {"allow": False}, "no device with serial %r" % authid
    if str(device["device_key"]) != device_key:
        return {"allow": False}, "device %s is not %s" % (device_key, device["device_key"])
    return {"allow": True, "cache_prefix": "%s.%s.%s.%s." % (swarm, device_key, app_seg, stage_seg)}, "own device function"
