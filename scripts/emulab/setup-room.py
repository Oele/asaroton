"""Create two lab users and an encrypted room with call permissions for members; writes lab.json.

usage: setup-room.py [HS_URL]  (default http://localhost:8008; registration must be enabled)
"""
import json, os, sys, urllib.request, urllib.error, urllib.parse
HS = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8008"
def req(method, path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(HS + path, data=data, method=method, headers={"Content-Type": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})})
    try:
        with urllib.request.urlopen(r) as resp: return json.load(resp)
    except urllib.error.HTTPError as e:
        return {"errcode": e.code, "body": e.read().decode()}
def login_or_register(user, pw):
    r = req("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": user}, "password": pw, "initial_device_display_name": "setup"})
    if "access_token" in r: return r["access_token"], r["user_id"]
    r = req("POST", "/_matrix/client/v3/register", {"username": user, "password": pw, "auth": {"type": "m.login.dummy"}, "initial_device_display_name": "setup"})
    if "access_token" not in r: sys.exit(f"register {user}: {r}")
    return r["access_token"], r["user_id"]
PW = os.environ.get("LAB_PASSWORD", "callTest2026x")  # alphanumeric: `adb shell input text` mangles punctuation
tw, uw = login_or_register("webdev", PW)
tx, ux = login_or_register("exdev", PW)
room = req("POST", "/_matrix/client/v3/createRoom", {"name": "Call lab", "invite": [ux], "preset": "private_chat"}, tw)
rid = room["room_id"]
q = urllib.parse.quote(rid, safe="")
print(req("POST", f"/_matrix/client/v3/rooms/{q}/join", {}, tx))
print(req("PUT", f"/_matrix/client/v3/rooms/{q}/state/m.room.encryption/", {"algorithm": "m.megolm.v1.aes-sha2"}, tw))
pl = req("GET", f"/_matrix/client/v3/rooms/{q}/state/m.room.power_levels/", None, tw)
pl.setdefault("events", {}).update({"org.matrix.msc3401.call.member": 0, "m.call.member": 0, "m.rtc.member": 0})
print(req("PUT", f"/_matrix/client/v3/rooms/{q}/state/m.room.power_levels/", pl, tw))
print("ROOM", rid); print("USERS", uw, ux, "PW", PW)
json.dump({"hs": HS, "room_id": rid, "web": [uw, PW, tw], "ex": [ux, PW, tx]}, open("lab.json", "w"))
