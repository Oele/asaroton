#!/usr/bin/env python3
"""Exercises tessera's MSC4195 endpoints against a dev server (HS, registration open) and a local
LiveKit started with the lab key (LK_KEY/LK_SECRET), using the `lk` CLI (LK_CLI) for participants."""
import base64, hashlib, hmac, json, os, subprocess, sys, time, urllib.request, urllib.error, urllib.parse

HS = os.environ.get("HS", "http://127.0.0.1:8008")
LK_URL = os.environ.get("LK_URL", "ws://127.0.0.1:7880")
LK_HTTP = LK_URL.replace("ws://", "http://")
LK_KEY = os.environ.get("LK_KEY", "APISXUj3b3J5g32")
LK_SECRET = os.environ.get("LK_SECRET", "MD6h9BYv5sqtN6Ao4JWpTiHXhXMdOO5juiYmfaTqqsI")
LK = os.environ.get("LK_CLI", "lk")
PREFIX = "/_matrix/client/unstable/io.element.msc4195/rtc/livekit"
fails = 0

def check(cond, what):
    global fails
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        fails += 1

def req(method, path, body=None, token=None, raw=False):
    data = None if body is None else json.dumps(body).encode()
    r = urllib.request.Request(HS + path, data=data, method=method)
    if body is not None:
        r.add_header("Content-Type", "application/json")
    if token:
        r.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        payload = e.read()
        try:
            return e.code, json.loads(payload)
        except Exception:
            return e.code, {"raw": payload.decode(errors="replace")}

def b64url(b): return base64.urlsafe_b64encode(b).rstrip(b"=").decode()
def jwt_payload(tok):
    h, p, s = tok.split(".")
    sig = hmac.new(LK_SECRET.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest()
    assert b64url(sig) == s, "jwt signature mismatch"
    return json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
def mint_admin(video):
    now = int(time.time())
    hdr = b64url(b'{"alg":"HS256","typ":"JWT"}')
    pl = b64url(json.dumps({"iss": LK_KEY, "iat": now, "nbf": now, "exp": now + 60, "video": video}).encode())
    sig = b64url(hmac.new(LK_SECRET.encode(), f"{hdr}.{pl}".encode(), hashlib.sha256).digest())
    return f"{hdr}.{pl}.{sig}"
def lk_api(method, body, video):
    r = urllib.request.Request(f"{LK_HTTP}/twirp/livekit.RoomService/{method}", data=json.dumps(body).encode(), method="POST")
    r.add_header("Content-Type", "application/json"); r.add_header("Authorization", "Bearer " + mint_admin(video))
    with urllib.request.urlopen(r, timeout=10) as resp:
        return json.loads(resp.read())
def participants(room):
    return [p["identity"] for p in lk_api("ListParticipants", {"room": room}, {"roomAdmin": True, "room": room}).get("participants", [])]
def h(parts): return base64.b64encode(hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).digest()).rstrip(b"=").decode()

def register(name):
    user = f"{name}{int(time.time())}"
    st, r = req("POST", "/_matrix/client/v3/register", {"username": user, "password": "pw", "auth": {"type": "m.login.dummy"}})
    assert st == 200, r
    return r["user_id"], r["access_token"], r["device_id"]

def join_lk(identity, room):
    return subprocess.Popen([LK, "room", "join", "--url", LK_URL, "--api-key", LK_KEY, "--api-secret", LK_SECRET,
                             "--identity", identity, "--room", room, "--publish-demo"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

alice, atok, adev = register("alice")
bob, btok, bdev = register("bob")
carol, ctok, cdev = register("carol")
st, r = req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, atok); room = r["room_id"]
st, r = req("POST", f"/_matrix/client/v3/join/{urllib.parse.quote(room)}", {}, btok); assert st == 200, r

# discovery
st, r = req("GET", "/_matrix/client/versions")
check(r["unstable_features"].get("org.matrix.msc4143") is True, "versions advertises org.matrix.msc4143")
st, r = req("GET", "/_matrix/client/unstable/org.matrix.msc4143/rtc/transports", token=atok)
check(st == 200 and r["rtc_transports"][0]["type"] == "livekit" and r["rtc_transports"][0]["url"] == LK_URL and r["rtc_transports"][0]["livekit_service_url"] == HS + PREFIX, f"transports: {r}")
st, r = req("GET", "/_matrix/client/v1/rtc/transports")
check(st == 401, f"transports without token -> 401 ({st})")
st, r = req("GET", "/.well-known/matrix/client")
foci = r.get("org.matrix.msc4143.rtc_foci", [])
check(foci and foci[0]["livekit_service_url"] == HS + PREFIX, f"well-known rtc_foci: {foci}")

# get_token (MSC4195)
slot = "m.call#ROOM"; member = "memberABC"
st, r = req("POST", PREFIX + "/get_token", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": member}, atok)
check(st == 200 and "jwt" in r, f"get_token -> {st} {r if st != 200 else ''}")
lk_room = h([room, slot]); identity_a = h([alice, member])
pl = jwt_payload(r["jwt"])
check(pl["sub"] == identity_a and pl["video"]["room"] == lk_room and pl["video"]["roomJoin"] and pl["video"]["canPublish"] and not pl["video"]["roomCreate"], f"token claims: {pl}")
check(pl["iss"] == LK_KEY and pl["exp"] - pl["nbf"] == 3600, "token issuer and ttl")
rooms = lk_api("ListRooms", {"names": [lk_room]}, {"roomList": True}).get("rooms", [])
check(any(x["name"] == lk_room for x in rooms), "LiveKit room was created")
st, r = req("POST", "/_matrix/client/v1/rtc/livekit/get_token", {"url": "ws://elsewhere:1", "room_id": room, "slot_id": slot, "member_id": member}, atok)
check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"wrong url -> 400 M_INVALID_PARAM ({st} {r.get('errcode')})")
st, r = req("POST", PREFIX + "/get_token", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": member}, ctok)
check(st == 403, f"non-member -> 403 ({st})")
st, r = req("POST", PREFIX + "/get_token", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": member, "server_name": "nowhere.invalid"}, atok)
check(st == 403, f"remote server not in room -> 403 ({st} {r})")
st, r = req("POST", PREFIX + "/delegate_delayed_leave")
check(st != 404, f"delegate probe without body is not 404 ({st})")

# OpenID + legacy flows
st, r = req("POST", f"/_matrix/client/v3/user/{urllib.parse.quote(alice)}/openid/request_token", {}, atok)
check(st == 200 and r["matrix_server_name"] == "localhost" and r["token_type"] == "Bearer", f"openid request_token: {r}")
oid = r
st, r = req("GET", "/_matrix/federation/v1/openid/userinfo?access_token=" + oid["access_token"])
check(st == 200 and r.get("sub") == alice, f"federation userinfo: {r}")
st, r = req("GET", "/_matrix/federation/v1/openid/userinfo?access_token=bogus")
check(st == 401 and r.get("errcode") == "M_UNKNOWN_TOKEN", f"bad openid token -> 401 M_UNKNOWN_TOKEN ({st} {r.get('errcode')})")
st, r = req("POST", PREFIX + "/sfu/get", {"room": room, "openid_token": oid, "device_id": adev})
check(st == 200 and r.get("url") == LK_URL, f"legacy sfu/get -> {st} {r if st != 200 else ''}")
pl = jwt_payload(r["jwt"])
check(pl["sub"] == f"{alice}:{adev}" and pl["video"]["room"] == h([room, "m.call#ROOM"]), f"legacy identity {pl['sub']}")
st, r = req("POST", PREFIX + "/get_token", {"room_id": room, "slot_id": slot, "openid_token": oid, "member": {"id": member, "claimed_user_id": alice, "claimed_device_id": adev}})
check(st == 200 and jwt_payload(r["jwt"])["sub"] == h([alice, adev, member]), f"legacy get_token hashed identity ({st})")
st, r = req("POST", PREFIX + "/get_token", {"room_id": room, "slot_id": slot, "openid_token": oid, "member": {"id": member, "claimed_user_id": bob, "claimed_device_id": adev}})
check(st == 400, f"legacy get_token with wrong claimed user -> 400 ({st})")
st, r = req("POST", PREFIX + "/sfu/get", {"room": room, "openid_token": {"access_token": "nope", "matrix_server_name": "localhost"}, "device_id": adev})
check(st == 403, f"legacy sfu/get with bad openid token -> 403 ({st})")

# Delegated delayed leave: schedule, delegate, connect, disconnect, observe the leave being sent.
ev_type = "org.matrix.msc4143.rtc.member"
st, r = req("PUT", f"/_matrix/client/v3/rooms/{urllib.parse.quote(room)}/state/{ev_type}/{urllib.parse.quote('_' + alice + '_' + adev)}?org.matrix.msc4140.delay=3600000",
            {"leave": True}, atok)
check(st == 200 and "delay_id" in r, f"scheduled delayed leave: {r}")
delay_id = r["delay_id"]
st, r = req("POST", PREFIX + "/delegate_delayed_leave", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": member, "delay_id": "nope"}, atok)
check(st == 400, f"delegate unknown delay_id -> 400 ({st})")
st, r = req("POST", PREFIX + "/delegate_delayed_leave", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": member, "delay_id": delay_id}, btok)
check(st == 400, f"delegate someone else's delay_id -> 400 ({st})")
st, r = req("POST", PREFIX + "/delegate_delayed_leave", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": member, "delay_id": delay_id}, atok)
check(st == 200, f"delegate -> {st} {r}")
p = join_lk(identity_a, lk_room)
time.sleep(6)
check(identity_a in participants(lk_room), "alice's participant connected to the SFU")
st, r = req("GET", "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", token=atok)
check(any(e["delay_id"] == delay_id for e in r["delayed_events"]), "delayed leave still pending while connected")
p.terminate(); p.wait()
time.sleep(6)
st, r = req("GET", "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", token=atok)
check(not any(e["delay_id"] == delay_id for e in r["delayed_events"]), "delayed leave gone after disconnect")
st, r = req("GET", f"/_matrix/client/v3/rooms/{urllib.parse.quote(room)}/state/{ev_type}/{urllib.parse.quote('_' + alice + '_' + adev)}", token=atok)
check(st == 200 and r == {"leave": True}, f"the leave event was sent: {st} {r}")

# Kick: bob connected, alice kicks bob, bob is removed from the SFU.
st, r = req("POST", PREFIX + "/get_token", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": "bobmember"}, btok)
check(st == 200, f"bob token ({st})")
identity_b = h([bob, "bobmember"])
p = join_lk(identity_b, lk_room)
time.sleep(5)
check(identity_b in participants(lk_room), "bob's participant connected")
st, r = req("POST", f"/_matrix/client/v3/rooms/{urllib.parse.quote(room)}/kick", {"user_id": bob}, atok)
check(st == 200, f"kick ({st})")
time.sleep(4)
check(identity_b not in participants(lk_room), "bob removed from the SFU after the kick")
p.terminate(); p.wait()

print("FAILURES:", fails)
sys.exit(1 if fails else 0)
