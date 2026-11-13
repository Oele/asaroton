#!/usr/bin/env python3
"""MSC4195 over federation: a user on hs2 joins a call hosted on hs1's SFU."""
import base64, hashlib, hmac, json, sys, time, urllib.request, urllib.error, urllib.parse
H1, H2, S1, S2 = "http://127.0.0.1:18008", "http://127.0.0.1:28008", "localhost:18448", "localhost:28448"
# The secret of the throwaway LiveKit key in rtctest.py: public, for a local test server only.
LK_URL, LK_SECRET = "ws://127.0.0.1:7880", "MD6h9BYv5sqtN6Ao4JWpTiHXhXMdOO5juiYmfaTqqsI"
PREFIX = "/_matrix/client/unstable/io.element.msc4195/rtc/livekit"
fails = 0
def check(c, what):
    global fails; print(("ok   " if c else "FAIL ") + what); fails += 0 if c else 1
def req(hs, method, path, body=None, token=None):
    data = None if body is None else json.dumps(body).encode()
    r = urllib.request.Request(hs + path, data=data, method=method)
    if body is not None: r.add_header("Content-Type", "application/json")
    if token: r.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp: return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read())
        except Exception: return e.code, {}
def b64url(b): return base64.urlsafe_b64encode(b).rstrip(b"=").decode()
def payload(tok):
    h, p, s = tok.split(".")
    assert b64url(hmac.new(LK_SECRET.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest()) == s
    return json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
def h(parts): return base64.b64encode(hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).digest()).rstrip(b"=").decode()
def register(hs, name):
    user = f"{name}{int(time.time())}"
    st, r = req(hs, "POST", "/_matrix/client/v3/register", {"username": user, "password": "pwpwpwpw", "auth": {"type": "m.login.dummy"}})
    assert st == 200, r
    return r["user_id"], r["access_token"], r["device_id"]
alice, atok, adev = register(H1, "alice"); bob, btok, bdev = register(H2, "bob")
st, r = req(H1, "POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, atok); room = r["room_id"]
st, r = req(H2, "POST", f"/_matrix/client/v3/join/{urllib.parse.quote(room)}?server_name={S1}", {}, btok); check(st == 200, f"bob (hs2) joined hs1's room ({st} {r})")
time.sleep(1)
slot, member = "m.call#ROOM", "bobmember"
st, r = req(H2, "POST", PREFIX + "/get_token", {"url": LK_URL, "server_name": S1, "room_id": room, "slot_id": slot, "member_id": member}, btok)
check(st == 200 and "jwt" in r, f"bob gets a token for hs1's SFU over federation ({st} {r if st != 200 else ''})")
if st == 200:
    pl = payload(r["jwt"])
    check(pl["sub"] == h([bob, member]) and pl["video"]["room"] == h([room, slot]) and pl["video"]["canPublish"] is True, f"federated token claims: {pl['video']}")
st, r = req(H2, "POST", PREFIX + "/get_token", {"url": "ws://wrong:1", "server_name": S1, "room_id": room, "slot_id": slot, "member_id": member}, btok)
check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"wrong remote url relayed as 400 ({st} {r.get('errcode')})")
st, r = req(H1, "POST", PREFIX + "/get_token", {"url": LK_URL, "server_name": S2, "room_id": room, "slot_id": slot, "member_id": "x"}, atok)
check(st == 502, f"SFU-less remote server -> 502 ({st} {r})")
st, r = req(H2, "POST", PREFIX + "/get_token", {"url": LK_URL, "room_id": room, "slot_id": slot, "member_id": member}, btok)
check(st == 404, f"hs2 has no SFU of its own -> 404 ({st})")
# legacy flow across servers: bob's OpenID token, verified by hs1 through hs2's userinfo endpoint
st, oid = req(H2, "POST", f"/_matrix/client/v3/user/{urllib.parse.quote(bob)}/openid/request_token", {}, btok)
check(st == 200 and oid["matrix_server_name"] == S2, f"bob's openid token from hs2 ({st})")
st, r = req(H1, "POST", PREFIX + "/sfu/get", {"room": room, "openid_token": oid, "device_id": bdev})
check(st == 200 and st == 200 and payload(r["jwt"])["sub"] == f"{bob}:{bdev}", f"legacy sfu/get on hs1 with hs2's openid token ({st} {r if st != 200 else ''})")
st, r = req(H1, "POST", PREFIX + "/sfu/get", {"room": room, "openid_token": oid, "device_id": bdev, "delay_id": "x"})
check(st == 400, f"legacy delegation refused for a remote user ({st})")
# a direct federation request from an unsigned client is rejected
st, r = req(H1, "POST", "/_matrix/federation/v1/rtc/livekit/get_token", {"url": LK_URL, "user_id": bob, "room_id": room, "slot_id": slot, "member_id": member})
check(st == 401, f"unsigned federation get_token -> 401 ({st})")
print("FAILURES:", fails); sys.exit(1 if fails else 0)
