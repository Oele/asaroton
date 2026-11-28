#!/usr/bin/env python3
"""Account locking (Matrix v1.12), suspension (v1.13), and the admin endpoints and capability
for both (v1.18, MSC4323).

The lab: an admin, a second admin, Alice (who gets locked and suspended), Bob and Carol. Bob's
public room has Alice in it with power to redact; Bob has a second room, and invites Alice to a
third. The server signs in through OAuth 2.0 as well (`[oauth]`, lithostroton), and Alice has
a session from there too.

- Who may ask: a non-admin gets 403 on all four endpoints, the same answer for an account that
  exists and one that does not (authorisation before any lookup). An admin gets 400 for another
  server's user, 404 for an unknown account, 403 for another admin and for changing their own
  account. The unstable MSC4323 paths answer too.
- The capability: an admin's `/capabilities` has `m.account_moderation`, Bob's does not.
- Locked: `/sync`, `/send`, setting and reading `/profile` (reading needs no token, but she
  sends hers; a made-up token is still ignored there), `/versions`, `/whoami`, `/capabilities`,
  a handler that reads its token itself (`/mutual_rooms`), `/login` with the right password
  and `/refresh` all give 401 `M_USER_LOCKED` with `soft_logout`; a wrong password is still a
  plain 403. `/logout` works and the other tokens stay. The OAuth session's token is locked
  the same way; lithostroton's sign-in turns her away; its refresh keeps the session (the new
  token locked too). The admin API's user object says `locked`. Unlocked, the same tokens and
  refresh token work at once.
- Suspended: `/sync`, `/messages`, `/leave` (and rejecting an invite), redacting her own event
  through `/send` and `/redact`, key backup, device keys, cross-signing, to-device, account
  data, presence without a message, and a read receipt (kept private) work; `/send` of a
  message, `/join` (also of the room she is invited to), `/invite`, display name, avatar, a
  custom profile field, `/createRoom`, a topic, typing, an upload and redacting Bob's event
  give 403 `M_USER_SUSPENDED`. Her capabilities say she cannot change her profile. A new login
  works and is suspended too, as is her OAuth session. Of two delayed events she scheduled before, the message is not
  sent (it ends with `M_USER_SUSPENDED`) and the emptied call membership is. Lifted, she can
  send, join and change her name again.

Needs what scripts/fedlab.py needs. Takes about twenty seconds.

Usage: scripts/moderationtest.py [--keep]
"""
import hashlib, hmac, json, os, time, urllib.error, urllib.parse, urllib.request, uuid

from fedlab import SERVER_NAME, SHARED_SECRET, check, query, run_lab
from oauthtest import REDIRECT, Browser, b64, post_form

q = urllib.parse.quote
LOCK, SUSPEND = "/_matrix/client/v1/admin/lock/", "/_matrix/client/v1/admin/suspend/"
UNSTABLE = "/_matrix/client/unstable/uk.timedout.msc4323/admin/"
DELAY = "org.matrix.msc4140.delay"


def run(hs, peers):
    def ok(st, r, what):
        assert st == 200, (what, st, r)
        return r

    def admin_register(name):
        st, r = hs.req("GET", "/_synapse/admin/v1/register")
        nonce = ok(st, r, "nonce")["nonce"]
        mac = hmac.new(SHARED_SECRET.encode(), b"\0".join([nonce.encode(), name.encode(), b"pwpwpwpw", b"admin"]), hashlib.sha1).hexdigest()
        st, r = hs.req("POST", "/_synapse/admin/v1/register",
                       {"nonce": nonce, "username": name, "password": "pwpwpwpw", "admin": True, "mac": mac})
        r = ok(st, r, "admin registration")
        return r["user_id"], r["access_token"]

    def login(user, password="pwpwpwpw", refresh=False):
        body = {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": user}, "password": password}
        if refresh:
            body["refresh_token"] = True
        return hs.req("POST", "/_matrix/client/v3/login", body)

    def raw(method, path, data, token, content_type):
        r = urllib.request.Request(hs.url + path, data=data, method=method)
        r.add_header("Content-Type", content_type)
        r.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def send(token, room, content, event_type="m.room.message", query_string=""):
        path = f"/_matrix/client/v3/rooms/{q(room)}/send/{q(event_type)}/{uuid.uuid4().hex}"
        return hs.req("PUT", path + (f"?{query_string}" if query_string else ""), content, token)

    def text(token, room, body):
        return send(token, room, {"msgtype": "m.text", "body": body})

    def sync(token):
        return hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, token)

    def caps(token):
        st, r = hs.req("GET", "/_matrix/client/v3/capabilities", None, token)
        return ok(st, r, "capabilities")["capabilities"]

    def locked(answer):
        st, r = answer
        return st == 401 and r.get("errcode") == "M_USER_LOCKED" and r.get("soft_logout") is True

    def suspended(answer):
        st, r = answer
        return st == 403 and r.get("errcode") == "M_USER_SUSPENDED"

    def messages(token, room):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/messages?dir=b&limit=50", None, token)
        return ok(st, r, "messages")["chunk"]

    def whoami(token):
        return hs.req("GET", "/_matrix/client/v3/account/whoami", None, token)

    st, meta = hs.req("GET", "/_matrix/client/v1/auth_metadata")
    ok(st, meta, "auth_metadata")
    reg = {"client_name": "moderationtest", "client_uri": "https://client.example.org/", "application_type": "native",
           "redirect_uris": [REDIRECT], "token_endpoint_auth_method": "none",
           "grant_types": ["authorization_code", "refresh_token"], "response_types": ["code"]}
    req = urllib.request.Request(meta["registration_endpoint"], data=json.dumps(reg).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        client_id = json.loads(resp.read())["client_id"]

    def oauth_sign_in(device):
        """Alice signs in through lithostroton's pages: the tokens, or None when the login page
        comes back after the right password."""
        browser, verifier = Browser(), b64(os.urandom(32))
        st, url, page = browser.open(meta["authorization_endpoint"] + "?" + urllib.parse.urlencode({
            "response_type": "code", "client_id": client_id, "redirect_uri": REDIRECT, "state": "s",
            "scope": f"urn:matrix:client:api:* urn:matrix:client:device:{device}",
            "code_challenge": b64(hashlib.sha256(verifier.encode()).digest()), "code_challenge_method": "S256"}))
        action, fields = Browser.form(page, url)
        st, url, page = browser.open(action, {**fields, "username": "alice", "password": "pwpwpwpw"})
        if 'name="password"' in page:
            return None
        action, fields = Browser.form(page, url, {"action": "allow"})
        st, location, _ = browser.open(action, fields)
        code = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(location).query)).get("code")
        st, tokens = post_form(meta["token_endpoint"], {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT,
                                                       "client_id": client_id, "code_verifier": verifier})
        return ok(st, tokens, "token exchange")

    admin, adm = admin_register("admin")
    other_admin, _ = admin_register("admin2")
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, _ = hs.register("carol")
    nobody = f"@nobody:{SERVER_NAME}"

    # Bob's rooms: Alice may redact in the first.
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom",
                   {"preset": "public_chat", "room_version": "11", "power_level_content_override": {"users": {bob: 100, alice: 100}}}, b)
    room = ok(st, r, "createRoom")["room_id"]
    ok(*hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, a), "Alice joins")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, b)
    second = ok(st, r, "createRoom")["room_id"]
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, b)
    to_leave = ok(st, r, "createRoom")["room_id"]
    ok(*hs.req("POST", f"/_matrix/client/v3/join/{q(to_leave)}", {}, a), "Alice joins the room she will leave")
    bob_said = ok(*text(b, room, "bob speaks"), "bob")["event_id"]

    print("-- who may ask")
    for path in (LOCK, SUSPEND):
        answers = {}
        for target in (alice, nobody):
            answers[target] = [hs.req("GET", path + q(target), None, b),
                               hs.req("PUT", path + q(target), {"locked": True, "suspended": True}, b)]
        same = answers[alice] == answers[nobody]
        check(all(st == 403 and r.get("errcode") == "M_FORBIDDEN" for st, r in answers[alice]) and same,
              f"{path}: Bob, no admin, gets 403 for Alice and for nobody alike ({answers[alice]} / {answers[nobody]})")
    flags = query(f"SELECT is_locked, is_suspended FROM users WHERE user_id = '{alice}'")
    check(flags == [["f", "f"]], f"and nothing changed ({flags})")
    st, r = hs.req("GET", LOCK + q("@someone:elsewhere.test"), None, adm)
    check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"another server's user: 400 ({st} {r})")
    st, r = hs.req("GET", SUSPEND + q(nobody), None, adm)
    check(st == 404 and r.get("errcode") == "M_NOT_FOUND", f"an unknown account: 404 ({st} {r})")
    st, r = hs.req("PUT", LOCK + q(other_admin), {"locked": True}, adm)
    check(st == 403 and r.get("errcode") == "M_FORBIDDEN", f"another admin: 403 ({st} {r})")
    st, r = hs.req("PUT", SUSPEND + q(admin), {"suspended": True}, adm)
    check(st == 403, f"the admin's own account: 403 ({st} {r})")
    st, r = hs.req("GET", LOCK + q(alice), None, adm)
    check(st == 200 and r == {"locked": False}, f"admin GET lock: not locked ({st} {r})")
    st, r = hs.req("GET", SUSPEND + q(alice), None, adm)
    check(st == 200 and r == {"suspended": False}, f"admin GET suspend: not suspended ({st} {r})")
    st, r = hs.req("GET", UNSTABLE + "lock/" + q(alice), None, adm)
    check(st == 200 and r == {"locked": False}, f"the MSC4323 unstable path ({st} {r})")

    print("-- the capability")
    moderation = caps(adm).get("m.account_moderation")
    check(moderation == {"suspend": True, "lock": True}, f"an admin is told they may suspend and lock ({moderation})")
    check("m.account_moderation" not in caps(b), "Bob is not told anything about it")

    # ---------------------------------------------------------------- locked
    print("-- locked")
    oauth = oauth_sign_in("MODOAUTH1")
    check(oauth is not None and whoami(oauth["access_token"])[0] == 200, "Alice also signs in through OAuth (lithostroton)")
    st, r = login("alice")
    second_token = ok(st, r, "second session")["access_token"]
    st, r = login("alice", refresh=True)
    refresh_token = ok(st, r, "refreshable session")["refresh_token"]
    ok(*sync(a), "sync before")
    st, r = hs.req("PUT", LOCK + q(alice), {"locked": True}, adm)
    check(st == 200 and r == {"locked": True}, f"admin PUT lock ({st} {r})")
    st, r = hs.req("GET", LOCK + q(alice), None, adm)
    check(r == {"locked": True}, f"admin GET lock says so ({r})")
    check(locked(sync(a)), f"/sync: 401 M_USER_LOCKED with soft_logout, at once ({sync(a)})")
    check(locked(text(a, room, "while locked")), "/send too")
    check(locked(hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/displayname", {"displayname": "Locked"}, a)), "setting a display name too")
    check(locked(hs.req("GET", f"/_matrix/client/v3/profile/{q(bob)}", None, a)), "and reading a profile (needs no token, but she sent hers)")
    st, r = hs.req("GET", f"/_matrix/client/v3/profile/{q(bob)}", None, "made-up-token")
    check(st == 200, f"a made-up token there is still ignored ({st} {r.get('errcode')})")
    check(locked(hs.req("GET", "/_matrix/client/versions", None, a)), "/versions (its token is optional)")
    check(locked(hs.req("GET", "/_matrix/client/v3/account/whoami", None, a)), "/whoami")
    check(locked(hs.req("GET", "/_matrix/client/v3/capabilities", None, a)), "/capabilities")
    check(locked(hs.req("GET", f"/_matrix/client/v1/mutual_rooms?user_id={q(bob)}", None, a)), "/mutual_rooms (reads its token itself)")
    check(locked(login("alice")), "/login with the right password")
    st, r = login("alice", password="wrong")
    check(st == 403 and r.get("errcode") == "M_FORBIDDEN", f"a wrong password learns nothing ({st} {r.get('errcode')})")
    check(locked(hs.req("POST", "/_matrix/client/v3/refresh", {"refresh_token": refresh_token})), "/refresh")
    check(locked(whoami(oauth["access_token"])), "the token from the OAuth sign-in as well")
    check(oauth_sign_in("MODOAUTH2") is None, "the OAuth sign-in turns her away (its login page comes back)")
    st, oauth = post_form(meta["token_endpoint"], {"grant_type": "refresh_token", "refresh_token": oauth["refresh_token"], "client_id": client_id})
    check(st == 200 and locked(whoami(oauth.get("access_token"))),
          f"an OAuth refresh keeps the session, and its new token is locked too ({st}, {oauth.get('error')})")
    st, r = hs.req("GET", f"/_synapse/admin/v2/users/{q(alice)}", None, adm)
    check(st == 200 and r.get("locked") is True and r.get("suspended") is False, f"the admin API's user says locked ({r.get('locked')}, {r.get('suspended')})")
    count_tokens = lambda: int(query(f"SELECT count(*) FROM access_tokens WHERE user_id = '{alice}'")[0][0])
    before = count_tokens()
    st, r = hs.req("POST", "/_matrix/client/v3/logout", {}, second_token)
    check(st == 200, f"/logout works while locked ({st} {r})")
    after = count_tokens()
    check(after == before - 1 and after >= 3, f"that token goes, the others stay ({before} -> {after})")

    st, r = hs.req("PUT", LOCK + q(alice), {"locked": False}, adm)
    check(st == 200 and r == {"locked": False}, f"admin PUT unlock ({st} {r})")
    check(sync(a)[0] == 200, "unlocked: the same token works again, at once")
    check(whoami(oauth["access_token"])[0] == 200, "and the OAuth one")
    check(text(a, room, "back")[0] == 200, "and sends")
    st, r = hs.req("POST", "/_matrix/client/v3/refresh", {"refresh_token": refresh_token})
    check(st == 200 and r.get("access_token"), f"the refresh token works again ({st})")
    st, r = hs.req("GET", "/_matrix/client/v3/account/whoami", None, second_token)
    check(st == 401 and r.get("errcode") == "M_UNKNOWN_TOKEN", f"the session logged out while locked stays out ({st} {r.get('errcode')})")

    # ---------------------------------------------------------------- suspended
    print("-- suspended")
    alice_said = ok(*text(a, room, "alice one"), "alice")["event_id"]
    alice_again = ok(*text(a, room, "alice two"), "alice")["event_id"]
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "invite": [alice]}, b)
    invited = ok(st, r, "createRoom with Alice invited")["room_id"]
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "invite": [alice]}, b)
    invited_too = ok(st, r, "createRoom with Alice invited")["room_id"]
    # Delayed events scheduled before the suspension, due after it.
    st, r = send(a, room, {"msgtype": "m.text", "body": "alice from before"}, query_string=f"{DELAY}=3000")
    delayed_message = ok(st, r, "delayed message")["delay_id"]
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/state/org.matrix.msc3401.call.member/_{q(alice)}_DEV?{DELAY}=3000", {}, a)
    delayed_leave = ok(st, r, "delayed call leave")["delay_id"]

    st, r = hs.req("PUT", SUSPEND + q(alice), {"suspended": True}, adm)
    check(st == 200 and r == {"suspended": True}, f"admin PUT suspend ({st} {r})")
    check(hs.req("GET", SUSPEND + q(alice), None, adm)[1] == {"suspended": True}, "admin GET suspend says so")
    c = caps(a)
    check(c.get("m.set_displayname") == {"enabled": False} and c.get("m.set_avatar_url") == {"enabled": False}
          and c.get("m.profile_fields") == {"enabled": False} and "m.account_moderation" not in c,
          f"her capabilities: no profile changes ({c.get('m.set_displayname')}, {c.get('m.profile_fields')})")

    print("   what she may do")
    check(sync(a)[0] == 200, "/sync")
    check(len(messages(a, room)) > 3, "/messages")
    st, r = send(a, room, {"redacts": alice_said}, "m.room.redaction")
    check(st == 200, f"redacting her own event through /send ({st} {r})")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/redact/{q(alice_again)}/{uuid.uuid4().hex}", {}, a)
    check(st == 200, f"and through /redact ({st} {r})")
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(to_leave)}/leave", {}, a)
    check(st == 200, f"leaving a room ({st} {r})")
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(invited_too)}/leave", {}, a)
    check(st == 200, f"rejecting an invite ({st} {r})")
    backup = {"algorithm": "m.megolm_backup.v1.curve25519-aes-sha2",
              "auth_data": {"public_key": "hSDwCYkwp1R0i33ctD73Wg2/Og0mOBr066SpjqqbTmo", "signatures": {}}}
    st, r = hs.req("POST", "/_matrix/client/v3/room_keys/version", backup, a)
    check(st == 200, f"creating a key backup ({st} {r})")
    version = r.get("version")
    session = {"first_message_index": 0, "forwarded_count": 0, "is_verified": False,
               "session_data": {"ephemeral": "e", "ciphertext": "c", "mac": "m"}}
    st, r = hs.req("PUT", f"/_matrix/client/v3/room_keys/keys/{q(room)}/sess1?version={version}", session, a)
    check(st == 200, f"filling it ({st} {r})")
    me = ok(*hs.req("GET", "/_matrix/client/v3/account/whoami", None, a), "whoami")
    device = me.get("device_id")
    dk = {"user_id": alice, "device_id": device, "algorithms": ["m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"],
          "keys": {f"curve25519:{device}": "c" * 43, f"ed25519:{device}": "e" * 43}, "signatures": {}}
    st, r = hs.req("POST", "/_matrix/client/v3/keys/upload", {"device_keys": dk}, a)
    check(st == 200, f"uploading device keys ({st} {r})")
    master = {"user_id": alice, "usage": ["master"], "keys": {"ed25519:" + "m" * 43: "m" * 43}}
    st, r = hs.req("POST", "/_matrix/client/v3/keys/device_signing/upload", {"master_key": master}, a)
    check(st == 200, f"uploading cross-signing keys ({st} {r})")
    st, r = hs.req("PUT", f"/_matrix/client/v3/sendToDevice/m.key.verification.request/{uuid.uuid4().hex}",
                   {"messages": {bob: {"*": {"from_device": device, "methods": ["m.sas.v1"], "transaction_id": "t"}}}}, a)
    check(st == 200, f"to-device (verification) ({st} {r})")
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/account_data/net.example.private", {"x": 1}, a)
    check(st == 200, f"account data ({st} {r})")
    st, r = hs.req("PUT", f"/_matrix/client/v3/presence/{q(alice)}/status", {"presence": "unavailable"}, a)
    check(st == 200, f"presence without a message ({st} {r})")
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room)}/receipt/m.read/{q(bob_said)}", {}, a)
    check(st == 200, f"a read receipt ({st} {r})")
    kinds = query(f"SELECT receipt_type FROM receipts WHERE user_id = '{alice}' AND room_id = '{room}'")
    check(kinds == [["m.read.private"]], f"kept as a private one: Bob is shown nothing ({kinds})")

    print("   what she may not do")
    check(suspended(text(a, room, "while suspended")), "/send of a message: 403 M_USER_SUSPENDED")
    check(suspended(send(a, room, {"redacts": bob_said}, "m.room.redaction")), "redacting Bob's event through /send")
    check(suspended(hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/redact/{q(bob_said)}/{uuid.uuid4().hex}", {}, a)),
          "and through /redact (she has the power for it)")
    check(suspended(hs.req("POST", f"/_matrix/client/v3/join/{q(second)}", {}, a)), "joining")
    check(suspended(hs.req("POST", f"/_matrix/client/v3/rooms/{q(invited)}/join", {}, a)), "accepting an invite")
    check(suspended(hs.req("POST", f"/_matrix/client/v3/rooms/{q(room)}/invite", {"user_id": carol}, a)), "inviting")
    check(suspended(hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/displayname", {"displayname": "Spam"}, a)), "a display name")
    check(suspended(hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/avatar_url", {"avatar_url": "mxc://x/y"}, a)), "an avatar")
    check(suspended(hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/m.tz", {"m.tz": "Europe/Amsterdam"}, a)), "a custom profile field")
    check(suspended(hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat"}, a)), "/createRoom")
    check(suspended(hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/state/m.room.topic/", {"topic": "spam"}, a)), "a topic")
    check(suspended(hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/typing/{q(alice)}", {"typing": True, "timeout": 3000}, a)), "typing")
    check(suspended(raw("POST", "/_matrix/media/v3/upload?filename=a.txt", b"hello", a, "text/plain")), "an upload")
    check(suspended(hs.req("PUT", f"/_matrix/client/v3/presence/{q(alice)}/status", {"presence": "online", "status_msg": "buy now"}, a)),
          "a status message")
    names = [e["content"].get("displayname") for e in messages(b, room) if e["type"] == "m.room.member" and e.get("state_key") == alice]
    check("Spam" not in names, f"none of it reached the room ({names})")

    check(sync(oauth["access_token"])[0] == 200 and suspended(text(oauth["access_token"], room, "via oauth")),
          "the OAuth session syncs and is suspended too")

    print("   a new session")
    st, r = login("alice")
    check(st == 200, f"logging in works while suspended ({st} {r.get('errcode')})")
    fresh = r.get("access_token")
    check(sync(fresh)[0] == 200, "the new session syncs")
    check(suspended(text(fresh, room, "from the new session")), "and is suspended too")
    check(hs.req("POST", "/_matrix/client/v3/logout", {}, fresh)[0] == 200, "and logs out")

    print("   what she scheduled before")
    time.sleep(3.5)
    view = lambda delay_id: hs.req("GET", f"/_matrix/client/v1/delayed_events/{q(delay_id)}", None, a)
    deadline = time.time() + 10
    while time.time() < deadline and not (view(delayed_message)[1].get("finalised") and view(delayed_leave)[1].get("finalised")):
        time.sleep(0.3)
    fin = view(delayed_message)[1].get("finalised", {})
    check(fin.get("error", {}).get("errcode") == "M_USER_SUSPENDED" and "event_id" not in fin,
          f"her delayed message is not sent: M_USER_SUSPENDED ({fin})")
    fin = view(delayed_leave)[1].get("finalised", {})
    check("event_id" in fin, f"her delayed call leave (empty content) is ({fin})")
    bodies = [e["content"].get("body") for e in messages(b, room) if e["type"] == "m.room.message"]
    check("alice from before" not in bodies and "while suspended" not in bodies, f"Bob sees neither message ({bodies[:4]})")

    st, r = hs.req("PUT", SUSPEND + q(alice), {"suspended": False}, adm)
    check(st == 200 and r == {"suspended": False}, f"admin lifts the suspension ({st} {r})")
    check(text(a, room, "free again")[0] == 200, "she sends again, at once")
    check(hs.req("POST", f"/_matrix/client/v3/join/{q(second)}", {}, a)[0] == 200, "joins")
    check(hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/displayname", {"displayname": "Alice"}, a)[0] == 200, "changes her name")
    check(hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/typing/{q(alice)}", {"typing": True, "timeout": 3000}, a)[0] == 200, "types")
    check(caps(a).get("m.set_displayname") == {"enabled": True}, "and her capabilities say so")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra="\n[oauth]\nenabled = true\n")
