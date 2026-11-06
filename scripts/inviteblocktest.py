#!/usr/bin/env python3
"""Regression test: a user can refuse all invites (`m.invite_permission_config`).

    scripts/inviteblocktest.py http://127.0.0.1:18099      (a test server with open registration)

With `default_action: "block"` every invite to the user is refused with M_INVITE_BLOCKED, and
invites they already hold are not shown while it is on. Any other content leaves invites as
they are, including the finer per-user rules of MSC4155.
"""
import json, secrets, sys, urllib.error, urllib.parse, urllib.request

HS = sys.argv[1].rstrip("/")
SS = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
failures = []
q = urllib.parse.quote


def call(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(HS + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def check(ok, what):
    print(("ok   " if ok else "FAIL ") + what)
    if not ok:
        failures.append(what)


def register(name):
    st, r = call("POST", "/_matrix/client/v3/register", {"username": f"{name}{secrets.token_hex(4)}", "password": secrets.token_urlsafe(16), "auth": {"type": "m.login.dummy"}})
    return r["user_id"], r["access_token"]


def main():
    alice, atok = register("alice")
    bob, btok = register("bob")
    carol, ctok = register("carol")

    def config(kind, content):
        st, r = call("PUT", f"/_matrix/client/v3/user/{q(bob)}/account_data/{kind}", content, btok)
        assert st == 200, r

    def room(token):
        st, r = call("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat"}, token)
        return r["room_id"]

    def invite(token, room_id):
        return call("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/invite", {"user_id": bob}, token)

    def invited_rooms():
        st, classic = call("GET", "/_matrix/client/v3/sync?timeout=0", None, btok)
        st, sliding = call("POST", f"{SS}?timeout=0", {"conn_id": secrets.token_hex(4), "lists": {"all": {"ranges": [[0, 50]], "required_state": [], "timeline_limit": 0}}}, btok)
        return set(classic.get("rooms", {}).get("invite", {})), {r for r, v in sliding.get("rooms", {}).items() if "invite_state" in v}

    # An invite bob already holds.
    held = room(carol and ctok)
    st, r = invite(ctok, held)
    check(st == 200 and invited_rooms() == ({held}, {held}), "an ordinary invite arrives in both kinds of sync")

    for kind in ("m.invite_permission_config", "org.matrix.msc4380.invite_permission_config"):
        config(kind, {"default_action": "block"})
        st, r = invite(atok, room(atok))
        check(st == 403 and r.get("errcode") == "M_INVITE_BLOCKED", f"{kind}: an invite is refused with M_INVITE_BLOCKED ({st} {r.get('errcode')})")
        check(invited_rooms() == (set(), set()), f"{kind}: the invite already held is not shown while invites are blocked")
        # Like any other invite that fails during room creation (also on Synapse): the request
        # reports the refusal.
        st, r = call("POST", "/_matrix/client/v3/createRoom", {"invite": [bob]}, atok)
        check(st == 403 and r.get("errcode") == "M_INVITE_BLOCKED", f"{kind}: creating a room with the user on the invite list reports the refusal ({st} {r.get('errcode')})")
        config(kind, {})
        check(invited_rooms() == ({held}, {held}), f"{kind}: switched off, the held invite is back")

    # A configuration that does not block must leave the finer rules in force.
    config("m.invite_permission_config", {"default_action": "allow"})
    config("org.matrix.msc4155.invite_permission_config", {"blocked_users": [alice]})
    st, r = invite(atok, room(atok))
    check(st == 403 and r.get("errcode") == "ORG.MATRIX.MSC4155.M_INVITE_BLOCKED", f"the per-user rules still apply next to a non-blocking switch ({st} {r.get('errcode')})")
    st, r = invite(ctok, room(ctok))
    check(st == 200, f"and users they do not name can still invite ({st})")
    config("org.matrix.msc4155.invite_permission_config", {})
    st, r = invite(atok, room(atok))
    check(st == 200, f"with both empty, invites work as usual ({st})")

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
