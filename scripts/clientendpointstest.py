#!/usr/bin/env python3
"""Small client endpoints answer what the specification says.

  - `GET /capabilities` is served under the old `r0` prefix as well as under `v3`.
  - `GET /account/3pid` is an empty list on a server that keeps no e-mail addresses or phone
    numbers, not an unknown endpoint.
  - `GET /keys/changes` reports, like sync does, whoever came to share a room with the user
    between two tokens (their device list was not followed until then) and whoever stopped
    sharing one.
  - A deletion names the event it deletes where every client looks. Up to room version 10
    that is a top-level `redacts`, from 11 on `content.redacts`; Element reads only the
    first, clients on the Rust SDK the second, so a client is given both whatever the room.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/clientendpointstest.py [--keep]
"""
import urllib.parse, uuid

from fedlab import check, run_lab

ENCRYPTION = {"type": "m.room.encryption", "state_key": "", "content": {"algorithm": "m.megolm.v1.aes-sha2"}}


def run(hs, peers):
    q = urllib.parse.quote
    (alice, a), (bob, b), (carol, c) = (hs.register(n) for n in ("alice", "bob", "carol"))

    print("-- capabilities under r0")
    st, v3 = hs.req("GET", "/_matrix/client/v3/capabilities", None, a)
    st0, r0 = hs.req("GET", "/_matrix/client/r0/capabilities", None, a)
    check(st == 200 and st0 == 200 and r0 == v3 and "m.room_versions" in r0.get("capabilities", {}), f"the r0 path gives the same answer as v3 ({st0})")
    st0, r0 = hs.req("GET", "/_matrix/client/r0/capabilities")
    check(st0 == 401, f"and asks for a token like v3 does ({st0} {r0.get('errcode')})")

    print("-- third-party identifiers")
    st, r = hs.req("GET", "/_matrix/client/v3/account/3pid", None, a)
    check(st == 200 and r == {"threepids": []}, f"the list is there and empty ({st} {r})")
    st, r = hs.req("GET", "/_matrix/client/v3/account/3pid")
    check(st == 401, f"for whoever has a token ({st} {r.get('errcode')})")

    print("-- /keys/changes and who shares a room")

    def token_of(access_token):
        st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, access_token)
        assert st == 200, r
        return r["next_batch"]

    def changes(access_token, since, until):
        st, r = hs.req("GET", f"/_matrix/client/v3/keys/changes?from={q(since)}&to={q(until)}", None, access_token)
        assert st == 200, r
        return sorted(r.get("changed", [])), sorted(r.get("left", []))

    def act(access_token, room_id, action, body=None):
        st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/{action}", body or {}, access_token)
        assert st == 200, (action, r)

    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "initial_state": [ENCRYPTION], "invite": [bob]}, a)
    assert st == 200, created
    room_id = created["room_id"]
    invited = token_of(a)
    act(b, room_id, "join")
    joined = token_of(a)
    check(changes(a, invited, joined) == ([bob], []), f"someone who goes from invited to joined is in `changed` ({changes(a, invited, joined)})")
    check(changes(a, joined, token_of(a)) == ([], []), "and not again when nothing happened since")
    act(b, room_id, "leave")
    gone = token_of(a)
    check(changes(a, joined, gone) == ([], [bob]), f"someone who leaves the only shared room is in `left` ({changes(a, joined, gone)})")
    # Alice comes into a room where others already are.
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "initial_state": [ENCRYPTION]}, c)
    assert st == 200, created
    carols_room = created["room_id"]
    before = token_of(a)
    act(a, carols_room, "join")
    after = token_of(a)
    check(changes(a, before, after)[0] == sorted([alice, carol]), f"the members of a room the user joins are in `changed` ({changes(a, before, after)})")
    act(a, carols_room, "leave")
    check(changes(a, after, token_of(a)) == ([], [carol]), f"and in `left` when the user goes again ({changes(a, after, token_of(a))})")

    print("-- a deletion, as clients are given it")
    sliding = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
    for version in ("12", "10"):
        st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "room_version": version, "invite": [bob]}, a)
        assert st == 200, created
        room_id = created["room_id"]
        room = f"/_matrix/client/v3/rooms/{q(room_id)}"
        act(b, room_id, "join")
        st, sent = hs.req("PUT", f"{room}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "said in haste"}, a)
        assert st == 200, sent
        target = sent["event_id"]
        since = token_of(b)
        st, r = hs.req("PUT", f"{room}/redact/{q(target)}/{uuid.uuid4().hex}", {"reason": "regret"}, a)
        assert st == 200, r

        def targets(events):
            return [(e.get("redacts"), e["content"].get("redacts")) for e in events if e["type"] == "m.room.redaction"]

        st, r = hs.req("GET", f"/_matrix/client/v3/sync?timeout=0&since={q(since)}", None, b)
        got = targets(r.get("rooms", {}).get("join", {}).get(room_id, {}).get("timeline", {}).get("events", []))
        check(got == [(target, target)], f"room version {version}, classic sync: the target is at the top level and in the content ({got})")
        body = {"conn_id": uuid.uuid4().hex[:8], "lists": {"all": {"ranges": [[0, 9]], "timeline_limit": 5, "required_state": []}}}
        st, r = hs.req("POST", f"{sliding}?timeout=0", body, b)
        got = targets(r.get("rooms", {}).get(room_id, {}).get("timeline", []))
        check(got == [(target, target)], f"room version {version}, sliding sync: the same ({got})")
        st, r = hs.req("GET", f"{room}/messages?dir=b&limit=5", None, b)
        got = targets(r.get("chunk", []))
        deleted = [e for e in r.get("chunk", []) if e["event_id"] == target]
        because = deleted[0].get("unsigned", {}).get("redacted_because", {}) if deleted else {}
        check(got == [(target, target)] and deleted and deleted[0]["content"] == {} and because.get("redacts") == target,
              f"room version {version}, /messages: the same, and the deleted message says what deleted it ({got}, {because.get('redacts') == target})")


if __name__ == "__main__":
    run_lab(["mock"], run)
