#!/usr/bin/env python3
"""Policy servers (Matrix v1.18, MSC4284), the homeserver's side.

A room uses a policy server when its current state has a valid `m.room.policy` event (empty
state key, a string `via` and a string `public_keys.ed25519`) and the server named by `via`
has a joined member. Every server in such a room asks the policy server to sign each event
before sending it (`POST /_matrix/policy/v1/sign`), except the `m.room.policy` event itself,
and checks the signature against the key in that event. A local event it does not sign is
refused with the policy server's error, verbatim, and goes nowhere. An event from another
server without a valid signature is asked about once more, and soft-failed if there is still
none.

The lab: Alice on the homeserver; mock servers `ps` (the policy server, with a member in the
room), `other` (Carol, and later Dave, Erin and Frank) and `idle` (named as policy server by
a room it has nobody in). Three rooms:

- "Open": no `m.room.policy`. Nothing is asked of anybody.
- "Unused": its `m.room.policy` names `idle`, which has no member there. Not in use: nothing
  is asked, events from Carol without a policy signature are accepted.
- "Guarded": names `ps`, whose bot is a member. Alice's message gets the signature (the
  request is a signed federation request, the stored and the sent event carry it); refused,
  the client gets the policy server's error as it was and nothing is stored or sent; a wrong
  signature is refused too. The `m.room.policy` event is never asked about (also not while the
  policy server refuses everything, which is how a room gets rid of it); other `m.room.policy`
  events and membership events are. Joins over `/send_join` and an invitation to another
  server are signed first, or refused. Carol's messages: without a signature and refused again,
  soft-failed (not in Alice's timeline); without a signature and signed when asked, accepted
  with the signature kept; already validly signed, accepted without asking; signed wrongly,
  asked again. Only the refusals the spec defines for /sign reach the client as they are (a
  401 M_UNKNOWN_TOKEN becomes a 403 M_FORBIDDEN); an upgrade the policy server refuses makes
  no successor room. The bot's own message, signed by its server and its policy key, is accepted.
  Last, a policy server that does not answer: the client gets a 502 after about five seconds,
  and for a while after that the policy server is not asked again (events fail at once).

Needs what scripts/fedlab.py needs. Takes about half a minute.

Usage: scripts/policytest.py [--keep]
"""
import json, re, time, urllib.parse, urllib.request, uuid

from nacl.signing import SigningKey, VerifyKey

from fedlab import SERVER_NAME, canonical, check, query, run_lab, signed_event, unb64, wait_for

q = urllib.parse.quote


def run(hs, peers):
    ps, other, idle = peers["ps"], peers["other"], peers["idle"]
    for p in peers.values():
        # Whoever is asked signs: a request to the wrong server is then seen, not refused.
        p.policy_mode = "sign"
    alice, token = hs.register("alice")

    def create(name):
        st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11", "name": name}, token)
        assert st == 200, r
        return r["room_id"]

    def send(room_id, body, event_type="m.room.message"):
        content = {"msgtype": "m.text", "body": body}
        return hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/send/{q(event_type)}/{uuid.uuid4().hex}", content, token)

    def set_state(room_id, event_type, key, content):
        return hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/state/{q(event_type)}/{q(key)}", content, token)

    def asked(room_id, since=None):
        """What any mock server was asked to sign for the room (since the counts in `since`)."""
        return [pdu for label, p in peers.items() for pdu in p.policy_asked((since or {}).get(label, 0)) if pdu.get("room_id") == room_id]

    def marks():
        return {label: len(p.policy_requests) for label, p in peers.items()}

    def stored(event_id):
        rows = query(f"SELECT json, soft_failed, rejected FROM events WHERE event_id = '{event_id}'")
        return (json.loads(rows[0][0]), rows[0][1] == "t", rows[0][2] == "t") if rows else (None, None, None)

    def stored_with(text):
        return int(query(f"SELECT count(*) FROM events WHERE json LIKE '%{text}%'")[0][0])

    def sent_to(peer, text):
        """The PDUs with this body that reached `peer` in transactions."""
        return [pdu for req in peer.requests() for pdu in req["body"].get("pdus", []) if pdu.get("content", {}).get("body") == text]

    def timeline(room_id):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/messages?dir=b&limit=100", None, token)
        assert st == 200, r
        return [e["content"].get("body") for e in r["chunk"] if e["type"] == "m.room.message"]

    def visible(room_id, event_id):
        st, _ = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/event/{q(event_id)}", None, token)
        return st == 200

    def remote_message(peer, sender, room_id, body, policy=None):
        """A message from a user of a mock server, sent in a transaction. `policy`: None (no
        policy signature), "valid" or "wrong". Returns the event ID."""
        st, state = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/state", None, token)
        assert st == 200, state
        ids = {(e["type"], e["state_key"]): e["event_id"] for e in state}
        ends = [row[0] for row in query(f"SELECT event_id FROM forward_extremities WHERE room_id = '{room_id}'")]
        depth = int(query(f"SELECT max(depth) FROM events WHERE room_id = '{room_id}'")[0][0]) + 1
        ev = {"type": "m.room.message", "room_id": room_id, "sender": sender, "content": {"msgtype": "m.text", "body": body},
              "auth_events": [ids[("m.room.create", "")], ids[("m.room.power_levels", "")], ids[("m.room.member", sender)]],
              "prev_events": ends, "depth": depth, "origin_server_ts": int(time.time() * 1000)}
        event_id, ev = signed_event(peer.name, peer.key, "ed25519:k1", ev)
        if policy:
            ev["signatures"].setdefault(ps.name, {})["ed25519:policy_server"] = ps.policy_sign(ev, None if policy == "valid" else SigningKey.generate())
        st, r = peer.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}", {"origin": peer.name, "origin_server_ts": ev["origin_server_ts"], "pdus": [ev], "edus": []})
        assert st == 200, (st, r)
        return event_id

    # ------------------------------------------------------------------------------------
    print("-- a room without a policy server")
    open_room = create("Open")
    carol = other.join(hs.fed, open_room, "carol")
    st, r = send(open_room, "nobody to ask")
    check(st == 200, f"Alice's message is sent ({st})")
    theirs = remote_message(other, carol, open_room, "unsigned and fine")
    _, soft, rejected = stored(theirs)
    check(soft is False and rejected is False and "unsigned and fine" in timeline(open_room), "Carol's message without any policy signature is accepted")
    check(asked(open_room) == [], f"nobody is asked to sign anything ({len(asked(open_room))})")

    # ------------------------------------------------------------------------------------
    print("-- a room whose m.room.policy names a server with nobody in it")
    unused = create("Unused")
    other.join(hs.fed, unused, "carol")
    st, r = set_state(unused, "m.room.policy", "", idle.policy_content())
    check(st == 200, f"the m.room.policy event is sent ({st})")
    st, r = send(unused, "idle is not in the room")
    check(st == 200, f"Alice's message is sent ({st})")
    theirs = remote_message(other, carol, unused, "idle cannot object")
    _, soft, _ = stored(theirs)
    check(soft is False and "idle cannot object" in timeline(unused), "Carol's message without a policy signature is accepted")
    check(asked(unused) == [], f"nobody is asked to sign anything ({len(asked(unused))})")

    # ------------------------------------------------------------------------------------
    print("-- a room with a policy server")
    room = create("Guarded")
    bot = ps.join(hs.fed, room, "bot")
    other.join(hs.fed, room, "carol")
    check(asked(room) == [], "before m.room.policy nothing is asked")
    st, r = set_state(room, "m.room.policy", "", ps.policy_content())
    check(st == 200, f"the m.room.policy event is sent ({st})")
    check(asked(room) == [], "and is not asked about")

    since = marks()
    st, r = send(room, "a message the policy server likes")
    check(st == 200, f"Alice's message is sent ({st}, {r})")
    msg = r.get("event_id")
    got = asked(room, since)
    check([p.get("content", {}).get("body") for p in got] == ["a message the policy server likes"], f"the policy server is asked once ({len(got)})")
    check(all(p.get("sender") == alice and "hashes" in p and SERVER_NAME in p.get("signatures", {}) for p in got), "with the event as it will be sent: hashed and signed by its server")
    request = ps.policy_requests[since["ps"]] if len(ps.policy_requests) > since["ps"] else {"auth": "", "path": "", "pdu": {}}
    check(x_matrix_valid(hs, ps, request), "as a federation request signed by the homeserver")
    ev, soft, _ = stored(msg)
    check(ev is not None and ps.policy_signed(ev) and SERVER_NAME in ev["signatures"], "the stored event carries the policy signature beside its own")
    check(wait_for(lambda: any(ps.policy_signed(p) for p in sent_to(ps, "a message the policy server likes")), 10, 0.1), "the policy server receives the event, signed")
    check(wait_for(lambda: any(ps.policy_signed(p) for p in sent_to(other, "a message the policy server likes")), 10, 0.1), "and so does the other server")

    print("-- refused")
    ps.policy_mode = "refuse"
    since = marks()
    st, r = send(room, "spam spam spam")
    check(st == 400 and r == ps.policy_refusal, f"the client gets the policy server's error as it was ({st}, {r})")
    check(len(asked(room, since)) == 1, "after asking once")
    time.sleep(2)
    check(stored_with("spam spam spam") == 0, "the event is not stored")
    check(sent_to(ps, "spam spam spam") == [] and sent_to(other, "spam spam spam") == [], "nor sent anywhere")
    check("spam spam spam" not in timeline(room), "nor in the timeline")

    # Only the answers the spec defines for /sign are passed on: an errcode that makes
    # clients act (sign out, here) is no policy server's to give.
    ps.policy_refusal, ps.policy_refusal_status = {"errcode": "M_UNKNOWN_TOKEN", "error": "sign out now", "soft_logout": False}, 401
    st, r = send(room, "a hostile answer")
    check(st == 403 and r == {"errcode": "M_FORBIDDEN", "error": "sign out now"}, f"an answer the spec does not define for /sign becomes a plain refusal ({st}, {r})")
    check(stored_with("a hostile answer") == 0, "and the event is not stored")
    ps.policy_refusal, ps.policy_refusal_status = {"errcode": "M_FORBIDDEN", "error": "ps does not like this event"}, 400

    ps.policy_mode = "wrong"
    st, r = send(room, "signed with another key")
    check(st == 403 and r.get("errcode") == "M_FORBIDDEN", f"a signature that is not valid for the room's key is refused too ({st}, {r})")
    check(stored_with("signed with another key") == 0, "and the event is not stored")

    print("-- what is asked about")
    ps.policy_mode = "sign"
    since = marks()
    st, r = set_state(room, "m.room.policy", "extra", {"note": "not the room's policy"})
    check(st == 200, f"an m.room.policy event with a state key is sent ({st})")
    st, r = send(room, "a policy message", "m.room.policy")
    check(st == 200, f"a non-state m.room.policy event is sent ({st})")
    st, r = set_state(room, "m.room.topic", "", {"topic": "guarded"})
    check(st == 200, f"and a topic ({st})")
    got = [(p["type"], p.get("state_key")) for p in asked(room, since)]
    check(got == [("m.room.policy", "extra"), ("m.room.policy", None), ("m.room.topic", "")], f"all three are asked about ({got})")

    print("-- the way out: m.room.policy needs no signature")
    ps.policy_mode = "refuse"
    since = marks()
    st, r = set_state(room, "m.room.policy", "", {})
    check(st == 200, f"while the policy server refuses everything, the room can drop it ({st}, {r})")
    st, r = send(room, "free again")
    check(st == 200 and asked(room, since) == [], f"then messages go out without asking ({st}, {len(asked(room, since))})")
    st, r = set_state(room, "m.room.policy", "", ps.policy_content())
    check(st == 200 and asked(room, since) == [], f"and it can be put back, also without asking ({st})")
    ps.policy_mode = "sign"

    print("-- membership")
    since = marks()
    dave, dave_id, ev = other.make_join_event(hs.fed, room, "dave")
    st, r = other.fed(hs.fed, "PUT", f"/_matrix/federation/v2/send_join/{q(room, safe='')}/{q(dave_id, safe='')}", ev)
    got = [(p["type"], p.get("state_key")) for p in asked(room, since)]
    check(st == 200 and got == [("m.room.member", dave)], f"a join over /send_join is asked about first ({st}, {got})")
    joined, soft, _ = stored(dave_id)
    check(joined is not None and ps.policy_signed(joined) and soft is False, "and taken in with the signature")
    check(ps.policy_signed(r.get("event", {})), "which the joining server gets back too")

    ps.policy_mode = "refuse"
    erin, erin_id, ev = other.make_join_event(hs.fed, room, "erin")
    st, r = other.fed(hs.fed, "PUT", f"/_matrix/federation/v2/send_join/{q(room, safe='')}/{q(erin_id, safe='')}", ev)
    check(st == 400 and r == ps.policy_refusal, f"a refused join fails with the policy server's error ({st}, {r})")
    check(stored(erin_id)[0] is None, "and is not stored")
    st, members = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/members", None, token)
    check(erin not in [m["state_key"] for m in members.get("chunk", [])], "Erin is no member")

    invitations = []

    def take_invitation(handler, m, body):
        ev = body["event"]
        redacted = {k: v for k, v in ev.items() if k in ("type", "room_id", "sender", "state_key", "content", "hashes", "depth", "prev_events", "auth_events", "origin_server_ts")}
        redacted["content"] = {"membership": ev["content"]["membership"]}
        ev["signatures"][other.name] = {"ed25519:k1": other.sign(redacted)}
        invitations.append(ev)
        handler.answer(200, {"event": ev})

    other.put_routes.append((re.compile(r"/_matrix/federation/v2/invite/"), take_invitation))
    frank = f"@frank:{other.name}"
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room)}/invite", {"user_id": frank}, token)
    check(st == 400 and r == ps.policy_refusal and invitations == [], f"a refused invitation does not reach the invitee's server ({st}, {len(invitations)})")
    ps.policy_mode = "sign"
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room)}/invite", {"user_id": frank}, token)
    check(st == 200 and len(invitations) == 1 and ps.policy_signed(invitations[0]), f"an invitation reaches it signed by the policy server ({st}, {len(invitations)})")

    print("-- upgrading the room")
    ps.policy_mode = "refuse"
    rooms_before = int(query("SELECT count(*) FROM rooms")[0][0])
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room)}/upgrade", {"new_version": "11"}, token)
    check(st == 400 and r == ps.policy_refusal, f"an upgrade the policy server refuses fails with its error ({st}, {r})")
    check(int(query("SELECT count(*) FROM rooms")[0][0]) == rooms_before, "before a successor room is made")
    ps.policy_mode = "sign"

    print("-- events from another server")
    ps.policy_mode = "refuse"
    since = marks()
    refused = remote_message(other, carol, room, "unsigned, refused again")
    check([p.get("content", {}).get("body") for p in asked(room, since)] == ["unsigned, refused again"], "an event without a policy signature is asked about once")
    _, soft, rejected = stored(refused)
    check(soft is True and rejected is False, f"refused again, it is soft-failed ({soft}, {rejected})")
    check(not visible(room, refused) and "unsigned, refused again" not in timeline(room), "and not shown to Alice")

    ps.policy_mode = "sign"
    since = marks()
    accepted = remote_message(other, carol, room, "unsigned, signed when asked")
    check(len(asked(room, since)) == 1, "an event without a policy signature is asked about")
    ev, soft, _ = stored(accepted)
    check(soft is False and ps.policy_signed(ev), "signed then, it is accepted and kept with the signature")
    check(visible(room, accepted) and "unsigned, signed when asked" in timeline(room), "and shown")

    since = marks()
    presigned = remote_message(other, carol, room, "signed before it came", policy="valid")
    _, soft, _ = stored(presigned)
    check(soft is False and visible(room, presigned) and asked(room, since) == [], "an event with a valid policy signature is accepted without asking")

    wrong = remote_message(other, carol, room, "signed by a stranger", policy="wrong")
    ev, soft, _ = stored(wrong)
    check(soft is False and ps.policy_signed(ev) and len(asked(room, since)) == 1, "one with a wrong signature is asked about again, and accepted with the new one")

    since = marks()
    own = remote_message(ps, bot, room, "the policy server speaks", policy="valid")
    _, soft, rejected = stored(own)
    check(soft is False and rejected is False and visible(room, own) and asked(room, since) == [], "the policy server's own message, with its server and policy signatures, is accepted")
    check(not hs.log_lines("ed25519:policy_server"), "and its policy key is never looked for as a server key")

    print("-- a policy server that does not answer")
    ps.policy_mode = "hang"
    since = marks()
    started = time.time()
    st, r = send(room, "waiting for nobody")
    took = time.time() - started
    check(st == 502 and 4 <= took < 7.5, f"the client gets a 502 after the timeout ({st}, {took:.1f}s, {r.get('error')})")
    check(stored_with("waiting for nobody") == 0, "the event is not stored")
    started = time.time()
    st, r = send(room, "still nobody")
    took = time.time() - started
    check(st == 502 and took < 2 and len(asked(room, since)) == 1, f"right after, events fail at once without asking again ({st}, {took:.1f}s)")
    late = remote_message(other, carol, room, "while it is down")
    _, soft, _ = stored(late)
    check(soft is True and len(asked(room, since)) == 1, "and an unsigned event from another server is soft-failed without asking")

    print("-- the rooms without one were never asked about")
    check(asked(open_room) == [] and asked(unused) == [], "no requests for them")


def x_matrix_valid(hs, peer, request):
    """Whether a recorded /sign request carries a valid X-Matrix signature of the homeserver."""
    params = dict(re.findall(r'(\w+)="?([^",]*)"?', request["auth"].removeprefix("X-Matrix ")))
    if params.get("origin") != SERVER_NAME or params.get("destination") != peer.name:
        return False
    with urllib.request.urlopen(hs.fed + "/_matrix/key/v2/server", timeout=10) as resp:
        keys = json.loads(resp.read())
    key = keys["verify_keys"].get(params.get("key"), {}).get("key")
    if not key:
        return False
    signed = {"method": "POST", "uri": request["path"], "origin": SERVER_NAME, "destination": peer.name, "content": request["pdu"]}
    try:
        VerifyKey(unb64(key)).verify(canonical(signed), unb64(params["sig"]))
        return True
    except Exception:
        return False


if __name__ == "__main__":
    run_lab(["ps", "other", "idle"], run)
