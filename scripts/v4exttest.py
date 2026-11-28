#!/usr/bin/env python3
"""`/v4/sync`: the extensions as their MSCs have them (MSC4508 and the four after it).

Alice syncs over `POST /_matrix/client/v4/sync`; Bob and Carol act. What is checked:

  1. to-device (MSC4538): the messages come as `messages`; a `limit` of 0 is `M_BAD_JSON`; a
     `since` the server cannot have given out (not a number, negative, ahead) is
     `M_INVALID_PARAM`; the queue is the device's, so a `since` sent on one connection
     acknowledges for the others; 250 waiting messages come in pages without waiting, and
     after that the request waits again.
  2. e2ee (MSC4543): no `device_lists` without a `pos` or when nothing changed; someone who
     comes to share an encrypted room is in `changed`, and `/keys/changes` between two `pos`
     says so too.
  3. account data (MSC4548): enabled on a later request, the extension sends all global
     account data, with `m.push_rules` once (the unstable endpoint keeps its rule: only what
     changed); a room entering scope gets all its account data.
  4. receipts (MSC4551): the bare `m.receipt` content; a room entering scope gets everyone's
     receipts on its timeline and all of Alice's own, also on events outside it (and so does
     every room when the extension is enabled later); a raised `timeline_limit` brings the
     receipts on the older events; another user's private receipt never shows; nothing for a
     room Alice was removed from.
  5. typing (MSC4508): bare `user_ids`; nothing for a room Alice is banned from.
  6. Scope: a room that drops out of a list's range and comes back gets the tag, the receipt
     and the typing of while it was out; with more than 100 receipts changed while it was out
     the connection expires (`M_UNKNOWN_POS`, as MSC4551 has it), and when it starts over the
     room gets its initial receipts. `rooms` names subscriptions only: a room in a list
     that is not subscribed is not covered by it. An unknown extension is ignored.

Needs what scripts/fedlab.py needs. Takes some seconds.

Usage: scripts/v4exttest.py [--keep]
"""
import time, urllib.parse, uuid

from fedlab import check, run_lab

V4 = "/_matrix/client/v4/sync"
UNSTABLE = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
NAME = {"include": [{"type": "m.room.name", "state_key": ""}]}


def run(hs, peers):
    q = urllib.parse.quote
    (alice, a), (bob, b), (carol, c) = (hs.register(n) for n in ("alice", "bob", "carol"))
    st, me = hs.req("GET", "/_matrix/client/v3/account/whoami", None, a)
    assert st == 200, me
    device = me["device_id"]

    def sync(body, pos=None, timeout=0, token=a):
        body = dict(body)
        if pos:
            body["pos"] = pos
            body["timeout"] = timeout
        return hs.req("POST", V4, body, token)

    def ok(body, pos=None, timeout=0):
        st, r = sync(body, pos, timeout)
        assert st == 200, (st, r)
        return r

    def create(token, body):
        st, r = hs.req("POST", "/_matrix/client/v3/createRoom", body, token)
        assert st == 200, r
        return r["room_id"]

    def act(token, room_id, action, body=None):
        st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/{action}", body or {}, token)
        assert st == 200, (action, r)

    def send(token, room_id, text):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": text}, token)
        assert st == 200, r
        return r["event_id"]

    def receipt(token, room_id, event_id, kind="m.read"):
        st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/receipt/{kind}/{q(event_id)}", {}, token)
        assert st == 200, r

    def tag(room_id, name):
        st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/rooms/{q(room_id)}/tags/{name}", {}, a)
        assert st == 200, r

    def ext(r, name):
        return r.get("extensions", {}).get(name, {})

    def readers(r, room_id):
        """{(event_id, receipt type, user)} of the room's receipts in an answer."""
        content = ext(r, "receipts").get("rooms", {}).get(room_id, {})
        return {(ev, kind, user) for ev, kinds in content.items() for kind, users in kinds.items() for user in users}

    def tags_of(r, room_id):
        for entry in ext(r, "account_data").get("rooms", {}).get(room_id, []):
            if entry["type"] == "m.tag":
                return sorted(entry["content"].get("tags", {}))
        return None

    # ------------------------------------------------------------------ to-device
    print("-- to-device")

    def to_alice(n, kind="m.test.marker"):
        for i in range(n):
            st, r = hs.req("PUT", f"/_matrix/client/v3/sendToDevice/{kind}/{uuid.uuid4().hex}", {"messages": {alice: {device: {"n": i}}}}, b)
            assert st == 200, r

    td = {"conn_id": "td1", "extensions": {"to_device": {"enabled": True}}}
    r = ok(td)
    first = ext(r, "to_device")
    check(first.get("messages") == [] and "events" not in first and first.get("next_batch"), f"the section has next_batch and messages, not events ({sorted(first)})")
    start = first["next_batch"]
    to_alice(3)
    td["extensions"]["to_device"]["since"] = start
    r = ok(td, r["pos"])
    got = [m["content"]["n"] for m in ext(r, "to_device").get("messages", [])]
    check(got == [0, 1, 2], f"the messages come as messages ({got})")
    acked = ext(r, "to_device")["next_batch"]
    other = {"conn_id": "td2", "extensions": {"to_device": {"enabled": True, "since": start}}}
    got = [m["content"]["n"] for m in ext(ok(other), "to_device").get("messages", [])]
    check(got == [0, 1, 2], f"another connection with the older since gets them too: nothing was acknowledged yet ({got})")
    td["extensions"]["to_device"]["since"] = acked
    r = ok(td, r["pos"])
    got = [m["content"]["n"] for m in ext(ok(other), "to_device").get("messages", [])]
    check(got == [], f"once one connection sends the newer since, they are gone for every connection of the device ({got})")
    for label, bad, code in (("a limit of 0", {"limit": 0}, "M_BAD_JSON"), ("a since that is not a number", {"since": "s1_2"}, "M_INVALID_PARAM"),
                             ("a negative since", {"since": "-1"}, "M_INVALID_PARAM"), ("a since ahead of the server", {"since": "99999999"}, "M_INVALID_PARAM")):
        st, e = sync({"conn_id": "td3", "extensions": {"to_device": {"enabled": True, **bad}}})
        check(st == 400 and e.get("errcode") == code, f"{label}: {code} ({st} {e.get('errcode')})")
    to_alice(250)
    td["extensions"]["to_device"]["limit"] = 100
    pages, took = [], []
    for _ in range(3):
        started = time.monotonic()
        r = ok(td, r["pos"], timeout=10000)
        took.append(time.monotonic() - started)
        page = ext(r, "to_device")
        pages.append(len(page.get("messages", [])))
        td["extensions"]["to_device"]["since"] = page.get("next_batch", td["extensions"]["to_device"]["since"])
    check(pages == [100, 100, 50] and max(took) < 5, f"250 waiting messages come in pages of 100, without waiting ({pages}, {[round(t, 1) for t in took]} s)")
    started = time.monotonic()
    r = ok(td, r["pos"], timeout=1500)
    check(time.monotonic() - started >= 1.2 and ext(r, "to_device").get("messages", []) == [], "and then the request waits again")

    # ------------------------------------------------------------------ e2ee
    print("-- e2ee")
    secret = create(a, {"name": "Secret", "preset": "public_chat", "initial_state": [{"type": "m.room.encryption", "state_key": "", "content": {"algorithm": "m.megolm.v1.aes-sha2"}}]})
    e2 = {"conn_id": "e2", "lists": {"all": {"timeline_limit": 0, "required_state": {}}}, "extensions": {"e2ee": {"enabled": True}}}
    r = ok(e2)
    section = ext(r, "e2ee")
    check("device_lists" not in section and "device_one_time_keys_count" in section and section.get("device_unused_fallback_key_types") == [],
          f"without a pos: the key counts and fallback types, no device_lists ({sorted(section)})")
    before = r["pos"]
    act(b, secret, "join")
    r = ok(e2, before)
    changed = ext(r, "e2ee").get("device_lists", {}).get("changed", [])
    check(bob in changed, f"Bob, who came to share an encrypted room, is in changed ({changed})")
    after = r["pos"]
    st, kc = hs.req("GET", f"/_matrix/client/v3/keys/changes?from={q(before)}&to={q(after)}", None, a)
    check(st == 200 and bob in kc.get("changed", []), f"/keys/changes between the two pos says the same ({st}, {kc.get('changed')})")
    r = ok(e2, after)
    check("device_lists" not in ext(r, "e2ee"), f"with nothing changed, no device_lists ({sorted(ext(r, 'e2ee'))})")

    # ------------------------------------------------------------------ the rooms
    one = create(a, {"name": "One", "preset": "public_chat"})
    two = create(a, {"name": "Two", "preset": "public_chat"})
    reads = create(a, {"name": "Reads", "preset": "public_chat"})
    for room in (one, two, reads):
        act(b, room, "join")
        act(c, room, "join")
    older = send(b, reads, "older")
    old = send(b, reads, "old")
    newest = send(b, reads, "newest")
    receipt(a, reads, older)
    receipt(b, reads, old)
    tag(one, "u.first")
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/account_data/org.example.setting", {"on": True}, a)
    assert st == 200, r

    # ------------------------------------------------------------------ account data
    print("-- account data")
    lists_all = {"all": {"timeline_limit": 1, "required_state": NAME}}
    later = {"conn_id": "ad", "lists": lists_all}
    r = ok(later)
    check("account_data" not in r.get("extensions", {}), "not asked for: no section")
    later["extensions"] = {"account_data": {"enabled": True}}
    r = ok(later, r["pos"])
    kinds = [e["type"] for e in ext(r, "account_data").get("global", [])]
    check("org.example.setting" in kinds and kinds.count("m.push_rules") == 1, f"enabled on a later request: all global account data, m.push_rules once ({sorted(kinds)})")
    check(tags_of(r, one) == ["u.first"], f"and every room in scope gets its account data, entering scope for the first time ({tags_of(r, one)})")
    unstable_later = {"conn_id": "ad", "lists": {"all": {"ranges": [[0, 50]], "timeline_limit": 1, "required_state": [["m.room.name", ""]]}}}
    st, u = hs.req("POST", UNSTABLE, unstable_later, a)
    assert st == 200, u
    unstable_later["extensions"] = {"account_data": {"enabled": True}}
    st, u = hs.req("POST", f"{UNSTABLE}?timeout=0&pos={q(u['pos'])}", unstable_later, a)
    kinds = [e["type"] for e in u.get("extensions", {}).get("account_data", {}).get("global", [])]
    check(st == 200 and "org.example.setting" not in kinds, f"(the unstable endpoint keeps its rule: only what changed after pos: {kinds})")

    # ------------------------------------------------------------------ receipts and typing
    print("-- receipts")
    sub = lambda limit: {reads: {"timeline_limit": limit, "required_state": NAME}}
    rc = {"conn_id": "rc", "room_subscriptions": sub(1), "extensions": {"receipts": {"enabled": True}, "typing": {"enabled": True}}}
    r = ok(rc)
    content = ext(r, "receipts").get("rooms", {}).get(reads, {})
    check(content and "type" not in content and "content" not in content, f"the bare m.receipt content ({sorted(content)[:3]})")
    got = readers(r, reads)
    check((older, "m.read", alice) in got, f"a room entering scope: Alice's own receipt, on an event outside the timeline ({sorted(got)})")
    check((old, "m.read", bob) not in got, "and not Bob's on an event outside the timeline")
    receipt(b, reads, newest, "m.read.private")
    receipt(c, reads, newest)
    r = ok(rc, r["pos"])
    got = readers(r, reads)
    check((newest, "m.read", carol) in got and not any(user == bob for _, _, user in got), f"another user's private receipt never shows ({sorted(got)})")
    rc["room_subscriptions"] = sub(3)
    r = ok(rc, r["pos"])
    room = r.get("rooms", {}).get(reads, {})
    got = readers(r, reads)
    check(room.get("expanded_timeline") is True and (old, "m.read", bob) in got, f"a raised timeline_limit brings the receipts on the older events ({sorted(got)})")
    deferred = {"conn_id": "rc2", "room_subscriptions": sub(1)}
    r2 = ok(deferred)
    deferred["extensions"] = {"receipts": {"enabled": True}}
    r2 = ok(deferred, r2["pos"])
    check((older, "m.read", alice) in readers(r2, reads), f"enabled on a later request: the room enters scope, with Alice's own receipts ({sorted(readers(r2, reads))})")

    print("-- typing")
    hs.typing(b, reads, bob, True)
    r = ok(rc, r["pos"])
    check(ext(r, "typing").get("rooms", {}).get(reads) == {"user_ids": [bob]}, f"bare user_ids ({ext(r, 'typing').get('rooms', {}).get(reads)})")
    hs.typing(b, reads, bob, False)
    r = ok(rc, r["pos"])
    check(ext(r, "typing").get("rooms", {}).get(reads) == {"user_ids": []}, f"and when nobody types any more ({ext(r, 'typing').get('rooms', {}).get(reads)})")

    print("-- rooms Alice was removed from")
    kicked = create(b, {"name": "Kicked", "preset": "public_chat"})
    banned = create(b, {"name": "Banned", "preset": "public_chat"})
    for room in (kicked, banned):
        act(a, room, "join")
    said = {room: send(b, room, "before") for room in (kicked, banned)}
    out = {"conn_id": "out", "lists": lists_all, "extensions": {"receipts": {"enabled": True}, "typing": {"enabled": True}}}
    r = ok(out)
    act(b, kicked, "kick", {"user_id": alice})
    act(b, banned, "ban", {"user_id": alice})
    r = ok(out, r["pos"])
    check(r.get("rooms", {}).get(kicked, {}).get("membership") == "leave" and r.get("rooms", {}).get(banned, {}).get("membership") == "ban", "the rooms stay in the list as left and banned")
    for room in (kicked, banned):
        receipt(b, room, said[room])
        hs.typing(b, room, bob, True)
    r = ok(out, r["pos"])
    check(not ({kicked, banned} & set(ext(r, "receipts").get("rooms", {}))), "no receipts for a room Alice was kicked or banned from")
    check(not ({kicked, banned} & set(ext(r, "typing").get("rooms", {}))), "and no typing")

    # ------------------------------------------------------------------ scope
    print("-- a room that drops out of range and comes back")
    top = {"conn_id": "top", "lists": {"top": {"range": [0, 0], "timeline_limit": 1, "required_state": NAME}},
           "extensions": {"account_data": {"enabled": True}, "receipts": {"enabled": True}, "typing": {"enabled": True}}}
    on_top = send(c, one, "One is on top")
    r = ok(top)
    check(list(r.get("rooms", {})) == [one], "One is on top")
    send(c, two, "now Two is")
    r = ok(top, r["pos"])
    check(list(r.get("rooms", {})) == [two], "then Two")
    # Tags, receipts and typing do not move a room up the list.
    tag(one, "u.later")
    receipt(b, one, on_top)
    hs.typing(b, one, bob, True)
    send(c, two, "Two stays on top")
    r = ok(top, r["pos"])
    check(tags_of(r, one) is None and not readers(r, one) and one not in ext(r, "typing").get("rooms", {}),
          f"while One is out of range nothing of it comes ({tags_of(r, one)}, {sorted(readers(r, one))})")
    send(c, one, "One comes back")
    r = ok(top, r["pos"])
    check(list(r.get("rooms", {})) == [one], "One comes back on top")
    check(tags_of(r, one) == ["u.first", "u.later"], f"with the tag set while it was out ({tags_of(r, one)})")
    check((on_top, "m.read", bob) in readers(r, one), f"the receipt made while it was out ({sorted(readers(r, one))})")
    check(ext(r, "typing").get("rooms", {}).get(one) == {"user_ids": [bob]}, f"and who is typing ({ext(r, 'typing').get('rooms', {}).get(one)})")
    hs.typing(b, one, bob, False)

    print("-- a long backlog of receipts")
    busy = create(a, {"name": "Busy", "preset": "public_chat"})
    calm = create(a, {"name": "Calm", "preset": "public_chat"})
    for room in (busy, calm):
        act(b, room, "join")
        act(c, room, "join")
    seen = send(b, busy, "seen by Alice")
    receipt(a, busy, seen)
    far = send(b, busy, "read in many threads")
    backlog = {"conn_id": "backlog", "lists": {"top": {"range": [0, 0], "timeline_limit": 1, "required_state": NAME}}, "extensions": {"receipts": {"enabled": True}}}
    send(c, busy, "Busy is on top")
    r = ok(backlog)
    check(list(r.get("rooms", {})) == [busy] and (seen, "m.read", alice) in readers(r, busy), "Busy enters scope with Alice's own receipt")
    send(c, calm, "now Calm is")
    r = ok(backlog, r["pos"])
    check(list(r.get("rooms", {})) == [calm], "then Calm")
    # More than 100 receipts change while Busy is out: Bob reads one event in 101 threads.
    for i in range(101):
        st, x = hs.req("POST", f"/_matrix/client/v3/rooms/{q(busy)}/receipt/m.read/{q(far)}", {"thread_id": f"$thread{i}:sender.test"}, b)
        assert st == 200, x
    send(c, busy, "Busy comes back")
    st, e = sync(backlog, r["pos"])
    check(st == 400 and e.get("errcode") == "M_UNKNOWN_POS", f"with a backlog of more than 100 receipts the connection expires: M_UNKNOWN_POS ({st} {e.get('errcode')})")
    st, e = sync(backlog, r["pos"])
    check(st == 400 and e.get("errcode") == "M_UNKNOWN_POS", f"for good: the same pos again ({st} {e.get('errcode')})")
    r = ok(backlog)
    got = readers(r, busy)
    check(list(r.get("rooms", {})) == [busy] and (seen, "m.read", alice) in got and not any(ev == far for ev, _, _ in got),
          f"the connection started over: Busy enters scope with its initial receipts, Alice's own and none of the backlog ({sorted(got)})")

    print("-- rooms names subscriptions")
    named = {"conn_id": "named", "lists": lists_all, "extensions": {"receipts": {"enabled": True, "lists": [], "rooms": [two]}}}
    r = ok(named)
    check(two not in ext(r, "receipts").get("rooms", {}), "a room in a list but not subscribed is not covered by rooms")
    read_me = send(b, two, "read me")
    receipt(c, two, read_me)
    receipt(a, two, read_me)
    r = ok(named, r["pos"])
    check(two not in ext(r, "receipts").get("rooms", {}), "not even with news")
    named["room_subscriptions"] = {two: {"timeline_limit": 1, "required_state": NAME}}
    r = ok(named, r["pos"])
    check((read_me, "m.read", alice) in readers(r, two), f"once it is subscribed, it is, entering scope with Alice's own receipt ({sorted(readers(r, two))})")
    st, r = sync({"conn_id": "unknown", "extensions": {"org.example.unknown": {"enabled": True}}})
    check(st == 200, f"an unknown extension is ignored ({st})")


if __name__ == "__main__":
    run_lab(["mock"], run)
