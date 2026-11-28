#!/usr/bin/env python3
"""`/v4/sync`: Simplified Sliding Sync as accepted (MSC4186), the rooms part.

Alice syncs over `POST /_matrix/client/v4/sync`; Bob and a few others act. What is checked:

  1. `pos` and `timeout` live in the body. A retried `pos` is answered again; a made-up one,
     another device's, and one of the unstable endpoint are `M_UNKNOWN_POS` (and a v4 `pos`
     on the unstable endpoint too). A `pos` serves as `from` in `/messages`. A waiting
     request wakes when there is news.
  2. What MSC4186 refuses is `M_INVALID_PARAM`: 101 lists or subscriptions, a list key outside
     the opaque-identifier grammar, an unknown `set_presence`, a range that ends before it
     starts.
  3. A list's single `range` windows the rooms; without one the list holds every room, and
     `count` is the number of rooms that pass the filters either way.
  4. Every room says its `membership` and the `lists` it is in (none when it is there only for
     a subscription). There are no notification counts and no `txn_id`.
  5. An invite and a knock come as `stripped_state` with the common fields only.
  6. `required_state`: `include` and `exclude`; `include: [{}]` with `lazy_members` sends all
     state but the memberships, which stay lazy (the timeline's people and Alice herself);
     lazily loaded members are not filtered by `exclude`; an include that names
     `m.room.member` asks for every member; one that names a member always gets them.
  7. Lazy loading's rule 3: every membership change since the last answer comes when the
     answer has no gap, and not when it has one.
  8. A name or an avatar that is removed comes as `null`; `is_dm` comes as `false` when the
     room stops being a DM.
  9. The filters `tags` / `not_tags` (a favourite moves between lists when tagged) and
     `spaces` (a child with an empty `via` is no child; a space Alice has not joined names no
     rooms).
 10. State that is gone from a room (a state reset, staged here in the table of current state)
     comes as a stub, `{type, state_key}` without `content`, once; the unstable endpoint
     says nothing.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/v4synctest.py [--keep]
"""
import threading, time, urllib.parse

from fedlab import check, query, run_lab

V4 = "/_matrix/client/v4/sync"
UNSTABLE = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
NAME_AND_CREATE = {"include": [{"type": "m.room.name", "state_key": ""}, {"type": "m.room.create", "state_key": ""}]}


def run(hs, peers):
    q = urllib.parse.quote
    (alice, a), (bob, b), (carol, c), (dave, d), (erin, e) = (hs.register(n) for n in ("alice", "bob", "carol", "dave", "erin"))

    def sync(token, body, pos=None, timeout=0):
        body = dict(body)
        if pos:
            body["pos"] = pos
            body["timeout"] = timeout
        return hs.req("POST", V4, body, token)

    def ok(token, body, pos=None, timeout=0):
        st, r = sync(token, body, pos, timeout)
        assert st == 200, (st, r)
        return r

    def create(token, body):
        st, r = hs.req("POST", "/_matrix/client/v3/createRoom", body, token)
        assert st == 200, r
        return r["room_id"]

    def act(token, room_id, action, body=None):
        st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/{action}", body or {}, token)
        assert st == 200, (action, r)

    def put_state(token, room_id, kind, content, key=""):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/state/{kind}/{q(key)}", content, token)
        assert st == 200, (kind, r)
        return r["event_id"]

    def account_data(kind, content, room_id=None):
        path = f"/_matrix/client/v3/user/{q(alice)}" + (f"/rooms/{q(room_id)}" if room_id else "") + f"/account_data/{kind}"
        st, r = hs.req("PUT", path, content, a)
        assert st == 200, r

    def members(room):
        return sorted(ev["state_key"] for ev in room.get("required_state", []) if ev["type"] == "m.room.member")

    def kinds(room):
        return sorted({ev["type"] for ev in room.get("required_state", [])})

    # ------------------------------------------------------------------ the rooms
    # Bob's invitation and Bob's room Alice knocks on come first, the rooms Alice is in after.
    invited = create(b, {"name": "Party", "invite": [alice]})
    knocked = create(b, {"name": "Knock knock", "initial_state": [{"type": "m.room.join_rules", "state_key": "", "content": {"join_rule": "knock"}}]})
    st, r = hs.req("POST", f"/_matrix/client/v3/knock/{q(knocked)}", {"reason": "let me in"}, a)
    assert st == 200, r
    space = create(a, {"name": "Studio", "creation_content": {"type": "m.space"}})
    child, stray = create(a, {"name": "Child"}), create(a, {"name": "Stray"})
    put_state(a, space, "m.space.child", {"via": ["sender.test"]}, child)
    put_state(a, space, "m.space.child", {"via": []}, stray)
    outside = create(b, {"name": "Bob's space", "preset": "public_chat", "creation_content": {"type": "m.space"}})
    put_state(b, outside, "m.space.child", {"via": ["sender.test"]}, child)
    dm = create(a, {"invite": [bob], "is_direct": True})
    act(b, dm, "join")
    account_data("m.direct", {bob: [dm]})
    crowd = create(a, {"name": "Crowd", "preset": "public_chat"})
    for token in (b, c, d):
        act(token, crowd, "join")
    hs.message(b, crowd, "Bob speaks")
    named = create(a, {"name": "Beta", "preset": "public_chat"})
    put_state(a, named, "m.room.avatar", {"url": "mxc://sender.test/beta"})
    hs.message(a, named, "the newest room")
    joined = [space, child, stray, dm, crowd, named]

    print("-- lists: range, count, membership, lists")
    # "all" leaves out the spaces, so the space is there only for its subscription.
    body = {"conn_id": "main",
            "lists": {"top": {"range": [0, 1], "timeline_limit": 1, "required_state": NAME_AND_CREATE},
                      "all": {"timeline_limit": 1, "required_state": NAME_AND_CREATE, "filters": {"not_room_types": ["m.space"]}},
                      "dms": {"timeline_limit": 1, "required_state": NAME_AND_CREATE, "filters": {"is_dm": True}}},
            "room_subscriptions": {space: {"timeline_limit": 1, "required_state": NAME_AND_CREATE},
                                   outside: {"timeline_limit": 1, "required_state": NAME_AND_CREATE}}}
    st, r = sync(a, body)
    check(st == 200, f"a request with no pos is answered ({st})")
    rooms = r.get("rooms", {})
    counts = {k: v.get("count") for k, v in r.get("lists", {}).items()}
    total = len(joined) + 2
    check(counts == {"top": total, "all": total - 1, "dms": 1}, f"each list counts the rooms that pass its filters, whatever its range ({counts})")
    in_top = sorted(rid for rid, room in rooms.items() if "top" in room.get("lists", []))
    check(in_top == sorted([named, crowd]), f"range [0, 1] holds the two most recently active rooms ({len(in_top)} rooms)")
    in_all = {rid for rid, room in rooms.items() if "all" in room.get("lists", [])}
    check(in_all == (set(joined) - {space}) | {invited, knocked}, f"a list without a range holds every room that passes its filters ({len(in_all)} of {total - 1})")
    check(rooms.get(dm, {}).get("lists") == ["all", "dms"], f"the DM is in two lists ({rooms.get(dm, {}).get('lists')})")
    check(rooms.get(named, {}).get("lists") == ["all", "top"], f"the newest room is in two others ({rooms.get(named, {}).get('lists')})")
    check(space in rooms and "lists" not in rooms[space], f"a room there only for its subscription has no lists ({sorted(rooms.get(space, {}))})")
    check(outside not in rooms, "a subscription to a room Alice is not in is ignored")
    memberships = {rid: room.get("membership") for rid, room in rooms.items()}
    check(all(memberships[rid] == "join" for rid in joined), "the rooms Alice is in say membership join")
    check(memberships.get(invited) == "invite" and memberships.get(knocked) == "knock", f"the invite and the knock say theirs ({memberships.get(invited)}, {memberships.get(knocked)})")
    counted = [rid for rid, room in rooms.items() if {"notification_count", "highlight_count", "unread_notifications"} & set(room)]
    check(counted == [] and "txn_id" not in r, f"no notification counts and no txn_id ({len(counted)} rooms with counts)")
    room = rooms.get(named, {})
    check(room.get("initial") is True and room.get("name") == "Beta" and room.get("avatar") == "mxc://sender.test/beta", f"a room seen for the first time is initial, with its name and avatar ({room.get('name')}, {room.get('avatar')})")
    check(rooms.get(dm, {}).get("is_dm") is True and "is_dm" not in room, "is_dm is true for the DM and absent for the others")

    print("-- invites and knocks: stripped state")
    for label, rid, name in (("an invite", invited, "Party"), ("a knock", knocked, "Knock knock")):
        room = rooms.get(rid, {})
        names = [ev["content"].get("name") for ev in room.get("stripped_state", []) if ev["type"] == "m.room.name"]
        check(names == [name], f"{label} has the room's name in its stripped state ({names})")
        check(set(room) == {"bump_stamp", "membership", "lists", "stripped_state"}, f"{label} has the common fields and nothing else ({sorted(room)})")

    print("-- pos")
    pos = r["pos"]
    hs.message(b, dm, "retry me")
    first = ok(a, body, pos)
    again = ok(a, body, pos)
    said = [[ev["content"].get("body") for ev in x.get("rooms", {}).get(dm, {}).get("timeline", [])] for x in (first, again)]
    check(said == [["retry me"], ["retry me"]], f"a retried pos is answered again, with the same news ({said})")
    pos = again["pos"]
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(named)}/messages?dir=b&limit=5&from={q(pos)}", None, a)
    bodies = [ev["content"].get("body") for ev in r.get("chunk", []) if ev["type"] == "m.room.message"]
    check(st == 200 and bodies == ["the newest room"], f"a pos serves as from in /messages ({st}, {bodies})")
    for label, bad in (("a made-up pos", "s1_2_3~abc~1"), ("a bare number", "1"), ("a pos with another nonce", pos.rsplit("~", 2)[0] + "~zzzzzzzz~" + pos.rsplit("~", 1)[1])):
        st, r = sync(a, body, bad)
        check(st == 400 and r.get("errcode") == "M_UNKNOWN_POS", f"{label}: M_UNKNOWN_POS ({st} {r.get('errcode')})")
    st, login = hs.req("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": "alice"}, "password": "pwpwpwpw"})
    assert st == 200, login
    st, r = sync(login["access_token"], body, pos)
    check(st == 400 and r.get("errcode") == "M_UNKNOWN_POS", f"another device's pos: M_UNKNOWN_POS ({st} {r.get('errcode')})")
    unstable_body = {"conn_id": "main", "lists": {"all": {"ranges": [[0, 9]], "timeline_limit": 1, "required_state": [["m.room.name", ""]]}}}
    st, r = hs.req("POST", UNSTABLE, unstable_body, a)
    assert st == 200, r
    st, r2 = sync(a, body, r["pos"])
    check(st == 400 and r2.get("errcode") == "M_UNKNOWN_POS", f"a pos of the unstable endpoint: M_UNKNOWN_POS on v4 ({st} {r2.get('errcode')})")
    st, r2 = hs.req("POST", f"{UNSTABLE}?pos={q(pos)}&timeout=0", unstable_body, a)
    check(st == 400 and r2.get("errcode") == "M_UNKNOWN_POS", f"and a v4 pos on the unstable endpoint ({st} {r2.get('errcode')})")
    # The v4 connection is still there, and waits until there is news.
    st, r = sync(a, body, pos)
    check(st == 200, f"the connection goes on after the refusals ({st})")
    pos = r["pos"]
    threading.Timer(1.0, lambda: hs.message(b, crowd, "news")).start()
    started = time.monotonic()
    r = ok(a, body, pos, timeout=20000)
    took = time.monotonic() - started
    bodies = [ev["content"].get("body") for ev in r.get("rooms", {}).get(crowd, {}).get("timeline", [])]
    check(0.8 < took < 10 and bodies == ["news"], f"a waiting request wakes with the news ({took:.1f}s, {bodies})")
    pos = r["pos"]
    started = time.monotonic()
    r = ok(a, body, pos, timeout=1500)
    took = time.monotonic() - started
    check(took >= 1.2 and r.get("rooms", {}) == {}, f"and without news it waits for its timeout ({took:.1f}s)")
    pos = r["pos"]

    print("-- validation")
    a_list = {"timeline_limit": 1, "required_state": {}}
    for label, bad in (
        ("101 lists", {"lists": {f"l{i}": a_list for i in range(101)}}),
        ("101 subscriptions", {"room_subscriptions": {f"!r{i}:sender.test": a_list for i in range(101)}}),
        ("a list key with a space", {"lists": {"my list": a_list}}),
        ("a list key of 256 characters", {"lists": {"x" * 256: a_list}}),
        ("an unknown set_presence", {"set_presence": "busy"}),
        ("a range that ends before it starts", {"lists": {"l": {**a_list, "range": [5, 2]}}}),
    ):
        st, r = sync(a, bad)
        check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"{label}: M_INVALID_PARAM ({st} {r.get('errcode')})")
    st, r = sync(a, {"lists": {f"l{i}": a_list for i in range(100)}, "set_presence": "unavailable"})
    check(st == 200, f"100 lists and set_presence unavailable are fine ({st})")

    print("-- required_state")
    crowd_sub = lambda rs, limit=1: {"conn_id": f"rs{time.monotonic_ns()}", "room_subscriptions": {crowd: {"timeline_limit": limit, "required_state": rs}}}
    room = ok(a, crowd_sub({"include": [{}], "exclude": [{"type": "m.room.create"}]}))["rooms"][crowd]
    check("m.room.create" not in kinds(room) and {"m.room.name", "m.room.power_levels", "m.room.join_rules"} <= set(kinds(room)), f"include everything, exclude the create event ({kinds(room)})")
    check(members(room) == sorted([alice, bob, carol, dave]), f"without lazy loading, include [{{}}] is every member ({len(members(room))})")
    room = ok(a, crowd_sub({"include": [{}], "lazy_members": True}))["rooms"][crowd]
    check(members(room) == sorted([alice, bob]), f"include [{{}}] with lazy_members stays lazy: Bob, who sent the timeline, and Alice ({members(room)})")
    check({"m.room.create", "m.room.name", "m.room.power_levels"} <= set(kinds(room)), f"and has all the other state ({kinds(room)})")
    room = ok(a, crowd_sub({"include": [{}], "exclude": [{"type": "m.room.member"}], "lazy_members": True}))["rooms"][crowd]
    check(members(room) == sorted([alice, bob]), f"lazily loaded members are not filtered by exclude ({members(room)})")
    room = ok(a, crowd_sub({"include": [{}, {"type": "m.room.member", "state_key": carol}], "lazy_members": True}))["rooms"][crowd]
    check(members(room) == sorted([alice, bob, carol]), f"a member named in include comes beside the lazy ones ({members(room)})")
    room = ok(a, crowd_sub({"include": [{"type": "m.room.member"}], "lazy_members": True}))["rooms"][crowd]
    check(members(room) == sorted([alice, bob, carol, dave]), f"an include that names m.room.member is every member, also with lazy_members ({members(room)})")
    room = ok(a, crowd_sub({"include": [{"type": "m.room.name", "state_key": "*"}, {"type": "m.room.member", "state_key": "$ME"}]}))["rooms"][crowd]
    check(kinds(room) == [], f"'*' and '$ME' are just strings on v4 ({kinds(room)})")

    print("-- lazy loading, rule 3")
    lazy = {"conn_id": "lazy", "room_subscriptions": {crowd: {"timeline_limit": 0, "required_state": {"include": [{"type": "m.room.name"}], "lazy_members": True}}}}
    r = ok(a, lazy)
    check(members(r["rooms"][crowd]) == [alice], f"a timeline of nothing brings Alice's own membership alone ({members(r['rooms'][crowd])})")
    st, r2 = hs.req("PUT", f"/_matrix/client/v3/profile/{q(dave)}/displayname", {"displayname": "Dave the quiet"}, d)
    assert st == 200, r2
    r = ok(a, lazy, r["pos"])
    names = {ev["state_key"]: ev["content"].get("displayname") for ev in r.get("rooms", {}).get(crowd, {}).get("required_state", []) if ev["type"] == "m.room.member"}
    check(names == {dave: "Dave the quiet"}, f"without a gap, a membership change comes although its timeline is not sent ({names})")
    unstable_lazy = {"conn_id": "lazy", "room_subscriptions": {crowd: {"timeline_limit": 0, "required_state": [["m.room.name", ""], ["m.room.member", "$LAZY"]]}}}
    st, u = hs.req("POST", UNSTABLE, unstable_lazy, a)
    assert st == 200, u
    st, r2 = hs.req("PUT", f"/_matrix/client/v3/profile/{q(carol)}/displayname", {"displayname": "Carol the quiet"}, c)
    assert st == 200, r2
    st, u = hs.req("POST", f"{UNSTABLE}?timeout=0&pos={q(u['pos'])}", unstable_lazy, a)
    check(st == 200 and members(u.get("rooms", {}).get(crowd, {})) == [], f"(the unstable endpoint keeps its rule: no change of someone it was not given: {members(u.get('rooms', {}).get(crowd, {}))})")
    r = ok(a, lazy, r["pos"])
    check(members(r.get("rooms", {}).get(crowd, {})) == [carol], f"the next v4 answer has Carol's ({members(r.get('rooms', {}).get(crowd, {}))})")
    lazy["room_subscriptions"][crowd]["timeline_limit"] = 1
    r = ok(a, lazy, r["pos"])
    act(e, crowd, "join")
    hs.message(b, crowd, "one")
    hs.message(b, crowd, "two")
    r = ok(a, lazy, r["pos"])
    room = r.get("rooms", {}).get(crowd, {})
    check(room.get("limited") is True and erin not in members(room), f"with a gap, Erin's join outside the timeline does not come ({members(room)}, limited {room.get('limited')})")

    print("-- name, avatar and is_dm")
    rooms_body = {"conn_id": "fields", "lists": {"all": {"timeline_limit": 1, "required_state": NAME_AND_CREATE}}}
    r = ok(a, rooms_body)
    put_state(a, named, "m.room.name", {})
    put_state(a, named, "m.room.avatar", {})
    r = ok(a, rooms_body, r["pos"])
    room = r.get("rooms", {}).get(named, {})
    check("name" in room and room["name"] is None, f"a removed name comes as null ({room.get('name', 'absent')!r})")
    check("avatar" in room and room["avatar"] is None, f"a removed avatar comes as null ({room.get('avatar', 'absent')!r})")
    hs.message(a, named, "after the name")
    r = ok(a, rooms_body, r["pos"])
    room = r.get("rooms", {}).get(named, {})
    check("name" not in room and "avatar" not in room, f"and after that they are left out ({sorted(room)})")
    account_data("m.direct", {})
    r = ok(a, rooms_body, r["pos"])
    room = r.get("rooms", {}).get(dm, {})
    check(room.get("is_dm") is False and room.get("timeline", []) == [], f"a room that stops being a DM comes with is_dm false, and nothing else new ({room.get('is_dm', 'absent')})")
    r = ok(a, rooms_body, r["pos"])
    check(dm not in r.get("rooms", {}), "once")
    account_data("m.direct", {bob: [dm]})
    r = ok(a, rooms_body, r["pos"])
    check(r.get("rooms", {}).get(dm, {}).get("is_dm") is True, "and with is_dm true when it is one again")

    print("-- tags and spaces")
    tagged = {"conn_id": "tags",
              "lists": {"fav": {"timeline_limit": 1, "required_state": {}, "filters": {"tags": ["m.favourite"]}},
                        "rest": {"timeline_limit": 1, "required_state": {}, "filters": {"not_tags": ["m.favourite"]}},
                        "space": {"timeline_limit": 1, "required_state": {}, "filters": {"spaces": [space]}},
                        "unjoined": {"timeline_limit": 1, "required_state": {}, "filters": {"spaces": [outside]}}}}
    r = ok(a, tagged)
    counts = {k: v.get("count") for k, v in r.get("lists", {}).items()}
    check(counts == {"fav": 0, "rest": total, "space": 1, "unjoined": 0}, f"no favourites yet; one child of Alice's space; none of a space she is not in ({counts})")
    check(r["rooms"].get(child, {}).get("lists") == ["rest", "space"], f"the child is in the space's list ({r['rooms'].get(child, {}).get('lists')})")
    check("space" not in r["rooms"].get(stray, {}).get("lists", []), "a child with an empty via is no child")
    pos = r["pos"]
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/rooms/{q(crowd)}/tags/m.favourite", {"order": 0.5}, a)
    assert st == 200, r
    started = time.monotonic()
    r = ok(a, tagged, pos, timeout=10000)
    counts = {k: v.get("count") for k, v in r.get("lists", {}).items()}
    check(time.monotonic() - started < 5 and counts["fav"] == 1 and counts["rest"] == total - 1, f"tagging a favourite wakes the connection and moves it between the lists ({counts})")
    r = ok(a, {**tagged, "conn_id": "tags2"})
    check(r["rooms"].get(crowd, {}).get("lists") == ["fav"], f"a fresh connection has it in the favourites only ({r['rooms'].get(crowd, {}).get('lists')})")
    tagged["lists"]["work"] = {"timeline_limit": 1, "required_state": {}, "filters": {"tags": ["m.favourite", "u.work"], "not_tags": ["u.later"]}}
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/rooms/{q(named)}/tags/u.work", {}, a)
    assert st == 200, r
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/rooms/{q(crowd)}/tags/u.later", {}, a)
    assert st == 200, r
    r = ok(a, {**tagged, "conn_id": "tags3"})
    work = sorted(rid for rid, room in r["rooms"].items() if "work" in room.get("lists", []))
    check(work == [named], f"tags is any of them, and not_tags wins ({len(work)} rooms)")

    print("-- a state stub")
    reset = create(a, {"name": "Reset", "topic": "to be lost"})
    stub_body = {"conn_id": "stub", "room_subscriptions": {reset: {"timeline_limit": 1, "required_state": {"include": [{"type": "m.room.topic"}, {"type": "m.room.name"}]}}}}
    unstable_stub = {"conn_id": "stub", "room_subscriptions": {reset: {"timeline_limit": 1, "required_state": [["m.room.topic", ""], ["m.room.name", ""]]}}}
    r = ok(a, stub_body)
    check(kinds(r["rooms"][reset]) == ["m.room.name", "m.room.topic"], f"the room comes with its topic ({kinds(r['rooms'][reset])})")
    st, u = hs.req("POST", UNSTABLE, unstable_stub, a)
    assert st == 200, u
    # A state reset that takes the topic away, done by hand in the table of current state.
    gone = query(f"DELETE FROM current_state_events WHERE room_id = '{reset}' AND event_type = 'm.room.topic' RETURNING 1")
    assert gone, "the topic row was not found"
    r = ok(a, stub_body, r["pos"])
    state = r.get("rooms", {}).get(reset, {}).get("required_state", [])
    check(state == [{"type": "m.room.topic", "state_key": ""}], f"state that is gone comes as a stub without content ({state})")
    r = ok(a, stub_body, r["pos"])
    check(reset not in r.get("rooms", {}), "once")
    st, u = hs.req("POST", f"{UNSTABLE}?timeout=0&pos={q(u['pos'])}", unstable_stub, a)
    check(st == 200 and reset not in u.get("rooms", {}), "the unstable endpoint says nothing of it")


if __name__ == "__main__":
    run_lab(["mock"], run)
