#!/usr/bin/env python3
"""The public room directory, ours and other servers': `server=`, searching and federation.

A client that names another server in `/publicRooms?server=` (Element's Explore does) must get
that server's directory, fetched over federation, and not ours. Asking another server on
somebody's behalf takes a signed-in user, so that the homeserver is no open relay; our own
directory stays open to anyone. A search (`filter.generic_search_term`) looks at every public
room before it cuts a page. Over federation the GET lists the directory and the POST searches
it.

The lab: Alice publishes three rooms and keeps a fourth private; a mock server has a directory
of its own.

Needs what scripts/fedlab.py needs. Takes about ten seconds.

Usage: scripts/publicroomstest.py [--keep]
"""
import json, re, time, urllib.error, urllib.parse, urllib.request

from fedlab import SERVER_NAME, check, query, run_lab


def run(hs, peers):
    mock = peers["mock"]
    q = urllib.parse.quote
    alice, token = hs.register("alice")

    def create(body):
        st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"room_version": "11", **body}, token)
        assert st == 200, r
        return r["room_id"]

    garden = create({"preset": "public_chat", "visibility": "public", "name": "Garden club", "room_alias_name": "garden"})
    kitchen = create({"preset": "public_chat", "visibility": "public", "name": "Kitchen", "topic": "Recipes and garden herbs"})
    workshop = create({"preset": "public_chat", "visibility": "public", "name": "Workshop"})
    secret = create({"preset": "private_chat", "name": "Garden secrets"})
    ours = {garden, kitchen, workshop}

    def ids(body):
        return {r["room_id"] for r in body.get("chunk", [])}

    # Our own directory, with and without our own name, and without a token.
    st, r = hs.req("GET", "/_matrix/client/v3/publicRooms", None, token)
    check(st == 200 and ids(r) == ours and r.get("total_room_count_estimate") == 3,
          f"our directory lists the three published rooms, not the private one ({st}, {len(ids(r))})")
    st, r = hs.req("GET", f"/_matrix/client/v3/publicRooms?server={q(SERVER_NAME)}", None, token)
    check(st == 200 and ids(r) == ours, f"naming our own server gives the same list ({st})")
    st, r = hs.req("GET", "/_matrix/client/v3/publicRooms")
    check(st == 200 and ids(r) == ours, f"our own directory needs no token ({st})")

    # A search looks at every room before it cuts the page.
    st, p1 = hs.req("POST", "/_matrix/client/v3/publicRooms", {"limit": 1, "filter": {"generic_search_term": "GARDEN"}}, token)
    st2, p2 = hs.req("POST", "/_matrix/client/v3/publicRooms",
                     {"limit": 1, "since": p1.get("next_batch"), "filter": {"generic_search_term": "GARDEN"}}, token)
    found = ids(p1) | ids(p2)
    check(st == 200 and st2 == 200 and len(ids(p1)) == 1 and found == {garden, kitchen} and "next_batch" not in p2,
          f"a search for 'GARDEN' pages through both matches (name and topic, any case), one per page ({st}, {st2}, {len(found)})")
    check(p1.get("total_room_count_estimate") == 2, f"and counts the matches ({p1.get('total_room_count_estimate')})")

    # Terms the database cannot narrow down for us (non-ASCII, quotes) and ones it must take
    # literally (% and _) find exactly the rooms whose text contains them.
    cafe = create({"preset": "public_chat", "visibility": "public", "name": "Café Crème"})
    quoted = create({"preset": "public_chat", "visibility": "public", "name": 'The "Kiln" corner'})
    percent = create({"preset": "public_chat", "visibility": "public", "name": "Glaze 100% matte", "topic": "pot_luck"})
    for term, want in (("CRÈME", {cafe}), ('"kiln"', {quoted}), ("100%", {percent}), ("t_l", {percent}),
                       ("%", {percent}), ("m.room", set()), ("garden", {garden, kitchen})):
        st, r = hs.req("POST", "/_matrix/client/v3/publicRooms", {"limit": 50, "filter": {"generic_search_term": term}}, token)
        check(st == 200 and ids(r) == want, f"a search for {term!r} finds exactly its {len(want)} room(s) ({st}, {len(ids(r))})")
    ours |= {cafe, quoted, percent}

    # Another server asks us over federation: GET lists, POST searches; unsigned is refused.
    st, r = mock.fed(hs.fed, "GET", "/_matrix/federation/v1/publicRooms?limit=10")
    check(st == 200 and ids(r) == ours, f"a signed federation GET lists our directory ({st}, {len(ids(r))})")
    st, r = mock.fed(hs.fed, "POST", "/_matrix/federation/v1/publicRooms", {"limit": 10, "filter": {"generic_search_term": "workshop"}})
    check(st == 200 and ids(r) == {workshop}, f"a signed federation POST applies its search ({st}, {len(ids(r))})")
    try:
        urllib.request.urlopen(hs.fed + "/_matrix/federation/v1/publicRooms", timeout=10)
        st = 200
    except urllib.error.HTTPError as e:
        st = e.code
    check(st == 401, f"an unsigned federation request is refused ({st})")

    # The mock server's own directory, for our clients.
    lounge = {"room_id": f"!lounge:{mock.name}", "name": "Mock lounge", "num_joined_members": 4,
              "world_readable": False, "guest_can_join": False, "join_rule": "public"}
    asked = []

    def directory_get(handler, m):
        asked.append(("GET", handler.path, handler.headers.get("Authorization", ""), None))
        handler.answer(200, {"chunk": [lounge], "total_room_count_estimate": 1})

    def directory_post(handler, m, body):
        asked.append(("POST", handler.path, handler.headers.get("Authorization", ""), body))
        term = body.get("filter", {}).get("generic_search_term", "").lower()
        hits = [lounge] if term in lounge["name"].lower() else []
        handler.answer(200, {"chunk": hits, "total_room_count_estimate": len(hits)})

    mock.routes.append((re.compile(r"/_matrix/federation/v1/publicRooms"), directory_get))
    mock.put_routes.append((re.compile(r"/_matrix/federation/v1/publicRooms"), directory_post))

    st, r = hs.req("GET", f"/_matrix/client/v3/publicRooms?server={q(mock.name)}&limit=5", None, token)
    check(st == 200 and ids(r) == {lounge["room_id"]} and r.get("total_room_count_estimate") == 1,
          f"server= another server gives that server's directory, not ours ({st}, {sorted(ids(r))})")
    check(len(asked) == 1 and asked[0][0] == "GET" and "limit=5" in asked[0][1]
          and asked[0][2].startswith("X-Matrix") and re.search(rf'origin="?{re.escape(SERVER_NAME)}"?(,|$)', asked[0][2]),
          f"fetched with a signed federation GET that passes the limit on ({[a[:2] for a in asked]})")

    st, r = hs.req("POST", f"/_matrix/client/v3/publicRooms?server={q(mock.name)}",
                   {"limit": 5, "filter": {"generic_search_term": "Lounge"}}, token)
    check(st == 200 and ids(r) == {lounge["room_id"]}, f"a search on another server's directory finds its room ({st})")
    check(len(asked) == 2 and asked[1][0] == "POST" and asked[1][3].get("filter", {}).get("generic_search_term") == "Lounge",
          f"as a signed federation POST that carries the search ({asked[-1][0] if asked else None}, {asked[-1][3] if asked else None})")
    st, r = hs.req("POST", f"/_matrix/client/v3/publicRooms?server={q(mock.name)}",
                   {"filter": {"generic_search_term": "nothing like it"}}, token)
    check(st == 200 and ids(r) == set(), f"and nothing when it has no match ({st}, {len(ids(r))})")

    before = len(asked)
    st, r = hs.req("GET", f"/_matrix/client/v3/publicRooms?server={q(mock.name)}")
    check(st == 401 and r.get("errcode") == "M_MISSING_TOKEN" and len(asked) == before,
          f"without a token another server's directory is refused, and that server is not asked ({st}, {r.get('errcode')})")


    # Thousands of public rooms (bare rows: no state, so they match nothing). A search reads
    # their text only; building their directory entries would cost per room, or fail on rooms
    # without state.
    query("INSERT INTO rooms (room_id, room_version, creator, is_public, has_auth_chain_index, created_ts)"
          " SELECT '!bulk' || g || ':' || 'sender.test', '11', NULL, true, false,"
          " (extract(epoch FROM now()) * 1000)::bigint + g FROM generate_series(1, 3000) g")
    st, r = hs.req("GET", "/_matrix/client/v3/publicRooms?limit=5", None, token)
    check(st == 200 and r.get("total_room_count_estimate") == 3006, f"with 3000 more public rooms the directory counts 3006 ({st}, {r.get('total_room_count_estimate')})")
    took = []
    for _ in range(3):
        t0 = time.monotonic()
        st, r = hs.req("POST", "/_matrix/client/v3/publicRooms", {"limit": 20, "filter": {"generic_search_term": "garden"}}, token)
        took.append(time.monotonic() - t0)
        check(st == 200 and ids(r) == {garden, kitchen} and r.get("total_room_count_estimate") == 2,
              f"a search among 3006 public rooms still finds the two gardens ({st}, {len(ids(r))})")
    check(max(took) < 1.0, f"each within a second ({', '.join(f'{x:.2f}s' for x in took)})")
    t0 = time.monotonic()
    st, r = mock.fed(hs.fed, "POST", "/_matrix/federation/v1/publicRooms", {"limit": 20, "filter": {"generic_search_term": "workshop"}})
    took = time.monotonic() - t0
    check(st == 200 and ids(r) == {workshop} and took < 1.0, f"and so does a search from another server ({st}, {len(ids(r))}, {took:.2f}s)")

    # The search keeps up with changes between searches: a new name, a redacted topic, a room
    # leaving the directory, a new public room.
    def search(term):
        st, r = hs.req("POST", "/_matrix/client/v3/publicRooms", {"limit": 50, "filter": {"generic_search_term": term}}, token)
        assert st == 200, r
        return ids(r)

    check(search("garden") == {garden, kitchen}, "before the changes: the two gardens")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(workshop)}/state/m.room.name", {"name": "Garden tools"}, token)
    assert st == 200, r
    check(search("garden") == {garden, kitchen, workshop}, f"a renamed room is found under its new name ({len(search('garden'))})")
    st, state = hs.req("GET", f"/_matrix/client/v3/rooms/{q(kitchen)}/state", None, token)
    topic_id = next(e["event_id"] for e in state if e["type"] == "m.room.topic")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(kitchen)}/redact/{q(topic_id)}/redact-topic", {}, token)
    assert st == 200, r
    check(search("garden") == {garden, workshop}, "a room whose matching topic was redacted is no longer found")
    st, r = hs.req("PUT", f"/_matrix/client/v3/directory/list/room/{q(garden)}", {"visibility": "private"}, token)
    assert st == 200, r
    check(search("garden") == {workshop}, "a room taken out of the directory is no longer found")
    shed = create({"preset": "public_chat", "visibility": "public", "name": "Garden shed"})
    check(search("garden") == {workshop, shed}, "a new public room is found at once")


if __name__ == "__main__":
    run_lab(["mock"], run)
