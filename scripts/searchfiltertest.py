#!/usr/bin/env python3
"""/search filters as conditions of the query (tracker #81), the `net.oele.search_filters`
extension (tracker #82) and search without words (`net.oele.search_without_words`, #83).

In one room alice, bob and carol write messages with the word "eet" in them: many from alice, a
few from bob, some from carol (whom alice ignores), pictures, a video, a file, links and not quite
links, edits, a topic, and two batches some time apart. Alice searches:

- `senders`, `not_senders`, `types`, `not_types` and `contains_url` are applied in the query: the
  count is of the filtered set, every page is full until the last, and `next_batch` stops there;
- ignored users stay out of the results and of the count;
- `net.oele.after`/`before` (a period), `net.oele.msgtypes` (message kind, an edit by its
  `m.new_content`) and `net.oele.contains_link` (true and false, an edit by its new body; an
  address written from `www.` on counts, #84), advertised in /versions;
- an ill-typed extension value or an unknown `net.oele.` field is a 400 M_BAD_JSON;
- without `event_context` a result has an empty context; with it, the events around it;
- `keys` limits the search to message bodies, room names or topics;
- an empty or blank search term with a filter finds everything the filter lets through (also
  messages without the word), newest first, with the count and pages of that set; without a
  filter it is a 400 M_INVALID_PARAM.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/searchfiltertest.py [--keep]
"""
import time, urllib.parse, uuid

from fedlab import check, run_lab

q = urllib.parse.quote


def run(hs, peers):
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    room = r["room_id"]
    for token in (b, c):
        st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, token)
        assert st == 200, r

    def send(token, content, ty="m.room.message"):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/{ty}/{uuid.uuid4().hex}", content, token)
        assert st == 200, r
        return r["event_id"]

    def text(token, body, msgtype="m.text", **extra):
        return send(token, {"msgtype": msgtype, "body": body, **extra})

    # The first batch.
    alices = [text(a, f"eet smakelijk {i}") for i in range(23)]
    bobs = [text(b, f"eet je mee {i}") for i in range(3)]
    carols = [text(c, f"eet ik niet {i}") for i in range(4)]
    image = text(b, "eet foto.jpg", "m.image", url="mxc://lab.test/foto")
    video = text(b, "eet film.mp4", "m.video", url="mxc://lab.test/film")
    file_ = text(a, "eet menu.pdf", "m.file", url="mxc://lab.test/menu")
    # Without the word: only a search without words finds these.
    quiet_photo = text(b, "zonnebloem.jpg", "m.image", url="mxc://lab.test/zon")
    quiet_text = text(b, "hallo allemaal")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/state/m.room.topic/", {"topic": "waar eet je"}, a)
    topic = r["event_id"]
    time.sleep(1.2)
    middle = int(time.time() * 1000)
    time.sleep(1.2)
    # The second batch, after `middle`.
    link = text(a, "eet hier https://example.org/menu")
    in_brackets = text(a, "eet (zie https://example.org)")
    not_a_link = text(a, "eet http:// of https://. of zo")
    plain = text(a, "eet zonder link")
    # An edit that adds a link; one whose fallback body has a link that the new body drops;
    # one that turns a text into an emote.
    edit_adds = send(a, {"msgtype": "m.text", "body": "* eet zonder link, nu https://example.org/nieuw",
                         "m.new_content": {"msgtype": "m.text", "body": "eet zonder link, nu https://example.org/nieuw"},
                         "m.relates_to": {"rel_type": "m.replace", "event_id": plain}})
    edit_drops = send(a, {"msgtype": "m.text", "body": "* eet hier https://example.org/menu",
                          "m.new_content": {"msgtype": "m.text", "body": "eet hier, link weg"},
                          "m.relates_to": {"rel_type": "m.replace", "event_id": link}})
    edit_emote = send(a, {"msgtype": "m.text", "body": "* eet als emote",
                          "m.new_content": {"msgtype": "m.emote", "body": "eet als emote"},
                          "m.relates_to": {"rel_type": "m.replace", "event_id": alices[0]}})
    second = {link, in_brackets, not_a_link, plain, edit_adds, edit_drops, edit_emote}
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/account_data/m.ignored_user_list",
                   {"ignored_users": {carol: {}}}, a)
    assert st == 200, r

    first = set(alices) | set(bobs) | {image, video, file_, topic}
    everything = first | second  # what alice may find (carol's are ignored)

    def search(flt=None, next_batch=None, context=None, order="recent", term="eet", keys=None):
        body = {"search_categories": {"room_events": {"search_term": term, "order_by": order,
                                                      "filter": {"rooms": [room], **(flt or {})}}}}
        if context is not None:
            body["search_categories"]["room_events"]["event_context"] = context
        if keys is not None:
            body["search_categories"]["room_events"]["keys"] = keys
        path = "/_matrix/client/v3/search" + (f"?next_batch={q(next_batch)}" if next_batch else "")
        st, r = hs.req("POST", path, body, a)
        return st, r.get("search_categories", {}).get("room_events", r)

    def all_pages(flt, limit, term="eet", keys=None, order="recent"):
        """(ids, count, page sizes, status) over every page."""
        ids, sizes, nb, count, st = [], [], None, None, 200
        for _ in range(60):
            st, page = search({**flt, "limit": limit}, nb, term=term, keys=keys, order=order)
            if st != 200:
                return ids, count, sizes, st
            got = [x["result"]["event_id"] for x in page.get("results", [])]
            ids += got
            sizes.append(len(got))
            count = page.get("count")
            nb = page.get("next_batch")
            if not nb:
                break
        return ids, count, sizes, st

    def pages_ok(sizes, limit):
        # Every page full but the last, which is shorter, or empty after a full one (a full page
        # has a next_batch, as in Synapse).
        *full, last = sizes
        return all(n == limit for n in full) and (0 < last < limit or (last == 0 and bool(full)))

    st, r = hs.req("GET", "/_matrix/client/versions", None, a)
    check(r.get("unstable_features", {}).get("net.oele.search_filters") is True, "/versions advertises net.oele.search_filters")

    # Nothing filtered: everything but carol's.
    ids, count, sizes, st = all_pages({}, 10)
    check(st == 200 and set(ids) == everything and count == len(everything) and pages_ok(sizes, 10),
          f"unfiltered: all {len(everything)} of alice's and bob's, none of ignored carol's, count {count}, pages {sizes}")

    # #81: senders, in the query.
    ids, count, sizes, st = all_pages({"senders": [bob]}, 2)
    check(st == 200 and set(ids) == set(bobs) | {image, video} and count == 5 and sizes == [2, 2, 1],
          f"senders=[bob]: his 5, count {count}, full pages {sizes}, no empty ones")
    ids, count, sizes, st = all_pages({"not_senders": [alice]}, 4)
    check(st == 200 and set(ids) == set(bobs) | {image, video} and count == 5 and sizes == [4, 1] and pages_ok(sizes, 4),
          f"not_senders=[alice]: bob's 5 (carol's still ignored), count {count}, pages {sizes}")
    ids, count, sizes, st = all_pages({"types": ["m.room.topic"]}, 5)
    check(st == 200 and ids == [topic] and count == 1 and sizes == [1], f"types=[m.room.topic]: the topic only, count {count}")
    ids, count, sizes, st = all_pages({"not_types": ["m.room.message"]}, 5)
    check(st == 200 and ids == [topic] and count == 1, f"not_types=[m.room.message]: the topic only, count {count}")
    ids, count, sizes, st = all_pages({"contains_url": True}, 2)
    check(st == 200 and set(ids) == {image, video, file_} and count == 3 and sizes == [2, 1],
          f"contains_url=true: the picture, video and file, count {count}, pages {sizes}")
    ids, count, sizes, st = all_pages({"contains_url": False}, 10)
    check(st == 200 and set(ids) == everything - {image, video, file_} and count == len(everything) - 3,
          f"contains_url=false: all but those three, count {count}")

    # #82: a period.
    ids, count, sizes, st = all_pages({"net.oele.after": middle}, 3)
    check(st == 200 and set(ids) == second and count == len(second) and pages_ok(sizes, 3),
          f"net.oele.after: the second batch only ({len(set(ids) & second)} of {len(second)}, count {count}, pages {sizes})")
    ids, count, sizes, st = all_pages({"net.oele.before": middle}, 10)
    check(st == 200 and set(ids) == first and count == len(first), f"net.oele.before: the first batch only, count {count}")
    ids, count, sizes, st = all_pages({"net.oele.after": middle, "net.oele.before": middle + 1}, 10)
    check(st == 200 and ids == [] and count == 0, f"an empty period: nothing, count {count}")

    # #82: message kind.
    ids, count, sizes, st = all_pages({"net.oele.msgtypes": ["m.image", "m.video"]}, 1)
    check(st == 200 and set(ids) == {image, video} and count == 2 and sizes == [1, 1, 0],
          f"net.oele.msgtypes=[m.image, m.video]: the picture and the video, count {count}, pages {sizes}")
    ids, count, sizes, st = all_pages({"net.oele.msgtypes": ["m.file", "m.audio"]}, 10)
    check(st == 200 and ids == [file_] and count == 1, f"net.oele.msgtypes=[m.file, m.audio]: the file, count {count}")
    ids, count, sizes, st = all_pages({"net.oele.msgtypes": ["m.emote"]}, 10)
    check(st == 200 and ids == [edit_emote], f"an edit counts with the msgtype of its m.new_content ({len(ids)} hits)")
    ids, count, sizes, st = all_pages({"net.oele.msgtypes": ["m.text"], "senders": [bob]}, 10)
    check(st == 200 and set(ids) == set(bobs) and count == 3, f"with senders: bob's texts, count {count}")

    # #82: links.
    ids, count, sizes, st = all_pages({"net.oele.contains_link": True}, 2)
    check(st == 200 and set(ids) == {link, in_brackets, edit_adds} and count == 3 and sizes == [2, 1],
          f"net.oele.contains_link=true: the link, the bracketed link and the edit that adds one, not the edit that drops it nor 'http://' alone (count {count}, pages {sizes})")
    ids, count, sizes, st = all_pages({"net.oele.contains_link": False, "net.oele.after": middle}, 10)
    check(st == 200 and set(ids) == {not_a_link, plain, edit_drops, edit_emote} and count == 4,
          f"net.oele.contains_link=false in the second batch: the rest of it, count {count}")

    # Wrong values are refused, not ignored.
    for bad in ({"net.oele.after": "gisteren"}, {"net.oele.before": 1.5}, {"net.oele.msgtypes": "m.image"},
                {"net.oele.msgtypes": ["m.image", 3]}, {"net.oele.contains_link": 1}, {"net.oele.since": 0}):
        st, r = search(bad)
        check(st == 400 and r.get("errcode") == "M_BAD_JSON", f"{bad} is refused ({st}, {r.get('errcode')})")

    # #81: context only when asked.
    st, page = search({"limit": 3})
    ctx = [x.get("context") for x in page.get("results", [])]
    check(st == 200 and len(ctx) == 3 and all(x == {} for x in ctx), f"without event_context: no events around the results ({ctx[:1]})")
    st, page = search({"limit": 3}, context={"before_limit": 1, "after_limit": 1})
    ctx = [x.get("context", {}) for x in page.get("results", [])]
    check(st == 200 and len(ctx) == 3 and all(len(x.get("events_before", [])) == 1 for x in ctx) and all("start" in x for x in ctx),
          f"with event_context 1/1: one event before each ({[len(x.get('events_before', [])) for x in ctx]})")
    st, page = search({"limit": 3}, order="rank")
    check(st == 200 and len(page.get("results", [])) == 3, f"order_by rank works with the filters too ({st})")

    # keys: which fields are searched.
    ids, count, sizes, st = all_pages({}, 10, keys=["content.body"])
    check(st == 200 and set(ids) == everything - {topic} and count == len(everything) - 1,
          f"keys=[content.body]: the messages, not the topic (count {count})")
    ids, count, sizes, st = all_pages({}, 10, keys=["content.topic"])
    check(st == 200 and ids == [topic] and count == 1, f"keys=[content.topic]: the topic only (count {count})")

    # #84: an address written from www. on is a link too, unless it is part of another word or
    # address; a bare name is not. In a room of its own, so the counts above stay as they are.
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    room2 = r["room_id"]

    def text2(body):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room2)}/send/m.room.message/{uuid.uuid4().hex}",
                       {"msgtype": "m.text", "body": body}, a)
        assert st == 200, r
        return r["event_id"]

    www_links = {text2(b) for b in ("eet op www.mandkind.nl", "eet (www.example.org/a)", '"www.example.org" eet',
                                    "www.example.org eet", "eet https://www.example.org")}
    www_not = {text2(b) for b in ("eet awww.example.org", "eet www.nodot", "eet example.org",
                                  "eet mail@www.example.org", "eet www..org", "eet x.www.example.org")}
    for want, expected in ((True, www_links), (False, www_not)):
        st, r = hs.req("POST", "/_matrix/client/v3/search", {"search_categories": {"room_events": {
            "search_term": "eet", "filter": {"rooms": [room2], "net.oele.contains_link": want, "limit": 20}}}}, a)
        got = {x["result"]["event_id"] for x in r.get("search_categories", {}).get("room_events", {}).get("results", [])}
        check(st == 200 and got == expected, f"www. addresses, contains_link={want}: {len(got & expected)} of {len(expected)}, {len(got - expected)} wrong")

    # #83: search without words.
    st, r = hs.req("GET", "/_matrix/client/versions", None, a)
    check(r.get("unstable_features", {}).get("net.oele.search_without_words") is True, "/versions advertises net.oele.search_without_words")
    pictures = [quiet_photo, video, image]  # newest first
    for term in ("", "   "):
        ids, count, sizes, st = all_pages({"net.oele.msgtypes": ["m.image", "m.video"]}, 10, term=term, keys=["content.body"])
        check(st == 200 and ids == pictures and count == 3 and sizes == [3],
              f"no words ({term!r}) and msgtypes=[m.image, m.video]: every picture and video, also the one without the word, newest first (count {count})")
    ids, count, sizes, st = all_pages({"net.oele.msgtypes": ["m.image", "m.video"]}, 10, term="", order="rank")
    check(st == 200 and ids == pictures, "with order_by rank too: there is nothing to rank by, so newest first")
    bobs_all = set(bobs) | {image, video, quiet_photo, quiet_text}
    ids, count, sizes, st = all_pages({"senders": [bob]}, 3, term="")
    check(st == 200 and set(ids) == bobs_all and count == 7 and sizes == [3, 3, 1],
          f"no words and senders=[bob]: all 7 of his messages, count {count}, pages {sizes}")
    ids, count, sizes, st = all_pages({"not_senders": [alice]}, 10, term="")
    check(st == 200 and set(ids) == bobs_all and count == 7, f"no words and not_senders=[alice]: bob's, carol's still ignored (count {count})")
    ids, count, sizes, st = all_pages({"net.oele.after": middle}, 10, term="", keys=["content.body"])
    check(st == 200 and set(ids) == second and count == len(second),
          f"no words and a period: the second batch, an edit beside its original (count {count})")
    ids, count, sizes, st = all_pages({"contains_url": True, "net.oele.before": middle}, 2, term="")
    check(st == 200 and set(ids) == {image, video, file_, quiet_photo} and count == 4 and sizes == [2, 2, 0],
          f"no words, contains_url and a period: the four with a url (count {count}, pages {sizes})")
    for flt in ({}, {"limit": 5}):
        st, r = search(flt, term="")
        check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"no words and no filter ({flt}): 400 ({st}, {r.get('errcode')})")
    st, r = search({}, term="  ", keys=["content.body"])
    check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"keys alone is no filter either ({st}, {r.get('errcode')})")


if __name__ == "__main__":
    run_lab(["mock"], run)
