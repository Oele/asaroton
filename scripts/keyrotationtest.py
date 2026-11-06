#!/usr/bin/env python3
"""Joining a room in which a member's server has changed signing keys, end to end.

A server signs an event with the key it has at that moment. When it later replaces the key
and stops publishing the old one, events from before the change can only be verified with a
key that notaries still hold. A server that joins the room afterwards is handed those old
events as part of the room's state and has to verify them.

The lab: a mock server hosts a room; a user of a second mock server joined, left and joined
again, and that server changed keys between the leave and the second join (so the second join
rests on a leave signed with the old key). A mock notary may or may not know the old key.

  1. While the notary does not know the old key, the old events cannot be verified. The
     join still succeeds, says in the log what it left out, and does not store anything as
     rejected that it merely could not check.
  2. Once the notary serves the old key, leaving and joining again makes the member appear,
     restores the message of theirs that had been rejected in the meantime, and their new
     messages arrive.
  3. A signature counts only while its key was valid: an event dated after the validity its
     server states for the key is refused.

Needs what scripts/fedlab.py needs. Takes about twenty seconds.

Usage: scripts/keyrotationtest.py [--keep]
"""
import hashlib, json, re, time, urllib.parse, uuid

from nacl.signing import SigningKey

from fedlab import SERVER_NAME, b64, canonical, check, run_lab, signed_event, wait_for

DAY = 86_400_000


class Room:
    """The room as the mock servers know it: events by ID, in the order they were made."""

    def __init__(self, resident, rotated, old_key, long_ago):
        self.id = f"!keys:{resident.name}"
        self.events, self.order = {}, []
        self.resident, self.rotated = resident, rotated
        owner, member = f"@a:{resident.name}", f"@c:{rotated.name}"
        self.member = member

        def add(peer, key, key_id, sender, kind, content, state_key, auth, ts):
            event = {"type": kind, "room_id": self.id, "sender": sender, "content": content, "auth_events": auth,
                     "prev_events": self.order[-1:], "depth": len(self.order) + 1, "origin_server_ts": ts}
            if state_key is not None:
                event["state_key"] = state_key
            event_id, event = signed_event(peer.name, key, key_id, event)
            self.events[event_id] = event
            self.order.append(event_id)
            return event_id

        by_resident = lambda *a: add(resident, resident.key, "ed25519:k1", owner, *a)
        self.create = by_resident("m.room.create", {"room_version": "11"}, "", [], long_ago)
        self.owner_join = by_resident("m.room.member", {"membership": "join"}, owner, [self.create], long_ago)
        self.power = by_resident("m.room.power_levels", {"users": {owner: 100}, "users_default": 0, "events_default": 0, "state_default": 50, "invite": 0}, "", [self.create, self.owner_join], long_ago)
        self.rules = by_resident("m.room.join_rules", {"join_rule": "public"}, "", [self.create, self.owner_join, self.power], long_ago)
        # The member's first stay, signed with the key their server had then...
        old = lambda *a: add(rotated, old_key, "ed25519:old", member, *a)
        self.first_join = old("m.room.member", {"membership": "join"}, member, [self.create, self.power, self.rules], long_ago + 1000)
        self.leave = old("m.room.member", {"membership": "leave"}, member, [self.create, self.power, self.first_join], long_ago + 3_600_000)
        # ...and their return, signed with the key it has now. It rests on the old leave.
        self.second_join = add(rotated, rotated.key, "ed25519:k1", member, "m.room.member", {"membership": "join"}, member, [self.create, self.power, self.rules, self.leave], long_ago + 7_200_000)
        self.state = [self.create, self.owner_join, self.power, self.rules, self.second_join]

    def auth_chain(self, ids):
        seen, todo = set(), list(ids)
        while todo:
            for a in self.events[todo.pop()]["auth_events"]:
                if a not in seen:
                    seen.add(a)
                    todo.append(a)
        return seen

    def message(self, body, prev, ts=None):
        event = {"type": "m.room.message", "room_id": self.id, "sender": self.member, "content": {"msgtype": "m.text", "body": body},
                 "auth_events": [self.create, self.power, self.second_join], "prev_events": [prev], "depth": len(self.order) + 5,
                 "origin_server_ts": ts or int(time.time() * 1000)}
        event_id, event = signed_event(self.rotated.name, self.rotated.key, "ed25519:k1", event)
        self.events[event_id] = event
        return event_id, event

    def serve_from(self, peer, joins):
        """Teach a mock server the federation requests a joining server makes."""
        unq = urllib.parse.unquote

        def make_join(handler, m):
            user = unq(m.group(1))
            handler.answer(200, {"room_version": "11", "event": {
                "type": "m.room.member", "room_id": self.id, "sender": user, "state_key": user, "content": {"membership": "join"},
                "auth_events": [self.create, self.power, self.rules], "prev_events": [self.order[-1]], "depth": len(self.order) + 1,
                "origin_server_ts": int(time.time() * 1000)}})

        def send_join(handler, m, body):
            joins.append((unq(m.group(1)), body))
            chain = self.auth_chain(self.state) - set(self.state)
            handler.answer(200, {"origin": peer.name, "members_omitted": False, "event": body,
                                 "state": [self.events[i] for i in self.state], "auth_chain": [self.events[i] for i in sorted(chain)]})

        def event(handler, m):
            found = self.events.get(unq(m.group(1)))
            if not found:
                return handler.answer(404, {"errcode": "M_NOT_FOUND", "error": "mock"})
            handler.answer(200, {"origin": peer.name, "origin_server_ts": int(time.time() * 1000), "pdus": [found]})

        def event_auth(handler, m):
            event_id = unq(m.group(1))
            if event_id not in self.events:
                return handler.answer(404, {"errcode": "M_NOT_FOUND", "error": "mock"})
            handler.answer(200, {"auth_chain": [self.events[i] for i in sorted(self.auth_chain([event_id]))]})

        # The room ID is not matched: this lab has one room, and how its ID is escaped in a path
        # is the caller's business.
        peer.routes += [
            (re.compile(r"/_matrix/federation/v1/make_join/[^/]+/([^/?]+)"), make_join),
            (re.compile(r"/_matrix/federation/v1/event/([^/?]+)"), event),
            (re.compile(r"/_matrix/federation/v1/event_auth/[^/]+/([^/?]+)"), event_auth),
        ]
        peer.put_routes.append((re.compile(r"/_matrix/federation/v2/send_join/[^/]+/([^/?]+)"), send_join))


def run(hs, peers):
    notary, resident, rotated = peers["notary"], peers["resident"], peers["rotated"]
    old_key = SigningKey.generate()
    long_ago = int(time.time() * 1000) - 400 * DAY
    room = Room(resident, rotated, old_key, long_ago)
    joins = []
    room.serve_from(resident, joins)
    room.serve_from(rotated, joins)

    # The notary: what each server publishes itself and, once it "knows" it, the key response
    # the rotated server gave out in its first week, which ran out long ago.
    notary.knows_old_key = False
    old_response = {"server_name": rotated.name, "valid_until_ts": long_ago + 7 * DAY, "old_verify_keys": {},
                    "verify_keys": {"ed25519:old": {"key": b64(bytes(old_key.verify_key))}}}
    old_response["signatures"] = {rotated.name: {"ed25519:old": b64(old_key.sign(canonical(old_response)).signature)}}

    notary.signs = True

    def query(handler, m):
        server = urllib.parse.unquote(m.group(1))
        known = [p.server_keys() for p in peers.values() if p.name == server]
        if server == rotated.name and notary.knows_old_key:
            known.append(old_response)
        out = []
        for document in known:
            document = json.loads(json.dumps(document))
            if notary.signs:
                # A notary adds its own signature over the document without its signatures.
                bare = {k: v for k, v in document.items() if k != "signatures"}
                document["signatures"][notary.name] = {"ed25519:k1": notary.sign(bare)}
            out.append(document)
        handler.answer(200, {"server_keys": out})

    notary.routes.append((re.compile(r"/_matrix/key/v2/query/([^/?]+)"), query))

    alice, token = hs.register("alice")
    q = urllib.parse.quote

    def join():
        st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room.id)}?server_name={q(resident.name)}", {}, token)
        return st, r

    def members():
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room.id)}/joined_members", None, token)
        return sorted(r.get("joined", {}))

    def bodies():
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room.id)}/messages?dir=b&limit=50", None, token)
        return [e["content"].get("body") for e in r.get("chunk", []) if e["type"] == "m.room.message"]

    def send(event):
        st, r = rotated.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}", {"origin": rotated.name, "origin_server_ts": int(time.time() * 1000), "pdus": [event], "edus": []})
        return st, r

    # ---- 1. the old key is nowhere to be had
    st, r = join()
    check(st == 200, f"the join succeeds although three of the room's events cannot be verified ({st} {r.get('error', '')})")
    check(members() == sorted([alice, f"@a:{resident.name}"]), f"the member whose history cannot be verified is not shown as joined ({members()})")
    left_out = hs.log_lines("leaving out an event of the join response that could not be verified")
    not_stored = hs.log_lines("not storing an event of the join response")
    check(len(left_out) == 2 and all("m.room.member" in l and "@c:" in l for l in left_out), f"the two events signed with the old key are named in the log ({len(left_out)} lines)")
    check(len(not_stored) == 1 and room.second_join in not_stored[0], "and so is the join that rests on them, which is not stored as rejected")
    first_join_id = joins[-1][0]
    m1_id, m1 = room.message("said while the old key was unknown", first_join_id)
    st, r = send(m1)
    check(st == 200, f"a message from that member is taken in ({st})")
    time.sleep(1)
    check("said while the old key was unknown" not in bodies(), "but not shown, as its sender is not a member here")

    # ---- 2. the notary learns the old key, but hands it out without its own signature
    notary.knows_old_key = True
    notary.signs = False
    hs.stop()
    hs.start()  # a key that could not be found is not looked for again for ten minutes
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room.id)}/leave", {}, token)
    st, r = join()
    check(st == 200 and room.member not in members(), f"a key the notary has not signed for is not believed ({st} {members()})")
    unsigned = hs.log_lines("ignoring a key response that the notary has not validly signed")
    check(len(unsigned) >= 1, "and the log says so")

    # ---- 3. the notary signs what it hands out; leave and join again
    notary.signs = True
    hs.stop()
    hs.start()
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room.id)}/leave", {}, token)
    check(st == 200, f"leaving works ({st})")
    st, r = join()
    check(st == 200, f"joining again works ({st} {r.get('error', '')})")
    check(members() == sorted([alice, f"@a:{resident.name}", room.member]), f"with the old key from the notary the member is joined ({members()})")
    wait_for(lambda: "said while the old key was unknown" in bodies(), 5)
    check("said while the old key was unknown" in bodies(), "the message that had been rejected is shown now")
    m2_id, m2 = room.message("said afterwards", joins[-1][0])
    st, r = send(m2)
    wait_for(lambda: "said afterwards" in bodies(), 5)
    check(st == 200 and "said afterwards" in bodies(), f"a new message from that member arrives ({st})")
    sent_to_rotated = [x for x in rotated.requests() if x["body"].get("pdus")]
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room.id)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "reply"}, token)
    wait_for(lambda: len([x for x in rotated.requests() if x["body"].get("pdus")]) > len(sent_to_rotated), 5)
    check(len([x for x in rotated.requests() if x["body"].get("pdus")]) > len(sent_to_rotated), "and our own messages are sent to that member's server")

    # ---- 4. a signature counts only while its key was valid (room versions 5 and later)
    # The mock states a validity of one day for its key. An event dated within it passes...
    now = int(time.time() * 1000)
    m3_id, m3 = room.message("dated an hour ahead", m2_id, now + 3_600_000)
    st, r = send(m3)
    wait_for(lambda: "dated an hour ahead" in bodies(), 5)
    check(st == 200 and r.get("pdus", {}).get(m3_id) == {} and "dated an hour ahead" in bodies(), f"an event dated within its key's validity is accepted ({st} {r.get('pdus')})")
    # ...and one dated beyond it does not: nothing says the key was (or will be) the server's then.
    m4_id, m4 = room.message("dated after the key runs out", m2_id, now + 2 * DAY)
    st, r = send(m4)
    verdict = r.get("pdus", {}).get(m4_id, {})
    time.sleep(1)
    check(st == 200 and "no keys" in verdict.get("error", "") and "dated after the key runs out" not in bodies(), f"an event dated after its key's validity is refused ({st} {verdict})")
    check(hs.proc.poll() is None, "the server is still running")


if __name__ == "__main__":
    run_lab(["notary", "resident", "rotated"], run, "unchecked_event_retry_secs = 2")
