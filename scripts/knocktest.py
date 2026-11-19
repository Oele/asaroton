#!/usr/bin/env python3
"""Knocking as a knocker's sliding sync sees it: the room in the lists as stripped state with its
name, and the end of the knock (let in, refused, withdrawn) reaching the knocker at once.

The lab: Roos runs a room with join rule `knock`, "Glaze course"; Siem and Tijn knock. Roos lets
Siem in and refuses Tijn; Tijn knocks again and withdraws. The knockers' sliding sync is what
matrix-rust-sdk asks for: a list over all rooms, with their own membership and the room name.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/knocktest.py [--keep]
"""
import urllib.parse

from fedlab import check, run_lab

SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"


def run(hs, peers):
    q = urllib.parse.quote
    roos, r_token = hs.register("roos")
    siem, s_token = hs.register("siem")
    tijn, t_token = hs.register("tijn")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom",
                         {"room_version": "11", "name": "Glaze course", "initial_state": [
                             {"type": "m.room.join_rules", "state_key": "", "content": {"join_rule": "knock"}}]}, r_token)
    assert st == 200, created
    room_id = created["room_id"]

    class Conn:
        def __init__(self, token):
            self.token, self.pos = token, None

        def sync(self):
            body = {"lists": {"all": {"ranges": [[0, 20]], "timeline_limit": 1,
                                      "required_state": [["m.room.member", "$ME"], ["m.room.name", ""], ["m.room.join_rules", ""]]}}}
            st, r = hs.req("POST", f"{SLIDING}?timeout=0" + (f"&pos={self.pos}" if self.pos else ""), body, self.token)
            assert st == 200, r
            self.pos = r["pos"]
            return r.get("rooms", {}).get(room_id)

    def own_membership(room, user):
        """The user's membership as the answer shows it: in the stripped state or required_state."""
        for ev in (room or {}).get("invite_state", []) + (room or {}).get("required_state", []):
            if ev.get("type") == "m.room.member" and ev.get("state_key") == user:
                return ev.get("content", {}).get("membership")
        return None

    siem_conn, tijn_conn = Conn(s_token), Conn(t_token)
    siem_conn.sync()
    tijn_conn.sync()

    # Siem knocks: his lists show the room as stripped state, with its name.
    st, r = hs.req("POST", f"/_matrix/client/v3/knock/{q(room_id)}", {"reason": "I'd like to learn glazing"}, s_token)
    assert st == 200, r
    room = siem_conn.sync()
    names = [e.get("content", {}).get("name") for e in (room or {}).get("invite_state", []) if e.get("type") == "m.room.name"]
    check(room is not None and own_membership(room, siem) == "knock",
          f"the knocker's lists show the room as knocked ({own_membership(room, siem)})")
    check(names == ["Glaze course"] and (room or {}).get("name") == "Glaze course",
          f"with the room's name, in the stripped state and as the room's name ({names}, {(room or {}).get('name')})")

    # Roos lets him in: he joins (an invite answers a knock).
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/invite", {"user_id": siem}, r_token)
    assert st == 200, r
    room = siem_conn.sync()
    check(own_membership(room, siem) == "invite", f"a knock let in comes to the knocker as an invite ({own_membership(room, siem)})")

    # Tijn knocks and is refused.
    st, r = hs.req("POST", f"/_matrix/client/v3/knock/{q(room_id)}", {"reason": "please"}, t_token)
    assert st == 200, r
    room = tijn_conn.sync()
    check(own_membership(room, tijn) == "knock", f"Tijn's lists show his knock ({own_membership(room, tijn)})")
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/kick", {"user_id": tijn, "reason": "full"}, r_token)
    assert st == 200, r
    room = tijn_conn.sync()
    check(room is not None and own_membership(room, tijn) == "leave",
          f"a refused knock reaches the knocker at once, as his leave ({own_membership(room, tijn)})")

    # Tijn knocks again and withdraws.
    st, r = hs.req("POST", f"/_matrix/client/v3/knock/{q(room_id)}", {"reason": "again"}, t_token)
    assert st == 200, r
    room = tijn_conn.sync()
    check(own_membership(room, tijn) == "knock", f"a second knock shows again ({own_membership(room, tijn)})")
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/leave", {}, t_token)
    assert st == 200, r
    room = tijn_conn.sync()
    check(room is not None and own_membership(room, tijn) == "leave", f"a withdrawn knock reaches him as his leave ({own_membership(room, tijn)})")
    room = tijn_conn.sync()
    check(room is None, "and the room is not sent again after that")

    # An invited room carries its name too.
    lotte, l_token = hs.register("lotte")
    lotte_conn = Conn(l_token)
    lotte_conn.sync()
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/invite", {"user_id": lotte}, r_token)
    assert st == 200, r
    room = lotte_conn.sync()
    check(own_membership(room, lotte) == "invite" and (room or {}).get("name") == "Glaze course",
          f"an invited room is named as well ({(room or {}).get('name')})")


if __name__ == "__main__":
    run_lab(["mock"], run)
