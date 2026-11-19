#!/usr/bin/env python3
"""Presence a user keeps to themselves: a sync with `set_presence=offline` is no activity.

`set_presence=offline` on a sync means "do not affect my presence". A user who sets themselves
offline and syncs that way must not show as "last active just now" to the people they share a
room with; an ordinary sync does count as activity and brings them back online.

The lab: Alice and Bob share a room; Bob watches Alice's presence.

Needs what scripts/fedlab.py needs. Takes about fifteen seconds.

Usage: scripts/presencetest.py [--keep]
"""
import time, urllib.parse

from fedlab import check, run_lab

SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"


def run(hs, peers):
    q = urllib.parse.quote
    alice, a_token = hs.register("alice")
    bob, b_token = hs.register("bob")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, a_token)
    assert st == 200, created
    room_id = created["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room_id)}", {}, b_token)
    assert st == 200, r

    def seen_by_bob():
        st, r = hs.req("GET", f"/_matrix/client/v3/presence/{q(alice)}/status", None, b_token)
        assert st == 200, r
        return r.get("presence"), r.get("last_active_ago")

    # Alice is around, then keeps her presence to herself.
    hs.req("GET", "/_matrix/client/v3/sync?timeout=0&set_presence=online", None, a_token)
    st, r = hs.req("PUT", f"/_matrix/client/v3/presence/{q(alice)}/status", {"presence": "offline"}, a_token)
    check(st == 200, f"Alice sets herself offline ({st})")
    time.sleep(4)

    # Her clients go on syncing, both kinds, with set_presence=offline.
    for _ in range(3):
        st1, _ = hs.req("GET", "/_matrix/client/v3/sync?timeout=0&set_presence=offline", None, a_token)
        st2, _ = hs.req("POST", f"{SLIDING}?timeout=0&set_presence=offline",
                        {"lists": {"all": {"ranges": [[0, 10]], "timeline_limit": 1}}}, a_token)
        assert st1 == 200 and st2 == 200, (st1, st2)
        time.sleep(0.3)
    presence, ago = seen_by_bob()
    check(presence == "offline", f"Bob sees her offline ({presence})")
    check(ago is not None and ago >= 4000, f"and her syncs with set_presence=offline do not reset her last activity ({ago} ms ago)")

    # An ordinary sync is activity again.
    hs.req("POST", f"{SLIDING}?timeout=0", {"lists": {"all": {"ranges": [[0, 10]], "timeline_limit": 1}}}, a_token)
    presence, ago = seen_by_bob()
    check(presence == "online" and ago is not None and ago < 2000,
          f"a sliding sync without set_presence brings her back online, active just now ({presence}, {ago} ms ago)")


if __name__ == "__main__":
    run_lab(["mock"], run)
