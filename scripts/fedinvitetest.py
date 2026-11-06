#!/usr/bin/env python3
"""An invitation to a user on another server goes there once, over `/invite`.

The invited user's server is not in the room. It gets the invitation through the invite
endpoint, signs it and hands it back. Sent to it again as an ordinary event in a transaction,
the invitation is something that server has to refuse ("this server is not in the room"): a
wasted request, and a warning in the sender's log for an invitation that did arrive.

What does have to reach a server that is not in the room is the end of an invitation: when
the invited user is removed again, their server must hear of it.

The lab: Alice on the homeserver invites a user of a mock server, then takes the invitation
back.

Needs what scripts/fedlab.py needs. Takes about ten seconds.

Usage: scripts/fedinvitetest.py [--keep]
"""
import re, time, urllib.parse

from fedlab import KEPT_TOP, check, run_lab, wait_for


def run(hs, peers):
    mock = peers["mock"]
    q = urllib.parse.quote
    alice, token = hs.register("alice")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "room_version": "11", "name": "Invitations"}, token)
    assert st == 200, created
    room_id = created["room_id"]
    guest = f"@guest:{mock.name}"
    invitations = []

    def take_invitation(handler, m, body):
        # The invited user's server adds its signature and hands the event back.
        ev = body["event"]
        redacted = {k: v for k, v in ev.items() if k in KEPT_TOP}
        redacted["content"] = {k: v for k, v in ev["content"].items() if k in ("membership", "join_authorised_via_users_server")}
        ev["signatures"][mock.name] = {"ed25519:k1": mock.sign(redacted)}
        invitations.append(ev)
        handler.answer(200, {"event": ev})

    mock.put_routes.append((re.compile(r"/_matrix/federation/v2/invite/"), take_invitation))

    def memberships_sent():
        """The membership events about the guest that arrived in transactions."""
        return [pdu["content"]["membership"] for req in mock.requests() for pdu in req["body"].get("pdus", [])
                if pdu.get("type") == "m.room.member" and pdu.get("state_key") == guest]

    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/invite", {"user_id": guest}, token)
    check(st == 200 and len(invitations) == 1, f"the invitation reaches the guest's server over /invite ({st}, {len(invitations)})")
    st, members = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/members?membership=invite", None, token)
    check([e["state_key"] for e in members.get("chunk", [])] == [guest], "and the guest is invited")
    # Give the sender time for a transaction, and something else to send so that it is seen
    # to be at work.
    hs.message(token, room_id, "while the guest thinks it over")
    time.sleep(3)
    check(memberships_sent() == [], f"the invitation is not sent again as an ordinary event ({memberships_sent()})")

    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/kick", {"user_id": guest, "reason": "another time"}, token)
    assert st == 200, r
    check(wait_for(lambda: memberships_sent() == ["leave"], 10, 0.2), f"taking the invitation back does reach the guest's server ({memberships_sent()})")
    check(not hs.log_lines("remote rejected PDU"), "and nothing was refused")


if __name__ == "__main__":
    run_lab(["mock"], run)
