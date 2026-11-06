#!/usr/bin/env python3
"""`POST /createRoom` under pressure: several rooms at once.

Rooms asked for at the same moment each come into being. Since room version 12 a room's ID
is the hash of its create event, and two create events of one user with the same content and
the same millisecond are one and the same event. The server has to tell them apart.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/createroomtest.py [--keep]
"""
from concurrent.futures import ThreadPoolExecutor

from fedlab import check, run_lab

AT_ONCE = 8
ROUNDS = 5


def run(hs, peers):
    def create(token, body):
        return hs.req("POST", "/_matrix/client/v3/createRoom", body, token)

    print("-- rooms asked for at the same moment")
    for version in ("12", "11"):
        _, token = hs.register(f"twins{version}")
        answers = []
        with ThreadPoolExecutor(AT_ONCE) as pool:
            for _ in range(ROUNDS):
                answers += list(pool.map(lambda _: create(token, {"preset": "private_chat", "room_version": version}), range(AT_ONCE)))
        refused = [(st, r.get("errcode")) for st, r in answers if st != 200]
        rooms = {r["room_id"] for st, r in answers if st == 200}
        check(not refused, f"room version {version}: all {len(answers)} requests are answered with a room ({len(refused)} were not: {sorted(set(refused))})")
        check(len(rooms) == len(answers) - len(refused), f"room version {version}: and each with a room of its own ({len(rooms)} different rooms)")
        st, joined = hs.req("GET", "/_matrix/client/v3/joined_rooms", None, token)
        check(st == 200 and set(joined.get("joined_rooms", [])) == rooms, f"room version {version}: the creator is in exactly those rooms ({len(joined.get('joined_rooms', []))})")


if __name__ == "__main__":
    run_lab(["mock"], run)
