#!/usr/bin/env python3
"""Key backup versions created at the same moment for one user each get their own number (no 500
from two creations taking the same next version), and the newest is the current one.

The lab: Alice's client creates twelve backup versions at once from twelve threads.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/backupversiontest.py [--keep]
"""
from concurrent.futures import ThreadPoolExecutor

from fedlab import check, run_lab

ALGORITHM = {"algorithm": "m.megolm_backup.v1.curve25519-aes-sha2",
             "auth_data": {"public_key": "hSDwCYkwp1R0i33ctD73Wg2/Og0mOBr066SpjqqbTmo", "signatures": {}}}


def run(hs, peers):
    alice, a = hs.register("alice")

    def create(_):
        return hs.req("POST", "/_matrix/client/v3/room_keys/version", ALGORITHM, a)

    with ThreadPoolExecutor(12) as pool:
        answers = list(pool.map(create, range(12)))
    statuses = [st for st, _ in answers]
    versions = [r.get("version") for _, r in answers]
    check(statuses == [200] * 12, f"twelve creations at once all succeed ({sorted(set(statuses))})")
    check(len(set(versions)) == 12, f"each with its own version ({len(set(versions))} distinct)")
    st, current = hs.req("GET", "/_matrix/client/v3/room_keys/version", None, a)
    check(st == 200 and current.get("version") == max(versions, key=int), f"the newest is the current one ({current.get('version')})")


if __name__ == "__main__":
    run_lab(["mock"], run)
