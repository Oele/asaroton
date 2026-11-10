#!/usr/bin/env python3
"""Dehydrated devices (MSC3814): a device that waits for room keys while its user is away.

A user whose devices are all signed out has no device a room key could be sent to. A
dehydrated device is one the client leaves with the server for that time. Others see it in
the user's device list and send it room keys; the client that signs in next fetches it and
reads what was waiting.

Alice leaves a dehydrated device. Bob finds it among her devices, claims a one-time key for
it and sends her devices 250 to-device messages. Alice reads them out page by page: all of
them, once, in order, and nothing is left afterwards. A second device replaces the first, a
device that is in use cannot be turned into one, and deleting it removes it for everybody.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/dehydratedtest.py [--keep]
"""
import uuid

from fedlab import check, query, run_lab

BASE = "/_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device"
WAITING = 250


def run(hs, peers):
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    st, r = hs.req("GET", "/_matrix/client/v3/account/whoami", None, a)
    own_device = r["device_id"]

    def device(device_id):
        return {
            "device_id": device_id,
            "initial_device_display_name": "Waiting for keys",
            "device_data": {"algorithm": "org.matrix.msc3814.v2", "device_pickle": "pickle-" + device_id, "nonce": "bm9uY2U"},
            "device_keys": {
                "user_id": alice, "device_id": device_id, "dehydrated": True,
                "algorithms": ["m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"],
                "keys": {f"curve25519:{device_id}": "c" + device_id, f"ed25519:{device_id}": "e" + device_id},
                "signatures": {alice: {f"ed25519:{device_id}": "sig"}},
            },
            "one_time_keys": {f"signed_curve25519:AAAA{n}": {"key": f"otk{n}", "signatures": {alice: {f"ed25519:{device_id}": "sig"}}} for n in range(3)},
            "fallback_keys": {"signed_curve25519:FALL": {"key": "fallback", "fallback": True, "signatures": {alice: {f"ed25519:{device_id}": "sig"}}}},
        }

    def seen_by_bob():
        st, r = hs.req("POST", "/_matrix/client/v3/keys/query", {"device_keys": {alice: []}}, b)
        assert st == 200, r
        return r["device_keys"].get(alice, {})

    print("-- before there is one")
    st, r = hs.req("GET", "/_matrix/client/versions")
    check(r.get("unstable_features", {}).get("org.matrix.msc3814") is True, "the server says that it has dehydrated devices")
    st, r = hs.req("GET", BASE, None, a)
    check(st == 404 and r.get("errcode") == "M_NOT_FOUND", f"asking for one that is not there is answered M_NOT_FOUND ({st} {r.get('errcode')})")
    st, r = hs.req("DELETE", BASE, None, a)
    check(st == 404 and r.get("errcode") == "M_NOT_FOUND", f"and so is deleting it ({st} {r.get('errcode')})")

    print("-- alice leaves a dehydrated device")
    st, r = hs.req("PUT", BASE, device("DEHYDRATED1"), a)
    check(st == 200 and r.get("device_id") == "DEHYDRATED1", f"it is stored ({st} {r})")
    st, r = hs.req("GET", BASE, None, a)
    check(st == 200 and r.get("device_id") == "DEHYDRATED1" and r.get("device_data", {}).get("device_pickle") == "pickle-DEHYDRATED1" and r["device_data"].get("nonce") == "bm9uY2U", f"and comes back as it was left ({st})")
    keys = seen_by_bob().get("DEHYDRATED1", {})
    check(keys.get("dehydrated") is True and keys.get("keys", {}).get("curve25519:DEHYDRATED1") == "cDEHYDRATED1", "bob finds it among alice's devices, marked as dehydrated")
    st, r = hs.req("POST", "/_matrix/client/v3/keys/claim", {"one_time_keys": {alice: {"DEHYDRATED1": "signed_curve25519"}}}, b)
    claimed = r.get("one_time_keys", {}).get(alice, {}).get("DEHYDRATED1", {})
    check(st == 200 and len(claimed) == 1 and next(iter(claimed.values())).get("key", "").startswith("otk"), f"and can claim a one-time key for it ({st} {list(claimed)})")
    st, r = hs.req("GET", BASE, None, b)
    check(st == 404, f"bob has none himself ({st})")

    print(f"-- {WAITING} to-device messages wait for it")
    for n in range(WAITING):
        st, r = hs.req("PUT", f"/_matrix/client/v3/sendToDevice/m.test.key/{uuid.uuid4().hex}", {"messages": {alice: {"*": {"n": n}}}}, b)
        assert st == 200, r
    st, r = hs.req("POST", f"{BASE}/{own_device}/events", {}, a)
    check(st == 403, f"the messages of another device cannot be read out this way ({st})")
    st, r = hs.req("POST", f"{BASE}/DEHYDRATED1/events", {}, b)
    check(st == 403, f"nor alice's by bob ({st})")
    got, pages, token = [], 0, None
    for _ in range(20):
        st, r = hs.req("POST", f"{BASE}/DEHYDRATED1/events", {"next_batch": token} if token else {}, a)
        assert st == 200, r
        token = r.get("next_batch")
        page = [(e["sender"], e["type"], e["content"]["n"]) for e in r["events"]]
        if not page:
            break
        got += page
        pages += 1
    check(got == [(bob, "m.test.key", n) for n in range(WAITING)], f"alice reads all of them out, once and in order ({len(got)} in {pages} pages)")
    check(pages > 1 and token, f"over more than one page ({pages})")
    left = int(query("SELECT count(*) FROM to_device_messages WHERE device_id = 'DEHYDRATED1'")[0][0])
    check(left == 0, f"and what was read is gone from the server ({left} left)")
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, a)
    mine = [e for e in r.get("to_device", {}).get("events", []) if e["type"] == "m.test.key"]
    check(len(mine) > 0, f"alice's own device got its copies through sync as always ({len(mine)} in the first answer)")

    print("-- another one takes its place")
    st, r = hs.req("PUT", BASE, device(own_device), a)
    check(st == 400, f"a device that is in use cannot be made a dehydrated one ({st} {r.get('errcode')})")
    st, r = hs.req("PUT", BASE, device("DEHYDRATED2"), a)
    check(st == 200, f"a second dehydrated device is stored ({st})")
    st, r = hs.req("GET", BASE, None, a)
    devices = seen_by_bob()
    check(r.get("device_id") == "DEHYDRATED2" and "DEHYDRATED2" in devices and "DEHYDRATED1" not in devices, f"and the first one is gone, for bob too ({sorted(devices)})")
    st, r = hs.req("POST", f"{BASE}/DEHYDRATED1/events", {}, a)
    check(st == 403, f"with its messages ({st})")

    print("-- alice deletes it")
    st, r = hs.req("PUT", f"/_matrix/client/v3/sendToDevice/m.test.key/{uuid.uuid4().hex}", {"messages": {alice: {"DEHYDRATED2": {"n": -1}}}}, b)
    assert st == 200, r
    st, r = hs.req("DELETE", BASE, None, a)
    check(st == 200 and r.get("device_id") == "DEHYDRATED2", f"deleting names the device ({st} {r})")
    st, r = hs.req("GET", BASE, None, a)
    rows = query("SELECT (SELECT count(*) FROM devices WHERE device_id LIKE 'DEHYDRATED%'), (SELECT count(*) FROM device_keys WHERE device_id LIKE 'DEHYDRATED%'), (SELECT count(*) FROM one_time_keys WHERE device_id LIKE 'DEHYDRATED%'), (SELECT count(*) FROM to_device_messages WHERE device_id LIKE 'DEHYDRATED%'), (SELECT count(*) FROM dehydrated_devices)")[0]
    check(st == 404 and "DEHYDRATED2" not in seen_by_bob() and rows == ["0"] * 5, f"and nothing of it is left: device, keys, waiting messages ({st}, {rows})")


if __name__ == "__main__":
    run_lab(["mock"], run)
