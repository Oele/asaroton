#!/usr/bin/env python3
"""Device lists and cross-signing as other servers see them.

  - A signature a user makes on one of their own devices (cross-signing) reaches the servers
    that share a room with them: in an `m.device_list_update` for that device, and in
    `GET /user/devices`. Servers that cache device lists, Synapse among them, take a device
    from there; without the signature a verified device looks unverified to their users.
  - A signature on another user's master key is for the signer alone: no server hears of it.
  - Every `m.device_list_update` names the user's previous one in `prev_id`, so that a server
    holding that one can apply the new one on top, and one that missed it knows to fetch the
    whole list. An empty `prev_id` makes the receiver fetch the whole list every time.
  - When a user comes to share a room with a server, each of their devices is announced with a
    stream ID of its own, also a device that never changed since it was imported.
  - Devices that carry their owner's signature are sent once more at the first start of a build
    that does the above, and only then.

Needs what scripts/fedlab.py needs. Takes about half a minute.

Usage: scripts/devicelisttest.py [--keep]
"""
import json, os, time, urllib.parse

from nacl.signing import SigningKey

import fedlab
from fedlab import b64, canonical, check, query, run_lab, wait_for

q = lambda s: urllib.parse.quote(s, safe="")


def signed(obj, user, key_id, key):
    """`obj` with a signature by `key` under `user`/`key_id` added to what it has."""
    bare = {k: v for k, v in obj.items() if k not in ("signatures", "unsigned")}
    sigs = json.loads(json.dumps(obj.get("signatures", {})))
    sigs.setdefault(user, {})[key_id] = b64(key.sign(canonical(bare)).signature)
    return {**bare, "signatures": sigs}


def cross_signing_key(user, usage, key):
    pub = b64(bytes(key.verify_key))
    return {"user_id": user, "usage": [usage], "keys": {f"ed25519:{pub}": pub}}, f"ed25519:{pub}"


def run(hs, peers):
    mock = peers["mock"]

    def register(name):
        st, r = hs.req("POST", "/_matrix/client/v3/register", {"username": name, "password": "pwpwpwpw", "auth": {"type": "m.login.dummy"}})
        assert st == 200, r
        return r["user_id"], r["access_token"], r["device_id"]

    def upload_device_keys(user, token, device):
        key = SigningKey.generate()
        dk = {
            "user_id": user, "device_id": device, "algorithms": ["m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"],
            "keys": {f"curve25519:{device}": b64(os.urandom(32)), f"ed25519:{device}": b64(bytes(key.verify_key))},
        }
        dk = signed(dk, user, f"ed25519:{device}", key)
        st, r = hs.req("POST", "/_matrix/client/v3/keys/upload", {"device_keys": dk}, token)
        assert st == 200, r
        return dk

    def upload_cross_signing(user, token):
        master, ssk, usk = SigningKey.generate(), SigningKey.generate(), SigningKey.generate()
        mk, mk_id = cross_signing_key(user, "master", master)
        sk, sk_id = cross_signing_key(user, "self_signing", ssk)
        uk, uk_id = cross_signing_key(user, "user_signing", usk)
        sk, uk = signed(sk, user, mk_id, master), signed(uk, user, mk_id, master)
        st, r = hs.req("POST", "/_matrix/client/v3/keys/device_signing/upload", {"master_key": mk, "self_signing_key": sk, "user_signing_key": uk}, token)
        assert st == 200, r
        return {"master": (mk, mk_id, master), "ssk": (sk, sk_id, ssk), "usk": (uk, uk_id, usk)}

    def edus(since, edu_type, user=None, device=None):
        out = []
        for request in mock.requests(since):
            for e in request["body"].get("edus", []):
                c = e.get("content", {})
                if e.get("edu_type") == edu_type and (user is None or c.get("user_id") == user) and (device is None or c.get("device_id") == device):
                    out.append(c)
        return out

    alice, a_token, a_dev = register("alice")
    a_keys = upload_device_keys(alice, a_token, a_dev)
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, a_token)
    assert st == 200, r
    room_id = r["room_id"]
    mock.join(hs.fed, room_id, "bob")
    a_cs = upload_cross_signing(alice, a_token)
    check(wait_for(lambda: edus(0, "m.signing_key_update", alice), 10), "the mock server hears of alice's cross-signing keys")

    print("-- a signature on one's own device")
    mark = mock.count()
    sk, sk_id, ssk = a_cs["ssk"]
    st, r = hs.req("POST", "/_matrix/client/v3/keys/signatures/upload", {alice: {a_dev: signed(a_keys, alice, sk_id, ssk)}}, a_token)
    assert st == 200 and not r.get("failures"), r
    got = wait_for(lambda: edus(mark, "m.device_list_update", alice, a_dev), 10)
    updates = edus(mark, "m.device_list_update", alice, a_dev)
    sig = updates[0].get("keys", {}).get("signatures", {}).get(alice, {}) if updates else {}
    check(got and sk_id in sig, f"it reaches the other server in an m.device_list_update, signature and all ({sorted(sig)})")
    signed_at = updates[0]["stream_id"] if updates else None
    time.sleep(1.5)
    check(not edus(mark, "m.signing_key_update", alice), "and not as a change of the cross-signing keys, which it is not")
    st, r = mock.fed(hs.fed, "GET", f"/_matrix/federation/v1/user/devices/{q(alice)}")
    device = next((d for d in r.get("devices", []) if d["device_id"] == a_dev), {})
    sig = device.get("keys", {}).get("signatures", {}).get(alice, {})
    check(st == 200 and sk_id in sig and f"ed25519:{a_dev}" in sig, f"GET /user/devices has it next to the device's own ({st}, {sorted(sig)})")
    check(r.get("master_key", {}).get("user_id") == alice and isinstance(r.get("stream_id"), int), f"and the master key and a stream position ({r.get('stream_id')})")

    print("-- prev_id")
    mark = mock.count()
    for name in ("one", "two"):
        st, r = hs.req("PUT", f"/_matrix/client/v3/devices/{q(a_dev)}", {"display_name": name}, a_token)
        assert st == 200, r
    wait_for(lambda: len(edus(mark, "m.device_list_update", alice, a_dev)) >= 2, 10)
    first, second = (edus(mark, "m.device_list_update", alice, a_dev) + [{}, {}])[:2]
    check(first.get("prev_id") == [signed_at], f"an update names the one before it ({first.get('prev_id')} after {signed_at})")
    check(second.get("prev_id") == [first.get("stream_id")] and second.get("stream_id") > first.get("stream_id", 0),
          f"and the next names that one ({first.get('stream_id')} → {second.get('stream_id')} with prev_id {second.get('prev_id')})")
    check(second.get("keys", {}).get("signatures", {}).get(alice, {}).get(sk_id), "every update of the device carries the signature")

    print("-- a user who comes to share a room with the server")
    carol, c_token, c1 = register("carol")
    upload_device_keys(carol, c_token, c1)
    st, r = hs.req("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": "carol"}, "password": "pwpwpwpw"})
    assert st == 200, r
    c2 = r["device_id"]
    # As for a device imported from Synapse: no change of it on record.
    query(f"DELETE FROM device_list_changes WHERE user_id = '{carol}' AND device_id = '{c2}' RETURNING 1")
    c_cs = upload_cross_signing(carol, c_token)
    mark = mock.count()
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/join", {}, c_token)
    assert st == 200, r
    wait_for(lambda: len(edus(mark, "m.device_list_update", carol)) >= 2, 10)
    announced = {c["device_id"]: c for c in edus(mark, "m.device_list_update", carol)}
    ids = [c["stream_id"] for c in announced.values()]
    check(set(announced) == {c1, c2}, f"each of the user's devices is announced ({sorted(announced)})")
    check(len(set(ids)) == len(ids) == 2, f"each with a stream ID of its own ({ids})")
    check(all(c.get("prev_id") == [] for c in announced.values()), "and no prev_id: the server is to fetch the whole list")

    print("-- a signature on another user's master key")
    mark = mock.count()
    mk, mk_id, _ = c_cs["master"]
    uk, uk_id, usk = a_cs["usk"]
    st, r = hs.req("POST", "/_matrix/client/v3/keys/signatures/upload", {carol: {mk_id.split(":", 1)[1]: signed(mk, alice, uk_id, usk)}}, a_token)
    assert st == 200 and not r.get("failures"), r
    st, r = hs.req("POST", "/_matrix/client/v3/keys/query", {"device_keys": {carol: []}}, a_token)
    check(uk_id in r.get("master_keys", {}).get(carol, {}).get("signatures", {}).get(alice, {}), "the signer sees their signature on the key")
    time.sleep(2)
    sent = edus(mark, "m.signing_key_update", carol) + edus(mark, "m.device_list_update", carol)
    check(not sent, f"no server hears of it ({len(sent)} updates for carol)")
    st, r = mock.fed(hs.fed, "GET", f"/_matrix/federation/v1/user/devices/{q(carol)}")
    check(alice not in r.get("master_key", {}).get("signatures", {}), "nor sees it on the key")

    print("-- signed devices are sent once more, once")
    marker = os.path.join(hs.work, "data", "done-reannounce-signed-devices")
    check(os.path.exists(marker) and open(marker).read() == "0\n", "the first start found nothing to send and noted it")
    os.remove(marker)
    hs.stop()
    mark = mock.count()
    hs.start()
    got = wait_for(lambda: edus(mark, "m.device_list_update", alice, a_dev), 15)
    again = edus(mark, "m.device_list_update", alice, a_dev)
    check(got and again[0].get("keys", {}).get("signatures", {}).get(alice, {}).get(sk_id),
          "a start without the note sends the signed device again, signature and all")
    lines = hs.log_lines("again (once)")
    check(len(lines) == 2 and "devices=1" in lines[-1] and wait_for(lambda: os.path.exists(marker) and open(marker).read() == "1\n", 5),
          f"says how many in the log, and notes it ({[l.split('INFO')[-1].strip()[:80] for l in lines]})")
    hs.stop()
    mark = mock.count()
    hs.start()
    time.sleep(3)
    check(not edus(mark, "m.device_list_update", alice) and len(hs.log_lines("again (once)")) == 2, "the next start does not send them again")


if __name__ == "__main__":
    run_lab(["mock"], run)
