#!/usr/bin/env python3
"""Regression test: a new device is announced only once its one-time keys can be claimed.

    scripts/keyuploadtest.py http://127.0.0.1:18099 [ROUNDS]     (a test server with open registration)

Carol and Dave share a room. Each round Dave signs in on a new device and uploads its device
keys together with 50 one-time keys in a single request, while Carol waits in /sync. The
moment Carol hears of the device (`device_lists.changed`, then `/keys/query` shows it) she
claims a one-time key for it, as a client that is about to send a message does. The claim has
to succeed every time: with no key she could not set up a session, and her message would stay
unreadable on Dave's new device.
"""
import base64, json, os, secrets, sys, threading, time, urllib.error, urllib.parse, urllib.request

HS = sys.argv[1].rstrip("/")
ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 25


def call(method, path, body=None, token=None, timeout=40):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(HS + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: {e.code} {e.read().decode()[:300]}")


def b64():
    return base64.b64encode(os.urandom(32)).decode().rstrip("=")


def register(name):
    password = secrets.token_urlsafe(16)
    r = call("POST", "/_matrix/client/v3/register", {"username": f"{name}{secrets.token_hex(4)}", "password": password, "auth": {"type": "m.login.dummy"}})
    return r["user_id"], r["access_token"], password


def main():
    carol, carol_token, _ = register("carol")
    dave, dave_token, dave_password = register("dave")
    room = call("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, carol_token)["room_id"]
    call("POST", f"/_matrix/client/v3/rooms/{urllib.parse.quote(room)}/join", {}, dave_token)
    since = call("GET", "/_matrix/client/v3/sync?timeout=0", None, carol_token)["next_batch"]

    empty, slowest = 0, 0.0
    for n in range(ROUNDS):
        login = call("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": dave}, "password": dave_password})
        device, token = login["device_id"], login["access_token"]
        # Let whatever the login itself announces pass.
        since = call("GET", f"/_matrix/client/v3/sync?timeout=0&since={since}", None, carol_token)["next_batch"]
        result = {}

        def watch(since=since, device=device):
            deadline = time.time() + 20
            while time.time() < deadline:
                r = call("GET", f"/_matrix/client/v3/sync?timeout=5000&since={since}", None, carol_token)
                since = r["next_batch"]
                result["since"] = since
                if dave not in r.get("device_lists", {}).get("changed", []):
                    continue
                seen = call("POST", "/_matrix/client/v3/keys/query", {"device_keys": {dave: []}}, carol_token)
                if device not in seen.get("device_keys", {}).get(dave, {}):
                    continue
                heard = time.time()
                claimed = call("POST", "/_matrix/client/v3/keys/claim", {"one_time_keys": {dave: {device: "signed_curve25519"}}}, carol_token)
                result["keys"] = claimed.get("one_time_keys", {}).get(dave, {}).get(device, {})
                result["heard"] = heard
                return

        watcher = threading.Thread(target=watch)
        watcher.start()
        time.sleep(0.2)  # Carol is waiting in /sync now
        signature = {dave: {f"ed25519:{device}": b64() + b64()}}
        upload = {
            "device_keys": {
                "user_id": dave,
                "device_id": device,
                "algorithms": ["m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"],
                "keys": {f"curve25519:{device}": b64(), f"ed25519:{device}": b64()},
                "signatures": signature,
            },
            "one_time_keys": {f"signed_curve25519:{secrets.token_hex(3)}{i:02d}": {"key": b64(), "signatures": signature} for i in range(50)},
            "fallback_keys": {f"signed_curve25519:fb{secrets.token_hex(3)}": {"key": b64(), "fallback": True, "signatures": signature}},
        }
        started = time.time()
        counts = call("POST", "/_matrix/client/v3/keys/upload", upload, token)
        took = time.time() - started
        slowest = max(slowest, took)
        watcher.join(25)
        since = result.get("since", since)
        if "keys" not in result:
            sys.exit(f"round {n + 1}: Carol never heard of the new device")
        if not result["keys"]:
            empty += 1
            print(f"round {n + 1}: /keys/claim came back empty {(result['heard'] - started) * 1000:.0f} ms into an upload that took {took * 1000:.0f} ms")
        if counts.get("one_time_key_counts", {}).get("signed_curve25519") != 50:
            sys.exit(f"round {n + 1}: unexpected key counts {counts}")

    print(f"{ROUNDS} rounds, slowest upload {slowest * 1000:.0f} ms, empty claims: {empty}")
    if empty:
        print("FAIL: a device was announced before its one-time keys could be claimed")
    sys.exit(1 if empty else 0)


if __name__ == "__main__":
    main()
