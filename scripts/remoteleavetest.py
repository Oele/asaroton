#!/usr/bin/env python3
"""Leaving a room the server has no record of, end to end.

After a migration that left rooms out, other servers still list accounts of this server as
members of rooms it knows nothing about. `POST /_asaroton/admin/v1/remote_leave` sends a proper
leave through a server in the room, and `scripts/leave-unrecorded.py` drives it from a list.
Events that keep arriving for such a room are refused without a warning for each.

The lab: a mock server hosts two rooms the homeserver has never heard of. In the first it
lists the homeserver's user as a member; in the second it does not.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/remoteleavetest.py [--keep]
"""
import base64, hashlib, hmac, json, os, re, subprocess, sys, time, urllib.parse, urllib.request, uuid

from nacl.signing import VerifyKey

from fedlab import KEPT_TOP, SERVER_NAME, SHARED_SECRET, canonical, check, run_lab, signed_event


def run(hs, peers):
    mock, silent = peers["mock"], peers["silent"]
    spam, spam_token = hs.register("spam")
    other, _ = hs.register("other")
    member_room, empty_room = f"!member:{mock.name}", f"!empty:{mock.name}"
    unq = urllib.parse.unquote
    templates, leaves = [], []

    def make_leave(handler, m):
        room, user = unq(m.group(1)), unq(m.group(2))
        templates.append((room, user))
        if room != member_room:
            return handler.answer(403, {"errcode": "M_FORBIDDEN", "error": "User is not in the room"})
        handler.answer(200, {"room_version": "11", "event": {
            "type": "m.room.member", "room_id": room, "sender": user, "state_key": user, "content": {"membership": "leave"},
            "auth_events": ["$" + "a" * 43, "$" + "b" * 43], "prev_events": ["$" + "c" * 43], "depth": 12,
            "origin_server_ts": int(time.time() * 1000)}})

    def send_leave(handler, m, body):
        leaves.append((unq(m.group(1)), unq(m.group(2)), body))
        handler.answer(200, {})

    mock.routes.append((re.compile(r"/_matrix/federation/v1/make_leave/([^/]+)/([^/?]+)"), make_leave))
    mock.put_routes.append((re.compile(r"/_matrix/federation/v2/send_leave/([^/]+)/([^/?]+)"), send_leave))

    def post(body, token=None):
        return hs.req("POST", "/_asaroton/admin/v1/remote_leave", body, token)

    def signed(room, user, secret=SHARED_SECRET, nonce=None):
        if nonce is None:
            st, r = hs.req("GET", "/_synapse/admin/v1/register")
            assert st == 200, r
            nonce = r["nonce"]
        mac = hmac.new(secret.encode(), b"\0".join([nonce.encode(), b"remote_leave", room.encode(), user.encode()]), hashlib.sha1).hexdigest()
        return {"nonce": nonce, "mac": mac}

    print("-- who may ask")
    request = {"room_id": member_room, "user_id": spam, "via": [mock.name]}
    st, r = post(request)
    check(st == 401, f"nobody without credentials ({st})")
    st, r = post(request, spam_token)
    check(st == 403, f"not an ordinary user ({st})")
    st, r = post({**request, **signed(member_room, spam, secret="guess")})
    check(st == 403, f"not somebody who guesses the secret ({st})")
    good = signed(empty_room, spam)
    st, r = post({**request, **good})
    check(st == 403, f"a signature for one room does not serve for another ({st})")
    check(not leaves and not templates, "and none of that reached the other server")

    print("-- the leave")
    st, r = post({**request, **signed(member_room, spam)})
    check(st == 200, f"an operator with the shared secret gets the leave done ({st} {r})")
    check(len(leaves) == 1 and leaves[0][0] == member_room, f"the mock received one leave ({len(leaves)})")
    if leaves:
        _, event_id, event = leaves[0]
        check(event.get("sender") == spam and event.get("state_key") == spam and event.get("content") == {"membership": "leave"} and event.get("depth") == 12,
              "it is the account's leave, built on what the room's server prepared")
        with urllib.request.urlopen(hs.fed + "/_matrix/key/v2/server") as resp:
            keys = json.load(resp)["verify_keys"]
        key_id, signature = next(iter(event["signatures"][SERVER_NAME].items()))
        redacted = {k: v for k, v in event.items() if k in KEPT_TOP}
        pad = lambda text: text + "=" * (-len(text) % 4)
        try:
            VerifyKey(base64.b64decode(pad(keys[key_id]["key"]))).verify(canonical(redacted), base64.b64decode(pad(signature)))
            valid = True
        except Exception as e:
            valid = str(e)
        check(valid is True, f"signed with the homeserver's key ({valid})")
        expected_id = "$" + base64.urlsafe_b64encode(hashlib.sha256(canonical(redacted)).digest()).decode().rstrip("=")
        check(event_id == expected_id, "and sent under its event ID")
    reused = signed(member_room, spam)
    st, r = post({**request, **reused})
    st2, r2 = post({**request, **reused})
    check(st == 200 and st2 == 403, f"a signature works once ({st} then {st2})")

    print("-- what it refuses or reports")
    st, r = post({"room_id": empty_room, "user_id": spam, "via": [mock.name], **signed(empty_room, spam)})
    check(st == 502 and "not in the room" in r.get("error", ""), f"the other server's refusal is passed on ({st} {r.get('error', '')[:120]})")
    st, r = post({"room_id": member_room, "user_id": spam, "via": [], **signed(member_room, spam)})
    check(st == 400, f"a request that names no server is turned down ({st})")
    st, r = post({"room_id": member_room, "user_id": f"@x:{mock.name}", "via": [mock.name], **signed(member_room, f"@x:{mock.name}")})
    check(st == 400, f"so is one for another server's user ({st})")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, spam_token)
    local_room = created["room_id"]
    st, r = post({"room_id": local_room, "user_id": spam, "via": [mock.name], **signed(local_room, spam)})
    check(st == 400 and "ordinary leave" in r.get("error", ""), f"and one for a membership the server does know ({st})")

    print("-- the script")
    work = os.path.dirname(hs.log)
    listing = os.path.join(work, "leaves.tsv")
    with open(listing, "w") as f:
        f.write(f"# room\tuser\tservers\n{member_room}\t{other}\t{silent.name},{mock.name}\n{empty_room}\t{other}\t{mock.name}\n")
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "leave-unrecorded.py")
    command = [sys.executable, script, listing, "--config", os.path.join(work, "asaroton.toml"), "--pace", "0"]
    before = len(leaves)
    dry = subprocess.run(command + ["--dry-run"], capture_output=True, text=True)
    check(dry.returncode == 0 and dry.stdout.count("would leave") == 2 and len(leaves) == before, "a dry run lists both memberships and sends nothing")
    ran = subprocess.run(command, capture_output=True, text=True)
    check(ran.returncode == 0 and "left: 1, nothing to leave: 1, failed: 0" in ran.stdout, f"a run leaves the one room and notes there is nothing to leave in the other ({ran.returncode} {ran.stdout.strip().splitlines()[-1:]} {ran.stderr.strip()[-200:]})")
    check(len(leaves) == before + 1 and leaves[-1][2].get("sender") == other, "the leave went through the second server listed, the first not answering it")
    again = subprocess.run(command, capture_output=True, text=True)
    check(again.returncode == 0 and "2 done already, 0 to go" in again.stdout and len(leaves) == before + 1, "a second run finds nothing left to do")
    check(SHARED_SECRET not in ran.stdout + ran.stderr + dry.stdout, "the script does not print the secret")

    print("-- events that still arrive for such a room")
    _, stray = signed_event(mock.name, mock.key, "ed25519:k1", {
        "type": "m.room.message", "room_id": member_room, "sender": f"@a:{mock.name}", "content": {"msgtype": "m.text", "body": "still here?"},
        "auth_events": ["$" + "a" * 43], "prev_events": ["$" + "c" * 43], "depth": 13, "origin_server_ts": int(time.time() * 1000)})
    st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}", {"origin": mock.name, "origin_server_ts": int(time.time() * 1000), "pdus": [stray], "edus": []})
    check(st == 200, f"the transaction is answered ({st})")
    quiet = hs.log_lines("refusing an event for a room this server is not in")
    loud = [l for l in hs.log_lines("failed to process PDU") if member_room in l]
    check(len(quiet) == 1 and not loud, f"the event is refused without a warning ({len(quiet)} quiet, {len(loud)} loud)")


if __name__ == "__main__":
    run_lab(["mock", "silent"], run)
