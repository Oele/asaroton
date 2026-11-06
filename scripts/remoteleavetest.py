#!/usr/bin/env python3
"""Leaving a room the server has no record of, end to end.

After a migration that left rooms out, other servers still list accounts of this server as
members of rooms it knows nothing about. `POST /_asaroton/admin/v1/remote_leave` sends a proper
leave through a server in the room, and `scripts/leave-unrecorded.py` drives it from a list.
Events that keep arriving for such a room are refused without a warning for each.

The lab: a mock server hosts rooms the homeserver has never heard of. In the first it lists
the homeserver's user as a member; in the second it does not; a third it no longer has, and a
fourth bans the homeserver. A second mock has no such rooms, and a third server is not there.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/remoteleavetest.py [--keep]
"""
import base64, collections, hashlib, hmac, json, os, re, subprocess, sys, threading, time, urllib.parse, urllib.request, uuid

from nacl.signing import VerifyKey

from fedlab import KEPT_TOP, SERVER_NAME, SHARED_SECRET, canonical, check, free_port, run_lab, signed_event


def run(hs, peers):
    mock, silent = peers["mock"], peers["silent"]
    spam, spam_token = hs.register("spam")
    other, _ = hs.register("other")
    member_room, empty_room = f"!member:{mock.name}", f"!empty:{mock.name}"
    gone_room, banning_room = f"!gone:{mock.name}", f"!banning:{mock.name}"
    unq = urllib.parse.unquote
    templates, leaves = [], []

    def make_leave(handler, m):
        room, user = unq(m.group(1)), unq(m.group(2))
        templates.append((room, user))
        if room == gone_room:
            return handler.answer(404, {"errcode": "M_NOT_FOUND", "error": f"Could not find room_version for {room}"})
        if room == banning_room:
            return handler.answer(403, {"errcode": "M_FORBIDDEN", "error": "Server is banned from room"})
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

    # A big room that both mocks are in. Each takes a while over a leave, and notes how many it
    # was working on at a time.
    big_room = f"!big:{mock.name}"
    at_work, most_at_work, taken = collections.Counter(), collections.Counter(), collections.Counter()
    counting = threading.Lock()

    def slow_make_leave(peer):
        def serve(handler, m):
            with counting:
                at_work[peer.name] += 1
                most_at_work[peer.name] = max(most_at_work[peer.name], at_work[peer.name])
            time.sleep(0.3)
            handler.answer(200, {"room_version": "11", "event": {
                "type": "m.room.member", "room_id": unq(m.group(1)), "sender": unq(m.group(2)), "state_key": unq(m.group(2)), "content": {"membership": "leave"},
                "auth_events": ["$" + "a" * 43], "prev_events": ["$" + "c" * 43], "depth": 7, "origin_server_ts": int(time.time() * 1000)}})
        return serve

    def slow_send_leave(peer):
        def serve(handler, m, body):
            with counting:
                at_work[peer.name] -= 1
                taken[peer.name] += 1
            handler.answer(200, {})
        return serve

    for peer in (mock, silent):
        peer.routes.insert(0, (re.compile(r"/_matrix/federation/v1/make_leave/((?:%21|!)big[^/]+)/([^/?]+)"), slow_make_leave(peer)))
        peer.put_routes.insert(0, (re.compile(r"/_matrix/federation/v2/send_leave/((?:%21|!)big[^/]+)/([^/?]+)"), slow_send_leave(peer)))

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
        f.write(f"# room\tuser\tservers\n{member_room}\t{other}\t{silent.name},{mock.name}\n{empty_room}\t{other}\t{mock.name}\n"
                f"{gone_room}\t{other}\t{mock.name}\n{banning_room}\t{other}\t{mock.name},{silent.name}\n{gone_room}\t{spam}\t{mock.name}\n")
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "leave-unrecorded.py")

    def run_script(path, *extra):
        return subprocess.run([sys.executable, script, path, "--config", os.path.join(work, "asaroton.toml"), "--pace", "0", *extra], capture_output=True, text=True)

    def recorded(path):
        with open(path + ".done") as f:
            return [line.rstrip("\n").split("\t") for line in f]

    before = len(leaves)
    dry = run_script(listing, "--dry-run")
    check(dry.returncode == 0 and dry.stdout.count("would leave") == 5 and len(leaves) == before, "a dry run lists the memberships and sends nothing")
    asked = len(templates)
    ran = run_script(listing)
    last = ran.stdout.strip().splitlines()[-1:]
    check(ran.returncode == 0 and "left: 1, nothing to leave: 1, room gone: 2, server banned: 1, failed: 0" in ran.stdout,
          f"a run leaves the one room and says why there was nothing to do in the others ({ran.returncode} {last} {ran.stderr.strip()[-200:]})")
    check(len(leaves) == before + 1 and leaves[-1][2].get("sender") == other, "the leave went through the second server listed, the first not answering it")
    rows = recorded(listing)
    check([row[2] for row in rows] == ["left", "nothing-to-leave", "room-gone", "server-banned", "room-gone"], f"the outcomes are recorded ({[row[2] for row in rows]})")
    check(len(rows) == 5 and silent.name + ": refused" in rows[0][4] and mock.name + ": left" in rows[0][4], "with what each server answered")
    check(len(rows) == 5 and silent.name not in rows[3][4], "a server is not asked once another said the room bans this one")
    check(len(rows) == 5 and "not asked" in rows[4][4] and len(templates) == asked + 4, f"nor twice about a room it said it no longer has ({len(templates) - asked} requests for 5 memberships)")
    again = run_script(listing)
    check(again.returncode == 0 and "5 done already, 0 to go" in again.stdout and len(leaves) == before + 1, "a second run finds nothing left to do")
    check(SHARED_SECRET not in ran.stdout + ran.stderr + dry.stdout, "the script does not print the secret")

    # A server that is not there: nothing is settled, and it is not asked for ever.
    absent = f"127.0.0.1:{free_port()}"
    unsettled = os.path.join(work, "unsettled.tsv")
    with open(unsettled, "w") as f:
        f.write(f"{gone_room}\t{spam}\t{absent},{mock.name}\n" + "".join(f"!r{n}:{mock.name}\t{spam}\t{absent}\n" for n in range(3)))
    asked = len(templates)
    ran = run_script(unsettled)
    rows = recorded(unsettled)
    check(ran.returncode == 1 and "failed: 4" in ran.stdout and [row[2] for row in rows] == ["failed"] * 4, f"with a server that cannot be reached the leaves fail ({ran.returncode} {ran.stdout.strip().splitlines()[-2:]})")
    check(len(rows) == 4 and absent + ": unreachable" in rows[0][4] and mock.name + ": room-unknown" in rows[0][4],
          f"a room is not called gone while a server that may still have it was not heard ({rows[0][3:] if rows else None})")
    check(len(rows) == 4 and "not asked" not in rows[2][4] and "not asked" in rows[3][4] and "could not be reached: " + absent in ran.stdout,
          "after three tries in a row the server is no longer asked")
    check(len(templates) == asked + 1, f"the mock was asked once in that run ({len(templates) - asked})")
    again = run_script(unsettled, "--dry-run")
    check("0 done already, 4 to go" in again.stdout, "and a later run tries the failed ones again")

    # Several at a time.
    many = os.path.join(work, "many.tsv")
    with open(many, "w") as f:
        f.write("".join(f"!e{n}:{mock.name}\t{other}\t{silent.name},{mock.name}\n" for n in range(12)))
    started = time.time()
    ran = run_script(many, "--parallel", "4", "--pace", "0.5")
    took = time.time() - started
    rows = recorded(many)
    check(ran.returncode == 0 and "nothing to leave: 12" in ran.stdout and len({row[0] for row in rows}) == 12 and all(row[2] == "nothing-to-leave" for row in rows),
          f"four at a time settle twelve memberships, each recorded once ({ran.returncode} {len(rows)} rows {ran.stderr.strip()[-200:]})")
    check(took < 4.5, f"in less time than one at a time would take ({took:.1f} s; the pauses alone are 6 s in sequence)")
    check(all(silent.name + ": refused (not asked" in row[4] for row in rows[-4:]), "a server that keeps refusing is no longer asked")

    # The leaves of one room: one at a time for each server, spread over those that take them.
    users = [hs.register(f"bulk{n}")[0] for n in range(8)]
    bulk = os.path.join(work, "bulk.tsv")
    with open(bulk, "w") as f:
        f.write("".join(f"{big_room}\t{user}\t{mock.name},{silent.name}\n" for user in users))
    ran = run_script(bulk, "--parallel", "4")
    check(ran.returncode == 0 and "left: 8," in ran.stdout, f"eight accounts leave a room that two servers are in ({ran.returncode} {ran.stdout.strip().splitlines()[-1:]} {ran.stderr.strip()[-200:]})")
    check(most_at_work[mock.name] == 1 and most_at_work[silent.name] == 1, f"neither server is asked for two leaves at once ({dict(most_at_work)})")
    check(taken[mock.name] >= 2 and taken[silent.name] >= 2 and sum(taken.values()) == 8, f"and both take their share ({dict(taken)})")

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
