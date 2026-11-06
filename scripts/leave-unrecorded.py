#!/usr/bin/env python3
"""Make accounts of this server leave rooms that the server has no record of.

    sudo scripts/leave-unrecorded.py LIST [--dry-run] [--limit N] [--pace SECONDS]
                                          [--config /etc/asaroton/asaroton.toml] [--done FILE]

After a migration that left rooms out, other servers still list this server's accounts as
members of those rooms (or as invited to them): the rooms' events keep being sent here and are
refused one by one, and the accounts stay visible in other people's member lists. This sends a
proper leave for each, through a server that is in the room.

LIST is a tab-separated file with one membership per line:

    room_id <TAB> user_id <TAB> server[,server...]

where the servers are ones that are in the room; they are tried in order. Lines starting with
`#` are skipped.

Every leave is an event in somebody else's room, signed by this server. Look at the list
before running this, and try `--dry-run` and `--limit 3` first. Outcomes are appended to the
`--done` file (default: LIST + ".done"); a membership recorded there as done is not tried again,
so the run can be interrupted and repeated. A failure that says the account is not in the
room counts as done.

Uses the server's own client listener and the registration shared secret from its
configuration file, hence sudo. Run it on the server's host.
"""
import argparse, hashlib, hmac, json, sys, time, tomllib, urllib.error, urllib.request

# What a server in the room answers when there is nothing to leave.
NOTHING_TO_LEAVE = ("not in room", "not in the room", "not a member", "M_NOT_FOUND", "unknown room", "Unknown room", "is banned", "already left")


def main():
    parser = argparse.ArgumentParser(description="Leave rooms this server has no record of.")
    parser.add_argument("list", help="tab-separated: room_id, user_id, servers in the room (comma-separated)")
    parser.add_argument("--dry-run", action="store_true", help="show what would be done")
    parser.add_argument("--limit", type=int, default=0, help="stop after this many leaves were tried")
    parser.add_argument("--pace", type=float, default=2.0, help="seconds between leaves (default 2)")
    parser.add_argument("--config", default="/etc/asaroton/asaroton.toml")
    parser.add_argument("--done", help="where outcomes are recorded (default: LIST.done)")
    args = parser.parse_args()
    done_path = args.done or args.list + ".done"

    wanted = []
    with open(args.list) as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 3 or not parts[0].startswith("!") or not parts[1].startswith("@") or not parts[2].strip():
                sys.exit(f"{args.list}:{n}: expected room_id<TAB>user_id<TAB>servers")
            wanted.append((parts[0], parts[1], [s for s in parts[2].split(",") if s]))

    done = set()
    try:
        with open(done_path) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 3 and parts[2] in ("left", "nothing-to-leave"):
                    done.add((parts[0], parts[1]))
    except FileNotFoundError:
        pass
    todo = [w for w in wanted if (w[0], w[1]) not in done]
    print(f"{len(wanted)} memberships listed, {len(wanted) - len(todo)} done already, {len(todo)} to go")
    if args.limit:
        todo = todo[: args.limit]
    if args.dry_run:
        for room, user, via in todo:
            print(f"would leave {room} as {user} through {', '.join(via)}")
        return

    try:
        with open(args.config, "rb") as f:
            config = tomllib.load(f)
    except PermissionError:
        sys.exit(f"cannot read {args.config}: run this with sudo")
    secret = config.get("registration", {}).get("shared_secret")
    if not secret:
        sys.exit(f"{args.config} has no registration.shared_secret; set one and restart the server")
    listener = config.get("listeners", {}).get("client", {})
    bind = listener.get("bind", "127.0.0.1:8008").replace("0.0.0.0", "127.0.0.1")
    base = ("https" if listener.get("tls_cert") else "http") + "://" + bind

    def call(method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(base + path, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=150) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"error": raw[:300]}

    counts = {"left": 0, "nothing-to-leave": 0, "failed": 0}
    with open(done_path, "a") as record:
        for n, (room, user, via) in enumerate(todo):
            if n:
                time.sleep(args.pace)
            try:
                status, nonce = call("GET", "/_synapse/admin/v1/register")
                if status != 200:
                    sys.exit(f"the server did not hand out a nonce: {status} {nonce}")
                mac = hmac.new(secret.encode(), b"\0".join([nonce["nonce"].encode(), b"remote_leave", room.encode(), user.encode()]), hashlib.sha1).hexdigest()
                status, answer = call("POST", "/_asaroton/admin/v1/remote_leave", {"room_id": room, "user_id": user, "via": via, "nonce": nonce["nonce"], "mac": mac})
            except urllib.error.URLError as e:
                sys.exit(f"cannot reach the server at {base}: {e.reason}")
            error = str(answer.get("error", ""))
            if status == 200:
                outcome = "left"
            elif status == 403 and "HMAC" in error:
                sys.exit("the server does not accept the shared secret from this configuration file")
            elif any(marker in error for marker in NOTHING_TO_LEAVE):
                outcome = "nothing-to-leave"
            else:
                outcome = "failed"
            counts[outcome] += 1
            record.write("\t".join([room, user, outcome, str(status), error.replace("\t", " ").replace("\n", " ")[:300]]) + "\n")
            record.flush()
            print(f"{outcome:17} {room} {user}" + (f"  ({status} {error[:160]})" if outcome != "left" else ""))
    print(f"left: {counts['left']}, nothing to leave: {counts['nothing-to-leave']}, failed: {counts['failed']}; outcomes are in {done_path}")
    sys.exit(1 if counts["failed"] else 0)


if __name__ == "__main__":
    main()
