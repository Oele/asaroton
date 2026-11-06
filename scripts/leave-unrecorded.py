#!/usr/bin/env python3
"""Make accounts of this server leave rooms that the server has no record of.

    sudo scripts/leave-unrecorded.py LIST [--dry-run] [--limit N] [--pace SECONDS] [--parallel N]
                                          [--config /etc/asaroton/asaroton.toml] [--done FILE]

After a migration that left rooms out, other servers still list this server's accounts as
members of those rooms (or as invited to them): the rooms' events keep being sent here and are
refused one by one, and the accounts stay visible in other people's member lists. This sends a
proper leave for each, through a server that is in the room.

LIST is a tab-separated file with one membership per line:

    room_id <TAB> user_id <TAB> server[,server...]

where the servers are ones that are in the room, or were when the list was made. Lines
starting with `#` are skipped.

Every leave is an event in somebody else's room, signed by this server. Look at the list
before running this, and try `--dry-run` and `--limit 3` first.

The servers of a membership are asked one at a time, in the order listed, until one settles
it. What comes of it is one of:

    left              a server accepted the leave
    nothing-to-leave  a server that has the room says the account is not a member of it
    room-gone         every server listed answered, and none of them has the room any more
    server-banned     the room bans this server, so no server in it accepts anything from here
    failed            none of the above: the servers could not be reached, or refused

Outcomes are appended to the `--done` file (default: LIST + ".done") with what each server
answered. Only a `failed` membership is tried again by a later run, so a run can be
interrupted and repeated. Within a run, a server that could not be reached three times in a
row is not asked again, and neither is a server about a room it said it no longer has.

`--parallel N` works on N memberships at a time, each of them pausing `--pace` seconds
between its leaves. Most of a slow run is spent waiting for servers that do not answer, which
this shortens; the servers that do answer get N requests at a time, so keep it modest.

Uses the server's own client listener and the registration shared secret from its
configuration file, hence sudo. Run it on the server's host.
"""
import argparse, collections, concurrent.futures, hashlib, hmac, json, re, sys, threading, time, tomllib
import urllib.error, urllib.request

SETTLED = ("left", "nothing-to-leave", "room-gone", "server-banned")
PREFIX = "No server completed the leave: "
GIVE_UP_AFTER = 3  # times in a row that a server could not be reached

# How a server's answer is read; the first kind with a matching marker wins. The texts are
# Synapse's, and this server's own for a server it cannot reach or that has no federation API.
# An answer that is not recognised counts as a refusal, and is recorded as it was given.
ANSWERS = (
    ("server-banned", ("erver is banned from",)),
    ("not-a-member", ("not in room", "not in the room", "not a member", "is banned", "already left")),
    ("room-unknown", ("Could not find room_version", "nknown room", "Room is unknown", "oom does not exist", "not known to this server")),
    ("unreachable", ("error sending request", "could not resolve server", "timed out", " answered 5", " answered 404", "Unrecognized request")),
)


class Fatal(Exception):
    """Something that makes going on pointless, such as this server not answering."""


def kind_of(status, error):
    """What one server's answer to a leave amounts to."""
    if status == 200:
        return "left"
    for kind, markers in ANSWERS:
        if any(marker in error for marker in markers):
            return kind
    return "refused"


def outcome_of(kinds):
    """What the answers of the servers asked amount to for the membership."""
    if "left" in kinds:
        return "left"
    if "not-a-member" in kinds:
        return "nothing-to-leave"
    if "server-banned" in kinds:
        return "server-banned"
    if kinds and all(kind == "room-unknown" for kind in kinds):
        return "room-gone"
    return "failed"


def main():
    parser = argparse.ArgumentParser(description="Leave rooms this server has no record of.")
    parser.add_argument("list", help="tab-separated: room_id, user_id, servers in the room (comma-separated)")
    parser.add_argument("--dry-run", action="store_true", help="show what would be done")
    parser.add_argument("--limit", type=int, default=0, help="stop after this many leaves were tried")
    parser.add_argument("--pace", type=float, default=2.0, help="seconds between leaves (default 2)")
    parser.add_argument("--parallel", type=int, default=1, help="memberships worked on at a time (default 1)")
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

    # The last outcome recorded for a membership is the one that counts.
    recorded = {}
    try:
        with open(done_path) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 3:
                    recorded[(parts[0], parts[1])] = parts[2]
    except FileNotFoundError:
        pass
    todo = [w for w in wanted if recorded.get((w[0], w[1])) not in SETTLED]
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

    def ask(room, user, server):
        """Have the leave made through one server; what came of it, and the server's own words."""
        try:
            status, nonce = call("GET", "/_synapse/admin/v1/register")
            if status != 200:
                raise Fatal(f"the server did not hand out a nonce: {status} {nonce}")
            mac = hmac.new(secret.encode(), b"\0".join([nonce["nonce"].encode(), b"remote_leave", room.encode(), user.encode()]), hashlib.sha1).hexdigest()
            status, answer = call("POST", "/_asaroton/admin/v1/remote_leave", {"room_id": room, "user_id": user, "via": [server], "nonce": nonce["nonce"], "mac": mac})
        except (urllib.error.URLError, TimeoutError) as e:
            if isinstance(getattr(e, "reason", e), TimeoutError):
                # This server was still waiting for the other one.
                return "unreachable", "no answer in two and a half minutes"
            raise Fatal(f"cannot reach the server at {base}: {getattr(e, 'reason', e)}") from None
        error = str(answer.get("error", ""))
        if status == 403 and "HMAC" in error:
            raise Fatal("the server does not accept the shared secret from this configuration file")
        if status not in (200, 502):
            # This homeserver turned the request down itself; no other server was asked.
            return "turned-down", f"{status} {error}"[:160]
        said = error.removeprefix(PREFIX).removeprefix(server + ": ").removeprefix(server + " ")
        said = re.sub(r" for url \([^)]*\)", "", said)
        return kind_of(status, error), " ".join(said.split())[:160]

    unreachable = collections.Counter()  # server -> times in a row it could not be reached
    without_room = set()  # (server, room): the server said it does not have the room
    lock = threading.Lock()
    stop = threading.Event()

    def settle(membership):
        """Ask the servers of one membership in turn; what each of them answered."""
        room, user, via = membership
        answers = []
        for server in via:
            if stop.is_set():
                return None
            with lock:
                given_up = unreachable[server] >= GIVE_UP_AFTER
                said_so = (server, room) in without_room
            if given_up:
                answers.append((server, "unreachable", "not asked: it could not be reached earlier in this run"))
                continue
            if said_so:
                answers.append((server, "room-unknown", "not asked: it said so for another account"))
                continue
            kind, said = ask(room, user, server)
            answers.append((server, kind, said))
            with lock:
                unreachable[server] = unreachable[server] + 1 if kind == "unreachable" else 0
                if kind == "room-unknown":
                    without_room.add((server, room))
            if kind in ("left", "not-a-member", "server-banned", "turned-down"):
                break
        time.sleep(args.pace)
        return answers

    counts = collections.Counter()
    fatal = None
    with open(done_path, "a") as record, concurrent.futures.ThreadPoolExecutor(max(1, args.parallel)) as pool:
        futures = {pool.submit(settle, membership): membership for membership in todo}
        try:
            for future in concurrent.futures.as_completed(futures):
                room, user, _ = futures[future]
                try:
                    answers = future.result()
                except Fatal as e:
                    fatal = fatal or str(e)
                    stop.set()
                    continue
                if answers is None:
                    continue
                kinds = [kind for _, kind, _ in answers]
                outcome = outcome_of(kinds)
                counts[outcome] += 1
                summary = ", ".join(f"{count} {kind}" for kind, count in collections.Counter(kinds).most_common())
                detail = "; ".join(f"{server}: {kind}" + (f" ({said})" if said else "") for server, kind, said in answers)
                record.write("\t".join([room, user, outcome, summary, detail]) + "\n")
                record.flush()
                through = f"  through {answers[-1][0]}" if outcome == "left" else f"  ({summary})"
                print(f"{outcome:17} {room} {user}{through}", flush=True)
        except KeyboardInterrupt:
            stop.set()
            for future in futures:
                future.cancel()
            fatal = "interrupted"
    print(f"left: {counts['left']}, nothing to leave: {counts['nothing-to-leave']}, room gone: {counts['room-gone']}, "
          f"server banned: {counts['server-banned']}, failed: {counts['failed']}; what each server answered is in {done_path}")
    gone = sorted(server for server, times in unreachable.items() if times >= GIVE_UP_AFTER)
    if gone:
        print(f"could not be reached: {', '.join(gone)}")
    if fatal:
        sys.exit(f"stopped early: {fatal}")
    sys.exit(1 if counts["failed"] else 0)


if __name__ == "__main__":
    main()
