#!/usr/bin/env python3
"""Email addresses on accounts (`[email]`), verified by this server without an identity server.

A code goes to the address (`requestToken`); it comes back through the client (`submit_url`) or
through the link in the mail; the confirmed address goes on the account with the user's
password (`/account/3pid/add`), shows in `/account/3pid` and comes off again
(`/account/3pid/delete`). An address belongs to one account; repeated requests are idempotent
by `send_attempt`; wrong codes run out; a deactivated account frees its addresses.

The lab sends its mail into a directory (`transport = "directory"`); the test reads the codes
from there.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/emailtest.py [--keep]
"""
import email, glob, os, re, tempfile, urllib.error, urllib.parse, urllib.request

from fedlab import check, run_lab

MAILDIR = tempfile.mkdtemp(prefix="emailtest-mail-")
SUBMIT = "/_matrix/client/unstable/add_threepid/email/submit_token"


def mails():
    out = []
    for path in sorted(glob.glob(os.path.join(MAILDIR, "*.eml"))):
        with open(path, "rb") as f:
            msg = email.message_from_binary_file(f)
        body = msg.get_payload(decode=True).decode()
        code = re.search(r"The code is: (\d{6})", body)
        link = re.search(r"(https?://\S+submit_token\S+)", body)
        out.append({"to": msg["To"], "subject": msg["Subject"], "code": code and code.group(1), "link": link and link.group(1)})
    return out


def run(hs, peers):
    alice, a_token = hs.register("alice")
    bob, b_token = hs.register("bob")

    st, caps = hs.req("GET", "/_matrix/client/v3/capabilities", None, a_token)
    check(caps.get("capabilities", {}).get("m.3pid_changes", {}).get("enabled") is True, "m.3pid_changes is on with mail set up")

    def request(token, address, secret, attempt):
        return hs.req("POST", "/_matrix/client/v3/account/3pid/email/requestToken",
                      {"client_secret": secret, "email": address, "send_attempt": attempt}, token)

    def add(token, sid, secret):
        st, r = hs.req("POST", "/_matrix/client/v3/account/3pid/add", {"sid": sid, "client_secret": secret}, token)
        if st != 401 or "session" not in r:
            return st, r
        user = "alice" if token == a_token else "bob"
        return hs.req("POST", "/_matrix/client/v3/account/3pid/add",
                      {"sid": sid, "client_secret": secret, "auth": {"type": "m.login.password", "session": r["session"],
                       "identifier": {"type": "m.id.user", "user": user}, "password": "pwpwpwpw"}}, token)

    def submit(sid, secret, token):
        return hs.req("POST", SUBMIT, {"sid": sid, "client_secret": secret, "token": token})

    # A code by mail, idempotent by send_attempt, not twice a minute.
    st, r = request(a_token, " Ann@Example.ORG ", "secret-a1", 1)
    sid = r.get("sid")
    check(st == 200 and sid and r.get("submit_url", "").endswith(SUBMIT), f"requestToken answers with a sid and the submit_url ({st}, {r.get('submit_url')})")
    sent = mails()
    check(len(sent) == 1 and sent[0]["to"] == "ann@example.org" and sent[0]["code"] and sent[0]["code"] in sent[0]["subject"],
          f"one mail went to the address, lower-cased, with a six-digit code ({[m['to'] for m in sent]})")
    st, r = request(a_token, "ann@example.org", "secret-a1", 1)
    check(st == 200 and r.get("sid") == sid and len(mails()) == 1, f"the same send_attempt again: same sid, no second mail ({st})")
    st, r = request(a_token, "ann@example.org", "secret-a1", 2)
    check(st == 429 and r.get("errcode") == "M_LIMIT_EXCEEDED", f"a new send_attempt within a minute is held back ({st}, {r.get('errcode')})")

    # Not before the code is back; a wrong code is refused; the right one works.
    st, r = add(a_token, sid, "secret-a1")
    check(st == 400 and r.get("errcode") == "M_THREEPID_AUTH_FAILED", f"adding before the code is back is refused ({st}, {r.get('errcode')})")
    wrong = "000000" if sent[0]["code"] != "000000" else "111111"
    st, r = submit(sid, "secret-a1", wrong)
    check(st == 400 and r.get("errcode") == "M_THREEPID_AUTH_FAILED", f"a wrong code is refused ({st}, {r.get('errcode')})")
    st, r = submit(sid, "other-secret", sent[0]["code"])
    check(st == 400, f"the right code with another client secret is refused ({st})")
    st, r = submit(sid, "secret-a1", sent[0]["code"])
    check(st == 200 and r.get("success") is True, f"the right code confirms the address ({st}, {r})")
    st, r = add(a_token, sid, "secret-a1")
    check(st == 200, f"with her password Alice adds it to her account ({st}, {r.get('errcode')})")
    st, r = hs.req("GET", "/_matrix/client/v3/account/3pid", None, a_token)
    tp = r.get("threepids", [])
    check(st == 200 and [(t["medium"], t["address"]) for t in tp] == [("email", "ann@example.org")] and tp[0]["validated_at"] > 0,
          f"/account/3pid lists it ({tp})")

    # One account per address.
    st, r = request(b_token, "ANN@example.org", "secret-b1", 1)
    check(st == 400 and r.get("errcode") == "M_THREEPID_IN_USE", f"another account cannot claim it ({st}, {r.get('errcode')})")

    # The link in the mail confirms it as well.
    st, r = request(b_token, "bob@example.org", "secret-b2", 1)
    bob_mail = [m for m in mails() if m["to"] == "bob@example.org"][-1]
    parts = urllib.parse.urlsplit(bob_mail["link"])
    with urllib.request.urlopen(hs.url + parts.path + "?" + parts.query, timeout=30) as resp:
        page = resp.read().decode()
    check("confirmed" in page, "opening the link in the mail confirms the address")
    st, r = add(b_token, r.get("sid"), "secret-b2")
    check(st == 200, f"and Bob adds it ({st})")

    # Wrong codes run out.
    st, r = request(b_token, "spare@example.org", "secret-b3", 1)
    sid3 = r.get("sid")
    code3 = [m for m in mails() if m["to"] == "spare@example.org"][-1]["code"]
    bad = "000000" if code3 != "000000" else "111111"
    for _ in range(5):
        submit(sid3, "secret-b3", bad)
    st, r = submit(sid3, "secret-b3", code3)
    check(st == 400, f"after five wrong codes even the right one is refused ({st})")

    # Off again, and freed by deactivation.
    st, r = hs.req("POST", "/_matrix/client/v3/account/3pid/delete", {"medium": "email", "address": "Ann@Example.org"}, a_token)
    check(st == 200 and r.get("id_server_unbind_result") == "no-support", f"Alice takes her address off ({st}, {r})")
    st, r = hs.req("GET", "/_matrix/client/v3/account/3pid", None, a_token)
    check(r.get("threepids") == [], "and the list is empty")
    st, r = request(b_token, "ann@example.org", "secret-b4", 1)
    check(st == 200, f"so another account may now ask for it ({st})")
    st, r = hs.req("POST", "/_matrix/client/v3/account/deactivate", {}, b_token)
    st, r = hs.req("POST", "/_matrix/client/v3/account/deactivate",
                   {"auth": {"type": "m.login.password", "session": r.get("session"),
                             "identifier": {"type": "m.id.user", "user": "bob"}, "password": "pwpwpwpw"}}, b_token)
    st, r = request(a_token, "bob@example.org", "secret-a2", 1)
    check(st == 200, f"a deactivated account frees its addresses ({st}, {r.get('errcode')})")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra=f"""
[email]
enabled = true
from = "Lab homeserver <matrix@sender.test>"
transport = "directory"
directory = "{MAILDIR}"
""")
