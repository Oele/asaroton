#!/usr/bin/env python3
"""Mail over SMTP (`[email] transport = "smtp"`), the way production sends it: plain SMTP to the
machine's own mail server on localhost, which passes it on.

emailtest.py covers the account side with mail written into a directory; this one runs a small
SMTP server on 127.0.0.1 and checks what arrives there:

- the code mail: envelope and headers from `from` to the address, the code in subject and body;
- a recipient the mail server refuses (550): no mail, a 500 for the client, the reason in the
  server log.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/smtptest.py [--keep]
"""
import email, email.utils, re, socketserver, threading

from fedlab import check, run_lab

RECEIVED = []


class Smtp(socketserver.StreamRequestHandler):
    def w(self, line):
        self.wfile.write((line + "\r\n").encode())

    def handle(self):
        sender, rcpt = None, []
        self.w("220 smtp.lab.test ESMTP")
        while True:
            line = self.rfile.readline()
            if not line:
                return
            cmd = line.decode(errors="replace").strip()
            up = cmd.upper()
            if up.startswith(("EHLO", "HELO")):
                self.w("250-smtp.lab.test")
                self.w("250 8BITMIME")
            elif up.startswith("MAIL FROM:"):
                sender, rcpt = cmd[10:].strip(), []
                self.w("250 OK")
            elif up.startswith("RCPT TO:"):
                address = cmd[8:].strip()
                if "refused@" in address:
                    self.w("550 5.1.1 no such user here")
                else:
                    rcpt.append(address)
                    self.w("250 OK")
            elif up == "DATA":
                self.w("354 go on")
                data = []
                while (l := self.rfile.readline()) and l not in (b".\r\n", b".\n"):
                    data.append(l)
                msg = email.message_from_bytes(b"".join(data))
                RECEIVED.append({"sender": sender, "rcpt": rcpt, "from": msg["From"], "to": msg["To"],
                                 "subject": msg["Subject"],
                                 "body": msg.get_payload(decode=True).decode(errors="replace")})
                self.w("250 queued")
            elif up == "QUIT":
                self.w("221 bye")
                return
            else:  # RSET, NOOP
                self.w("250 OK")


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


server = Server(("127.0.0.1", 0), Smtp)
PORT = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()


def run(hs, peers):
    alice, a = hs.register("alice")

    def request(address, secret):
        return hs.req("POST", "/_matrix/client/v3/account/3pid/email/requestToken",
                      {"client_secret": secret, "email": address, "send_attempt": 1}, a)

    st, r = request("ann@example.org", "secret-1")
    got = RECEIVED[-1] if RECEIVED else {}
    code = re.search(r"The code is: (\d{6})", got.get("body", ""))
    check(st == 200 and len(RECEIVED) == 1, f"the code mail went to the mail server over SMTP ({st}, {len(RECEIVED)} mail)")
    check(got.get("sender") == "<noreply@sender.test>" and got.get("rcpt") == ["<ann@example.org>"],
          f"envelope from the configured sender to the address ({got.get('sender')} -> {got.get('rcpt')})")
    check(email.utils.parseaddr(got.get("from") or "") == ("Lab homeserver", "noreply@sender.test")
          and email.utils.parseaddr(got.get("to") or "")[1] == "ann@example.org",
          f"and the same in the headers ({got.get('from')} -> {got.get('to')})")
    check(code and code.group(1) in (got.get("subject") or ""), f"the code is in the body and the subject ({got.get('subject')})")

    st, r = request("refused@example.org", "secret-2")
    check(st == 500 and len(RECEIVED) == 1, f"a recipient the mail server refuses: no mail, a 500 ({st}, {r.get('errcode')})")
    check(any("no such user here" in l for l in hs.log_lines("sending mail failed")), "and the server log says why")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra=f"""
[email]
enabled = true
from = "Lab homeserver <noreply@sender.test>"
smtp_host = "localhost"
smtp_port = {PORT}
""")
