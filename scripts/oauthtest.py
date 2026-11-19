#!/usr/bin/env python3
"""Sign-in through OAuth 2.0 with the authorization server in asaroton's process (`[oauth]`,
lithostroton): discovery, client registration, the authorization code flow with PKCE through the
login and consent pages, the tokens working as asaroton's own, refresh rotation with the retry
window, revocation, the device authorization grant, and password login beside it.

The lab: Alice has an account; a client registers itself and signs her in, as a browser would
(the pages' forms are filled in here).

Needs what scripts/fedlab.py needs. Takes about fifteen seconds.

Usage: scripts/oauthtest.py [--keep]
"""
import base64, hashlib, html, http.cookiejar, json, os, re, time, urllib.error, urllib.parse, urllib.request

from fedlab import check, query, run_lab

REDIRECT = "http://127.0.0.1/callback"
DEVICE = "OAUTHDEV1"


class Browser:
    """Keeps cookies, submits a page's form, and stops at a redirect back to the client."""

    class _Stop(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            if newurl.startswith(REDIRECT):
                raise urllib.error.HTTPError(newurl, code, "back at the client", headers, fp)
            return super().redirect_request(req, fp, code, msg, headers, newurl)

    class _LoopbackIsSecure(http.cookiejar.DefaultCookiePolicy):
        # Browsers treat http://127.0.0.1 as a secure context and send Secure cookies there;
        # Python's cookie jar does not, which would break the pages' CSRF check in the lab.
        def return_ok_secure(self, cookie, request):
            return urllib.parse.urlsplit(request.get_full_url()).hostname == "127.0.0.1" or super().return_ok_secure(cookie, request)

        def set_ok(self, cookie, request):
            return True if urllib.parse.urlsplit(request.get_full_url()).hostname == "127.0.0.1" else super().set_ok(cookie, request)

    def __init__(self):
        self.jar = http.cookiejar.CookieJar(policy=self._LoopbackIsSecure())
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar), self._Stop)

    def open(self, url, data=None):
        """(status, final url, body) or, at the redirect to the client, (code, location, '')."""
        req = urllib.request.Request(url, data=None if data is None else urllib.parse.urlencode(data).encode())
        try:
            with self.opener.open(req, timeout=30) as r:
                return r.status, r.geturl(), r.read().decode()
        except urllib.error.HTTPError as e:
            if e.filename.startswith(REDIRECT):
                return e.code, e.filename, ""
            return e.code, url, e.read().decode(errors="replace")

    @staticmethod
    def form(page, base, want=None):
        """The first form of the page whose hidden fields include `want` (a dict), or the first
        form: (action URL, hidden fields)."""
        for m in re.finditer(r'<form[^>]*action="([^"]*)"[^>]*>(.*?)</form>', page, re.S):
            fields = {}
            for inp in re.finditer(r"<input([^>]*)>", m.group(2)):
                attrs = dict(re.findall(r'([\w-]+)="([^"]*)"', inp.group(1)))
                if attrs.get("type") == "hidden" and "name" in attrs:
                    fields[attrs["name"]] = html.unescape(attrs.get("value", ""))
            if not want or all(fields.get(k) == v for k, v in want.items()):
                return urllib.parse.urljoin(base, html.unescape(m.group(1))), fields
        raise AssertionError(f"no form with {want}: {page[:300]}")


def b64(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def post_form(url, data):
    req = urllib.request.Request(url, data=urllib.parse.urlencode(data).encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def run(hs, peers):
    alice, a_token = hs.register("alice")

    # Discovery and registration.
    st, meta = hs.req("GET", "/_matrix/client/v1/auth_metadata")
    check(st == 200 and meta.get("issuer", "").endswith("/_lithostroton/"), f"asaroton serves the metadata ({st}, {meta.get('issuer')})")
    reg = {"client_name": "oauthtest", "client_uri": "https://client.example.org/", "application_type": "native",
           "redirect_uris": [REDIRECT], "token_endpoint_auth_method": "none",
           "grant_types": ["authorization_code", "refresh_token", "urn:ietf:params:oauth:grant-type:device_code"],
           "response_types": ["code"]}
    req = urllib.request.Request(meta["registration_endpoint"], data=json.dumps(reg).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        client = json.loads(r.read())
    client_id = client["client_id"]
    check(bool(client_id), "a client registers itself")

    # The authorization code flow with PKCE, through the pages.
    browser = Browser()
    verifier = b64(os.urandom(32))
    challenge = b64(hashlib.sha256(verifier.encode()).digest())
    scope = f"urn:matrix:client:api:* urn:matrix:client:device:{DEVICE}"
    authorize = meta["authorization_endpoint"] + "?" + urllib.parse.urlencode({
        "response_type": "code", "client_id": client_id, "redirect_uri": REDIRECT, "scope": scope,
        "state": "s1", "code_challenge": challenge, "code_challenge_method": "S256"})
    st, url, page = browser.open(authorize)
    check(st == 200 and 'name="password"' in page, f"the authorization request shows the login page ({st})")
    action, fields = Browser.form(page, url)
    st, url, page = browser.open(action, {**fields, "username": "alice", "password": "wrong"})
    check('name="password"' in page and "expired" not in page, f"a wrong password shows the login page again ({st})")
    action, fields = Browser.form(page, url)
    st, url, page = browser.open(action, {**fields, "username": "alice", "password": "pwpwpwpw"})
    check(st == 200 and "oauthtest" in page and DEVICE in page,
          f"the right password leads to the consent page, naming the client and the device ({st})"
          + ("" if st == 200 else " " + re.sub(r"<[^>]+>|\s+", " ", page)[:400]))
    action, fields = Browser.form(page, url, {"action": "allow"})
    st, location, _ = browser.open(action, fields)
    params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(location).query))
    check(location.startswith(REDIRECT) and params.get("state") == "s1" and "code" in params
          and params.get("iss") == meta["issuer"], f"consent sends the browser back with a code and the issuer ({st})")

    st, tokens = post_form(meta["token_endpoint"], {"grant_type": "authorization_code", "code": params["code"],
                                                   "redirect_uri": REDIRECT, "client_id": client_id, "code_verifier": verifier})
    check(st == 200 and tokens.get("access_token") and tokens.get("refresh_token") and tokens.get("expires_in"),
          f"the code is exchanged for tokens ({st}, {sorted(tokens)})")

    st, who = hs.req("GET", "/_matrix/client/v3/account/whoami", None, tokens["access_token"])
    check(st == 200 and who.get("user_id") == alice and who.get("device_id") == DEVICE,
          f"the access token is asaroton's own: whoami says Alice on {DEVICE} ({st}, {who.get('device_id')})")
    rows = query(f"SELECT oauth_client_id, oauth_scope FROM devices WHERE device_id = '{DEVICE}'")
    check(rows and rows[0][0] == client_id and rows[0][1] == scope, f"the device remembers the client and the scope ({rows})")

    # Refresh rotation: a lost answer can be asked again; once the new tokens are used the old
    # refresh token is gone.
    st, fresh = post_form(meta["token_endpoint"], {"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"], "client_id": client_id})
    check(st == 200 and fresh.get("access_token") not in (None, tokens["access_token"]), f"a refresh hands out new tokens ({st})")
    st, retry = post_form(meta["token_endpoint"], {"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"], "client_id": client_id})
    check(st == 200 and retry.get("access_token") == fresh.get("access_token"),
          f"asking again with the same refresh token (a lost answer) gets the same tokens ({st})")
    st, who = hs.req("GET", "/_matrix/client/v3/account/whoami", None, fresh["access_token"])
    check(st == 200, f"the new access token works ({st})")
    st, refresh2 = post_form(meta["token_endpoint"], {"grant_type": "refresh_token", "refresh_token": fresh["refresh_token"], "client_id": client_id})
    check(st == 200, f"and so does the new refresh token ({st})")
    st, old = post_form(meta["token_endpoint"], {"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"], "client_id": client_id})
    check(st == 400 and old.get("error") == "invalid_grant", f"the replaced refresh token no longer works ({st}, {old.get('error')})")

    # Revocation ends the session: the device goes, its tokens stop working.
    st, _ = post_form(meta["revocation_endpoint"], {"token": refresh2["access_token"], "client_id": client_id})
    check(st == 200, f"revocation is accepted ({st})")
    st, who = hs.req("GET", "/_matrix/client/v3/account/whoami", None, refresh2["access_token"])
    check(st == 401, f"the revoked access token no longer works ({st})")
    st, devices = hs.req("GET", "/_matrix/client/v3/devices", None, a_token)
    check(st == 200 and DEVICE not in [d["device_id"] for d in devices.get("devices", [])], "and the device is gone")

    # A code that comes back a second time: refused, and what it was exchanged for ends
    # (someone else may hold it). The browser is still signed in, so consent comes at once.
    verifier3 = b64(os.urandom(32))
    authorize3 = meta["authorization_endpoint"] + "?" + urllib.parse.urlencode({
        "response_type": "code", "client_id": client_id, "redirect_uri": REDIRECT,
        "scope": "urn:matrix:client:api:* urn:matrix:client:device:OAUTHDEV3", "state": "s3",
        "code_challenge": b64(hashlib.sha256(verifier3.encode()).digest()), "code_challenge_method": "S256"})
    st, url, page = browser.open(authorize3)
    action, fields = Browser.form(page, url, {"action": "allow"})
    st, location, _ = browser.open(action, fields)
    code3 = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(location).query)).get("code")
    exchange = {"grant_type": "authorization_code", "code": code3, "redirect_uri": REDIRECT, "client_id": client_id, "code_verifier": verifier3}
    st, t3 = post_form(meta["token_endpoint"], exchange)
    check(st == 200, f"a signed-in browser goes straight to consent, and its code works ({st})")
    st, again = post_form(meta["token_endpoint"], exchange)
    check(st == 400 and again.get("error") == "invalid_grant", f"the same code a second time is refused ({st}, {again.get('error')})")
    st, who = hs.req("GET", "/_matrix/client/v3/account/whoami", None, t3.get("access_token"))
    check(st == 401, f"and the tokens it was exchanged for stop working ({st})")

    # The device authorization grant (what signing in with a QR code builds on).
    st, grant = post_form(meta["device_authorization_endpoint"], {"client_id": client_id, "scope": "urn:matrix:client:api:* urn:matrix:client:device:OAUTHDEV2"})
    check(st == 200 and grant.get("device_code") and grant.get("user_code") and grant.get("verification_uri_complete"),
          f"a device asks to be signed in ({st}, {sorted(grant)})")
    st, pending = post_form(meta["token_endpoint"], {"grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                                                    "device_code": grant["device_code"], "client_id": client_id})
    check(st == 400 and pending.get("error") == "authorization_pending", f"until someone approves, it waits ({pending.get('error')})")
    st, url, page = browser.open(grant["verification_uri_complete"])
    check(st == 200 and "OAUTHDEV2" in page, f"the signed-in browser opens the device's link and sees what it would allow ({st})")
    action, fields = Browser.form(page, url, {"action": "allow"})
    st, url, page = browser.open(action, fields)
    check(st == 200, f"and allows it ({st})")
    time.sleep(max(1, grant.get("interval", 1)))
    st, dtokens = post_form(meta["token_endpoint"], {"grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                                                    "device_code": grant["device_code"], "client_id": client_id})
    check(st == 200 and dtokens.get("access_token"), f"once approved in the signed-in browser, the device gets its tokens ({st}, {dtokens.get('error')})")
    if st == 200:
        st, who = hs.req("GET", "/_matrix/client/v3/account/whoami", None, dtokens["access_token"])
        check(st == 200 and who.get("device_id") == "OAUTHDEV2", f"as Alice on its own device ({st}, {who.get('device_id')})")

    # The rendezvous of sign-in with a QR code: the 2024 MSC4108 protocol (what released
    # matrix-rust-sdk versions speak), advertised in /versions, with asaroton's CORS on it and
    # the ETag exposed to browsers; MSC4388 answers its discovery.
    st, versions = hs.req("GET", "/_matrix/client/versions")
    check(versions.get("unstable_features", {}).get("org.matrix.msc4108") is True, "/versions advertises the MSC4108 rendezvous")

    def raw(method, url, body=None, headers=None):
        req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
        except urllib.error.HTTPError as e:
            return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()

    st, h, created = raw("POST", hs.url + "/_matrix/client/unstable/org.matrix.msc4108/rendezvous", b"hello from the new device",
                         {"Content-Type": "text/plain", "Origin": "https://app.example.org"})
    session = json.loads(created or b"{}").get("url")
    check(st == 201 and session and session.startswith(meta["issuer"]) and h.get("etag"),
          f"a new device opens a rendezvous ({st}, {session})")
    check(h.get("access-control-allow-origin") == "*" and "etag" in h.get("access-control-expose-headers", "").lower(),
          f"with CORS and the ETag exposed for web clients ({h.get('access-control-allow-origin')}, {h.get('access-control-expose-headers')})")
    etag = h["etag"]
    st, h2, body = raw("GET", session)
    check(st == 200 and body == b"hello from the new device" and h2.get("etag") == etag, f"the other device reads it ({st})")
    st, h3, _ = raw("PUT", session, b"hello back", {"Content-Type": "text/plain", "If-Match": etag})
    check(st in (200, 202) and h3.get("etag") and h3["etag"] != etag, f"and answers on top of what it read ({st})")
    st, _, _ = raw("PUT", session, b"too late", {"Content-Type": "text/plain", "If-Match": etag})
    check(st == 412, f"an answer on top of an old version is refused ({st})")
    st, _, _ = raw("GET", session, None, {"If-None-Match": h3["etag"]})
    check(st == 304, f"asking for news without any gets 304 ({st})")
    st, _, _ = raw("DELETE", session)
    st2, _, _ = raw("GET", session)
    check(st in (200, 204) and st2 == 404, f"a rendezvous ends on DELETE ({st}, then {st2})")
    st, _, _ = raw("GET", hs.url + "/_matrix/client/v1/rendezvous")
    check(st == 200, f"the MSC4388 rendezvous answers its discovery ({st})")

    # Password login is still there beside it.
    st, r = hs.req("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": "alice"}, "password": "pwpwpwpw"})
    check(st == 200 and r.get("access_token"), f"password login still works ({st})")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra="\n[oauth]\nenabled = true\n")
