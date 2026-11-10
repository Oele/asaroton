#!/usr/bin/env python3
"""A refresh whose answer is lost does not end the session.

A client with a refreshable session trades its refresh token for a new access token and a new
refresh token. If the answer does not arrive, the client still has the old pair and asks
again. The old refresh token therefore has to stay good until the client has used what it
got for it, the new access token or the new refresh token; only then is it revoked (that is
what the spec says). A client that asks again gets the same pair again, so that an answer
that did arrive stays good as well.

The server used to revoke the old refresh token and the old access token at once: the second
attempt was answered M_UNKNOWN_TOKEN, which tells a client that its session is over.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/refreshtest.py [--keep]
"""
import json, random, socket, time

from fedlab import check, query, run_lab

AGAIN = 40  # refreshes that are cut off and tried again


def run(hs, peers):
    user, _ = hs.register("rita")
    st, r = hs.req("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": user}, "password": "pwpwpwpw", "refresh_token": True})
    assert st == 200 and r.get("refresh_token"), r
    a0, r0 = r["access_token"], r["refresh_token"]

    def refresh(token):
        st, r = hs.req("POST", "/_matrix/client/v3/refresh", {"refresh_token": token})
        return st, r.get("access_token"), r.get("refresh_token"), r

    def works(access_token):
        st, r = hs.req("GET", "/_matrix/client/v3/account/whoami", None, access_token)
        return st == 200 and r.get("user_id") == user

    def revoked(token):
        st, _, _, r = refresh(token)
        return st == 401 and r.get("errcode") == "M_UNKNOWN_TOKEN"

    print("-- the answer to a refresh is lost, and the client asks again")
    st, a1, r1, body = refresh(r0)
    check(st == 200 and a1 and r1 and a1 != a0 and r1 != r0 and body.get("expires_in_ms", 0) > 0, f"a refresh hands out a new access token and a new refresh token ({st})")
    st, again_a, again_r, body = refresh(r0)
    check(st == 200, f"the second request with the same refresh token is answered ({st} {body.get('errcode')})")
    check((again_a, again_r) == (a1, r1), "with the same pair, so that a first answer that did arrive stays good")
    check(works(a0), "the access token from before the refresh still works, as long as the new one was not used")

    print("-- the client uses what it got")
    check(works(a1), "the new access token works")
    check(revoked(r0), "and from then on the old refresh token is revoked")
    check(not works(a0), "and the old access token with it")

    print("-- a client that goes on with the new refresh token without ever using the access token")
    st, a2, r2, _ = refresh(r1)
    st, a3, r3, _ = refresh(r2)
    check(st == 200 and len({a1, a2, a3}) == 3 and len({r1, r2, r3}) == 3, f"each refresh token gives the next pair ({st})")
    check(revoked(r1), "using a refresh token revokes the one it replaced")
    check(works(a3) and revoked(r2) and not works(a2), "and so on down the line")

    print(f"-- {AGAIN} refreshes that are cut off halfway and tried again at once")
    rnd, current, lost = random.Random(1), r3, 0
    for _ in range(AGAIN):
        data = json.dumps({"refresh_token": current}).encode()
        s = socket.create_connection(("127.0.0.1", hs.client_port))
        s.sendall(f"POST /_matrix/client/v3/refresh HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n".encode() + data)
        time.sleep(rnd.uniform(0, 0.004))
        s.close()
        st, access, following, _ = refresh(current)
        if st != 200 or not works(access):
            lost += 1
            break
        current = following
    check(lost == 0, "the session lives through all of them")
    devices = query("SELECT count(*) FROM devices")[0][0]
    tokens = query("SELECT (SELECT count(*) FROM refresh_tokens), (SELECT count(*) FROM access_tokens WHERE refresh_token IS NOT NULL)")[0]
    check(devices == "2" and int(tokens[0]) <= 2 and int(tokens[1]) <= 2, f"and no tokens pile up: the device has at most the pair in use and the one before it ({tokens[0]} refresh tokens, {tokens[1]} access tokens)")


if __name__ == "__main__":
    run_lab(["mock"], run)
