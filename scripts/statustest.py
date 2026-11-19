#!/usr/bin/env python3
"""A status of one's own (MSC4426) as a profile field, under its unstable name.

`org.matrix.msc4426.status` is `{"text", "emoji"}`, both strings: set with PUT, read with GET,
cleared with DELETE. A text over 256 bytes or an emoji over 32 bytes is refused with
M_TOO_LARGE; an emoji of several graphemes that fits is taken. Sizes are bytes of UTF-8, not
characters. While the MSC is open the stable name `m.status` stays an ordinary field.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/statustest.py [--keep]
"""
import urllib.parse

from fedlab import check, run_lab


def run(hs, peers):
    q = urllib.parse.quote
    alice, token = hs.register("alice")
    bob, b_token = hs.register("bob")

    for field in ("org.matrix.msc4426.status",):
        path = f"/_matrix/client/v3/profile/{q(alice)}/{q(field)}"

        def put(value):
            return hs.req("PUT", path, {field: value}, token)

        status = {"text": "Glazing pots, back at four", "emoji": "🏺"}
        st, r = put(status)
        check(st == 200, f"{field}: a status is set ({st}, {r})")
        st, r = hs.req("GET", path, None, b_token)
        check(st == 200 and r.get(field) == status, f"{field}: and others read it back ({st})")

        # 256 bytes of text (128 two-byte letters) and a 26-byte family-plus-flag emoji fit.
        family_flag = "👩‍👩‍👧🇳🇱"
        st, r = put({"text": "é" * 128, "emoji": family_flag})
        check(st == 200, f"{field}: 256 bytes of text and an emoji of several graphemes ({len(family_flag.encode())} bytes) fit ({st}, {r})")
        st, r = put({"text": "é" * 128 + "x", "emoji": "🏺"})
        check(st == 400 and r.get("errcode") == "M_TOO_LARGE", f"{field}: 257 bytes of text is too large ({st}, {r.get('errcode')})")
        st, r = put({"text": "busy", "emoji": "🇳🇱" * 5})
        check(st == 400 and r.get("errcode") == "M_TOO_LARGE", f"{field}: a 40-byte emoji is too large ({st}, {r.get('errcode')})")
        st, r = put({"text": "busy"})
        check(st == 400 and r.get("errcode") == "M_BAD_JSON", f"{field}: a status without an emoji is refused ({st}, {r.get('errcode')})")
        st, r = hs.req("GET", path, None, b_token)
        check(st == 200 and r.get(field, {}).get("emoji") == family_flag, f"{field}: refused updates leave the last good status ({st})")

        st, r = hs.req("DELETE", path, None, token)
        check(st == 200, f"{field}: DELETE clears it ({st})")
        st, r = hs.req("GET", path, None, b_token)
        check(st == 404, f"{field}: and then it is gone ({st})")

    # The stable name is not the MSC's yet: any shape goes (Complement's MSC4429 tests use it).
    st, r = hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/m.status", {"m.status": {"text": "busy"}}, token)
    check(st == 200, f"m.status without an emoji is taken while MSC4426 is open ({st}, {r.get('errcode')})")

    # Other fields keep their own rules: no status check on them.
    st, r = hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/org.matrix.msc4426.call",
                   {"org.matrix.msc4426.call": {"call_joined_ts": 1795000000}}, token)
    check(st == 200, f"the call field (MSC4426 m.call) is set without status rules ({st})")


if __name__ == "__main__":
    run_lab(["mock"], run)
