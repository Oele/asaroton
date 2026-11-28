#!/usr/bin/env python3
"""Application services managing their users' devices (v1.17: MSC4190, MSC4326).

A bridge registers users in its namespace without UIA, creates devices for them with
`PUT /devices/{deviceId}`, acts as a given device with `?user_id=&device_id=`, and deletes
devices with `DELETE /devices/{deviceId}` and `POST /delete_devices` without
User-Interactive Authentication (it has no password to give). A normal user still needs UIA
to delete a device, and the access token of a user the bridge registered gives no appservice
powers (before 2026-11-28 it could act as any user in the namespace).

    ASAROTON_BIN=target/debug/asaroton python3 scripts/appservicedevicetest.py
"""
import os, tempfile, urllib.parse

from fedlab import SERVER_NAME, check, run_lab

AS_TOKEN = "as-token-of-the-device-bridge"
REG_DIR = tempfile.mkdtemp(prefix="fedlab-asdev-")
with open(os.path.join(REG_DIR, "bridge.yaml"), "w") as f:
    f.write(f"""id: devicebridge
url: http://127.0.0.1:9
as_token: {AS_TOKEN}
hs_token: hs-token-of-the-device-bridge
sender_localpart: devicebridgebot
namespaces:
  users:
    - exclusive: true
      regex: '@bridge_.*:{SERVER_NAME.replace(".", chr(92) + ".")}'
  aliases: []
  rooms: []
""")

q = lambda s: urllib.parse.quote(s, safe="")


def run(hs, peers):
    user = f"@bridge_alice:{SERVER_NAME}"
    as_user = f"user_id={q(user)}"

    print("-- registration without UIA")
    st, r = hs.req("POST", "/_matrix/client/v3/register", {"type": "m.login.application_service", "username": "bridge_alice"}, AS_TOKEN)
    check(st == 200 and r.get("user_id") == user, f"the bridge registers a user in its namespace ({st}, {r.get('user_id')})")

    print("-- devices created by the bridge")
    for dev in ("BRIDGEDEV1", "BRIDGEDEV2", "BRIDGEDEV3"):
        st, r = hs.req("PUT", f"/_matrix/client/v3/devices/{dev}?{as_user}", {"display_name": f"bridge {dev}"}, AS_TOKEN)
        check(st == 201, f"PUT /devices/{dev} creates the device, answered with 201 ({st}, {r})")
    st, r = hs.req("PUT", f"/_matrix/client/v3/devices/BRIDGEDEV1?{as_user}", {"display_name": "renamed"}, AS_TOKEN)
    check(st == 200, f"PUT on an existing device renames it, answered with 200 ({st}, {r})")
    st, r = hs.req("GET", f"/_matrix/client/v3/devices?{as_user}", None, AS_TOKEN)
    have = sorted(d["device_id"] for d in r.get("devices", []))
    check({"BRIDGEDEV1", "BRIDGEDEV2", "BRIDGEDEV3"} <= set(have), f"the user has them ({have})")
    st, r = hs.req("GET", f"/_matrix/client/v3/account/whoami?{as_user}&device_id=BRIDGEDEV1", None, AS_TOKEN)
    check(st == 200 and r.get("user_id") == user and r.get("device_id") == "BRIDGEDEV1",
          f"the bridge acts as one of them with ?device_id= ({st}, {r})")

    print("-- deleting them without UIA")
    st, r = hs.req("DELETE", f"/_matrix/client/v3/devices/BRIDGEDEV1?{as_user}", {}, AS_TOKEN)
    check(st == 200, f"DELETE /devices/BRIDGEDEV1 without auth succeeds for the bridge ({st}, {r})")
    st, r = hs.req("POST", f"/_matrix/client/v3/delete_devices?{as_user}", {"devices": ["BRIDGEDEV2", "BRIDGEDEV3"]}, AS_TOKEN)
    check(st == 200, f"POST /delete_devices without auth succeeds for the bridge ({st}, {r})")
    st, r = hs.req("GET", f"/_matrix/client/v3/devices?{as_user}", None, AS_TOKEN)
    left = sorted(d["device_id"] for d in r.get("devices", []))
    check(not ({"BRIDGEDEV1", "BRIDGEDEV2", "BRIDGEDEV3"} & set(left)), f"they are gone ({left})")

    print("-- a user's own token gives no appservice powers")
    st, r = hs.req("POST", "/_matrix/client/v3/register", {"type": "m.login.application_service", "username": "bridge_bob"}, AS_TOKEN)
    check(st == 200, f"the bridge registers a second user ({st})")
    st, r = hs.req("POST", "/_matrix/client/v3/login", {"type": "m.login.application_service",
                                                       "identifier": {"type": "m.id.user", "user": "bridge_alice"}}, AS_TOKEN)
    check(st == 200 and r.get("access_token"), f"the bridge logs in as its user ({st})")
    alice_token, alice_dev = r.get("access_token"), r.get("device_id")
    st, r = hs.req("GET", f"/_matrix/client/v3/account/whoami?user_id={q('@bridge_bob:' + SERVER_NAME)}", None, alice_token)
    check(st == 200 and r.get("user_id") == user, f"with that user's token, ?user_id= does not act as another user ({st}, {r.get('user_id')})")
    st, r = hs.req("DELETE", f"/_matrix/client/v3/devices/{q(alice_dev)}", {}, alice_token)
    check(st == 401 and r.get("flows"), f"and deleting a device asks for UIA ({st}, {sorted(r)})")
    st, r = hs.req("POST", "/_matrix/client/v3/register", {"type": "m.login.application_service", "username": "bridge_carl"}, alice_token)
    check(st != 200, f"and it cannot register users as the appservice ({st}, {r.get('errcode')})")

    print("-- a normal user still needs UIA")
    carol, c = hs.register("carol")
    st, r = hs.req("GET", "/_matrix/client/v3/account/whoami", None, c)
    own = r.get("device_id")
    st, r = hs.req("DELETE", f"/_matrix/client/v3/devices/{q(own)}", {}, c)
    check(st == 401 and r.get("flows"), f"DELETE /devices without auth asks for UIA ({st}, {sorted(r)})")
    st, r = hs.req("POST", "/_matrix/client/v3/delete_devices", {"devices": [own]}, c)
    check(st == 401 and r.get("flows"), f"POST /delete_devices too ({st}, {sorted(r)})")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra=f'allowed_internal_ranges = ["127.0.0.0/8"]\n[appservices]\nregistration_dir = "{REG_DIR}"\n')
