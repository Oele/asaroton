#!/usr/bin/env python3
"""Create a user on an asaroton server whose registration is closed.

    sudo scripts/adduser.py NAME [--admin] [--generate] [--config /etc/asaroton/asaroton.toml]

Uses the shared-secret registration endpoint (`registration.shared_secret` in the server's
configuration), talking to the server's own client listener. Run it on the server's host as a
user who may read the configuration file, hence sudo.

The password is asked for twice without echo. With --generate a random one is made and shown
once; note it down, it is not stored anywhere else.
"""
import argparse, getpass, hashlib, hmac, json, secrets, string, sys, tomllib, urllib.error, urllib.request


def main():
    parser = argparse.ArgumentParser(description="Create a user on an asaroton server.")
    parser.add_argument("name", help="the local part: NAME becomes @NAME:server")
    parser.add_argument("--admin", action="store_true", help="make the user a server admin")
    parser.add_argument("--generate", action="store_true", help="generate a password and show it once")
    parser.add_argument("--config", default="/etc/asaroton/asaroton.toml")
    args = parser.parse_args()

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
    scheme = "https" if listener.get("tls_cert") else "http"
    base = f"{scheme}://{bind}"

    if args.generate:
        alphabet = string.ascii_letters + string.digits
        password = "".join(secrets.choice(alphabet) for _ in range(20))
    else:
        password = getpass.getpass("Password: ")
        if len(password) < 8:
            sys.exit("that is short; use at least 8 characters")
        if getpass.getpass("Again: ") != password:
            sys.exit("the two entries differ")

    url = base + "/_synapse/admin/v1/register"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            nonce = json.load(r)["nonce"]
        role = b"admin" if args.admin else b"notadmin"
        mac = hmac.new(secret.encode(), b"\0".join([nonce.encode(), args.name.encode(), password.encode(), role]), hashlib.sha1).hexdigest()
        body = json.dumps({"nonce": nonce, "username": args.name, "password": password, "admin": args.admin, "mac": mac}).encode()
        request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request, timeout=20) as r:
            user_id = json.load(r)["user_id"]
    except urllib.error.HTTPError as e:
        sys.exit(f"the server refused: {e.code} {e.read().decode()[:300]}")
    except urllib.error.URLError as e:
        sys.exit(f"cannot reach the server at {base}: {e.reason}")

    print(f"created {user_id}" + (" (admin)" if args.admin else ""))
    public = config.get("public_base_url") or f"https://{config.get('server_name', '')}"
    print(f"sign in with user name {args.name!r} on homeserver {public}")
    if args.generate:
        print(f"password: {password}")


if __name__ == "__main__":
    main()
