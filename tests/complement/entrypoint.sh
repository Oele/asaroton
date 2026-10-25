#!/usr/bin/env bash
# Entrypoint for the Complement image. Starts a local PostgreSQL, mints a federation
# certificate for $SERVER_NAME signed by Complement's CA, writes the config, runs tessera.
set -euo pipefail

: "${SERVER_NAME:=localhost}"
PGDATA=/var/lib/postgresql/data
PGBIN=$(ls -d /usr/lib/postgresql/*/bin | head -1)

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA" && chown postgres:postgres "$PGDATA"
  su postgres -c "$PGBIN/initdb -D $PGDATA --auth=trust -U postgres" >/dev/null
fi
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -o '-c listen_addresses=localhost -c fsync=off -c synchronous_commit=off -c full_page_writes=off' -w start" >/dev/null
su postgres -c "psql -tc \"SELECT 1 FROM pg_database WHERE datname='tessera'\" | grep -q 1 || createdb tessera" >/dev/null

# Federation TLS: certificate for SERVER_NAME signed by the Complement CA.
mkdir -p /etc/tessera
if [ -f /complement/ca/ca.crt ] && [ -f /complement/ca/ca.key ]; then
  openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -keyout /etc/tessera/tls.key -subj "/CN=$SERVER_NAME" \
    -addext "subjectAltName=DNS:$SERVER_NAME" -out /etc/tessera/tls.csr >/dev/null 2>&1
  openssl x509 -req -in /etc/tessera/tls.csr -CA /complement/ca/ca.crt -CAkey /complement/ca/ca.key \
    -CAcreateserial -days 30 -sha256 -copy_extensions copy -out /etc/tessera/tls.crt >/dev/null 2>&1
  cp /complement/ca/ca.crt /usr/local/share/ca-certificates/complement.crt
  update-ca-certificates >/dev/null 2>&1 || true
else
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 30 -subj "/CN=$SERVER_NAME" \
    -addext "subjectAltName=DNS:$SERVER_NAME" -keyout /etc/tessera/tls.key -out /etc/tessera/tls.crt >/dev/null 2>&1
fi

cat > /etc/tessera/tessera.toml <<CFG
server_name = "$SERVER_NAME"
database_url = "postgres://postgres@localhost/tessera"

[listeners.client]
bind = "0.0.0.0:8008"

[listeners.federation]
bind = "0.0.0.0:8448"
tls_cert = "/etc/tessera/tls.crt"
tls_key = "/etc/tessera/tls.key"

[federation]
# Complement's CA signs every peer's certificate.
extra_ca_certs = ["/complement/ca/ca.crt"]

[registration]
enabled = true
shared_secret = "complement"

[rate_limiting]
enabled = false

[server_notices]
enabled = true
user_localpart = "_server"
CFG

exec tessera --config /etc/tessera/tessera.toml
