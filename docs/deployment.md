# Deployment

## Container

`docker build -t tessera .` builds a distroless image with the release binary. It expects a
configuration file at `/data/tessera.toml` and keeps signing keys and media under `/data`.
`deploy/docker-compose.yml` runs it next to PostgreSQL 18; copy `deploy/tessera.example.toml`
to `deploy/data/tessera.toml` and set `server_name` before `docker compose up -d`.

## Federation

Other servers reach `https://<server_name>:8448` (or the address given by
`/.well-known/matrix/server` or an SRV record). Either give the federation listener a
certificate (`listeners.federation.tls`) or terminate TLS in a reverse proxy in front of it.
The client listener is plain HTTP by default and is normally proxied with TLS as well.

## First admin

Register the first user (enable `registration` temporarily, or use the shared-secret
registration endpoint with `registration.shared_secret`), then list the user ID under
`admins` and restart, or set `is_admin` through the admin API from an existing admin. Admins use
the Synapse-compatible API under `/_synapse/admin/` (users, rooms, deletion, history purge,
server notices).

## Authentication service

With `[mas] enabled = true` tessera validates access tokens through Matrix Authentication
Service and MAS provisions users and devices through `/_synapse/mas/*`. In the MAS
configuration set `matrix.kind: synapse`, `matrix.homeserver` to the server name,
`matrix.endpoint` to tessera's client listener and `matrix.secret` to the same value as
`mas.secret`. Route `/_matrix/client/*/login`, `/logout` and `/refresh` to MAS in the reverse
proxy; tessera answers them with an error when delegation is on. Clients discover MAS through
`/_matrix/client/v1/auth_metadata` and `/.well-known/matrix/client`.

## Operations

- `rate_limiting`: token buckets per client address (logins, registration) and per access
  token (sends, joins, invites), returning `M_LIMIT_EXCEEDED` with `retry_after_ms`.
- `retention`: a periodic job purges non-state events older than a room's
  `m.room.retention` `max_lifetime` (or the configured default).
- Logs go to standard output; `RUST_LOG` selects the level (`info` by default,
  `tessera_federation=debug` is useful when debugging federation).
- `scripts/loadtest.py` is a synthetic client swarm for capacity checks and
  `scripts/fuzz.sh` runs the fuzz targets under `fuzz/` (needs the nightly toolchain).

## Behind a reverse proxy on port 443

Set `public_base_url = "https://matrix.example.org"` so `.well-known/matrix/client` and login
responses carry the public URL, and `federation.well_known_server = "matrix.example.org:443"` so
tessera serves `.well-known/matrix/server` and other servers reach it through the proxy instead
of port 8448 (the federation listener can then be disabled). Proxy `/_matrix`, `/_synapse` and
`/.well-known/matrix` to the client listener with `nocanon` and `AllowEncodedSlashes NoDecode`
(Apache) or the equivalent, keep the proxy timeout above the longest `/sync` long-poll, and set
`rate_limiting.trust_x_forwarded_for = true`.

## Message search

`/search` uses PostgreSQL full-text search by default (`search.mode = "word"`): English lexemes,
so only whole words match and URLs, version strings and stop words are poor search terms.
`mode = "substring"` matches case-insensitive substrings of the plain text through a `pg_trgm`
index, language-agnostic; `"both"` returns the union, ranked by the full-text rank with substring-
only hits last. Terms shorter than `substring_min_length` (default 3) are ignored; double-quoted
phrases are matched as one substring. The plain text is stored from the moment substring search is
enabled; run `tessera --config ... search-backfill` once (safe while the server runs) to cover
older messages. Expect the text plus its trigram index to add roughly a third to the size of the
search table.
