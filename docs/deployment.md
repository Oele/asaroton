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

## Voice and video calls

Calls (Element Call, in Element Web, Element X and the standalone app) use MatrixRTC over
LiveKit: the clients exchange call membership as room events and send media through a LiveKit
SFU. tessera implements the homeserver side of MSC4195 itself, so the only extra service is the
SFU. Run `livekit-server` with a config like

```yaml
port: 7880
rtc:
  tcp_port: 7881
  port_range_start: 50000
  port_range_end: 60000
  use_external_ip: true
keys:
  APIabc123: the-secret-from-livekit-server-generate-keys
webhook:
  api_key: APIabc123
  urls:
    - https://matrix.example.org/_matrix/client/unstable/io.element.msc4195/rtc/livekit/sfu_webhook
```

open UDP 50000–60000 and TCP 7881 to the world, put TLS in front of port 7880 (a WebSocket
proxy at `wss://livekit.example.org`), and tell tessera about it:

```toml
[rtc.livekit]
url = "wss://livekit.example.org"
api_key = "APIabc123"
api_secret = "the-secret-from-livekit-server-generate-keys"
# api_url = "https://livekit.example.org"   # defaults to url over https
# remote_can_publish = true                 # users of other servers may publish here
```

That is the whole setup: tessera advertises the SFU through `/rtc/transports` and the
`org.matrix.msc4143.rtc_foci` entry of `.well-known/matrix/client`, checks room membership,
signs the LiveKit access tokens, creates the LiveKit rooms, serves the federation endpoint
through which users of other homeservers join calls hosted here (and asks the remote server for
tokens when a local user joins a call on another server's SFU), keeps the delayed leave event
alive while a participant is connected and sends it the moment the SFU reports the
disconnection (webhook, with a poll every fifteen seconds as the fallback), and removes
participants who leave or are kicked from the Matrix room. Older clients that still expect a
separate LiveKit JWT service find one at the same URL prefix, including the OpenID token exchange
it relies on; no `lk-jwt-service` is needed. A TURN server is not required for LiveKit: every
participant talks to the SFU directly. LiveKit's built-in TURN (its `turn:` section) only helps
users on networks that block UDP entirely.
