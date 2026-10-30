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
SFU. tessera implements the homeserver side of MSC4195 itself, relays the SFU's signalling, and
writes the SFU's configuration, so the only extra service is `livekit-server` on the same host.

```toml
[rtc.livekit]
api_key = "APIabc123"                        # from `livekit-server generate-keys`
api_secret = "the-secret-from-livekit-server-generate-keys"
# remote_can_publish = true                  # users of other servers may publish here

[rtc.livekit.turn]                           # media relay over TLS for clients without UDP
# domain = "matrix.example.org"              # defaults to the host of public_base_url
# tls_port = 5349
# cert_file = "/etc/livekit/tls/fullchain.pem"
# key_file = "/etc/livekit/tls/privkey.pem"
# allow_peer_cidrs = ["10.0.0.5/32"]         # the SFU's own address, only when it is private (NAT)
```

```sh
tessera --config /etc/tessera/tessera.toml livekit-config > /etc/livekit/livekit.yaml
```

produces the complete `livekit.yaml`: signalling and API on 127.0.0.1:7880, media on UDP
50000–60000 and TCP 7881, the key, the webhook back to tessera and the TURN section.
`[rtc.livekit.sfu]` changes those ports and the bind address. Open UDP 50000–60000, TCP 7881
and (with TURN) TCP 5349 to the world; nothing else. Clients open their LiveKit WebSocket on
tessera's own URL, under `/_matrix/client/unstable/io.element.msc4195/rtc/livekit/signal`,
which the reverse proxy already forwards along with the rest of `/_matrix`, and tessera relays
it to the SFU. If the SFU's WebSocket is exposed in some other way, set `url` to it instead.

tessera advertises the SFU through `/rtc/transports` and the `org.matrix.msc4143.rtc_foci`
entry of `.well-known/matrix/client`, checks room membership, signs the LiveKit access tokens,
creates the LiveKit rooms, serves the federation endpoint through which users of other
homeservers join calls hosted here (and asks the remote server for tokens when a local user
joins a call on another server's SFU), keeps the delayed leave event alive while a participant
is connected and sends it the moment the SFU reports the disconnection (webhook, with a poll
every fifteen seconds as the fallback), and removes participants who leave or are kicked from
the Matrix room. Older clients that still expect a separate LiveKit JWT service find one at the
same URL prefix, including the OpenID token exchange it relies on; no `lk-jwt-service` is needed.

### Clients without UDP (VPNs, strict firewalls)

Every participant talks to the SFU directly, over UDP when it can. On a VPN or corporate network
that blocks UDP, a browser only falls back to the SFU's TCP port after its UDP attempts time out,
which means up to a minute of black tiles. The `[rtc.livekit.turn]` section makes
`livekit-server` also offer a TURN relay over TLS: the client then has a working candidate in
its first round of connectivity checks and connects at once. It is a listener of the SFU itself,
not a separate TURN server, and the relayed media never leaves the host.

LiveKit tells clients to find that relay on port 443 whatever port it listens on (its
maintainers assume a load balancer in front). tessera's signalling relay corrects the port in
the SFU's answers, so the relay can use 5349 or any other free port, and 443 is only needed
for the strictest networks, which allow nothing else: give it a second address then, or an
SNI router in front of 443. The relay forwards only to the SFU itself, and LiveKit refuses to
relay to private addresses unless they are listed, so on a host behind NAT (cloud 1:1 NAT
included) put the SFU's own private address in `allow_peer_cidrs`. The SFU needs a readable
copy of a certificate for the TURN domain;
with certbot a deploy hook keeps it current:

```sh
#!/bin/sh
# /etc/letsencrypt/renewal-hooks/deploy/livekit  (run once by hand with RENEWED_LINEAGE set)
install -d -o livekit -g livekit -m 0750 /etc/livekit/tls
install -o livekit -g livekit -m 0640 "$RENEWED_LINEAGE/fullchain.pem" "$RENEWED_LINEAGE/privkey.pem" /etc/livekit/tls/
systemctl try-restart livekit
```
