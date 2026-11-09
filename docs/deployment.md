# Deployment

## Container

`docker build -t asaroton .` builds a distroless image with the release binary. It expects a
configuration file at `/data/asaroton.toml` and keeps signing keys and media under `/data`.
`deploy/docker-compose.yml` runs it next to PostgreSQL 18; copy `deploy/asaroton.example.toml`
to `deploy/data/asaroton.toml` and set `server_name` before `docker compose up -d`.

## Federation

Other servers reach `https://<server_name>:8448` (or the address given by
`/.well-known/matrix/server` or an SRV record). Either give the federation listener a
certificate (`listeners.federation.tls`) or terminate TLS in a reverse proxy in front of it.
The client listener is plain HTTP by default and is normally proxied with TLS as well.

## First admin

Register the first user with `sudo scripts/adduser.py NAME --admin` on the server's host. It
uses the shared-secret registration endpoint, so `registration.shared_secret` must be set;
the alternative is to enable `registration` temporarily. A user created without `--admin`
becomes one when listed under `admins` (restart), or through the admin API by an existing
admin. Admins use
the Synapse-compatible API under `/_synapse/admin/` (users, rooms, deletion, history purge,
server notices).

## Authentication service

With `[mas] enabled = true` asaroton validates access tokens through Matrix Authentication
Service and MAS provisions users and devices through `/_synapse/mas/*`. In the MAS
configuration set `matrix.kind: synapse`, `matrix.homeserver` to the server name,
`matrix.endpoint` to asaroton's client listener and `matrix.secret` to the same value as
`mas.secret`. Route `/_matrix/client/*/login`, `/logout` and `/refresh` to MAS in the reverse
proxy; asaroton answers them with an error when delegation is on. Clients discover MAS through
`/_matrix/client/v1/auth_metadata` and `/.well-known/matrix/client`.

## Operations

- `rate_limiting`: token buckets per client address (logins, registration) and per access
  token (sends, joins, invites), returning `M_LIMIT_EXCEEDED` with `retry_after_ms`.
- `retention`: a periodic job purges non-state events older than a room's
  `m.room.retention` `max_lifetime` (or the configured default).
- Logs go to standard output; `RUST_LOG` selects the level (`info` by default,
  `asaroton_federation=debug` is useful when debugging federation).
- Outbound federation: events and to-device messages are sent as soon as they are queued.
  Transactions that carry only presence, typing, receipts or device lists are paced: bursts
  share a transaction, each destination gets at most 30 at once and then one every 5 seconds,
  and all destinations together stay under `federation.max_edu_transactions_per_minute`
  (600 by default). A minute with more transactions than that logs an error ("outbound
  federation is unusually busy"). A transaction that fails is retried unchanged, with the
  same ID, backing off from 2 seconds to an hour and waiting longer when the destination
  asks; only a `400` or `403` answer discards it. Once an hour asaroton forgets inbound
  transactions older than a day and, for destinations that have been failing for a week,
  drops queued EDUs and all but the newest queued event per room.
- Inbound federation: an event is checked against the events it cites as its authorisation.
  When one of those cannot be obtained (the server that has to hand it over is down, or it
  is signed with a key that nobody can produce at the moment) the event cannot be checked.
  It is then set aside: kept, but not shown, and logged as a warning ("an event could not be
  checked and is set aside"). It is tried again after `federation.unchecked_event_retry_secs`
  (120 by default), then after ever longer waits up to six hours, for a week. Once the
  missing event can be had, the event gets its normal checks and reaches clients as a
  message that arrived late, together with the events that were rejected meanwhile only
  because they cite it. A warning "giving up on an event that was set aside a week ago"
  means it never could be checked. Leaving and rejoining a room has asaroton fetch the
  room's state afresh and looks at its set-aside and rejected messages again; that is the
  way to repair a room by hand.
- Events for a room this server is not in are refused and counted; once an hour a line
  "refused events for rooms this server is not in" gives the total and the busiest room. They
  arrive as long as another server lists an account of this server as a member there. If
  this server has no record of that membership (the room was left out of a migration),
  `scripts/leave-unrecorded.py` sends a proper leave through a server in the room; it takes
  a list of room, user and servers, and uses `POST /_asaroton/admin/v1/remote_leave`.
- A signature on an event counts only while its key was valid: in room versions 5 and later
  an event dated after the validity its server states for the signing key is refused, as
  the specification requires.
- URLs that somebody else chose only lead to the public internet: URL previews (the page,
  its redirects and its image), the gateways of users' pushers, and media that another
  homeserver redirects to. Loopback, private, link-local and similar addresses are refused,
  also when a public name resolves to them, so that nobody can make the server talk to
  services on its own host or network. A push gateway or other target on an internal
  address needs its range listed in `federation.allowed_internal_ranges`. Application
  services are configured by the operator and are not affected, and neither is federation
  itself.
- `deploy/systemd/asaroton.service` is the unit the server runs under. It restarts the server
  when it dies, and gives up after ten failed starts within five minutes, so that a start
  that cannot succeed is not tried every three seconds for ever. A unit that gave up has to
  be cleared with `systemctl reset-failed asaroton` before it starts again.
- `deploy/systemd/` also holds a watchdog that runs beside asaroton as its own service and stops
  it when it sends more than 1000 federation transactions in a minute, queues more than 5000
  outbox rows, or stays above 60% CPU. A homeserver that misbehaves bothers every server it
  federates with, so it errs on the side of stopping; it logs the reason to
  `/var/log/asaroton-watchdog.log` and syslog and mails it to `WATCHDOG_MAIL_TO` (default
  root; set it where root's mail stays on the host). It also mails, once each time, when the
  disk that holds PostgreSQL's data has less than `MIN_FREE_GB` free (default 10); it stops
  nothing for that. Install it with
  `deploy/systemd/install-watchdog.sh [UNIT [DBNAME]]` as root; it is enabled at boot and
  never starts asaroton again by itself. It counts transactions in asaroton's log, so it needs
  `RUST_LOG` to include `asaroton_federation=debug`. Thresholds go in
  `/etc/default/asaroton-watchdog`.
- `deploy/systemd/asaroton-deploy.sh` installs a new binary on a host: it compares the
  checksum with the one announced, runs `--version` as the service user, restarts, waits for
  the client API to answer and puts the previous binary back when it does not (unless the new
  build changed the database schema, which needs a person). It clears the unit's failed state
  before each restart: a binary that fails at once runs into the unit's start limit within the
  minute the tool waits, and the restart with the old binary would otherwise be refused.
  `install-deploy.sh` sets it up and
  can admit an ssh key that is allowed to run only this tool. `scripts/deploy.sh` drives it:
  it deploys what is on `origin/main` after `scripts/check.sh` passes, the building machine
  first and the remote host half a minute later.
- `scripts/loadtest.py` is a synthetic client swarm for capacity checks and
  `scripts/fuzz.sh` runs the fuzz targets under `fuzz/` (needs the nightly toolchain).

## Behind a reverse proxy on port 443

Set `public_base_url = "https://matrix.example.org"` so `.well-known/matrix/client` and login
responses carry the public URL, and `federation.well_known_server = "matrix.example.org:443"` so
asaroton serves `.well-known/matrix/server` and other servers reach it through the proxy instead
of port 8448 (the federation listener can then be disabled). Proxy `/_matrix`, `/_synapse` and
`/.well-known/matrix` to the client listener with `nocanon` and `AllowEncodedSlashes NoDecode`
(Apache) or the equivalent, keep the proxy timeout above the longest `/sync` long-poll, let
WebSocket upgrades through on `/_matrix` if calls are enabled (Apache: `upgrade=websocket` on the
`ProxyPass`), and set `rate_limiting.trust_x_forwarded_for = true`.

## Message search

`/search` uses PostgreSQL full-text search by default (`search.mode = "word"`): English lexemes,
so only whole words match and URLs, version strings and stop words are poor search terms.
`mode = "substring"` matches case-insensitive substrings of the plain text through a `pg_trgm`
index, language-agnostic; `"both"` returns the union, ranked by the full-text rank with substring-
only hits last. Terms shorter than `substring_min_length` (default 3) are ignored; double-quoted
phrases are matched as one substring. The plain text is stored from the moment substring search is
enabled; run `asaroton --config ... search-backfill` once (safe while the server runs) to cover
older messages. Expect the text plus its trigram index to add roughly a third to the size of the
search table.

## Voice and video calls

Calls (Element Call, in Element Web, Element X and the standalone app) use MatrixRTC over
LiveKit: the clients exchange call membership as room events and send media through a LiveKit
SFU. asaroton implements the homeserver side of MSC4195 itself, relays the SFU's signalling, and
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
asaroton --config /etc/asaroton/asaroton.toml livekit-config > /etc/livekit/livekit.yaml
```

produces the complete `livekit.yaml`: signalling and API on 127.0.0.1:7880, media on UDP
50000–60000 and TCP 7881, the key, the webhook back to asaroton and the TURN section.
`[rtc.livekit.sfu]` changes those ports and the bind address. Open UDP 50000–60000, TCP 7881
and (with TURN) TCP 5349 to the world; nothing else. Behind a router, forward the same ports
and, when the public IPv4 address is fixed, state it with `public_ip = "203.0.113.7"` in
`[rtc.livekit.sfu]`: the SFU otherwise discovers the address at every start and checks that
it leads back to itself through the router, and when that check fails it may tell clients
only its local addresses, so that calls work at home and nowhere else. Clients open their LiveKit WebSocket on
asaroton's own URL, under `/_matrix/client/unstable/io.element.msc4195/rtc/livekit/signal`, and
asaroton relays it to the SFU. The reverse proxy must let WebSocket upgrades through on
`/_matrix`, which a proxy set up for Synapse does not do by default: Apache 2.4.47+ takes
`upgrade=websocket` on the existing `ProxyPass /_matrix ...` line, nginx needs
`proxy_set_header Upgrade $http_upgrade;` and `proxy_set_header Connection "upgrade";` in the
`/_matrix` location, Caddy and Traefik need nothing. If the SFU's WebSocket is exposed in some
other way, set `url` to it instead.

asaroton advertises the SFU through `/rtc/transports` and the `org.matrix.msc4143.rtc_foci`
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
maintainers assume a load balancer in front). asaroton's signalling relay corrects the port in
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
