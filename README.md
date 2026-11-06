# asaroton

A Matrix homeserver written in Rust, built from scratch against the current Matrix
specification (v1.19, room version 12 by default).

The name is that of the *asarotos oikos*, the "unswept floor": an ancient mosaic that shows
everything that ever fell on it. A homeserver never sweeps an event away either. The project
was called tessera until November 2026.

- Client-server API: registration and login (password and application-service), rooms,
  membership (incl. knocking, restricted rooms, upgrades), `/sync`, Simplified Sliding Sync
  (MSC4186), relations and threads, search, media (authenticated and legacy), end-to-end
  encryption support (device and cross-signing keys, to-device messages, key backups), push
  rules and push gateway delivery, spaces, server notices.
- Server-server API: signing keys, joins/leaves/invites/knocks, transactions with full auth
  chain and state fetching, backfill, server ACLs, media, hierarchy.
- Application services (registration files, namespaces, transactions, third-party lookups).
- Authentication delegated to Matrix Authentication Service (MSC3861), or classic tokens.
- Synapse-compatible admin API subset, rate limiting, message retention.
- Voice and video calls: MatrixRTC over LiveKit (MSC4195) with the token service built in;
  a LiveKit server is the only extra component.
- PostgreSQL behind a storage trait; one binary; distroless container image.

Conformance is measured with [Complement](https://github.com/matrix-org/complement); the
coverage matrix and known deviations are in `docs/spec-coverage.md`. The design is described in
`docs/ARCHITECTURE.md`, and running it in `docs/deployment.md`.

## Development

Requirements: Rust stable (nightly for `scripts/fuzz.sh`), PostgreSQL 15+, Docker and Go (for
the Complement conformance suite).

```sh
cp .env.example .env            # DATABASE_URL etc.
cargo run -p asaroton -- --config deploy/asaroton.example.toml serve
scripts/check.sh                # fmt + clippy + tests
scripts/complement.sh -run TestRegistration   # conformance suite (builds a Docker image)
scripts/loadtest.py --users 50  # synthetic client swarm against a running server
scripts/emulab/                 # Element X in an Android emulator + headless Element Web, see its README
scripts/fedpresencetest.py LAB   # presence fan-out over federation stays bounded (two-server lab)
scripts/ssinvitetest.py URL      # a pending invite does not stop sliding sync from long-polling (test server with open registration)
scripts/syncaccesstest.py URL    # sync shows a user only what they may read: knocks, bans, declined invites, history visibility
scripts/syncwaketest.py URL      # a waiting sync wakes for an invitation or a room created on another device
scripts/sslazytest.py            # sliding sync with lazily loaded members hands out the memberships a timeline is about (starts its own server)
scripts/createroomtest.py        # createRoom under pressure: several rooms at once, and a sync that shows a new room whole or not yet (starts its own server)
scripts/deploy.sh [status]      # check, build and deploy: this machine first, then the remote host (see docs/deployment.md)
watch/                          # weekly watch job: spec, Synapse, tests and server logs become issues, see its README
```

## License

AGPL-3.0-or-later.
