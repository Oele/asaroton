# tessera

A Matrix homeserver written in Rust, built from scratch against the current Matrix
specification (v1.19, room version 12 by default).

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
cargo run -p tessera -- --config deploy/tessera.example.toml serve
scripts/check.sh                # fmt + clippy + tests
scripts/complement.sh -run TestRegistration   # conformance suite (builds a Docker image)
scripts/loadtest.py --users 50  # synthetic client swarm against a running server
scripts/emulab/                 # Element X in an Android emulator + headless Element Web, see its README
scripts/fedpresencetest.py LAB   # presence fan-out over federation stays bounded (two-server lab)
watch/                          # weekly watch job: spec, Synapse, tests and server logs become issues, see its README
```

## License

AGPL-3.0-or-later.
