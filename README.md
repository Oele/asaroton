# asaroton

A [Matrix](https://matrix.org) homeserver written in Rust, from scratch, against the current
Matrix specification (v1.19, room version 12 by default). One binary and a PostgreSQL
database.

The name is that of the *asarotos oikos*, the "unswept floor": an ancient mosaic that shows
everything that ever fell on it. A homeserver never sweeps an event away either.

## Where it stands

asaroton is young: the first commit is from October 2026. It runs two small homeservers
that federate with the public network, one of them moved over from Synapse with its history,
and people use it every day with Element Web, Element X and other clients. It has not been
run with many users or in many rooms of the largest size, and nobody outside its author has
operated it yet. Expect rough edges, and read `docs/deployment.md` before trusting it with
data you cannot lose.

It was written by one person working with an AI coding assistant, Claude, which wrote most
of the code; the commit history says so commit by commit. What keeps it honest is its tests:
the unit and store tests, a set of end-to-end tests that run a real server against scripted
clients and homeservers, and [Complement](https://github.com/matrix-org/complement), the
conformance suite that Synapse is tested with. 207 of Complement's tests pass. Eleven tests
and one subtest are skipped by decision, ten for features that are not implemented and two
for deliberate deviations, each with its reason in `tests/complement/expected-failures.txt`.

## What it does

- **Client-server API:** registration and login, rooms and membership (knocking, restricted
  rooms, upgrades), `/sync` and Simplified Sliding Sync (MSC4186), relations and threads,
  search, media, push rules and push gateways, spaces, server notices, delayed events
  (MSC4140).
- **End-to-end encryption support:** device keys and cross-signing, one-time and fallback
  keys, to-device messages, key backups, dehydrated devices (MSC3814). The server stores and
  delivers; the cryptography is the clients'.
- **Federation:** signing keys, joins, leaves, invites and knocks, transactions with the full
  checks on authorisation and state, backfill, server ACLs, media, space hierarchy.
- **Application services:** registration files, namespaces, transactions, third-party
  lookups.
- **Authentication:** its own passwords and tokens, or delegated to Matrix Authentication
  Service (MSC3861).
- **Voice and video calls:** MatrixRTC over LiveKit (MSC4195) with the token service built
  in; a LiveKit server is the only extra component.
- **For the operator:** a subset of Synapse's admin API, rate limiting, message retention, an
  importer for an existing Synapse database (`asaroton import-synapse`), a container image.

What is implemented, what is not and where it deviates on purpose is in
[docs/spec-coverage.md](docs/spec-coverage.md).

## Running it

[docs/deployment.md](docs/deployment.md) covers the container image, federation, the first
admin account, the reverse proxy, calls and the systemd units in `deploy/`. The settings are
described in [deploy/asaroton.example.toml](deploy/asaroton.example.toml).

To try it from a checkout, with PostgreSQL running and a role as in `.env.example`:

```sh
cp .env.example .env
cargo run -p asaroton -- --config deploy/asaroton.example.toml serve
```

## Working on it

Rust stable and PostgreSQL 15 or newer are enough for most things; Docker and Go are needed
for Complement, and the nightly toolchain for fuzzing.

```sh
scripts/check.sh                              # formatting, clippy, all unit and store tests
scripts/complement.sh -run TestRegistration   # part of the conformance suite
scripts/droppedrequesttest.py                 # one of the end-to-end tests
```

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) describes how the crates fit together, how an
event travels through the server and what a request may rely on.
[scripts/README.md](scripts/README.md) lists the end-to-end tests and the operator's tools.

Bug reports and pull requests are welcome. A report is most useful with the request that
went wrong and what the server logged for it; a change, with a test that fails without it.

## Security

Please report a vulnerability privately, not in a public issue: see
[SECURITY.md](SECURITY.md).

## License

MIT, see [LICENSE](LICENSE).
