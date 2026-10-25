# tessera

A modern Matrix homeserver written in Rust.

Goals: full compatibility with the current Matrix specification (client-server and
server-server APIs), federation with the public Matrix network, first-class support for
next-generation auth (Matrix Authentication Service / MSC3861) and Simplified Sliding Sync,
and PostgreSQL as the primary store.

Status: early development. See `docs/ARCHITECTURE.md` and `docs/spec-coverage.md`.

## Development

Requirements: Rust stable, PostgreSQL 15+, Docker and Go (for the Complement conformance suite).

```sh
cp .env.example .env            # DATABASE_URL etc.
cargo run -p tessera -- --help
scripts/check.sh                # fmt + clippy + tests
scripts/complement.sh           # conformance suite (builds a Docker image)
```

## License

AGPL-3.0-or-later.
