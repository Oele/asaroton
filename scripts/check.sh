#!/usr/bin/env bash
# Pre-commit / CI check: format, lint, test.
set -euo pipefail
cd "$(dirname "$0")/.."
# Store tests need a database; #[sqlx::test] creates throwaway databases from this URL.
export DATABASE_URL="${DATABASE_URL:-postgres://tessera:tessera@localhost/tessera}"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
