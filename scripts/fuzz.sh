#!/usr/bin/env bash
# Run each fuzz target for a while (needs the nightly toolchain and cargo-fuzz).
# Usage: scripts/fuzz.sh [seconds per target]   (default 60)
set -euo pipefail
cd "$(dirname "$0")/.."
secs=${1:-60}
for target in canonical_json pdu_parse redaction; do
  echo "== $target ($secs s)"
  cargo +nightly fuzz run "$target" -- -max_total_time="$secs" -max_len=4096 2>&1 | tail -3
done
