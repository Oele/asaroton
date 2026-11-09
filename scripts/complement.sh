#!/usr/bin/env bash
# Build the Complement image for asaroton and run the conformance suite.
# Usage: scripts/complement.sh [go test args...]   e.g. scripts/complement.sh -run TestVersionStructure
# Tests listed in tests/complement/expected-failures.txt are skipped unless COMPLEMENT_ALL=1.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$PWD
COMPLEMENT_DIR=${COMPLEMENT_DIR:-$HOME/src/complement}
IMAGE=${IMAGE:-complement-asaroton:latest}

# Two Complement runs on one Docker daemon remove each other's homeserver containers and both
# fail with "connection refused". Wait for a run that is already going instead. The lock is
# inherited by go test and its test binaries, so it also covers a run whose parent was killed.
exec 9>"/tmp/asaroton-complement-$(id -u).lock"
flock 9

DOCKER_BUILDKIT=1 docker build -q -t "$IMAGE" -f tests/complement/Dockerfile . >/dev/null
echo "built $IMAGE"
# Every build leaves the image before it untagged and adds to BuildKit's cache, which BuildKit
# did not keep down by itself: 18 GB in a day on a disk that also holds the live database
# (issue #44). Keep the most recently used 10 GB of the cache (the cargo target mount, about
# 2 GB, among them) and remove the untagged images of earlier builds.
docker builder prune -f --max-used-space 10GB >/dev/null || echo "complement.sh: pruning the build cache failed" >&2
docker image prune -f --filter "label=org.asaroton.complement=1" >/dev/null || true
cd "$COMPLEMENT_DIR"
export COMPLEMENT_BASE_IMAGE="$IMAGE"
export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=${COMPLEMENT_SPAWN_HS_TIMEOUT_SECS:-60}
export COMPLEMENT_ENABLE_DIRTY_RUNS=${COMPLEMENT_ENABLE_DIRTY_RUNS:-1}

# Tests that fail by decision are skipped, so that every failure of a run is a real one and a
# clean run exits 0. Each name becomes an anchored pattern, level by level for subtests;
# go test takes alternatives separated by `|`. COMPLEMENT_ALL=1 runs everything.
SKIP=()
if [ -z "${COMPLEMENT_ALL:-}" ]; then
  PATTERN=$(grep -vE '^[[:space:]]*(#|$)' "$REPO/tests/complement/expected-failures.txt" \
    | awk '{ n = split($1, part, "/"); out = ""; for (i = 1; i <= n; i++) out = out (i > 1 ? "/" : "") "^" part[i] "$"; print out }' \
    | paste -sd'|')
  [ -n "$PATTERN" ] && SKIP=(-skip "$PATTERN")
fi
exec go test -count=1 -v -timeout "${COMPLEMENT_TIMEOUT:-90m}" "${SKIP[@]}" ./tests/... "$@"
