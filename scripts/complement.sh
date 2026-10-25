#!/usr/bin/env bash
# Build the Complement image for tessera and run the conformance suite.
# Usage: scripts/complement.sh [go test args...]   e.g. scripts/complement.sh -run TestVersionStructure
set -euo pipefail
cd "$(dirname "$0")/.."
COMPLEMENT_DIR=${COMPLEMENT_DIR:-$HOME/src/complement}
IMAGE=${IMAGE:-complement-tessera:latest}

DOCKER_BUILDKIT=1 docker build -q -t "$IMAGE" -f tests/complement/Dockerfile . >/dev/null
echo "built $IMAGE"
cd "$COMPLEMENT_DIR"
export COMPLEMENT_BASE_IMAGE="$IMAGE"
export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=${COMPLEMENT_SPAWN_HS_TIMEOUT_SECS:-60}
export COMPLEMENT_ENABLE_DIRTY_RUNS=${COMPLEMENT_ENABLE_DIRTY_RUNS:-1}
exec go test -count=1 -v -timeout "${COMPLEMENT_TIMEOUT:-90m}" ./tests/... "$@"
