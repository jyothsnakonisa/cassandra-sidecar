#!/usr/bin/env bash
# Builds the sidecar image and starts the CDC demo stack.
#
# Usage (from anywhere in the repo):
#   ./scripts/start.sh            # build + start, keep existing data volumes
#   ./scripts/start.sh --clean    # build + start, wipe all data
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CLEAN=false

for arg in "$@"; do
    case "$arg" in
        --clean) CLEAN=true ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# Run all docker compose commands from the demo directory so no -f flag is needed.
cd "$DEMO_DIR"

echo "==> Stopping stack..."
if $CLEAN; then
    docker compose down -v --remove-orphans
else
    docker compose down --remove-orphans
fi

echo "==> Building sidecar image..."
docker build \
    -f "$REPO_ROOT/docker/cdc-demo/Dockerfile.sidecar" \
    -t cassandra-sidecar:dev \
    "$REPO_ROOT"

echo "==> Starting stack..."
docker compose up -d

echo ""
echo "Stack is starting. Follow logs with:"
echo "  docker compose logs -f cassandra cassandra-init sidecar"
echo ""
echo "Wait for 'CDC iterators started successfully' in the sidecar logs,"
echo "then run the following command to write a test mutation:"
echo "  docker exec -it cdc-demo-cassandra-1 cqlsh -e \"INSERT INTO cdc_demo.events (id, msg, ts) VALUES (uuid(), 'hello', toTimestamp(now()));\""
