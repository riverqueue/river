#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

container_id=
cleanup() {
    status=$?
    if [[ -n "$container_id" ]]; then
        if [[ "$status" -ne 0 ]]; then
            docker logs --tail 200 "$container_id" >&2 || true
        fi
        docker rm --force --volumes "$container_id" >/dev/null || true
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Let Docker choose a free loopback port so a local Postgres can keep running.
# Pull on every run so a cached image cannot silently test an older release.
container_id=$(docker run --detach --pull always \
    --publish 127.0.0.1::5432 \
    --env POSTGRES_PASSWORD=postgres \
    --env POSTGRES_DB=river_test \
    malisper/pgrust:latest)

for attempt in {1..60}; do
    # TCP readiness avoids connecting to the temporary initialization server.
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d river_test >/dev/null 2>&1; then
        break
    fi
    if [[ "$attempt" -eq 60 || "$(docker inspect --format '{{.State.Running}}' "$container_id")" != true ]]; then
        echo "pgRust failed to become ready" >&2
        exit 1
    fi
    sleep 1
done

docker exec --env PGPASSWORD=postgres "$container_id" \
    psql -h 127.0.0.1 -U postgres -d river_test -c 'SELECT version()'

port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$container_id")

# Don't reuse cached results when the image behind latest may have changed.
"${MAKE:-make}" test/race \
    GOFLAGS="${GOFLAGS:+$GOFLAGS }-count=1" \
    TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$port/river_test?pool_max_conns=15&sslmode=disable"
