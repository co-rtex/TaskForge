#!/usr/bin/env bash
# Block until the local infrastructure is actually usable.
#
# `docker compose up -d` returns as soon as containers are created, which is well
# before PostgreSQL accepts connections. Starting the services before then
# produces confusing connection errors, so wait here instead.
set -euo pipefail

POSTGRES_TIMEOUT_SECONDS="${POSTGRES_TIMEOUT_SECONDS:-60}"
BROKER_TIMEOUT_SECONDS="${BROKER_TIMEOUT_SECONDS:-60}"
BROKER_URL="${TASKFORGE_BROKER_ENDPOINT:-http://127.0.0.1:9324}"

wait_for() {
    local name="$1" timeout="$2" probe="$3"
    local deadline=$(( SECONDS + timeout ))
    printf 'waiting for %s' "$name"
    until eval "$probe" >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            printf ' FAILED\n' >&2
            echo "error: $name did not become ready within ${timeout}s" >&2
            echo "hint: run 'docker compose logs' to see why" >&2
            return 1
        fi
        printf '.'
        sleep 1
    done
    printf ' ready\n'
}

wait_for "postgres" "$POSTGRES_TIMEOUT_SECONDS" \
    "docker compose exec -T postgres pg_isready -U taskforge -d taskforge"

wait_for "elasticmq" "$BROKER_TIMEOUT_SECONDS" \
    "curl -fsS '${BROKER_URL}/?Action=ListQueues&Version=2012-11-05'"

echo "local infrastructure is ready"
