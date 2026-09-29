#!/bin/sh
#: Exercises the compiled CLI inside the smoke runner's disposable database.
set -eu
cd "$(dirname "$0")/../.."
: "${DATABASE_URL:?run through ./examples/operations/smoke.sh}"
binary=$1
export RUST_LOG=info
entry_id=01900000-0000-7000-8000-000000000001
worker_pid=

#: Reaps the worker on both successful and failed scenarios.
cleanup() {
    if [ -n "$worker_pid" ]; then
        kill -TERM "$worker_pid" 2>/dev/null || :
        wait "$worker_pid" || :
    fi
}
trap cleanup 0
trap 'exit 1' HUP INT TERM

psql -X "$DATABASE_URL" -v ON_ERROR_STOP=1 -f testdata/absurd.sql >/dev/null
"$binary" init
"$binary" submit --entry-id "$entry_id" --amount 42
"$binary" submit --entry-id "$entry_id" --amount 42
if "$binary" submit --entry-id "$entry_id" --amount 99; then
    printf '%s\n' 'conflicting input unexpectedly succeeded' >&2
    exit 1
fi
"$binary" worker --worker-id smoke-worker &
worker_pid=$!
attempt=0
while [ "$(psql -X "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -c "SELECT EXISTS (SELECT 1 FROM absurd.r_operations WHERE state = 'running' AND claimed_by = 'smoke-worker')")" != t ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 15 ]; then
        printf '%s\n' 'worker did not claim the task' >&2
        exit 1
    fi
    sleep 1
done
kill -TERM "$worker_pid"
if wait "$worker_pid"; then
    worker_pid=
else
    worker_pid=
    printf '%s\n' 'worker failed to drain successfully' >&2
    exit 1
fi
"$binary" show --entry-id "$entry_id"
test "$(psql -X "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -c 'SELECT count(*) = 1 AND min(amount) = 42 FROM operations.postings')" = t
test "$(psql -X "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -c "SELECT count(*) = 1 AND bool_and(state = 'completed' AND attempts = 1) FROM absurd.t_operations")" = t
printf '%s\n' 'Operations example: idempotency, progress, and SIGTERM drain passed.'
