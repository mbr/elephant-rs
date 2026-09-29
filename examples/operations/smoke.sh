#!/bin/sh
#: Tests real termination handling in a fresh database with a hard process budget.
set -eu
cd "$(dirname "$0")/../.."
exec pgdb -F timeout --kill-after=5s 45s ./examples/operations/smoke-db.sh "${1:-target/debug/examples/operations}"
