#!/bin/sh
#: Describes the example's application queries inside the preparation database.
set -eu
cd "$(dirname "$0")/../.."
cargo sqlx migrate run --source examples/operations/migrations
SQLX_OFFLINE=false cargo sqlx prepare -- --all-targets
