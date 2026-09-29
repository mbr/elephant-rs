#!/bin/sh
#: Refreshes offline query metadata using an automatically removed database.
set -eu
cd "$(dirname "$0")/../.."
exec pgdb -F ./examples/operations/prepare-db.sh
