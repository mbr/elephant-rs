#!/bin/sh

#: Runs formatting, compilation, tests, docs, and linting.

set -e

if [ "$(id -u)" -eq 0 ]; then
    printf '%s\n' 'PostgreSQL tests require an unprivileged user; do not run ./check.sh as root.' >&2
    exit 1
fi

echo "rustc $(rustc --version) at $(which rustc), cargo $(cargo --version) at $(which cargo)"

./format.sh --check
RUSTFLAGS="-D warnings" cargo check --all-targets
RUSTFLAGS="-D warnings" cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
RUSTFLAGS="-D warnings" cargo clippy --all-targets -- -D warnings
