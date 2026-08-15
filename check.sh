#!/bin/sh

#: Runs formatting, compilation, tests, docs, and linting.

set -e

echo "rustc $(rustc --version) at $(which rustc), cargo $(cargo --version) at $(which cargo)"

./format.sh --check
RUSTFLAGS="-D warnings" cargo check
RUSTFLAGS="-D warnings" cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
RUSTFLAGS="-D warnings" cargo clippy -- -D warnings
