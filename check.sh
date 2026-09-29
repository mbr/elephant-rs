#!/bin/sh

#: Runs formatting, compilation, tests, docs, and linting.

set -e

echo "rustc $(rustc --version) at $(which rustc), cargo $(cargo --version) at $(which cargo)"

./format.sh --check
RUSTFLAGS="-D warnings" cargo check --all-targets
RUSTFLAGS="-D warnings" cargo test
RUSTFLAGS="-D warnings" cargo build --example operations
./examples/operations/smoke.sh
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
RUSTFLAGS="-D warnings" cargo clippy --all-targets -- -D warnings
