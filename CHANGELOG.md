# Changelog

All notable changes to `elephant-rs` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed

- Operational worker example, its development-only dependencies, and supporting build tooling.

## [0.1.0] - 2026-09-30

### Added

- Initial unofficial Rust SDK for Absurd, published as `elephant-rs` and imported as `elephant`.
- Enum job contracts with shared output types, named task registration, and typed result retrieval.
- Workflow checkpoints, durable sleeps, event waits, and child-task result waits.
- Transactional task submission and event emission, queue management, and retry and cancellation policies.
- Concurrent workers with lease renewal, execution and progress limits, and graceful draining.

[Unreleased]: https://github.com/mbr/elephant-rs/compare/baebcdd3948f50744ad49eae8ea8578c15d65841...HEAD
[0.1.0]: https://crates.io/crates/elephant-rs/0.1.0
