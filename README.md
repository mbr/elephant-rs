# room

`room` is a Rust SDK for Absurd durable workflows on PostgreSQL.

The library exposes Absurd's native model: tasks, runs, checkpoints, sleeps,
events, retries, cancellation, and queue operations. Workers are convenience
helpers over public claim streams, so applications can own their supervision,
backpressure, and shutdown logic.
