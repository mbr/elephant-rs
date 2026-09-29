# Application-owned worker operations

A complete, runnable example of transactional submission, progress-aware work,
idempotent effects, signal-driven drain, and external process supervision. It writes
only a simulated ledger in PostgreSQL; it does not make payments or external calls.
This is a reference for building an application, not a ready-made financial service.

The example is part of this crate, using only development dependencies. Its worker
lifecycle is in `main.rs`; checked application SQL is in `store.rs`. It deliberately
leaves database creation, Absurd installation, migrations, credentials, and process
restarts outside the SDK.

## Try it without existing infrastructure

From the repository's Nix development shell:

```sh
cargo build --example operations
./examples/operations/smoke.sh
```

The smoke runner creates and removes its own PostgreSQL instance, installs the
pinned Absurd test fixture, and runs the actual executable. It submits the same
request twice, rejects a conflicting amount, waits for a real claim, sends
`SIGTERM`, and verifies one completed attempt and one business effect. Its external
45-second timeout escalates to a hard kill after another five seconds. It never
uses an existing `DATABASE_URL` as its target. Supply a compiled binary path as its
first argument if using a custom target directory or build profile.

`./check.sh` and `nix build` include this process-level check and the example's
database tests. Tests also simulate a committed effect with no corresponding step
checkpoint, and verify rollback when recording the input fails after enqueue.

## Run the individual commands

Continue in the Nix development shell, which sets `SQLX_OFFLINE=true` so compiling
`init` does not require application tables to exist yet. Use a dedicated scratch
database with Absurd `0.5.0` already installed. For local
testing, `psql -X "$DATABASE_URL" -v ON_ERROR_STOP=1 -f testdata/absurd.sql` installs
the repository's fixture. Do not apply this fixture to an existing production
Absurd installation. Application initialization uses its own migration and SQLx's
migration bookkeeping; it belongs in a separately controlled administration step.

```sh
# Supply DATABASE_URL through your environment or secret manager.
cargo run --example operations -- init
cargo run --example operations -- submit \
  --entry-id 01900000-0000-7000-8000-000000000001 --amount 42
cargo run --example operations -- worker
```

In another terminal using the same database:

```sh
cargo run --example operations -- show \
  --entry-id 01900000-0000-7000-8000-000000000001
```

`submit` prints the SDK handle as JSON. `show` prints the authoritative input, its
business effect, and the current SDK task snapshot. Those reads are not an atomic
history view; an effect can exist before the task is marked complete, and a task
can disappear after retention cleanup while the business record remains.

Prefer `DATABASE_URL` to `--database-url` so credentials are not placed in argv.
The CLI wraps it in `sec::Secret`, hides environment values from help, and never
logs the URL. That is accidental-disclosure protection, not memory zeroization or
a replacement for a secret manager. Configure verified TLS in the URL for remote
databases, for example with `sslmode=verify-full` and the appropriate trust roots.

Workers generate a UUID-based process identity by default. `WORKER_ID` or
`worker --worker-id ...` can supply a deployment identity; distinguish hosts and
restarts. That identity appears in PostgreSQL activity, SDK claim records, and the
worker's tracing span. Logs go to stderr; use `RUST_LOG=info` for normal operation.

## What the application owns

Submission stores immutable input and enqueues its workflow in one transaction.
The business UUID identifies the operation; the SDK idempotency key includes the
task name. Repeating the same input reuses the retained task; business-key
uniqueness still protects effects after workflow retention cleanup. Reusing the
UUID with a different amount fails and rolls back. Tasks carry only that UUID and load the
amount from the application's authoritative request row.

Preparation simulates four three-second chunks inside a durable step. Each
completed chunk reports progress with `TaskContext::heartbeat`. The twelve-second
operation intentionally exceeds the ten-second initial claim. Replace the sleeps
with real bounded, cancellation-aware work. Do not move the heartbeat into an
unconditional timer: automatic renewal already keeps the lease alive, whereas
explicit heartbeats must mean actual application progress. Preparation can repeat
before its step commits, so real preparation effects must also tolerate replay.

The posting commits in an application transaction, then the SDK checkpoints the
successful step result. A crash can occur between those commits. The primary key
on `operations.postings.entry_id` prevents a repeated execution from creating a
second effect; the code also rejects a conflicting stored amount. This guarantee
comes from the application schema, not an exactly-once SDK promise. An HTTP effect
would need the remote service's idempotency mechanism or an application outbox.

## Budgets and process lifecycle

The concrete values are teaching defaults, not production recommendations:

| Limit | Value | Purpose |
| --- | --- | --- |
| SQLx pool acquisition | 2 seconds | Bound waiting for a connection. |
| PostgreSQL lock timeout | 2 seconds | Abort a statement waiting too long for a lock. |
| PostgreSQL statement timeout | 5 seconds | Bound server execution on this pool, including application SQL. |
| Connections / dispatch concurrency | 16 / 4 | Leave room for handlers, claims, and renewal. |
| Claim and default inactivity window | 10 seconds | Recover lost ownership and work without progress. |
| Per-dispatch execution deadline | 30 seconds | Bound a dispatch even while it reports progress. |
| Cancellation grace | 1 second | Allow cooperative cleanup after execution interruption. |
| External service stop budget | 45 seconds | Enforce a final process-level bound. |

On Unix, `SIGTERM` and `SIGINT` stop new claims and drain issued claims and active
handlers. Elsewhere, `Ctrl-C` requests the same drain. Shutdown does not cancel
active handlers; execution interruption has a separate context cancellation token.
The example never drops the worker future merely because termination was requested.

Worker infrastructure failures exit the executable with a failure status. The
process supervisor decides whether and when to restart it. Errors returned by the
handler, including its own SQL failures, instead use the task's five-attempt
exponential retry policy, capped at thirty seconds between attempts. The process can remain healthy
while a task exhausts its attempts, so monitor failed tasks as well as process exits.

`operations.service` is a systemd template for the compiled executable. Build with
`cargo build --release --example operations`, install that binary at the configured
`ExecStart` path, create the service account, and supply a root-owned mode-0600
`EnvironmentFile` containing `DATABASE_URL`. Adjust paths, credentials, database
permissions, and budgets before use. For a Nix-built binary, preserve its runtime
closure through deployment packaging rather than copying an unrooted executable.
Run migrations separately; ordinary worker
restarts do not perform DDL. The template is not installed automatically and is not
a complete deployment or monitoring system. Its restart burst limit prevents an
endless rapid restart loop; exhausted limits require operator attention.

Server timeouts cannot bound every network failure, and an issued claim must not
be discarded after a possible database commit. If graceful drain cannot finish,
the external supervisor kills the process; after restart, claims recover expired
leases through PostgreSQL. Neither async deadlines nor signal handling can preempt
arbitrary blocking native code. Keep effects idempotent across that recovery boundary.

## Changing the example

SQLx `0.8.6` queries compile using the repository's `.sqlx` cache. After changing
queries or migrations, regenerate it in the Nix development shell:

```sh
./examples/operations/prepare.sh
./format.sh
./check.sh
```

Preparation uses another disposable database and the matching SQLx CLI. Library
consumers do not inherit the example's CLI, logging, signal, or migration dependencies.
