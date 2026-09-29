# Elephant architecture and execution contract

`elephant` is a Rust SDK for Absurd's PostgreSQL-native durable workflows. It
exposes tasks, runs, checkpoints, sleeps, events, retries, cancellation, and
queue operations directly. It is not a job-queue facade or a framework that
owns the application's runtime.

The public examples in `README.md` are compiled as doctests. This specification
records architectural boundaries and semantics rather than hypothetical APIs.

## Architecture

The implementation remains one crate with public primitives below convenience
workers:

- `client`: typed stored-procedure access, spawning, results, and operations.
- `task`: producer contracts, executable registrations, routing, and wrappers.
- `run`: claimed runs, consuming leases, execution supervision, and renewal policy.
- `context`: durable capabilities and execution metadata available to handlers.
- `worker`: claim streams, capacity management, polling, and graceful shutdown.
- `types`: identifiers, validated names, JSON policies, snapshots, and references.
- `error`: typed operation errors and stable serialized failure records.
- `schema`: explicit schema installation/version inspection.

The database owns distributed state, scheduling, retry decisions, and crash
recovery. The SDK owns local execution, serialization, cancellation notification,
and lease supervision. Applications can use the client without a router, the
router without a worker, or the worker with their own outer supervisor.

No generic database-backend abstraction is required. PostgreSQL stored
procedures are the protocol. Tokio, SQLx, serde, jiff, and tracing provide the
runtime, transport, serialization, time representation, and instrumentation.

## Contracts and handlers

`Task<P, R>` describes a task's name, optional queue, and spawn defaults without
containing its handler. A producer can depend on a shared contract module and
JSON types without linking worker implementations.

`Task::builder(...).build()` creates a contract. `task.handler(...)` binds a
worker implementation and returns `TaskRegistration<P, R>`. The combined
`Task::builder(...).handler(...).build()` form remains available. A registration
dereferences to its contract for spawning and metadata access.

`Router::task` accepts registrations and rejects duplicate task names. Router
clones share an immutable dispatch table; extending a clone uses copy-on-write.
Handler parameter/result serialization is erased internally, not in the
producer-facing API. Contract and registration cloning does not require
parameter or result cloning.

`Spawned<R>` retains queue and expected result type. Its serde representation
contains only the queue and spawn metadata, not `R`. Identifiers and names also
support serde, with name validation applied on deserialization. Handles from
rolled-back transactions must not be used.

## Database and transaction boundaries

`Client::builder` wraps an existing `PgPool`. It never installs or migrates
Absurd. Production applications install `absurd.sql` and upgrades with their
normal migration system. Explicit schema checks are available to startup or
operator code.

Pool-backed methods normally execute each stored-procedure call independently.
`SpawnBuilder::send_on`, `Client::spawn_untyped_on`, and `Client::emit_event_on`
accept `&mut PgConnection`, including a dereferenced SQLx transaction. They
execute the same SQL as the convenience methods and never commit. Applications
can atomically mutate business rows, spawn work, and publish an event. Tasks and
events become visible only after commit; awaiting results before committing can
deadlock application logic.

Execution and administrative operations remain pool-backed. Transaction support
is intentionally focused on enqueue/event boundaries rather than holding a
transaction across an entire workflow or introducing backend-generic traits.

Queries are runtime-checked by policy. Absurd stored functions and dynamic queue
tables are covered with real PostgreSQL integration tests. Bound parameters are
used for names and payloads. Queue names honor Absurd's byte limit. Conversion
to integer-second arguments rounds nonzero fractions upward.

## Claims and local ownership

`Client::claim_task` returns `RunLease` values. A lease is `#[must_use]`, and
terminal methods consume it. `complete`, `fail`, `sleep_until`, `sleep_for`, and
`forget` make local resolution explicit. Public low-level methods remain escape
hatches; consuming wrappers do not enforce distributed ownership by themselves.

Dropping an unresolved lease emits a warning and performs no asynchronous
cleanup. `forget` deliberately leaves database recovery responsible for the
run. Claimed metadata records the effective lease duration used by the database
and the monotonic local request start. Automatic renewal accounts for time spent
buffered before dispatch and refuses to start work under a locally expired
claim. Request start, rather than response receipt, also anchors renewal
deadlines conservatively.

`RunLease::run` resolves successful values, records ordinary failures, and
recognizes owning-run suspension/cancellation/already-failed control signals.
`RunLease::run_supervised` additionally implements renewal, deadlines, and
cooperative cancellation. Router dispatch adds checkpoint loading, typed handler
selection, panic conversion, execution wrappers, and tracing.

## Worker scheduling and shutdown

The convenience worker selects between active dispatches, an issued claim query,
poll timing, and shutdown. Pushing a future into `FuturesUnordered` does not
start it: active dispatches are always polled even while waiting for more work.

Each claim batch is capped by currently available execution slots. A claim query
is retained until it returns, even after shutdown or another execution's error;
its returned leases are dispatched and drained. Dropping an in-flight claim
query could abandon database-committed claims whose rows were not yet received.
Zero concurrency and nonpositive batch sizes are rejected.

Shutdown stops new claim requests and drains issued claims and active work. It
does not cancel handlers. The first infrastructure error also stops claiming;
after draining, the worker returns that error to the application supervisor.
Ordinary handler failures are persisted and retried according to database
policy, not returned as worker infrastructure failures.

Unknown task names are deferred with jitter rather than failed immediately,
allowing rolling deployments with different registered task sets.

The manual `ClaimStream` exposes batch semantics, including already-claimed
buffered leases. Its caller owns capacity planning, continuous polling, and safe
shutdown of issued queries. Keep its batch size within immediately available
capacity. `work_batch` and bare `Router::dispatch` enforce default stall recovery
without background renewal; `RunLease::run` remains explicitly unsupervised.

## Execution supervision

`ExecutionOptions` is shared by `Router::dispatch_with`,
`RunLease::run_supervised`, and `WorkerOptions.execution`. `LeaseRenewal` selects
`Automatic`, `Custom`, or `Disabled` background renewal. Automatic renewal derives
its interval and extension from the claimed lease, rather than a separate fixed
worker default. Custom intervals must be positive and shorter than both the
initial and renewed lease durations.

Default `StallTimeout::ClaimDuration` bounds inactivity by the latest
application-requested effective lease duration, initially the claimed duration.
Only successful checkpoint writes, event checkpoint writes, and explicit context
heartbeats reset application progress. Cached reads, failed operations, raw
client calls, and background renewal cannot keep an otherwise hung handler
alive. Context clones share one progress tracker. Heartbeat/checkpoint durations
use the same whole-second rounding as their database lease extensions.

`StallTimeout::After` chooses a fixed inactivity window; `Disabled` explicitly
allows indefinite renewal without progress. An optional overall timeout bounds
one dispatch, including checkpoint loading and execution wrappers, independently
of progress. It is not a limit on workflow lifetime across sleeps and retries.
Raw supervised futures without a routed context receive no progress reports and
must explicitly configure a different stall policy for long opaque work.
After execution/cleanup, terminal persistence has a separate budget equal to the
original claim duration. A blocked completion/failure query returns
`RunResolutionTimeout` rather than holding capacity indefinitely. Its database
outcome is uncertain; cancellation of the local future is not a rollback promise.

The context exposes a cancellation token for cooperative cleanup. Local
cancellation, stalled execution, dispatch deadlines, detected terminal lease
states, and renewal infrastructure failures signal it. After `cancellation_grace`,
the future is dropped if it has not returned. Its late result does not override the reason
execution was interrupted.

Local cancellation, stalls, and deadlines fail the run normally, enabling retry
and releasing local capacity. Renewal continues during cooperative cleanup unless
ownership is lost. A known database cancelled/already-failed run is not failed
again. Renewal infrastructure failures
abandon the local lease for database recovery and return an error; the SDK does
not continue executing indefinitely under uncertain ownership. Renewal queries
are bounded by a locally tracked lease deadline. This is a conservative local
supervision mechanism, not proof that external side effects are fenced.

Blocking code cannot be preempted by Tokio timeouts. Dropping a future cannot
undo an external request or stop detached application tasks. Handlers must be
cooperative, avoid detached work, and use idempotency keys. The library never
terminates the application process.

## Durable context operations

A handler receives `TaskContext`, including task/run identifiers, attempt, name,
queue, raw application headers, and cancellation notification. Context clones
share metadata and checkpoint state. Checkpoint writes renew the claim by
default; explicit heartbeat is available for long work under manual dispatch.

`step` returns a cached successful value or executes and checkpoints its closure.
An error writes no successful checkpoint. `begin_step` returns `Step::Done` or a
consuming `PendingStep`; completing one handle twice is prevented locally.
Each call allocates the next occurrence of its base name before reading cached
state. Concurrent calls are not single-flight and can execute different
occurrences simultaneously.

`sleep_for` and `sleep_until` checkpoint the wake time and schedule the run if the
time has not arrived. Relative sleeps use the database clock. Named variants
support multiple sleeps and loops. A sleep suspends execution through a control
signal; it does not hold a worker slot until wakeup.

`await_event` delegates atomic event waiting/suspension to Absurd. Named variants
separate event identity from checkpoint identity. Events are immutable per queue
and name: the first emit wins. Repeated waits for one name do not represent a
stream of successive events. Missing timeout payloads and JSON null payloads
remain distinguishable. On timeout resumption, the first uncached wait for that
event raises `Error::EventTimeout`. If the handler catches it and waits for the
same event again within that dispatch, the next wait acknowledges the database
timeout marker with JSON null instead of suspending again. This acknowledgement
is not an emitted event, a persisted checkpoint, or durable application progress.

The shared protocol does not retain a checkpointed timeout outcome. A later
suspension or task retry can therefore re-enter the wait and accept a late event.
Applications can mitigate this by wrapping the wait and timeout fallback in an
ordinary step that returns a successful decision value. Once committed, that
step replays without re-entering the wait. The observation and checkpoint write
are not atomic, so this is not a complete fix for durable deadline enforcement.

`await_task_result` and its named variant poll another queue while retaining a
worker slot. They explicitly heartbeat to maintain the claim and report continued
application activity, even when a background supervisor is also renewing it.
A terminal raw snapshot is checkpointed before decoding, including failed and
cancelled child states. Replay survives child retention cleanup. JSON null
results are preserved distinctly from absent results.

Same-queue context waits are rejected to reduce worker-slot deadlocks. Cross-queue
cycles can still deadlock. Raw client result polling is intentionally
non-durable and does not enforce context restrictions. Child creation should use
stable idempotency keys: a crash can occur between spawning and checkpointing
its reference.

## Checkpoint identity and cross-language compatibility

Checkpoint naming and built-in payloads follow the Go, Python, and TypeScript
SDKs at Absurd `0.5.0`. Each base name has a per-dispatch occurrence counter,
shared by context clones and all operation kinds. The first occurrence uses
the base name; subsequent ones append `#2`, `#3`, and so on. Replay resets the
counters rather than deriving them from stored checkpoints.

- Steps use caller base names and serialize successful values directly.
- Sleeps use caller base names or `sleep`, storing RFC 3339 timestamp strings.
- Events use caller base names or `$awaitEvent:{event_name}`, storing raw payloads.
- Child waits use caller base names or `$awaitTaskResult:{task_id}`, storing a
  terminal snapshot with optional result/failure payloads, including JSON null.

Logical IDs such as `charge:{order_id}` are preferable for unordered or parallel
work, where occurrence order can otherwise change on replay. Generated suffixes
and operation prefixes must not be reused for unrelated application steps;
operation kinds share one namespace. Changing value shape or meaning requires
a new name or an explicit migration. An immutable event remains the same event
across numbered waits; this is not stream consumption.

Go-generated PostgreSQL checkpoint fixtures cover both Rust replay and matching
Rust writes. The pinned generator also verifies Go replay. Applications must
still agree on names, call order, and JSON schemas across languages; these are
workflow contracts, not properties guaranteed by the shared SQL schema.

## Errors, panics, and instrumentation

Owning-run SQLSTATE `AB001` maps to `Error::Cancelled`; `AB002` maps to
`RunAlreadyFailed`; retry validation `AB003` maps to `InvalidRetryStrategy`.
These are distinct from a cancelled child result, which produces
`TaskCancelled { task_id }`. Propagating a child result error must never abandon
its parent's live lease as if the parent had already been cancelled.

Failure JSON has stable category names and messages, with optional traceback
space. Handler errors need not implement serde. Panics in handler construction,
polling, serialization, or execution wrappers are caught at the router boundary.
String panic messages are persisted; arbitrary payloads receive an explicit
fallback. The application's panic hook controls origin backtrace collection;
a post-unwind backtrace would not reproduce the panic's origin stack.

Dispatch spans carry queue, task ID, run ID, attempt, and task name. Payloads and
headers are never logged automatically. `Router::wrap_execution` establishes
application context, including trace propagation from metadata headers, for
both manual and convenience execution. Dispatch recognizes `Suspended`,
`Cancelled`, and `RunAlreadyFailed` anywhere in `std::error::Error::source` chains.
Wrappers may add domain errors provided they retain the original SDK error as a
source; formatting it into an opaque string discards its control-flow identity.
`TaskCancelled`, `EventTimeout`, and local execution interruptions are not owning-run
control signals. Applications inject propagation headers through their enqueue wrapper
using `SpawnBuilder::headers`; a separate spawn middleware framework is not
required.

## Validation and maintenance

`./check.sh` runs formatting checks, compilation, unit/integration tests,
doctests, documentation, and clippy with warnings denied. Run `./format.sh`
after successful checks. Integration tests use independent ephemeral PostgreSQL
databases with the pinned fixture identified in `testdata/README.md`.

Regressions cover partial-capacity execution, bounded claims, graceful shutdown,
child cancellation identity, child-result replay after cleanup, transactional
enqueue/events, producer-only contracts, persisted handles, headers, panic
messages, manual renewal, deadlines, cancellation, and renewal infrastructure
errors, alongside the original workflow operations.

SDK compatibility changes require deliberate fixture updates. Runtime behavior
is at least once: neither Rust ownership nor durable checkpoints makes external
side effects exactly once. Preserve this distinction in every public example.
