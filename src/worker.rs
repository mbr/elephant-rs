//! Worker and claim stream helpers.

use std::{collections::VecDeque, pin::Pin, task::Poll, time::Duration};

use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::time::{Instant, sleep, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    client::Client,
    error::{Error, Result},
    run::{ClaimedRun, RunLease},
    task::Router,
    types::{QueueName, RunId, TaskId, TaskName},
};

/// Configures calls to `absurd.claim_task`.
#[derive(Clone, Debug)]
pub struct ClaimOptions {
    /// Identifies the worker claiming tasks.
    pub worker_id: String,
    /// Configures the claim lease length.
    pub claim_timeout: Duration,
    /// Configures the maximum rows claimed per poll.
    pub batch_size: i32,
    /// Configures delay after empty polls.
    pub empty_poll_delay: Duration,
}

impl Default for ClaimOptions {
    /// Creates default claim options.
    fn default() -> Self {
        Self {
            worker_id: "elephant-worker".to_string(),
            claim_timeout: Duration::from_secs(30),
            batch_size: 1,
            empty_poll_delay: Duration::from_millis(250),
        }
    }
}

/// Configures automatic claim extension.
#[derive(Clone, Debug)]
pub struct LeaseWatchdogOptions {
    /// Configures how often the claim is extended.
    pub interval: Duration,
    /// Configures the extension requested on each heartbeat.
    pub extend_by: Duration,
}

impl LeaseWatchdogOptions {
    /// Creates watchdog options from a claim timeout.
    pub fn for_claim_timeout(claim_timeout: Duration) -> Self {
        Self {
            interval: (claim_timeout / 3).max(Duration::from_secs(1)),
            extend_by: claim_timeout,
        }
    }
}

impl Default for LeaseWatchdogOptions {
    /// Creates default watchdog options.
    fn default() -> Self {
        Self::for_claim_timeout(ClaimOptions::default().claim_timeout)
    }
}

/// Streams claimed runs from one queue.
pub struct ClaimStream {
    /// Holds the inner stream implementation.
    inner: Pin<Box<dyn Stream<Item = Result<RunLease>> + Send>>,
}

impl ClaimStream {
    /// Creates a claim stream.
    pub fn new(client: Client, queue_name: String, options: ClaimOptions) -> Self {
        let state = ClaimState {
            client,
            queue_name,
            options,
            buffer: VecDeque::new(),
        };
        let inner = futures::stream::unfold(state, |mut state| async move {
            loop {
                if let Some(lease) = state.buffer.pop_front() {
                    return Some((Ok(lease), state));
                }
                match state
                    .client
                    .claim_task(&state.queue_name, &state.options)
                    .await
                {
                    Ok(leases) if leases.is_empty() => sleep(state.options.empty_poll_delay).await,
                    Ok(leases) => state.buffer = leases.into(),
                    Err(error) => return Some((Err(error), state)),
                }
            }
        });
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for ClaimStream {
    /// Polls the next claimed run.
    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(context)
    }

    /// Names the stream item type.
    type Item = Result<RunLease>;
}

/// Carries claim stream state.
struct ClaimState {
    /// Holds the client.
    client: Client,
    /// Names the queue.
    queue_name: String,
    /// Carries claim options.
    options: ClaimOptions,
    /// Buffers claimed runs from a batch.
    buffer: VecDeque<RunLease>,
}

/// Configures a convenience worker.
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// Names the queue to claim from.
    pub queue_name: String,
    /// Configures claim calls.
    pub claim: ClaimOptions,
    /// Configures dispatch concurrency.
    pub concurrency: usize,
    /// Configures automatic claim extension.
    pub lease_watchdog: Option<LeaseWatchdogOptions>,
}

impl Default for WorkerOptions {
    /// Creates default worker options.
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            claim: ClaimOptions::default(),
            concurrency: 1,
            lease_watchdog: Some(LeaseWatchdogOptions::default()),
        }
    }
}

/// Builds a convenience worker.
pub struct WorkerBuilder {
    /// Holds the client.
    client: Client,
    /// Holds the router.
    router: Router,
    /// Carries worker options.
    options: WorkerOptions,
}

impl WorkerBuilder {
    /// Creates a worker builder.
    pub fn new(client: Client, router: Router) -> Self {
        Self {
            client,
            router,
            options: WorkerOptions::default(),
        }
    }

    /// Sets the queue name.
    pub fn queue(mut self, queue_name: impl Into<String>) -> Self {
        self.options.queue_name = queue_name.into();
        self
    }

    /// Sets dispatch concurrency.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.options.concurrency = concurrency;
        self
    }

    /// Sets claim timeout.
    pub fn claim_timeout(mut self, claim_timeout: Duration) -> Self {
        self.options.claim.claim_timeout = claim_timeout;
        self.options.lease_watchdog = Some(LeaseWatchdogOptions::for_claim_timeout(claim_timeout));
        self
    }

    /// Sets automatic claim extension options.
    pub fn lease_watchdog(mut self, options: LeaseWatchdogOptions) -> Self {
        self.options.lease_watchdog = Some(options);
        self
    }

    /// Disables automatic claim extension.
    pub fn without_lease_watchdog(mut self) -> Self {
        self.options.lease_watchdog = None;
        self
    }

    /// Runs the worker until cancellation.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        run_worker(self.client, self.router, self.options, shutdown).await
    }
}

/// Dispatches one claimed batch.
pub async fn work_batch(
    client: &Client,
    router: &Router,
    queue_name: impl AsRef<str>,
) -> Result<()> {
    let leases = client
        .claim_task(queue_name.as_ref(), &ClaimOptions::default())
        .await?;
    let mut futures = FuturesUnordered::new();
    for lease in leases {
        let router = router.clone();
        futures.push(async move { router.dispatch(lease).await }.boxed());
    }
    let mut first_error = None;
    while let Some(result) = futures.next().await {
        if let Err(error) = result
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Runs a capacity-bounded worker, draining active work on shutdown or error.
///
/// An issued claim query is always awaited, including during shutdown, so its
/// leases are dispatched rather than abandoned after a database commit.
pub async fn run_worker(
    client: Client,
    router: Router,
    options: WorkerOptions,
    shutdown: CancellationToken,
) -> Result<()> {
    if options.concurrency == 0 || options.claim.batch_size <= 0 {
        return Err(Error::InvalidWorkerOptions {
            reason: "concurrency and batch size must be positive",
        });
    }
    let mut in_flight = FuturesUnordered::<BoxFuture<'static, Result<()>>>::new();
    let mut pending_claim: Option<BoxFuture<'static, Result<Vec<RunLease>>>> = None;
    let mut next_poll = Instant::now();
    let mut stopping = false;
    let mut first_error = None;
    loop {
        stopping |= shutdown.is_cancelled();
        if stopping && pending_claim.is_none() && in_flight.is_empty() {
            return first_error.map_or(Ok(()), Err);
        }
        let error = tokio::select! {
            _ = shutdown.cancelled(), if !stopping => {
                stopping = true;
                None
            }
            _ = sleep_until(next_poll), if !stopping
                && pending_claim.is_none() && in_flight.len() < options.concurrency => {
                let mut claim = options.claim.clone();
                claim.batch_size = (claim.batch_size as usize)
                    .min(options.concurrency - in_flight.len()) as i32;
                let client = client.clone();
                let queue = options.queue_name.clone();
                pending_claim = Some(async move { client.claim_task(queue, &claim).await }.boxed());
                None
            }
            result = async { pending_claim.as_mut().expect("claim branch is enabled").await },
                if pending_claim.is_some() => {
                pending_claim = None;
                match result {
                    Ok(leases) => {
                        next_poll = Instant::now();
                        if leases.is_empty() {
                            next_poll += options.claim.empty_poll_delay;
                        }
                        for lease in leases {
                            let router = router.clone();
                            let watchdog = options.lease_watchdog.clone();
                            in_flight.push(async move {
                                dispatch_with_watchdog(router, lease, watchdog).await
                            }.boxed());
                        }
                        None
                    }
                    Err(error) => Some(error),
                }
            }
            result = in_flight.next(), if !in_flight.is_empty() => {
                result.and_then(Result::err)
            }
        };
        if let Some(error) = error {
            stopping = true;
            if first_error.is_none() {
                first_error = Some(error);
            } else {
                warn!(error = %error, "task dispatch failed during worker shutdown");
            }
        }
    }
}

/// Dispatches a run while extending its claim.
async fn dispatch_with_watchdog(
    router: Router,
    lease: RunLease,
    options: Option<LeaseWatchdogOptions>,
) -> Result<()> {
    let Some(options) = options else {
        return router.dispatch(lease).await;
    };
    let metadata = LeaseMetadata::from_run(lease.claimed_run());
    let client = lease.client().clone();
    let shutdown = CancellationToken::new();
    let watchdog_shutdown = shutdown.clone();
    let dispatch = async {
        let result = router.dispatch(lease).await;
        shutdown.cancel();
        result
    };
    let (result, ()) = tokio::join!(
        dispatch,
        watch_lease(client, metadata, options, watchdog_shutdown)
    );
    result
}

/// Extends a lease until the watchdog is cancelled.
async fn watch_lease(
    client: Client,
    metadata: LeaseMetadata,
    options: LeaseWatchdogOptions,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            () = sleep(options.interval) => {}
        }
        if shutdown.is_cancelled() {
            return;
        }
        match client
            .extend_claim(
                metadata.queue_name.as_str(),
                metadata.run_id.as_uuid(),
                options.extend_by,
            )
            .await
        {
            Ok(()) => {}
            Err(Error::Cancelled | Error::RunAlreadyFailed) => return,
            Err(error) => warn!(
                queue = %metadata.queue_name,
                task_id = %metadata.task_id,
                run_id = %metadata.run_id,
                task_name = %metadata.task_name,
                error = %error,
                "lease extension failed"
            ),
        }
    }
}

/// Carries metadata needed to extend a claim.
#[derive(Clone, Debug)]
struct LeaseMetadata {
    /// Names the queue.
    queue_name: QueueName,
    /// Identifies the task.
    task_id: TaskId,
    /// Identifies the run.
    run_id: RunId,
    /// Names the task.
    task_name: TaskName,
}

impl LeaseMetadata {
    /// Creates metadata from a claimed run.
    fn from_run(run: &ClaimedRun) -> Self {
        Self {
            queue_name: run.queue_name.clone(),
            task_id: run.task_id,
            run_id: run.run_id,
            task_name: run.task_name.clone(),
        }
    }
}
