//! Worker and claim stream helpers.

use std::{collections::VecDeque, pin::Pin, task::Poll, time::Duration};

use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::{client::Client, error::Result, run::RunLease, task::Router};

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
            worker_id: "room-worker".to_string(),
            claim_timeout: Duration::from_secs(30),
            batch_size: 1,
            empty_poll_delay: Duration::from_millis(250),
        }
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
}

impl Default for WorkerOptions {
    /// Creates default worker options.
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            claim: ClaimOptions::default(),
            concurrency: 1,
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
        self.options.concurrency = concurrency.max(1);
        self
    }

    /// Sets claim timeout.
    pub fn claim_timeout(mut self, claim_timeout: Duration) -> Self {
        self.options.claim.claim_timeout = claim_timeout;
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
    while let Some(result) = futures.next().await {
        result?;
    }
    Ok(())
}

/// Runs a convenience worker loop.
pub async fn run_worker(
    client: Client,
    router: Router,
    options: WorkerOptions,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut claims = client.claims(&options.queue_name, options.claim.clone());
    let mut in_flight = FuturesUnordered::<BoxFuture<'static, Result<()>>>::new();
    loop {
        while in_flight.len() < options.concurrency {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                item = claims.next() => {
                    let Some(lease) = item else {
                        break;
                    };
                    let lease = lease?;
                    let router = router.clone();
                    in_flight.push(async move { router.dispatch(lease).await }.boxed());
                }
            }
        }
        if shutdown.is_cancelled() && in_flight.is_empty() {
            return Ok(());
        }
        tokio::select! {
            _ = shutdown.cancelled(), if in_flight.is_empty() => return Ok(()),
            result = in_flight.next(), if !in_flight.is_empty() => {
                if let Some(result) = result {
                    result?;
                }
            }
        }
    }
}
