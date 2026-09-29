//! An application-owned worker with explicit budgets, progress, and idempotency.

use std::{convert::Infallible, process::ExitCode, str::FromStr, time::Duration};

use clap::{Parser, Subcommand};
use elephant::{
    client::Client,
    context::TaskContext,
    error::{Error as WorkflowError, FailureReason},
    schema,
    task::{Router, Task},
    types::{CreateQueueOptions, RetryStrategy, Spawned, TaskResultSnapshot},
};
use sec::Secret;
use serde::Serialize;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

mod store;
#[cfg(test)]
mod tests;

/// Names this application's separate work queue.
const QUEUE: &str = "operations";
/// Bounds ownership and default progress inactivity, not SQL execution.
const CLAIM: Duration = Duration::from_secs(10);
/// Bounds one handler dispatch independently of progress heartbeats.
const EXECUTION: Duration = Duration::from_secs(30);

/// Protects connection credentials from accidental debug output.
#[derive(Clone, Debug)]
struct DatabaseUrl(Secret<String>);

impl FromStr for DatabaseUrl {
    /// Wraps credentials without echoing them during argument parsing.
    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(Secret::new(value.to_owned())))
    }

    /// Accepts the secret without exposing it through a parser diagnostic.
    type Err = Infallible;
}

/// Configures application entry points without installing SDK-owned signal handlers.
#[derive(Debug, Parser)]
struct Cli {
    /// Connects to the application's database; prefer the environment over argv.
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: DatabaseUrl,
    /// Selects an administrative, producer, inspection, or worker operation.
    #[command(subcommand)]
    command: Command,
}

/// Separates schema administration from ordinary worker restarts.
#[derive(Debug, Subcommand)]
enum Command {
    /// Applies application migrations and provisions the work queue.
    Init,
    /// Atomically enqueues an immutable business request.
    Submit {
        /// Identifies the request across client retries and repeated executions.
        #[arg(long)]
        entry_id: Uuid,
        /// Carries an integer amount for the simulated local ledger.
        #[arg(long, allow_negative_numbers = true)]
        amount: i64,
    },
    /// Displays both business effects and the current workflow snapshot.
    Show {
        /// Selects the business request rather than a particular attempt.
        #[arg(long)]
        entry_id: Uuid,
    },
    /// Runs until a termination signal or an infrastructure failure.
    Worker {
        /// Overrides the generated per-process identity with a deployment identity.
        #[arg(long, env = "WORKER_ID", value_parser = clap::builder::NonEmptyStringValueParser::new())]
        worker_id: Option<String>,
    },
}

/// Retains actionable causes while giving domain failures stable categories.
#[derive(Debug, thiserror::Error)]
enum Error {
    /// Rejects malformed credentials without displaying their original value.
    #[error("invalid database URL")]
    InvalidDatabaseUrl,
    /// Preserves database failures for the application supervisor.
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    /// Preserves SDK errors, including owning-run control signals.
    #[error("workflow operation failed")]
    Workflow(#[source] WorkflowError),
    /// Reports schema administration failure without retrying it in a worker.
    #[error("application migration failed")]
    Migration(#[source] sqlx::migrate::MigrateError),
    /// Reports signal monitoring failure after draining the worker.
    #[error("signal monitoring failed")]
    Signal(#[source] std::io::Error),
    /// Prevents a business identity from being reused for different input.
    #[error("entry {entry_id} already has a different amount")]
    ConflictingAmount {
        /// Identifies the conflicting business operation.
        entry_id: Uuid,
    },
    /// Detects an unexpectedly missing committed effect.
    #[error("entry {entry_id} has no posting after insertion")]
    MissingPosting {
        /// Identifies the missing business effect.
        entry_id: Uuid,
    },
    /// Preserves output serialization errors.
    #[error("could not serialize output")]
    Json(#[source] serde_json::Error),
}

/// Carries failures through domain and supervisor boundaries without flattening them.
type Result<T = ()> = std::result::Result<T, Error>;

/// Combines durable business state with a non-atomic workflow observation.
#[derive(Debug, Serialize)]
struct Inspection {
    /// Contains the immutable input and its associated task identity.
    request: store::Request,
    /// Shows whether the business effect has already committed.
    posting: Option<store::Posting>,
    /// May be absent after workflow retention cleanup.
    task: Option<TaskResultSnapshot>,
}

/// Configures caller-owned acquisition and PostgreSQL execution budgets.
async fn connect(url: &DatabaseUrl, application_name: &str) -> Result<Client> {
    let options = url
        .0
        .reveal()
        .parse::<PgConnectOptions>()
        .map_err(|_| Error::InvalidDatabaseUrl)?
        .application_name(application_name)
        .options([("statement_timeout", "5000"), ("lock_timeout", "2000")]);
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(2))
        .connect_with(options)
        .await?;
    Ok(Client::builder(pool).build())
}

/// Installs only application state; Absurd must already be provisioned.
async fn initialize(client: &Client) -> Result {
    sqlx::migrate!("examples/operations/migrations")
        .run(client.pool())
        .await
        .map_err(Error::Migration)?;
    client
        .create_queue(QUEUE, CreateQueueOptions::default())
        .await
        .map_err(Error::Workflow)?;
    Ok(())
}

/// Defines a producer contract which carries only the business identity.
fn posting_task() -> elephant::error::Result<Task<Uuid, store::Posting>> {
    Ok(Task::builder("post-entry-v1")?
        .queue(QUEUE)?
        .default_max_attempts(5)
        .build())
}

/// Enqueues and records immutable input in one caller-owned transaction.
async fn submit(client: &Client, entry_id: Uuid, amount: i64) -> Result<Spawned<store::Posting>> {
    let mut transaction = client.pool().begin().await?;
    let handle = client
        .spawn(&posting_task().map_err(Error::Workflow)?, entry_id)
        .idempotency_key(format!("post-entry-v1:{entry_id}"))
        .retry_strategy(RetryStrategy::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            max: Some(Duration::from_secs(30)),
        })
        .send_on(&mut transaction)
        .await
        .map_err(Error::Workflow)?;
    store::request(
        &mut transaction,
        entry_id,
        amount,
        handle.result.task_id.as_uuid(),
    )
    .await?;
    transaction.commit().await?;
    Ok(handle)
}

/// Wraps domain errors while retaining SDK control-flow identity in their sources.
fn router() -> elephant::error::Result<Router> {
    Router::new().task(posting_task()?.handler(|context, entry_id| async move {
        post_entry(context, entry_id)
            .await
            .map_err(|error| WorkflowError::handler(Box::new(error)))
    }))
}

/// Models bounded chunks of work and reports progress only after completing each one.
async fn prepare(context: &TaskContext) -> elephant::error::Result<()> {
    for chunk in 1..=4 {
        tokio::select! {
            _ = context.cancellation_token().cancelled() => return Err(WorkflowError::ExecutionCancelled),
            _ = sleep(Duration::from_secs(3)) => {},
        }
        context.heartbeat(CLAIM).await?;
        tracing::info!(chunk, "finished a simulated preparation chunk");
    }
    Ok(())
}

/// Applies a unique business effect despite a crash before its checkpoint commits.
async fn post_entry(context: TaskContext, entry_id: Uuid) -> Result<store::Posting> {
    context
        .step("prepare-v1", || prepare(&context))
        .await
        .map_err(Error::Workflow)?;
    context
        .step("post-v1", || async {
            let effect = async {
                let mut transaction = context.client().pool().begin().await?;
                let request = store::load_request(&mut transaction, entry_id).await?;
                let posting = store::post(&mut transaction, &request).await?;
                transaction.commit().await?;
                tracing::info!(%entry_id, "business effect committed; step checkpoint follows");
                Ok::<_, Error>(posting)
            };
            effect
                .await
                .map_err(|error| WorkflowError::handler(Box::new(error)))
        })
        .await
        .map_err(Error::Workflow)
}

/// Drains active work on shutdown and returns infrastructure failures to its owner.
async fn run_worker(client: &Client, worker_id: &str, shutdown: CancellationToken) -> Result {
    client
        .worker(router().map_err(Error::Workflow)?)
        .queue(QUEUE)
        .worker_id(worker_id)
        .concurrency(4)
        .claim_timeout(CLAIM)
        .execution_timeout(EXECUTION)
        .cancellation_grace(Duration::from_secs(1))
        .run(shutdown)
        .instrument(tracing::info_span!("worker", worker_id))
        .await
        .map_err(Error::Workflow)
}

/// Registers termination handling before starting any claims and never abandons drain.
async fn serve(client: &Client, worker_id: &str) -> Result {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(Error::Signal)?;
    let signal = async {
        #[cfg(unix)]
        {
            tokio::select! {
                result = tokio::signal::ctrl_c() => result,
                _ = terminate.recv() => Ok(()),
            }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await
    };
    let shutdown = CancellationToken::new();
    let worker = run_worker(client, worker_id, shutdown.clone());
    tokio::pin!(worker);
    tracing::info!(worker_id, "worker starting");
    tokio::select! {
        biased;
        result = signal => {
            tracing::info!(worker_id, "termination requested; draining issued claims and active work");
            shutdown.cancel();
            if let Err(error) = &result { tracing::error!(%error, "signal monitoring failed"); }
            worker.await?;
            result.map_err(Error::Signal)
        },
        result = &mut worker => result,
    }
}

/// Prints structured output without including connection credentials.
fn print_json(value: &impl Serialize) -> Result {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(Error::Json)?
    );
    Ok(())
}

/// Dispatches application commands without silently restarting failed workers.
async fn run(cli: Cli) -> Result {
    let identity = match &cli.command {
        Command::Worker {
            worker_id: Some(id),
        } => id.clone(),
        _ => format!("operations-{}", Uuid::now_v7()),
    };
    let client = connect(&cli.database_url, &identity).await?;
    schema::assert_version(client.pool(), "0.5.0")
        .await
        .map_err(Error::Workflow)?;
    match cli.command {
        Command::Init => initialize(&client).await?,
        Command::Submit { entry_id, amount } => {
            print_json(&submit(&client, entry_id, amount).await?)?
        }
        Command::Show { entry_id } => {
            let request =
                store::load_request(&mut *client.pool().acquire().await?, entry_id).await?;
            let posting =
                store::load_posting(&mut *client.pool().acquire().await?, entry_id).await?;
            let task = client
                .fetch_task_result(QUEUE, request.task_id)
                .await
                .map_err(Error::Workflow)?;
            print_json(&Inspection {
                request,
                posting,
                task,
            })?;
        }
        Command::Worker { .. } => serve(&client, &identity).await?,
    }
    client.pool().close().await;
    Ok(())
}

/// Logs complete error sources and exits unsuccessfully for external supervision.
#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %FailureReason::from_error(&error), "application operation failed");
            ExitCode::FAILURE
        }
    }
}
