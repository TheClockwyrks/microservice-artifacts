//! Stopping without failing a request: the control plane's "Graceful
//! termination".
//!
//! Told to stop, the service fails its readiness probe (`GET /readyz`) at once,
//! so its endpoint leaves the `Service`, while it keeps accepting requests for a
//! further [`DRAIN_DELAY`], so a request routed by an endpoint list not yet
//! updated still lands. Then it stops accepting and finishes every request in
//! flight, a run-tree upload or a streamed download among them, and exits with
//! status 0, at the latest [`STOP_DEADLINE`] after the signal, abandoning
//! whatever is still running then so the exit stays inside the pod's grace. An
//! interrupt at a terminal skips the drain delay. The service holds no
//! long-lived stream, so there is none to close.
//!
//! The signal handlers are installed before the start ([`stop_signal`]), so a
//! stop that arrives while the service is still waiting for its run-tree
//! container ends the start and exits 0 too, rather than killing the process.
//!
//! Liveness (`GET /healthz`) is untouched by all of this: a draining process is
//! alive and still serving the requests it holds.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

/// How long the service keeps accepting requests after `SIGTERM`, answering its
/// readiness probe `503` meanwhile.
pub const DRAIN_DELAY: Duration = Duration::from_secs(5);

/// How long after the stop the process exits however much is still in flight.
///
/// The service's termination grace is 300 seconds and starts with the
/// 5-second `preStop` hook that precedes `SIGTERM`, so 290 seconds from the
/// signal leaves a margin of 5 before the kubelet would kill the process.
pub const STOP_DEADLINE: Duration = Duration::from_secs(290);

/// Whether the service is ready to take new requests: until it is told to stop.
///
/// Cloneable and cheap; every clone shares one flag, so the probe handler sees
/// what the stop marked.
#[derive(Clone, Debug, Default)]
pub struct Readiness {
    /// Set once the service was told to stop; never cleared.
    draining: Arc<AtomicBool>,
}

impl Readiness {
    /// A service ready to serve.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the readiness probe passes.
    pub fn is_ready(&self) -> bool {
        !self.is_draining()
    }

    /// Whether the service was told to stop.
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// Fail readiness from now on. Idempotent.
    pub fn mark_draining(&self) {
        self.draining.store(true, Ordering::Release);
    }
}

/// What told the service to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    /// `SIGTERM`, which Kubernetes sends a pod it stops.
    Terminate,
    /// An interrupt (`Ctrl-C`) at a terminal.
    Interrupt,
}

impl Stop {
    /// How long to keep accepting requests after this stop: the
    /// [`DRAIN_DELAY`] for `SIGTERM`, none for an interrupt.
    pub fn drain_delay(self) -> Duration {
        match self {
            Self::Terminate => DRAIN_DELAY,
            Self::Interrupt => Duration::ZERO,
        }
    }
}

/// Install the handlers for `SIGTERM` and the interrupt, and answer a future
/// that resolves when either arrives, naming which.
///
/// The handlers are installed by this call, not when the future is first
/// polled, so a signal delivered at any point afterwards is held for the future
/// rather than ending the process by the signal's default action. Call it before
/// anything that can take time, the start's wait for the run-tree container
/// among it, so a stop is never lost to a slow start.
pub fn stop_signal() -> std::io::Result<impl Future<Output = Stop> + Send + 'static> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        Ok(async move {
            tokio::select! {
                _ = terminate.recv() => Stop::Terminate,
                _ = interrupt.recv() => Stop::Interrupt,
            }
        })
    }
    #[cfg(not(unix))]
    {
        Ok(async move {
            // Only an interrupt reaches a process here. An error installing its
            // handler leaves no way to be asked to stop, so wait forever then.
            match tokio::signal::ctrl_c().await {
                Ok(()) => Stop::Interrupt,
                Err(err) => {
                    tracing::warn!(error = %err, "could not listen for an interrupt");
                    std::future::pending().await
                }
            }
        })
    }
}

/// A stop request every part of the process can wait on: the start, the
/// server's drain and the stop deadline. Cloneable; every clone sees the one
/// request, with the moment it arrived.
#[derive(Clone, Debug)]
pub struct StopRequest {
    requested: watch::Receiver<Option<(Stop, Instant)>>,
}

impl StopRequest {
    /// Wait on `stop` in a task of its own, recording what it answers and when.
    /// Must be called inside a Tokio runtime.
    pub fn spawn(stop: impl Future<Output = Stop> + Send + 'static) -> Self {
        let (sender, requested) = watch::channel(None);
        tokio::spawn(async move {
            let stop = stop.await;
            tracing::info!(signal = ?stop, "stop requested");
            sender.send_replace(Some((stop, Instant::now())));
            // Keep the sender, so a waiter never sees the channel close.
            std::future::pending::<()>().await;
        });
        Self { requested }
    }

    /// Resolve once the stop was requested, answering what requested it and
    /// when. Resolves at once when it already was.
    pub async fn requested(&self) -> (Stop, Instant) {
        let mut requested = self.requested.clone();
        let request = requested
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|value| *value);
        match request {
            Some(request) => request,
            // The task holds the sender forever; a closed channel means the
            // runtime is going away, and no stop will come.
            None => std::future::pending().await,
        }
    }
}

/// The future a server's graceful shutdown waits on: resolves once the stop is
/// requested, readiness has been failed and the stop's drain delay has passed.
/// The server stops accepting when it resolves and then finishes the requests
/// in flight.
pub async fn drain(readiness: Readiness, stop: StopRequest) -> Stop {
    let (stop, at) = stop.requested().await;
    readiness.mark_draining();
    let delay = stop.drain_delay();
    tracing::info!(
        signal = ?stop,
        drain_delay_ms = delay.as_millis() as u64,
        "stopping: readiness failed, draining"
    );
    tokio::time::sleep_until(at + delay).await;
    tracing::info!("stopping: no longer accepting; finishing the requests in flight");
    stop
}

/// Resolve [`STOP_DEADLINE`] after the stop was requested: the moment the
/// process exits however much is still in flight.
pub async fn deadline(stop: StopRequest) {
    let (_, at) = stop.requested().await;
    tokio::time::sleep_until(at + STOP_DEADLINE).await;
}

#[cfg(test)]
#[path = "shutdown.test.rs"]
mod tests;
