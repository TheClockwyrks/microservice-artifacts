//! Stopping without failing a request: the control plane's "Graceful
//! termination".
//!
//! Told to stop, the service fails its readiness probe (`GET /readyz`) at once,
//! so its endpoint leaves the `Service`, while it keeps accepting requests for a
//! further [`DRAIN_DELAY`], so a request routed by an endpoint list not yet
//! updated still lands. Then it stops accepting and finishes every request in
//! flight, a run-tree upload or a streamed download among them, and exits with
//! status 0. An interrupt at a terminal skips the drain delay. The service holds
//! no long-lived stream, so there is none to close.
//!
//! Liveness (`GET /healthz`) is untouched by all of this: a draining process is
//! alive and still serving the requests it holds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How long the service keeps accepting requests after `SIGTERM`, answering its
/// readiness probe `503` meanwhile.
pub const DRAIN_DELAY: Duration = Duration::from_secs(5);

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

/// Resolve on `SIGTERM` or on `Ctrl-C`, answering which.
pub async fn stop_requested() -> Stop {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(err) => {
                tracing::warn!("cannot listen for SIGTERM ({err}); stopping on Ctrl-C only");
                let _ = interrupt.await;
                return Stop::Interrupt;
            }
        };
        tokio::select! {
            _ = interrupt => Stop::Interrupt,
            _ = terminate.recv() => Stop::Terminate,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
        Stop::Interrupt
    }
}

/// The future a server's graceful shutdown waits on: resolves once `stop`
/// resolves, readiness has been failed and the stop's drain delay has passed.
/// The server stops accepting when it resolves and then finishes the requests
/// in flight.
pub async fn drain(readiness: Readiness, stop: impl std::future::Future<Output = Stop>) -> Stop {
    let stop = stop.await;
    readiness.mark_draining();
    let delay = stop.drain_delay();
    tracing::info!(
        signal = ?stop,
        drain_delay_ms = delay.as_millis() as u64,
        "stopping: readiness failed, draining"
    );
    tokio::time::sleep(delay).await;
    tracing::info!("stopping: no longer accepting; finishing the requests in flight");
    stop
}

#[cfg(test)]
#[path = "shutdown.test.rs"]
mod tests;
