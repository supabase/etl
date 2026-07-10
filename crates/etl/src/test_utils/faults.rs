//! Fault injection primitives for destination test wrappers.
//!
//! Destinations report through two channels: the method return value
//! (dispatch) and the async result handle (completion). Faults target either
//! channel: [`FaultAction::FailDispatch`] rejects work before the inner
//! destination runs, while the result actions let the inner destination run
//! and then fail, hold, or delay what the apply loop observes. Faults are
//! queued FIFO per operation and consumed one per call; an empty queue means
//! fully transparent behavior.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{Mutex, oneshot, watch},
    time::{sleep, timeout},
};

use crate::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    test_utils::notify::DEFAULT_NOTIFY_TIMEOUT,
};

/// Destination operation a fault applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultyOp {
    Startup,
    DropTableForCopy,
    WriteTableRows,
    WriteEvents,
    Shutdown,
}

/// Error details injected by a fault, materialized at fire time.
///
/// Stored as kind plus message because [`EtlError`] is not reusable across
/// firings. The kind matters: retry policy derives from it.
#[derive(Debug, Clone)]
pub struct InjectedError {
    kind: ErrorKind,
    message: String,
}

impl InjectedError {
    /// Creates injected error details with the given kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    /// Builds the [`EtlError`] this injection fires with.
    pub fn to_etl_error(&self) -> EtlError {
        etl_error!(self.kind, "Injected destination fault", self.message.clone())
    }
}

/// Scripted fault behavior, consumed FIFO per operation.
#[derive(Debug)]
pub enum FaultAction {
    /// The method returns `Err` immediately; the inner destination never runs.
    FailDispatch(InjectedError),
    /// The inner destination runs; its async result is replaced with `Err`.
    ///
    /// This models the lost-response ambiguity: the destination applied the
    /// write but the apply loop observes a failure.
    FailResult(InjectedError),
    /// The inner destination runs; its async result is withheld until the
    /// paired [`HoldHandle`] releases it.
    HoldResult(HoldGate),
    /// The inner destination runs; its async result is delayed.
    DelayResult(Duration),
}

impl FaultAction {
    /// Creates a dispatch failure with the given error kind and message.
    pub fn fail_dispatch(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::FailDispatch(InjectedError::new(kind, message))
    }

    /// Creates a result failure with the given error kind and message.
    pub fn fail_result(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::FailResult(InjectedError::new(kind, message))
    }

    /// Creates a hold action and the handle that releases it.
    pub fn hold() -> (Self, HoldHandle) {
        let (reached_tx, reached_rx) = watch::channel(false);
        let (release_tx, release_rx) = oneshot::channel();

        let action = Self::HoldResult(HoldGate { reached_tx, release_rx });
        let handle = HoldHandle { reached_rx: Mutex::new(reached_rx), release_tx };

        (action, handle)
    }
}

/// Release decision sent from a [`HoldHandle`] to its [`HoldGate`].
#[derive(Debug)]
enum Release {
    Ok,
    Err(InjectedError),
}

/// Wrapper-side half of a hold: blocks an async result until released.
#[derive(Debug)]
pub struct HoldGate {
    reached_tx: watch::Sender<bool>,
    release_rx: oneshot::Receiver<Release>,
}

impl HoldGate {
    /// Holds `inner_result` until the paired handle releases it.
    ///
    /// On [`HoldHandle::release_ok`] the inner destination's captured result
    /// passes through unchanged, preserving its genuine write status. On
    /// [`HoldHandle::release_err`] the injected error replaces it. A handle
    /// dropped without releasing fails loudly instead of unblocking silently.
    pub async fn apply<T>(self, inner_result: EtlResult<T>) -> EtlResult<T> {
        let _ = self.reached_tx.send(true);

        match self.release_rx.await {
            Ok(Release::Ok) => inner_result,
            Ok(Release::Err(injected)) => Err(injected.to_etl_error()),
            Err(_) => Err(etl_error!(
                ErrorKind::InvalidState,
                "Fault hold handle dropped without release",
                "A HoldHandle was dropped before release_ok or release_err was called"
            )),
        }
    }
}

/// Test-side half of a hold: observes and releases a held async result.
#[derive(Debug)]
pub struct HoldHandle {
    reached_rx: Mutex<watch::Receiver<bool>>,
    release_tx: oneshot::Sender<Release>,
}

impl HoldHandle {
    /// Waits until the held operation has completed on the inner destination
    /// and its result is being withheld.
    ///
    /// # Panics
    ///
    /// Panics after [`DEFAULT_NOTIFY_TIMEOUT`] if the hold point is never
    /// reached, mirroring [`crate::test_utils::notify::TimedNotify`].
    pub async fn wait_reached(&self) {
        let mut reached_rx = self.reached_rx.lock().await;

        timeout(DEFAULT_NOTIFY_TIMEOUT, reached_rx.wait_for(|reached| *reached))
            .await
            .expect("timed out waiting for a held destination operation to be reached")
            .expect("hold gate dropped before the hold point was reached");
    }

    /// Releases the held result, passing the inner destination's captured
    /// result through unchanged.
    pub fn release_ok(self) {
        let _ = self.release_tx.send(Release::Ok);
    }

    /// Releases the held result, replacing it with an injected error.
    pub fn release_err(self, kind: ErrorKind, message: impl Into<String>) {
        let _ = self.release_tx.send(Release::Err(InjectedError::new(kind, message)));
    }
}

/// Applies a consumed fault to an operation's completion result.
///
/// [`FaultAction::FailDispatch`] is normally handled before the inner
/// destination runs; if it reaches the completion phase it fails the result
/// defensively.
pub async fn apply_result_fault<T>(
    fault: Option<FaultAction>,
    inner_result: EtlResult<T>,
) -> EtlResult<T> {
    match fault {
        None => inner_result,
        Some(FaultAction::FailDispatch(injected) | FaultAction::FailResult(injected)) => {
            Err(injected.to_etl_error())
        }
        Some(FaultAction::HoldResult(gate)) => gate.apply(inner_result).await,
        Some(FaultAction::DelayResult(duration)) => {
            sleep(duration).await;
            inner_result
        }
    }
}

/// Per-operation FIFO queues of scripted faults.
///
/// Cloning shares the underlying queues, so a clone held by a test scripts
/// faults for the wrapper that owns the original.
#[derive(Debug, Clone, Default)]
pub struct FaultInjector {
    queues: Arc<Mutex<HashMap<FaultyOp, VecDeque<FaultAction>>>>,
}

impl FaultInjector {
    /// Creates an injector with no scripted faults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a fault for the next unconsumed call of the operation.
    pub async fn inject(&self, op: FaultyOp, action: FaultAction) {
        let mut queues = self.queues.lock().await;
        queues.entry(op).or_default().push_back(action);
    }

    /// Consumes the next queued fault for the operation, if any.
    pub async fn next(&self, op: FaultyOp) -> Option<FaultAction> {
        let mut queues = self.queues.lock().await;
        queues.get_mut(&op)?.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn injector_returns_none_without_scripted_faults() {
        // --- GIVEN: an injector with no faults ---
        let injector = FaultInjector::new();

        // --- THEN: every operation passes through ---
        assert!(injector.next(FaultyOp::WriteEvents).await.is_none());
        assert!(injector.next(FaultyOp::Shutdown).await.is_none());
    }

    #[tokio::test]
    async fn injector_consumes_faults_in_fifo_order_per_operation() {
        // --- GIVEN: two faults on write_events and one on shutdown ---
        let injector = FaultInjector::new();
        injector
            .inject(
                FaultyOp::WriteEvents,
                FaultAction::fail_dispatch(ErrorKind::DestinationQueryFailed, "first"),
            )
            .await;
        injector
            .inject(
                FaultyOp::WriteEvents,
                FaultAction::fail_result(ErrorKind::DestinationTimeout, "second"),
            )
            .await;
        injector
            .inject(
                FaultyOp::Shutdown,
                FaultAction::fail_dispatch(ErrorKind::DestinationQueryFailed, "shutdown"),
            )
            .await;

        // --- WHEN: write_events faults are consumed ---
        let first = injector.next(FaultyOp::WriteEvents).await;
        let second = injector.next(FaultyOp::WriteEvents).await;
        let third = injector.next(FaultyOp::WriteEvents).await;

        // --- THEN: they fire in injection order, then the queue is empty ---
        assert!(matches!(first, Some(FaultAction::FailDispatch(_))));
        assert!(matches!(second, Some(FaultAction::FailResult(_))));
        assert!(third.is_none());

        // --- THEN: the shutdown queue is independent ---
        assert!(injector.next(FaultyOp::Shutdown).await.is_some());
    }

    #[tokio::test]
    async fn injected_error_carries_kind_and_message() {
        // --- GIVEN: an injected error ---
        let injected = InjectedError::new(ErrorKind::DestinationTimeout, "boom");

        // --- WHEN: it fires ---
        let err = injected.to_etl_error();

        // --- THEN: the error carries the injected kind and message ---
        assert_eq!(err.kind(), ErrorKind::DestinationTimeout);
        assert_eq!(err.detail(), Some("boom"));
    }

    #[tokio::test]
    async fn hold_gate_passes_captured_result_through_on_release_ok() {
        // --- GIVEN: a held operation whose inner result is Ok(42) ---
        let (action, handle) = FaultAction::hold();
        let FaultAction::HoldResult(gate) = action else {
            panic!("hold() must produce a HoldResult action");
        };
        let held = tokio::spawn(gate.apply(Ok::<_, EtlError>(42)));

        // --- WHEN: the test observes the hold and releases it ---
        handle.wait_reached().await;
        handle.release_ok();

        // --- THEN: the captured inner result passes through unchanged ---
        assert_eq!(held.await.unwrap().unwrap(), 42);
    }

    #[tokio::test]
    async fn hold_gate_replaces_result_on_release_err() {
        // --- GIVEN: a held operation whose inner result is Ok ---
        let (action, handle) = FaultAction::hold();
        let FaultAction::HoldResult(gate) = action else {
            panic!("hold() must produce a HoldResult action");
        };
        let held = tokio::spawn(gate.apply(Ok::<_, EtlError>(())));

        // --- WHEN: the test releases it with an injected error ---
        handle.wait_reached().await;
        handle.release_err(ErrorKind::DestinationConnectionFailed, "lost response");

        // --- THEN: the injected error replaces the inner result ---
        let err = held.await.unwrap().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationConnectionFailed);
        assert_eq!(err.detail(), Some("lost response"));
    }

    #[tokio::test]
    async fn apply_result_fault_passes_through_or_replaces_the_inner_result() {
        // --- GIVEN: an inner result of Ok(7) ---
        // --- WHEN: no fault is scripted ---
        let ok = apply_result_fault(None, Ok::<_, EtlError>(7)).await;

        // --- THEN: the inner result passes through unchanged ---
        assert_eq!(ok.unwrap(), 7);

        // --- WHEN: a result failure is scripted ---
        let failed = apply_result_fault(
            Some(FaultAction::fail_result(ErrorKind::DestinationTimeout, "late boom")),
            Ok::<_, EtlError>(7),
        )
        .await;

        // --- THEN: the injected error replaces the inner result ---
        let err = failed.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTimeout);
        assert_eq!(err.detail(), Some("late boom"));
    }

    #[tokio::test]
    async fn hold_gate_fails_loudly_when_handle_dropped_without_release() {
        // --- GIVEN: a hold whose handle is dropped without releasing ---
        let (action, handle) = FaultAction::hold();
        let FaultAction::HoldResult(gate) = action else {
            panic!("hold() must produce a HoldResult action");
        };
        drop(handle);

        // --- WHEN: the held operation applies ---
        let result = gate.apply(Ok::<_, EtlError>(())).await;

        // --- THEN: it fails instead of unblocking silently ---
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidState);
    }
}
