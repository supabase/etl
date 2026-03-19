use std::future::Future;
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::destination::BatchFlushResult;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;

/// Outcome of processing one flush batch in a [`DestinationActor`].
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DestinationActorOutcome {
    /// Keep the actor running and continue processing future flush batches.
    Continue,
    /// Stop the actor gracefully without surfacing an error.
    Stop,
}

/// Commands accepted by a [`DestinationActor`] task.
#[derive(Debug)]
enum DestinationActorCommand<T, R> {
    /// Processes a single flush batch.
    FlushBatch(T, BatchFlushResult<R>),
    /// Stops the actor loop.
    Shutdown,
}

/// Shared state for a [`DestinationActor`].
#[derive(Debug)]
struct DestinationActorState<T, R> {
    tx: mpsc::UnboundedSender<DestinationActorCommand<T, R>>,
    task: Option<JoinHandle<EtlResult<()>>>,
}

/// A destination-owned actor loop for ordered background work.
///
/// This helper owns a single Tokio task and an unbounded command channel. Destinations enqueue
/// flush batches with [`DestinationActor::send`]. Each queued command carries both the payload
/// and the [`BatchFlushResult`] that should be completed for that payload, and the actor task
/// processes them sequentially using the handler provided to [`DestinationActor::start`]. On
/// shutdown, the actor is asked to stop and the caller waits for the worker task to exit.
#[derive(Debug)]
pub struct DestinationActor<T, R = ()> {
    state: Arc<Mutex<DestinationActorState<T, R>>>,
}

impl<T, R> DestinationActor<T, R>
where
    T: Send + 'static,
    R: Send + 'static,
{
    /// Starts a new destination actor.
    pub fn start<F, Fut>(mut handle_flush_batch: F) -> Self
    where
        F: FnMut(T, BatchFlushResult<R>) -> Fut + Send + 'static,
        Fut: Future<Output = EtlResult<DestinationActorOutcome>> + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    DestinationActorCommand::FlushBatch(payload, flush_result) => {
                        match handle_flush_batch(payload, flush_result).await? {
                            DestinationActorOutcome::Continue => {}
                            DestinationActorOutcome::Stop => break,
                        }
                    }
                    DestinationActorCommand::Shutdown => break,
                }
            }

            Ok(())
        });

        Self {
            state: Arc::new(Mutex::new(DestinationActorState {
                tx,
                task: Some(task),
            })),
        }
    }

    /// Enqueues a flush batch for the actor loop.
    ///
    /// Returns an error if the actor has already been shut down, if the worker task has stopped
    /// unexpectedly, or if actor state access fails.
    pub fn send(&self, payload: T, flush_result: BatchFlushResult<R>) -> EtlResult<()> {
        let tx = self.lock_state()?.tx.clone();

        tx.send(DestinationActorCommand::FlushBatch(payload, flush_result))
            .map_err(|_| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Destination actor is not running"
                )
            })
    }

    /// Stops the actor loop and waits for the worker task to exit.
    ///
    /// This method is idempotent.
    pub async fn shutdown(&self) -> EtlResult<()> {
        let task = {
            let mut state = self.lock_state()?;
            let _ = state.tx.send(DestinationActorCommand::Shutdown);
            state.task.take()
        };

        let Some(task) = task else {
            return Ok(());
        };

        match task.await {
            Ok(result) => result,
            Err(err) => Err(etl_error!(
                ErrorKind::DestinationError,
                "Destination actor task failed",
                err.to_string()
            )),
        }
    }

    /// Returns the locked actor state.
    fn lock_state(&self) -> EtlResult<MutexGuard<'_, DestinationActorState<T, R>>> {
        self.state.lock().map_err(|_| {
            etl_error!(
                ErrorKind::DestinationError,
                "Destination actor state lock poisoned"
            )
        })
    }
}

impl<T, R> Clone for DestinationActor<T, R> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{DestinationActor, DestinationActorOutcome};
    use crate::destination::{BatchFlushMetrics, BatchFlushResult};
    use crate::error::ErrorKind;
    use crate::etl_error;

    #[tokio::test]
    async fn destination_actor_processes_messages_in_order() {
        let processed = Arc::new(Mutex::new(Vec::new()));
        let processed_for_task = processed.clone();

        let actor = DestinationActor::start(move |value, flush_result: BatchFlushResult<()>| {
            let processed = processed_for_task.clone();
            async move {
                processed.lock().unwrap().push(value);
                let _ = flush_result.send(Ok(()));

                Ok(DestinationActorOutcome::Continue)
            }
        });

        let (flush_result, pending_result) = BatchFlushResult::new(
            None,
            BatchFlushMetrics {
                events_count: 1,
                dispatched_at: std::time::Instant::now(),
            },
        );
        actor.send(1, flush_result).unwrap();
        pending_result.await.into_result().unwrap();

        let (flush_result, pending_result) = BatchFlushResult::new(
            None,
            BatchFlushMetrics {
                events_count: 1,
                dispatched_at: std::time::Instant::now(),
            },
        );
        actor.send(2, flush_result).unwrap();
        pending_result.await.into_result().unwrap();
        actor.shutdown().await.unwrap();

        assert_eq!(*processed.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn destination_actor_rejects_messages_after_shutdown() {
        let actor: DestinationActor<()> = DestinationActor::start(|(), _flush_result| async move {
            Ok(DestinationActorOutcome::Continue)
        });

        actor.shutdown().await.unwrap();

        let (flush_result, _pending_result) = BatchFlushResult::new(
            None,
            BatchFlushMetrics {
                events_count: 0,
                dispatched_at: std::time::Instant::now(),
            },
        );
        assert!(actor.send((), flush_result).is_err());
    }

    #[tokio::test]
    async fn destination_actor_can_stop_gracefully_from_handler() {
        let actor = DestinationActor::start(|(), _flush_result: BatchFlushResult<()>| async move {
            Ok(DestinationActorOutcome::Stop)
        });

        let (flush_result, _pending_result) = BatchFlushResult::new(
            None,
            BatchFlushMetrics {
                events_count: 0,
                dispatched_at: std::time::Instant::now(),
            },
        );
        actor.send((), flush_result).unwrap();

        actor.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn destination_actor_continues_after_dropped_flush_receiver() {
        let processed = Arc::new(Mutex::new(Vec::new()));
        let processed_for_task = processed.clone();

        let actor = DestinationActor::start(move |value, flush_result: BatchFlushResult<()>| {
            let processed = processed_for_task.clone();
            async move {
                processed.lock().unwrap().push(value);
                let _ = flush_result.send(Ok(()));

                Ok(DestinationActorOutcome::Continue)
            }
        });

        let (flush_result, pending_result) = BatchFlushResult::new(
            None,
            BatchFlushMetrics {
                events_count: 1,
                dispatched_at: std::time::Instant::now(),
            },
        );
        actor.send(1, flush_result).unwrap();
        drop(pending_result);

        let (flush_result, pending_result) = BatchFlushResult::new(
            None,
            BatchFlushMetrics {
                events_count: 1,
                dispatched_at: std::time::Instant::now(),
            },
        );
        actor.send(2, flush_result).unwrap();
        pending_result.await.into_result().unwrap();

        actor.shutdown().await.unwrap();

        assert_eq!(*processed.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn destination_actor_propagates_handler_errors_on_shutdown() {
        let actor = DestinationActor::start(|(), _flush_result: BatchFlushResult<()>| async move {
            Err(etl_error!(
                ErrorKind::DestinationError,
                "Destination actor handler failed"
            ))
        });

        let (flush_result, _pending_result) = BatchFlushResult::new(
            None,
            BatchFlushMetrics {
                events_count: 0,
                dispatched_at: std::time::Instant::now(),
            },
        );
        actor.send((), flush_result).unwrap();

        let err = actor.shutdown().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationError);
        assert_eq!(err.description(), Some("Destination actor handler failed"));
    }
}
