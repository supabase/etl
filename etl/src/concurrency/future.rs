use futures::FutureExt;
use futures::future::{BoxFuture, CatchUnwind};
use pin_project_lite::pin_project;
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, panic};
use tokio::sync::Mutex;

/// A callback interface for receiving notifications about future completion.
///
/// The generic parameter `I` represents the identifier type used to track futures.
pub trait ReactiveFutureCallback<I> {
    /// Called when a future completes successfully.
    fn on_complete(&mut self, id: I);

    /// Called when a future encounters an error.
    fn on_error(&mut self, id: I, error: String);
}

/// Represents the internal state of a [`ReactiveFuture`].
enum ReactiveFutureState {
    /// The inner future is currently being polled.
    PollInnerFuture,
    /// The inner future has completed, and we have to notify the callback.
    InvokeCallback(Option<String>),
    /// The future has finished all processing.
    Finished,
}

pin_project! {
    /// A future that notifies a callback when it completes or encounters an error.
    ///
    /// The generic parameters are:
    /// - `Fut`: The underlying future type
    /// - `I`: The identifier type for tracking futures
    /// - `C`: The callback type that implements [`ReactiveFutureCallback`]
    pub struct ReactiveFuture<Fut, I, C, E> {
        #[pin]
        future: CatchUnwind<AssertUnwindSafe<Fut>>,
        #[pin]
        callback_fut: Option<BoxFuture<'static, ()>>,
        state: ReactiveFutureState,
        id: I,
        callback_source: Arc<Mutex<C>>,
        original_error: Option<E>,
        original_panic: Option<Box<dyn Any + Send>>
    }
}

impl<Fut, I, C, E> ReactiveFuture<Fut, I, C, E>
where
    Fut: Future<Output = Result<(), E>>,
{
    /// Creates a new reactive future that will notify the callback when it completes.
    ///
    /// The callback will be notified of either success or failure, and will receive the provided
    /// identifier to track which future completed.
    pub fn wrap(future: Fut, id: I, callback_source: Arc<Mutex<C>>) -> Self {
        Self {
            future: AssertUnwindSafe(future).catch_unwind(),
            callback_fut: None,
            state: ReactiveFutureState::PollInnerFuture,
            id,
            callback_source,
            original_error: None,
            original_panic: None,
        }
    }
}

impl<Fut, I, C, E> Future for ReactiveFuture<Fut, I, C, E>
where
    Fut: Future<Output = Result<(), E>>,
    I: Clone + Send + 'static,
    C: ReactiveFutureCallback<I> + Send + Sync + 'static,
    E: fmt::Display,
{
    type Output = Result<(), E>;

    /// Polls the future, handling both successful completion and errors.
    ///
    /// This implementation ensures that the callback is notified appropriately when the future
    /// completes, whether successfully or with an error. It also handles panics by converting
    /// them into error notifications.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state {
                ReactiveFutureState::PollInnerFuture => match this.future.as_mut().poll(cx) {
                    Poll::Ready(Ok(inner_result)) => match inner_result {
                        Ok(_) => {
                            *this.state = ReactiveFutureState::InvokeCallback(None);
                        }
                        Err(err) => {
                            let casted_err = format!("{err}");

                            *this.original_error = Some(err);
                            *this.state = ReactiveFutureState::InvokeCallback(Some(casted_err));
                        }
                    },
                    Poll::Ready(Err(err)) => {
                        // We want to handle the most common panic types.
                        let casted_err = if let Some(s) = err.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = err.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "Unknown panic error".to_string()
                        };

                        *this.original_panic = Some(Box::new(err));
                        *this.state = ReactiveFutureState::InvokeCallback(Some(casted_err));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                ReactiveFutureState::InvokeCallback(error) => {
                    if this.callback_fut.is_none() {
                        let id = this.id.clone();
                        let callback_source = this.callback_source.clone();
                        let error = error.clone();

                        let callback_fut = async move {
                            let mut guard = callback_source.lock().await;
                            match error {
                                Some(err) => guard.on_error(id, err),
                                None => guard.on_complete(id),
                            };
                        }
                        .boxed();

                        *this.callback_fut = Some(callback_fut);
                    }

                    if let Some(callback_fut) = this.callback_fut.as_mut().as_pin_mut() {
                        match callback_fut.poll(cx) {
                            Poll::Ready(()) => {
                                *this.state = ReactiveFutureState::Finished;
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }
                }
                ReactiveFutureState::Finished => {
                    // If we have a panic, we want to take it and resume it.
                    if let Some(original_panic) = this.original_panic.take() {
                        panic::resume_unwind(original_panic);
                    }

                    // If we have an error, we want to take it and propagate it.
                    if let Some(original_error) = this.original_error.take() {
                        return Poll::Ready(Err(original_error));
                    }

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::concurrency::future::{ReactiveFuture, ReactiveFutureCallback};
    use futures::FutureExt;
    use std::fmt;
    use std::future::Future;
    use std::panic;
    use std::panic::AssertUnwindSafe;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::sync::Mutex;

    #[derive(Debug)]
    struct SimpleError(String);

    impl fmt::Display for SimpleError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    struct ImmediateFuture;

    impl Future for ImmediateFuture {
        type Output = Result<(), SimpleError>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }

    struct PanicFuture {
        panic_any: bool,
    }

    impl Future for PanicFuture {
        type Output = Result<(), SimpleError>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.panic_any {
                panic::panic_any(150);
            } else {
                panic!("The future had an error");
            }
        }
    }

    struct PendingFuture {
        polled: bool,
    }

    impl PendingFuture {
        pub fn new() -> Self {
            Self { polled: false }
        }
    }

    impl Future for PendingFuture {
        type Output = Result<(), SimpleError>;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.polled {
                Poll::Ready(Ok(()))
            } else {
                self.polled = true;
                Poll::Pending
            }
        }
    }

    struct ErrorFuture;

    impl Future for ErrorFuture {
        type Output = Result<(), SimpleError>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(SimpleError("Test error".to_string())))
        }
    }

    #[derive(Default)]
    struct MockCallback {
        complete_called: bool,
        error_called: bool,
        error_message: Option<String>,
    }

    impl ReactiveFutureCallback<i32> for MockCallback {
        fn on_complete(&mut self, _id: i32) {
            self.complete_called = true;
        }

        fn on_error(&mut self, _id: i32, error: String) {
            self.error_called = true;
            self.error_message = Some(error);
        }
    }

    #[tokio::test]
    async fn test_successful_completion() {
        let callback = Arc::new(Mutex::new(MockCallback::default()));
        let table_id = 1;

        let future = ReactiveFuture::wrap(ImmediateFuture, table_id, callback.clone());
        future.await.unwrap();

        let guard = callback.lock().await;
        assert!(guard.complete_called);
        assert!(!guard.error_called);
    }

    #[tokio::test]
    async fn test_panic_handling() {
        let callback = Arc::new(Mutex::new(MockCallback::default()));
        let table_id = 1;

        // Test string panic
        let future = AssertUnwindSafe(ReactiveFuture::wrap(
            PanicFuture { panic_any: false },
            table_id,
            callback.clone(),
        ))
        .catch_unwind();
        let result = future.await;
        assert!(result.is_err());

        {
            let guard = callback.lock().await;
            assert!(!guard.complete_called);
            assert!(guard.error_called);
            assert_eq!(
                guard.error_message,
                Some("The future had an error".to_string())
            );
        }

        // Reset callback state
        let mut guard = callback.lock().await;
        guard.complete_called = false;
        guard.error_called = false;
        guard.error_message = None;
        drop(guard);

        // Test any panic
        let future = AssertUnwindSafe(ReactiveFuture::wrap(
            PanicFuture { panic_any: true },
            table_id,
            callback.clone(),
        ))
        .catch_unwind();
        let result = future.await;
        assert!(result.is_err());

        {
            let guard = callback.lock().await;
            assert!(!guard.complete_called);
            assert!(guard.error_called);
            assert_eq!(guard.error_message, Some("Unknown panic error".to_string()));
        }
    }

    #[tokio::test]
    async fn test_pending_state() {
        let callback = Arc::new(Mutex::new(MockCallback::default()));
        let table_id = 1;

        let pending_future = PendingFuture::new();
        let future = ReactiveFuture::wrap(pending_future, table_id, callback.clone());

        // First poll should return pending
        let mut pinned_future = Box::pin(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(
            pinned_future.as_mut().poll(&mut cx),
            Poll::Pending
        ));

        // The future will be ready on the second poll
        assert!(matches!(
            pinned_future.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));

        // Verify the callback was notified of success
        let guard = callback.lock().await;
        assert!(guard.complete_called);
        assert!(!guard.error_called);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let callback = Arc::new(Mutex::new(MockCallback::default()));
        let table_id = 1;

        let future = ReactiveFuture::wrap(ErrorFuture, table_id, callback.clone());
        let result = future.await;

        // Verify the error was propagated
        assert!(result.is_err());

        // Verify the callback was notified of the error
        let guard = callback.lock().await;
        assert!(!guard.complete_called);
        assert!(guard.error_called);
        assert_eq!(guard.error_message, Some("Test error".to_string()));
    }
}
