use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use pin_project_lite::pin_project;

pin_project! {
    /// Future adapter that gracefully handles optional inner futures.
    ///
    /// This future resolves to the inner future output when provided,
    /// and remains pending indefinitely if the inner future is absent.
    #[derive(Debug)]
    pub struct OptionalFuture<F> {
        #[pin]
        inner: Option<F>,
    }
}

impl<F> OptionalFuture<F> {
    /// Creates a new [`OptionalFuture`] wrapping the given `inner` future.
    pub const fn new(inner: Option<F>) -> Self {
        Self { inner }
    }
}

impl<F> Future for OptionalFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.inner.as_mut().as_pin_mut() {
            Some(inner) => inner.poll(cx),
            None => Poll::Pending,
        }
    }
}

/// Helper for constructing an [`OptionalFuture`] without naming the type.
#[inline]
pub fn optional_future<F>(inner: Option<F>) -> OptionalFuture<F> {
    OptionalFuture::new(inner)
}
