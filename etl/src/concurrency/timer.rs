use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use pin_project_lite::pin_project;
use tokio::time::{sleep, Sleep};

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct Timer {
        #[pin]
        deadline: Option<Sleep>,
        duration: Duration
    }
}

impl Timer {

    pub fn new(duration: Duration) -> Self {
        Self {
            deadline: None,
            duration,
        }
    }

    pub fn start(&mut self) {
        self.deadline = Some(sleep(self.duration));
    }
}

impl Future for Timer {

    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let Some(deadline) = this.deadline.as_pin_mut() else {
            return Poll::Pending;
        };

        ready!(deadline.poll(cx));

        Poll::Ready(())
    }
}