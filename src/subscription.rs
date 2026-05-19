use tokio_stream::wrappers::ReceiverStream;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::message::AnyMessage;

pub struct Subscription {
    inner: ReceiverStream<AnyMessage>,
}

impl Subscription {
    pub fn new(rx: tokio::sync::mpsc::Receiver<AnyMessage>) -> Self {
        Self {
            inner: ReceiverStream::new(rx),
        }
    }
}

impl Stream for Subscription {
    type Item = AnyMessage;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
