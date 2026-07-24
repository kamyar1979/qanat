use crate::bus::{Bus, BusStream};
use crate::errors::BusError;
use crate::internal_router::RouterHandle;
use crate::message::{AnyMessage, Envelope};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct InternalBus {
    router: RouterHandle<AnyMessage>,
    next_msg_id: AtomicU64,
}

impl InternalBus {
    pub fn new() -> Self {
        Self {
            router: RouterHandle::new(),
            next_msg_id: AtomicU64::new(1),
        }
    }

    /// Wrap `payload` in an `Arc`, build the envelope, and route to subscribers.
    /// No serialization — the object travels as-is through the in-process channels.
    pub async fn publish<T: Any + Send + Sync + 'static>(
        &self,
        subject: &str,
        payload: T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError> {
        let msg = AnyMessage {
            envelope: Envelope {
                subject: subject.to_string(),
                timestamp: Instant::now(),
                id: self.next_msg_id.fetch_add(1, Ordering::Relaxed),
                headers,
                attempts: 0,
            },
            payload: Arc::new(payload),
        };
        self.dispatch(subject, msg).await
    }
}

impl Bus for InternalBus {
    type Message = AnyMessage;
    type Subscription = BusStream<AnyMessage>;

    fn dispatch<'a>(
        &'a self,
        subject: &'a str,
        msg: AnyMessage,
    ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
        async move { self.router.dispatch(subject, msg).await }
    }

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        self.router.subscribe(pattern).await
    }

    async fn subscribe_group(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError> {
        self.router.subscribe_group(pattern, group).await
    }
}
