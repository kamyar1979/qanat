use crate::bus::{Bus, BusStream};
use crate::errors::BusError;
use crate::internal_router::RouterHandle;
use crate::message::{AnyMessage, Envelope};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct InMemoryBus {
    router: RouterHandle<AnyMessage>,
    next_msg_id: AtomicU64,
}

impl InMemoryBus {
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

impl Default for InMemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Bus for InMemoryBus {
    type Message = AnyMessage;
    type Subscription = BusStream<AnyMessage>;

    fn dispatch<'a>(
        &'a self,
        subject: &'a str,
        msg: AnyMessage,
    ) -> impl Future<Output = Result<(), BusError>> + Send + 'a {
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn publish_preserves_envelope_metadata_and_increments_ids() {
        let bus = InMemoryBus::new();
        let mut subscription = bus.subscribe("orders.created").await.unwrap();
        let headers = HashMap::from([("trace_id".to_string(), "trace-1".to_string())]);

        bus.publish("orders.created", 41u32, Some(headers.clone()))
            .await
            .unwrap();
        bus.publish("orders.created", 42u32, None).await.unwrap();

        let first = subscription.next().await.unwrap();
        let second = subscription.next().await.unwrap();

        assert_eq!(first.envelope.subject.as_str(), "orders.created");
        assert_eq!(first.envelope.id, 1);
        assert_eq!(first.envelope.headers.as_ref(), Some(&headers));
        assert_eq!(first.envelope.attempts, 0);
        assert_eq!(*first.downcast::<u32>().unwrap().payload, 41);

        assert_eq!(second.envelope.id, 2);
        assert!(second.envelope.headers.is_none());
        assert_eq!(*second.downcast::<u32>().unwrap().payload, 42);
    }

    #[tokio::test]
    async fn default_constructs_a_working_bus() {
        let bus = InMemoryBus::default();
        let mut subscription = bus.subscribe("health.ready").await.unwrap();

        bus.publish("health.ready", true, None).await.unwrap();

        assert!(
            *subscription
                .next()
                .await
                .unwrap()
                .downcast::<bool>()
                .unwrap()
                .payload
        );
    }
}
