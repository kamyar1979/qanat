use std::collections::HashMap;
use std::sync::Arc;
use crate::errors::BusError;
use crate::message_bus::MessageBus;
use crate::router::Router;
use crate::subscription::Subscription;

pub struct QanatBus {
    router: Arc<Router>,
}

impl QanatBus {
    pub fn new() -> Self {
        Self {
            router: Arc::new(Router::new()),
        }
    }
}

impl MessageBus for QanatBus {
    async fn publish<T: Send + Sync + 'static>(
        &self,
        subject: &str,
        payload: T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError> {
        self.router.publish(subject, payload, headers).await
    }

    async fn bind_queue(
        &self,
        pattern: &str,
        queue: &str,
    ) -> Result<(), BusError> {
        self.router.bind_queue(pattern, queue).await
    }

    async fn subscribe(
        &self,
        pattern: &str,
    ) -> Result<Subscription, BusError> {
        self.router.subscribe(pattern).await
    }

    async fn consume(
        &self,
        queue: &str,
    ) -> Result<Subscription, BusError> {
        self.router.consume(queue).await
    }
}
