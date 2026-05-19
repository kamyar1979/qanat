use std::collections::HashMap;
use crate::errors::BusError;
use crate::subscription::Subscription;

pub trait MessageBus: Send + Sync {
    async fn publish<T: Send + Sync + 'static>(
        &self,
        subject: &str,
        payload: T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError>;

    async fn bind_queue(
        &self,
        subject_pattern: &str,
        queue: &str,
    ) -> Result<(), BusError>;

    async fn subscribe(
        &self,
        subject_pattern: &str,
    ) -> Result<Subscription, BusError>;

    async fn consume(
        &self,
        queue: &str,
    ) -> Result<Subscription, BusError>;
}
