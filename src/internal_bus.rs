use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use crate::bus::{Bus, BusStream};
use crate::errors::BusError;
use crate::message::{AnyMessage, Envelope};
use crate::routing::{ConsumerId, SubjectRouter};

pub struct InternalBus {
    router: Mutex<SubjectRouter>,
    senders: Mutex<HashMap<ConsumerId, mpsc::Sender<AnyMessage>>>,
    next_msg_id: AtomicU64,
}

impl InternalBus {
    pub fn new() -> Self {
        Self {
            router: Mutex::new(SubjectRouter::new()),
            senders: Mutex::new(HashMap::new()),
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

    async fn dispatch(&self, subject: &str, msg: AnyMessage) -> Result<(), BusError> {
        let targets = self.router.lock().await.route(subject);

        let mut to_send: Vec<(ConsumerId, mpsc::Sender<AnyMessage>)> = Vec::new();
        {
            let senders = self.senders.lock().await;
            for id in &targets {
                if let Some(tx) = senders.get(id) {
                    to_send.push((*id, tx.clone()));
                }
            }
        }

        let mut dead: Vec<ConsumerId> = Vec::new();
        for (id, tx) in to_send {
            if tx.send(msg.clone()).await.is_err() {
                dead.push(id);
            }
        }

        if !dead.is_empty() {
            let mut router = self.router.lock().await;
            let mut senders = self.senders.lock().await;
            for id in dead {
                router.remove_consumer(id);
                senders.remove(&id);
            }
        }

        Ok(())
    }

    async fn subscribe(&self, pattern: &str) -> Result<BusStream<AnyMessage>, BusError> {
        let (tx, rx) = mpsc::channel(128);
        let id = self.router.lock().await.add_fanout(pattern);
        self.senders.lock().await.insert(id, tx);
        Ok(BusStream::new(rx))
    }

    async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError> {
        self.router.lock().await.bind_queue(pattern, queue)
    }

    async fn consume(&self, queue: &str) -> Result<BusStream<AnyMessage>, BusError> {
        let (tx, rx) = mpsc::channel(128);
        let id = self.router.lock().await.add_consumer(queue)?;
        self.senders.lock().await.insert(id, tx);
        Ok(BusStream::new(rx))
    }
}
