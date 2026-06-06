use crate::bus::BusStream;
use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::internal_router::InternalRouter;
use crate::raw_message::RawMessage;
use crate::routing::{ConsumerId, SubjectRouter};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, mpsc};

pub(crate) struct LocalRouter<C: Codec = JsonCodec> {
    pub codec: C,
    router: Mutex<SubjectRouter>,
    senders: Mutex<HashMap<ConsumerId, mpsc::Sender<RawMessage>>>,
    next_msg_id: AtomicU64,
}

impl<C: Codec> LocalRouter<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            router: Mutex::new(SubjectRouter::new()),
            senders: Mutex::new(HashMap::new()),
            next_msg_id: AtomicU64::new(1),
        }
    }

    pub fn next_message_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn dispatch_local(&self, msg: RawMessage) -> Result<(), BusError> {
        let subject = msg.envelope.subject.clone();
        self.dispatch_internal(&self.router, &self.senders, &subject, msg)
            .await
    }

    pub async fn subscribe(&self, pattern: &str) -> BusStream<RawMessage> {
        let (tx, rx) = mpsc::channel(128);
        let id = self.router.lock().await.add_fanout(pattern);
        self.senders.lock().await.insert(id, tx);
        BusStream::new(rx)
    }

    pub async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError> {
        self.router.lock().await.bind_queue(pattern, queue)
    }

    pub async fn consume(&self, queue: &str) -> Result<BusStream<RawMessage>, BusError> {
        let (tx, rx) = mpsc::channel(128);
        let id = self.router.lock().await.add_consumer(queue)?;
        self.senders.lock().await.insert(id, tx);
        Ok(BusStream::new(rx))
    }
}

impl<C: Codec> InternalRouter for LocalRouter<C> {
    type Message = RawMessage;
}
