use crate::bus::BusStream;
use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::internal_router::RouterHandle;
use crate::raw_message::RawMessage;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct LocalRouter<C: Codec = JsonCodec> {
    pub codec: C,
    router: RouterHandle<RawMessage>,
    next_msg_id: AtomicU64,
}

impl<C: Codec> LocalRouter<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            router: RouterHandle::new(),
            next_msg_id: AtomicU64::new(1),
        }
    }

    pub fn next_message_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn dispatch_local(&self, msg: RawMessage) -> Result<(), BusError> {
        let subject = msg.envelope.subject.clone();
        self.router.dispatch(&subject, msg).await
    }

    pub async fn subscribe(&self, pattern: &str) -> Result<BusStream<RawMessage>, BusError> {
        self.router.subscribe(pattern).await
    }

    pub async fn subscribe_group(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<BusStream<RawMessage>, BusError> {
        self.router.subscribe_group(pattern, group).await
    }
}
