use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Envelope {
    pub subject: String,
    pub timestamp: Instant,
    pub id: u64,
    pub headers: Option<HashMap<String, String>>,
    pub attempts: u32,
}

/// Type-erased message used by `InternalBus`. Payload travels as `Arc<dyn Any>`
/// so no serialization is needed for in-process delivery.
#[derive(Clone)]
pub struct AnyMessage {
    pub envelope: Envelope,
    pub payload: Arc<dyn Any + Send + Sync>,
}

impl fmt::Debug for AnyMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnyMessage")
            .field("envelope", &self.envelope)
            .finish_non_exhaustive()
    }
}

/// Typed view produced by `AnyMessage::downcast`.
pub struct Message<T> {
    pub envelope: Envelope,
    pub payload: Arc<T>,
}

impl AnyMessage {
    pub fn downcast<T: Send + Sync + 'static>(self) -> Result<Message<T>, Self> {
        match self.payload.downcast::<T>() {
            Ok(arc_t) => Ok(Message { envelope: self.envelope, payload: arc_t }),
            Err(arc_any) => Err(AnyMessage { envelope: self.envelope, payload: arc_any }),
        }
    }
}
