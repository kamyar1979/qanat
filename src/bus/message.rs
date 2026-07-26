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

/// Type-erased message used by `InMemoryBus`. Payload travels as `Arc<dyn Any>`
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
            Ok(arc_t) => Ok(Message {
                envelope: self.envelope,
                payload: arc_t,
            }),
            Err(arc_any) => Err(AnyMessage {
                envelope: self.envelope,
                payload: arc_any,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(payload: impl Any + Send + Sync) -> AnyMessage {
        AnyMessage {
            envelope: Envelope {
                subject: "orders.created".to_string(),
                timestamp: Instant::now(),
                id: 7,
                headers: None,
                attempts: 0,
            },
            payload: Arc::new(payload),
        }
    }

    #[test]
    fn downcast_returns_typed_payload_and_preserves_envelope() {
        let message = message(42u32).downcast::<u32>().unwrap();

        assert_eq!(message.envelope.subject, "orders.created");
        assert_eq!(message.envelope.id, 7);
        assert_eq!(*message.payload, 42);
    }

    #[test]
    fn failed_downcast_returns_the_original_message() {
        let message = message(42u32).downcast::<String>().err().unwrap();

        assert_eq!(message.envelope.id, 7);
        assert_eq!(*message.downcast::<u32>().unwrap().payload, 42);
    }
}
