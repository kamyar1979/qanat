use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

pub const MESSAGE_ID_HEADER: &str = "qanat-message-id";

pub(crate) fn new_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub(crate) fn message_id_from_headers(headers: &HashMap<String, String>) -> Option<String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(MESSAGE_ID_HEADER))
        .map(|(_, value)| value.clone())
        .filter(|value| !value.is_empty())
}

pub(crate) fn ensure_message_id(headers: &mut HashMap<String, String>) -> String {
    if let Some(message_id) = message_id_from_headers(headers) {
        headers.retain(|name, _| !name.eq_ignore_ascii_case(MESSAGE_ID_HEADER));
        headers.insert(MESSAGE_ID_HEADER.into(), message_id.clone());
        return message_id;
    }
    let message_id = new_message_id();
    headers.insert(MESSAGE_ID_HEADER.into(), message_id.clone());
    message_id
}

#[derive(Clone, Debug)]
pub struct Envelope {
    pub subject: String,
    pub timestamp: Instant,
    pub id: u64,
    pub message_id: String,
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
    // Returning the original message lets callers recover from a failed downcast
    // without another allocation; boxing would undermine the in-memory fast path.
    #[allow(clippy::result_large_err)]
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
                message_id: "message-7".to_string(),
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
