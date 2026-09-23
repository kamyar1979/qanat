use crate::codec::Codec;
use crate::errors::BusError;
use crate::message::Envelope;
use bytes::Bytes;
use serde::de::DeserializeOwned;

/// Byte-payload message used by external buses (NNG, Redis, NATS, …).
/// The payload is opaque bytes; callers decode with their chosen `Codec`.
#[derive(Clone, Debug)]
pub struct RawMessage {
    pub envelope: Envelope,
    pub payload: Bytes,
}

impl RawMessage {
    /// Decode metadata-bearing messages, using the supplied codec's header policy.
    /// Use decode for headerless transports, which must use a fixed configured format.
    pub fn decode_with_headers<T: DeserializeOwned>(
        &self,
        codec: &crate::codec::HeaderAwareCodec<impl crate::codec::CodecCollection>,
    ) -> Result<T, BusError> {
        codec.decode_with_content_type(
            &self.payload,
            self.envelope
                .headers
                .as_ref()
                .and_then(crate::codec::content_type),
        )
    }

    pub fn decode<T: DeserializeOwned>(&self, codec: &impl Codec) -> Result<T, BusError> {
        codec.decode(&self.payload)
    }

    pub fn decode_json<T: DeserializeOwned>(&self) -> Result<T, BusError> {
        self.decode(&crate::codec::JsonCodec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Codec, JsonCodec};
    use std::time::Instant;

    fn raw_message(payload: Bytes) -> RawMessage {
        RawMessage {
            envelope: Envelope {
                subject: "orders.created".to_string(),
                timestamp: Instant::now(),
                id: 1,
                headers: None,
                attempts: 0,
            },
            payload,
        }
    }

    #[test]
    fn decode_uses_the_supplied_codec() {
        let message = raw_message(JsonCodec.encode(&42u32).unwrap());

        assert_eq!(message.decode::<u32>(&JsonCodec).unwrap(), 42);
    }

    #[test]
    fn explicit_header_decoding_rejects_unknown_format() {
        let mut message = raw_message(JsonCodec.encode(&42u32).unwrap());
        message.envelope.headers = Some(std::collections::HashMap::from([(
            "Content-Type".into(),
            "application/unknown".into(),
        )]));
        let codec = crate::codec::HeaderAwareCodec::default();
        assert!(message.decode_with_headers::<u32>(&codec).is_err());
        assert_eq!(message.decode::<u32>(&codec).unwrap(), 42);
    }

    #[test]
    fn decode_json_reports_invalid_payloads() {
        let message = raw_message(Bytes::from_static(b"not json"));

        assert!(matches!(
            message.decode_json::<u32>(),
            Err(BusError::Serialization(_))
        ));
    }
}
