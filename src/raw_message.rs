use bytes::Bytes;
use serde::de::DeserializeOwned;
use crate::codec::Codec;
use crate::errors::BusError;
use crate::message::Envelope;

/// Byte-payload message used by external buses (NNG, Redis, NATS, …).
/// The payload is opaque bytes; callers decode with their chosen `Codec`.
#[derive(Clone, Debug)]
pub struct RawMessage {
    pub envelope: Envelope,
    pub payload: Bytes,
}

impl RawMessage {
    pub fn decode<T: DeserializeOwned>(&self, codec: &impl Codec) -> Result<T, BusError> {
        codec.decode(&self.payload)
    }

    pub fn decode_json<T: DeserializeOwned>(&self) -> Result<T, BusError> {
        self.decode(&crate::codec::JsonCodec)
    }
}
