use crate::errors::BusError;
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

pub trait Codec: Send + Sync + 'static {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError>;
}

pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|e| BusError::Serialization(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError> {
        serde_json::from_slice(bytes).map_err(|e| BusError::Serialization(e.to_string()))
    }
}
