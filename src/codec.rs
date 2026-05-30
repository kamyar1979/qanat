use crate::errors::BusError;
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

pub trait Codec: Send + Sync + 'static {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError>;
}

#[derive(Clone, Copy, Debug, Default)]
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

#[cfg(feature = "cbor")]
#[derive(Clone, Copy, Debug, Default)]
pub struct CborCodec;

#[cfg(feature = "cbor")]
impl Codec for CborCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(value, &mut buf)
            .map_err(|e| BusError::Serialization(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError> {
        ciborium::de::from_reader(bytes).map_err(|e| BusError::Serialization(e.to_string()))
    }
}

#[cfg(feature = "msgpack")]
#[derive(Clone, Copy, Debug, Default)]
pub struct MsgPackCodec;

#[cfg(feature = "msgpack")]
impl Codec for MsgPackCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError> {
        rmp_serde::to_vec(value)
            .map(Bytes::from)
            .map_err(|e| BusError::Serialization(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError> {
        rmp_serde::from_slice(bytes).map_err(|e| BusError::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_codec_round_trips() {
        let encoded = JsonCodec.encode(&("hello", 42u32)).unwrap();
        let decoded: (String, u32) = JsonCodec.decode(&encoded).unwrap();
        assert_eq!(decoded, ("hello".to_string(), 42));
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn cbor_codec_round_trips() {
        let encoded = CborCodec.encode(&("hello", 42u32)).unwrap();
        let decoded: (String, u32) = CborCodec.decode(&encoded).unwrap();
        assert_eq!(decoded, ("hello".to_string(), 42));
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_codec_round_trips() {
        let encoded = MsgPackCodec.encode(&("hello", 42u32)).unwrap();
        let decoded: (String, u32) = MsgPackCodec.decode(&encoded).unwrap();
        assert_eq!(decoded, ("hello".to_string(), 42));
    }
}
