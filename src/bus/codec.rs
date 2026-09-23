use crate::errors::BusError;
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;

pub trait Codec: Send + Sync + 'static {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError>;

    fn content_type(&self) -> &'static str {
        "application/octet-stream"
    }
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

    fn content_type(&self) -> &'static str {
        "application/json"
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

    fn content_type(&self) -> &'static str {
        "application/cbor"
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

    fn content_type(&self) -> &'static str {
        "application/msgpack"
    }
}

fn media_type(value: &str) -> &str {
    value.split(';').next().unwrap_or(value).trim()
}

pub fn content_type(headers: &HashMap<String, String>) -> Option<&str> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.as_str())
}

pub fn set_content_type(headers: &mut HashMap<String, String>, value: &str) {
    headers.retain(|name, _| !name.eq_ignore_ascii_case("content-type"));
    headers.insert("content-type".into(), value.into());
}

/// A statically composed collection. Implementations are registered in code;
/// the incoming header and configured default select among them at runtime.
pub trait CodecCollection: Send + Sync + 'static {
    fn contains(&self, media_type: &str) -> bool;
    fn encode_selected<T: Serialize>(&self, media_type: &str, value: &T)
    -> Result<Bytes, BusError>;
    fn decode_selected<T: DeserializeOwned>(
        &self,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<T, BusError>;
    fn canonical_type(&self, media_type: &str) -> Option<&'static str>;
}

impl CodecCollection for () {
    fn contains(&self, _: &str) -> bool {
        false
    }
    fn encode_selected<T: Serialize>(&self, name: &str, _: &T) -> Result<Bytes, BusError> {
        Err(unsupported(name))
    }
    fn decode_selected<T: DeserializeOwned>(&self, name: &str, _: &[u8]) -> Result<T, BusError> {
        Err(unsupported(name))
    }
    fn canonical_type(&self, _: &str) -> Option<&'static str> {
        None
    }
}

impl<C: Codec, Rest: CodecCollection> CodecCollection for (C, Rest) {
    fn contains(&self, name: &str) -> bool {
        matches_content_type(self.0.content_type(), name) || self.1.contains(name)
    }
    fn encode_selected<T: Serialize>(&self, name: &str, value: &T) -> Result<Bytes, BusError> {
        if matches_content_type(self.0.content_type(), name) {
            self.0.encode(value)
        } else {
            self.1.encode_selected(name, value)
        }
    }
    fn decode_selected<T: DeserializeOwned>(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<T, BusError> {
        if matches_content_type(self.0.content_type(), name) {
            self.0.decode(bytes)
        } else {
            self.1.decode_selected(name, bytes)
        }
    }
    fn canonical_type(&self, name: &str) -> Option<&'static str> {
        if matches_content_type(self.0.content_type(), name) {
            Some(self.0.content_type())
        } else {
            self.1.canonical_type(name)
        }
    }
}

fn unsupported(name: &str) -> BusError {
    BusError::Serialization(format!(
        "no codec registered for content type '{name}' (check registration and Cargo features)"
    ))
}

/// Selects incoming decoding by Content-Type; encoding always uses the
/// configured default. No codec trait objects or intermediate values are used.
#[derive(Clone, Debug)]
pub struct HeaderAwareCodec<C = BuiltinCodecs> {
    codecs: C,
    default: &'static str,
}

impl HeaderAwareCodec {
    pub fn new<C: Codec>(codec: C) -> HeaderAwareCodec<(C, ())> {
        let default = codec.content_type();
        HeaderAwareCodec {
            codecs: (codec, ()),
            default,
        }
    }
}

impl Default for HeaderAwareCodec {
    fn default() -> Self {
        let codecs = (JsonCodec, ());
        #[cfg(feature = "cbor")]
        let codecs = (CborCodec, codecs);
        #[cfg(feature = "msgpack")]
        let codecs = (MsgPackCodec, codecs);
        Self {
            codecs,
            default: "application/json",
        }
    }
}

#[cfg(all(feature = "cbor", feature = "msgpack"))]
pub type BuiltinCodecs = (MsgPackCodec, (CborCodec, (JsonCodec, ())));
#[cfg(all(feature = "cbor", not(feature = "msgpack")))]
pub type BuiltinCodecs = (CborCodec, (JsonCodec, ()));
#[cfg(all(not(feature = "cbor"), feature = "msgpack"))]
pub type BuiltinCodecs = (MsgPackCodec, (JsonCodec, ()));
#[cfg(not(any(feature = "cbor", feature = "msgpack")))]
pub type BuiltinCodecs = (JsonCodec, ());

impl<C: CodecCollection> HeaderAwareCodec<C> {
    pub fn register<D: Codec>(self, codec: D) -> HeaderAwareCodec<(D, C)> {
        HeaderAwareCodec {
            codecs: (codec, self.codecs),
            default: self.default,
        }
    }

    pub fn with_default_content_type(mut self, name: &str) -> Result<Self, BusError> {
        self.default = self
            .codecs
            .canonical_type(name)
            .ok_or_else(|| unsupported(name))?;
        Ok(self)
    }
}

impl<C: CodecCollection> Codec for HeaderAwareCodec<C> {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError> {
        self.codecs.encode_selected(self.default, value)
    }
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError> {
        self.codecs.decode_selected(self.default, bytes)
    }
    fn content_type(&self) -> &'static str {
        self.default
    }
}

impl<C: CodecCollection> HeaderAwareCodec<C> {
    pub fn decode_with_content_type<T: DeserializeOwned>(
        &self,
        bytes: &[u8],
        name: Option<&str>,
    ) -> Result<T, BusError> {
        self.codecs
            .decode_selected(name.unwrap_or(self.default), bytes)
    }
}

fn matches_content_type(canonical: &str, incoming: &str) -> bool {
    let incoming = media_type(incoming);
    canonical.eq_ignore_ascii_case(incoming)
        || (canonical == "application/msgpack"
            && incoming.eq_ignore_ascii_case("application/x-msgpack"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct CustomJson;

    impl Codec for CustomJson {
        fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, BusError> {
            JsonCodec.encode(value)
        }
        fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, BusError> {
            JsonCodec.decode(bytes)
        }
        fn content_type(&self) -> &'static str {
            "application/custom+json"
        }
    }

    #[test]
    fn header_selection_and_runtime_default_are_extensible() {
        let codec = HeaderAwareCodec::default()
            .register(CustomJson)
            .with_default_content_type("APPLICATION/CUSTOM+JSON; charset=utf-8")
            .unwrap();
        assert_eq!(codec.content_type(), "application/custom+json");
        let payload = codec.encode(&42u32).unwrap();
        assert_eq!(codec.decode::<u32>(&payload).unwrap(), 42);
        assert_eq!(
            codec
                .decode_with_content_type::<u32>(&payload, Some("Application/JSON; charset=utf-8"))
                .unwrap(),
            42
        );
        assert!(
            codec
                .decode_with_content_type::<u32>(&payload, Some("application/unknown"))
                .is_err()
        );
        assert!(
            HeaderAwareCodec::default()
                .with_default_content_type("application/unknown")
                .is_err()
        );
    }

    #[test]
    fn fixed_codec_ignores_content_type_and_headers_are_replaced() {
        assert_eq!(JsonCodec.decode::<u32>(b"42").unwrap(), 42);
        let mut headers = HashMap::from([
            ("Content-Type".into(), "application/cbor".into()),
            ("content-type".into(), "application/msgpack".into()),
            ("request-id".into(), "123".into()),
        ]);
        set_content_type(&mut headers, "application/json");
        assert_eq!(headers.len(), 2);
        assert_eq!(content_type(&headers), Some("application/json"));
    }

    #[cfg(not(feature = "cbor"))]
    #[test]
    fn disabled_cbor_is_rejected() {
        assert!(
            HeaderAwareCodec::default()
                .decode_with_content_type::<u32>(b"42", Some("application/cbor"))
                .is_err()
        );
    }

    #[cfg(not(feature = "msgpack"))]
    #[test]
    fn disabled_msgpack_is_rejected() {
        assert!(
            HeaderAwareCodec::default()
                .with_default_content_type("application/msgpack")
                .is_err()
        );
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn enabled_cbor_is_selected_by_header_and_configuration() {
        let codec = HeaderAwareCodec::default();
        let bytes = CborCodec.encode(&42u32).unwrap();
        assert_eq!(
            codec
                .decode_with_content_type::<u32>(&bytes, Some("application/cbor"))
                .unwrap(),
            42
        );
        let codec = codec.with_default_content_type("application/cbor").unwrap();
        assert_eq!(
            CborCodec
                .decode::<u32>(&codec.encode(&42u32).unwrap())
                .unwrap(),
            42
        );
        assert!(
            codec
                .decode_with_content_type::<u32>(b"invalid", Some("application/cbor"))
                .is_err()
        );
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn enabled_msgpack_alias_is_selected() {
        let codec = HeaderAwareCodec::default()
            .with_default_content_type("application/x-msgpack")
            .unwrap();
        assert_eq!(codec.content_type(), "application/msgpack");
        let bytes = codec.encode(&42u32).unwrap();
        assert_eq!(
            codec
                .decode_with_content_type::<u32>(&bytes, Some("application/x-msgpack"))
                .unwrap(),
            42
        );
    }

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
