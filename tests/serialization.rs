use bytes::Bytes;
use futures::{future::LocalBoxFuture, stream};
#[cfg(any(feature = "cbor", feature = "axum"))]
use qanat::codec::HeaderAwareCodec;
use qanat::{
    Bus,
    codec::{Codec, JsonCodec},
    errors::BusError,
    http::{HttpRequest, HttpResponse, HttpTarget},
    raw_message::RawMessage,
    router::{
        BrokerSource, BrokerTarget, PayloadValue, RouteMessage, RouteSource, RouteStream,
        RouteTarget, Router,
    },
};
#[cfg(feature = "axum")]
use std::collections::HashMap;

struct Transport<const HEADERS: bool>;

impl<const HEADERS: bool> Bus for Transport<HEADERS> {
    type Message = RawMessage;
    type Subscription = stream::Empty<RawMessage>;

    fn supports_content_type_headers(&self) -> bool {
        HEADERS
    }
    async fn dispatch(&self, _: &str, _: RawMessage) -> Result<(), BusError> {
        Ok(())
    }
    async fn subscribe(&self, _: &str) -> Result<Self::Subscription, BusError> {
        Ok(stream::empty())
    }
    async fn subscribe_group(&self, _: &str, _: &str) -> Result<Self::Subscription, BusError> {
        Ok(stream::empty())
    }
}

#[test]
fn headerless_transport_ignores_incoming_format_hint() {
    let source = BrokerSource::new(Transport::<false>, "input", "group");
    let mut message = RouteMessage::new("input", Bytes::from_static(b"42"));
    message
        .headers
        .insert("Content-Type".into(), "application/unknown".into());
    assert_eq!(source.decode(&message).unwrap(), PayloadValue::U64(42));
    let target = BrokerTarget::new(Transport::<false>, "output");
    let mut output = RouteMessage::new("output", Bytes::new());
    target.encode(&PayloadValue::U32(43), &mut output).unwrap();
    assert_eq!(JsonCodec.decode::<u32>(&output.payload).unwrap(), 43);
    assert!(output.headers.is_empty());
}

#[test]
fn header_capable_transport_rejects_unknown_format_and_labels_output() {
    let source = BrokerSource::new(Transport::<true>, "input", "group");
    let mut message = RouteMessage::new("input", Bytes::from_static(b"42"));
    message
        .headers
        .insert("Content-Type".into(), "application/unknown".into());
    assert!(source.decode(&message).is_err());
    let target = BrokerTarget::new(Transport::<true>, "output");
    target.encode(&PayloadValue::U32(43), &mut message).unwrap();
    assert!(!message.headers.contains_key("Content-Type"));
    assert_eq!(message.headers["content-type"], "application/json");
}

#[cfg(all(feature = "cbor", feature = "msgpack"))]
#[test]
fn broker_decodes_cbor_and_http_encodes_configured_msgpack() {
    use qanat::codec::{CborCodec, MsgPackCodec};
    let source = BrokerSource::new(Transport::<true>, "input", "group");
    let mut message = RouteMessage::new("input", CborCodec.encode(&42u32).unwrap());
    message
        .headers
        .insert("Content-Type".into(), "application/cbor".into());
    message
        .headers
        .insert("correlation_id".into(), "request-1".into());
    let value = source.decode(&message).unwrap();
    let codec = HeaderAwareCodec::default()
        .with_default_content_type("application/msgpack")
        .unwrap();
    let target = HttpTarget::post("http://example.test/", |_req: HttpRequest| async {
        Ok(HttpResponse::new(202))
    })
    .with_codec(codec);
    target.encode(&value, &mut message).unwrap();
    assert_eq!(MsgPackCodec.decode::<u32>(&message.payload).unwrap(), 42);
    assert_eq!(message.headers["content-type"], "application/msgpack");
    assert_eq!(message.headers["correlation_id"], "request-1");
    assert!(!message.headers.contains_key("Content-Type"));
}

#[cfg(feature = "cbor")]
#[test]
fn headerless_transport_uses_configured_cbor_both_ways() {
    use qanat::codec::CborCodec;
    let source = BrokerSource::new(Transport::<false>, "input", "group")
        .with_codec(HeaderAwareCodec::new(CborCodec));
    let target = BrokerTarget::new(Transport::<false>, "output").with_codec(CborCodec);
    let mut message = RouteMessage::new("input", CborCodec.encode(&42u32).unwrap());
    message
        .headers
        .insert("content-type".into(), "application/json".into());
    let value = source.decode(&message).unwrap();
    target.encode(&value, &mut message).unwrap();
    assert_eq!(CborCodec.decode::<u32>(&message.payload).unwrap(), 42);
}

// A predefined protocol adapter supplies its message model without owning a Codec.
struct ProtocolSource;

impl RouteSource for ProtocolSource {
    fn decode(&self, message: &RouteMessage) -> Result<PayloadValue, BusError> {
        Ok(PayloadValue::U8(message.payload[0]))
    }
    fn open(&mut self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>> {
        Box::pin(async {
            Ok(Box::pin(stream::iter([RouteMessage::new(
                "protocol",
                Bytes::from_static(&[21]),
            )])) as RouteStream)
        })
    }
}

#[tokio::test]
async fn protocol_source_routes_without_a_codec_to_http() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let target = HttpTarget::post("http://example.test/output", move |request: HttpRequest| {
        let tx = tx.clone();
        async move {
            tx.send(request).await.unwrap();
            Ok(HttpResponse::new(202))
        }
    });
    let mut router = Router::new()
        .bind(|value: u32| async move { Ok::<_, std::convert::Infallible>(value * 2) })
        .from(ProtocolSource)
        .to(target);
    router.install().await.unwrap();
    let output = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(JsonCodec.decode::<u32>(&output.body).unwrap(), 42);
    assert_eq!(output.headers["content-type"], "application/json");
}

#[cfg(feature = "axum")]
#[test]
fn http_source_rejects_unregistered_formats_even_with_one_codec() {
    use qanat::http::HttpSource;
    let mut message = RouteMessage::new("/input", Bytes::from_static(b"42"));
    message.headers = HashMap::from([(
        "Content-Type".into(),
        "APPLICATION/JSON; charset=utf-8".into(),
    )]);
    assert!(HttpSource::post("/input").decode(&message).is_ok());
    message
        .headers
        .insert("Content-Type".into(), "application/unknown".into());
    assert!(HttpSource::post("/input").decode(&message).is_err());
    assert!(
        HttpSource::post("/input")
            .with_codec(HeaderAwareCodec::new(JsonCodec))
            .decode(&message)
            .is_err()
    );
}
