# Qanat Router

Qanat Router is an async Rust routing library for in-process messages, external
message brokers, and HTTP handlers. It provides one `Bus` abstraction across
backends while keeping serialization outside the in-memory bus.

The crates.io package is named `qanat-routing`; the Rust library name remains
`qanat`.

> This project is currently in beta. Public APIs may change before `1.0`.

## Features

- Typed, in-memory pub/sub without serialization
- Fanout subscriptions and queue-group delivery
- `*` and `>` subject wildcards
- User-selected JSON, CBOR, or MessagePack codecs
- NATS, NNG, RabbitMQ, and Redis backends
- A transport-neutral router with user-extensible sources and targets
- Typed handlers, broker replies, and request/reply proxies
- Axum routing without an extra HTTP abstraction layer
- Optional dependencies for every external backend and non-JSON codec

The neutral router supports every built-in source/target pairing:

| Source | Target | Supported |
| --- | --- | --- |
| Broker | Broker | Yes |
| Broker | HTTP | Yes |
| HTTP | Broker | Yes |
| HTTP | HTTP | Yes |

Headers pass through each route by default. A handler can return modified
headers when a route needs to add, replace, or remove them.

## Installation

The default build includes the in-memory bus and JSON codec without any
external broker dependency:

```toml
[dependencies]
qanat-routing = "0.2.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures = "0.3"
```

Enable only the integrations required by the application:

```toml
[dependencies]
qanat-routing = { version = "0.2.0", features = ["nats", "axum", "http-client"] }
```

## In-Memory Bus

`InMemoryBus` sends Rust objects directly through Tokio channels. Payloads do
not implement `Serialize` and are not copied into a wire format.

```rust
use futures::StreamExt;
use qanat::{Bus, in_memory_bus::InMemoryBus};

#[derive(Debug)]
struct Order {
    id: u64,
}

#[tokio::main]
async fn main() {
    let bus = InMemoryBus::new();
    let mut orders = bus.subscribe("orders.*").await.unwrap();

    bus.publish("orders.created", Order { id: 42 }, None)
        .await
        .unwrap();

    let message = orders.next().await.unwrap();
    let order = message.downcast::<Order>().unwrap();
    assert_eq!(order.payload.id, 42);
}
```

## External Bus

External backends serialize payloads with their configured `Codec` and produce
`RawMessage` values. NATS performs wildcard and queue-group routing on the
server.

```rust,no_run
use futures::StreamExt;
use qanat::{
    Bus, ExternalBus,
    codec::JsonCodec,
    nats_bus::NatsBus,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[tokio::main]
async fn main() -> Result<(), qanat::errors::BusError> {
    let bus = NatsBus::connect(JsonCodec, "nats://localhost:4222").await?;
    let mut orders = bus.subscribe("orders.*").await?;

    bus.publish("orders.created", &Order { id: 42 }, None).await?;

    let message = orders.next().await.expect("subscription ended");
    let order: Order = message.decode(bus.codec())?;
    assert_eq!(order.id, 42);
    Ok(())
}
```

Use `subscribe_group` when one consumer in a group should receive each matching
message:

```rust,ignore
let jobs = bus.subscribe_group("jobs.*", "workers").await?;
```

## Typed Routing

`Router` owns one codec and connects any `RouteSource` to any `RouteTarget`.
`BrokerSource` and `BrokerTarget` adapt a `Bus`; user-defined transports can
implement the same source and target traits.

```rust,no_run
use qanat::{
    codec::JsonCodec,
    nats_bus::NatsBus,
    router::{BrokerSource, BrokerTarget, Router},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct ProcessOrder {
    id: u64,
}

#[derive(Serialize)]
struct OrderProcessed {
    id: u64,
}

async fn process_order(
    order: ProcessOrder,
) -> Result<OrderProcessed, std::convert::Infallible> {
    Ok(OrderProcessed { id: order.id })
}

#[tokio::main]
async fn main() -> Result<(), qanat::errors::BusError> {
    let bus = NatsBus::connect(JsonCodec, "nats://localhost:4222").await?;
    let mut router = Router::new()
        .bind(process_order)
        .from(BrokerSource::new(
            bus.clone(),
            "orders.process",
            "order-workers",
        ))
        .to(BrokerTarget::new(bus, "orders.processed"));

    router.install().await?;
    std::future::pending::<()>().await;
    Ok(())
}
```

Serialization belongs to the source and target adapters. Use their
`.with_codec(...)` builders to configure each endpoint; the neutral `Router`
does not own a codec.

Handlers can extract the decoded body, complete broker envelope, all headers,
individual typed headers, or the raw broker message. The source decodes handler
input and the target encodes handler output, including error targets.

### Header-Aware Serialization

`HeaderAwareCodec::default()` registers JSON and, when enabled, the optional
`cbor` and `msgpack` formats. HTTP, NATS, and RabbitMQ route sources use the
incoming `Content-Type` to choose a decoder. Header names and media types are
case-insensitive; media-type parameters are ignored. Missing headers use the
configured default. Unknown formats and disabled codecs return serialization
errors, without falling back to a different format.

Outgoing serialization uses the configured default and replaces `Content-Type`
with the actual format. Other application headers are preserved. RabbitMQ also
maps this value to its native AMQP content-type property.

```rust
use qanat::codec::{Codec, HeaderAwareCodec};

let format = std::env::var("QANAT_CONTENT_TYPE")
    .unwrap_or_else(|_| "application/json".into());
let codec = HeaderAwareCodec::default().with_default_content_type(&format)?;
assert!(!codec.content_type().is_empty());
# Ok::<(), qanat::errors::BusError>(())
```

Pass the codec to `BrokerSource::with_codec`, `BrokerTarget::with_codec`,
`HttpSource::with_codec`, or `HttpTarget::with_codec`. Environment/TOML loading
is the application's responsibility.

NNG and Redis adapters ignore format headers and use their configured codec
for both encoding and decoding. Their peers must agree on that format.
`InMemoryBus` continues transporting Rust objects directly without codecs.

`Codec` supplies typed encoding/decoding and a content-type label, with no
header-based methods. Implement it to register a custom format with
`HeaderAwareCodec::default().register(MyCodec)`.
The wrapper's inherent `decode_with_content_type` method performs selection.
Sources and proxies accept this wrapper directly; targets accept any codec.
For a single configured format use `HeaderAwareCodec::new(MyCodec)`.
Registration builds a statically typed collection: codec selection uses no
codec trait objects and no closed format enum.

Protocol adapters need not implement a codec. They implement `RouteSource::decode`
and `RouteTarget::encode` directly, bridging their native message representation
to `PayloadValue` (Serde's value model). Typed handlers use that model at the
heterogeneous routing boundary; format-specific Serde extensions may require a
custom adapter. Sources now implement `open(&mut self)` so the router can retain
the source's decoding behavior for the lifetime of the route.

Migration: replace `Router::with_codec(...)` with endpoint `.with_codec(...)`
calls, and `HttpRouter::with_codec(...)` with `HttpRouter::new()`.

Header and payload extraction is transport-neutral:

```rust,ignore
use qanat::router::{RouteHeader, RouteHeaders, RoutePayload};

async fn handle(
    RouteHeader(request_id): RouteHeader<RequestId>,
    mut headers: RouteHeaders,
    RoutePayload(raw_payload): RoutePayload,
    input: ProcessOrder,
) -> Result<(RouteHeaders, OrderProcessed), std::convert::Infallible> {
    headers.insert("x-processed-by".into(), "orders-service".into());
    headers.remove("x-internal-token");

    Ok((headers, OrderProcessed { id: input.id }))
}
```

Every `RouteSource` produces a `RouteMessage` containing `headers` and
`payload`; every `RouteTarget` receives the same fields after handler
processing. Returning `Ok(output)` preserves incoming headers automatically.
Returning `Ok((RouteHeaders, output))` replaces them with the returned,
potentially modified headers.

### Broker Input to HTTP Output

Use `.to(HttpTarget)` to deliver a broker handler's encoded return value to an
HTTP endpoint. The router codec determines the request body and content type.
Broker headers, including the correlation ID, are forwarded; the internal
`reply_to` header is removed.

```rust,no_run
use qanat::{
    codec::JsonCodec,
    http::{HttpTarget, ReqwestHttpInvoker},
    nats_bus::NatsBus,
    router::{BrokerSource, Router},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct ProcessOrder {
    id: u64,
}

#[derive(Serialize)]
struct OrderProcessed {
    id: u64,
}

async fn process_order(
    order: ProcessOrder,
) -> Result<OrderProcessed, std::convert::Infallible> {
    Ok(OrderProcessed { id: order.id })
}

#[tokio::main]
async fn main() -> Result<(), qanat::errors::BusError> {
    let bus = NatsBus::connect(JsonCodec, "nats://localhost:4222").await?;
    let target = HttpTarget::post(
        "https://orders.example/events",
        ReqwestHttpInvoker::new(),
    );
    let mut router = Router::new()
        .bind(process_order)
        .from(BrokerSource::new(
            bus,
            "orders.process",
            "order-workers",
        ))
        .to(target);

    router.install().await?;
    std::future::pending::<()>().await;
    Ok(())
}
```

`HttpTarget` also accepts an async closure or a custom `HttpInvoker`
implementation, allowing an existing HTTP client to be adapted without
enabling `http-client`.

### HTTP Input to Broker Output

With the `axum` feature, mount an `HttpSource` into `HttpRouter`, then move that
source into the neutral router. The HTTP request method, URI, headers, and body
become a `RouteMessage`; accepted requests receive HTTP `202`.

```rust,ignore
use serde::{Deserialize, Serialize};

use qanat::router::{
    BrokerTarget, HttpPath, HttpQuery, HttpRouter, HttpSource, Router,
};

#[derive(Deserialize)]
struct OrderPath {
    id: u64,
}

#[derive(Deserialize)]
struct OrderQuery {
    include_events: bool,
}

#[derive(Deserialize)]
struct ProcessOrder {
    sku: String,
}

#[derive(Serialize)]
struct OrderProcessed {
    id: u64,
    sku: String,
    include_events: bool,
}

async fn process_order(
    HttpPath(path): HttpPath<OrderPath>,
    HttpQuery(query): HttpQuery<OrderQuery>,
    order: ProcessOrder,
) -> Result<OrderProcessed, std::convert::Infallible> {
    Ok(OrderProcessed {
        id: path.id,
        sku: order.sku,
        include_events: query.include_events,
    })
}

let source = HttpSource::post("/orders/{id}");
let http = HttpRouter::new()
    .source(&source)
    .into_router();

let mut routes = Router::new()
    .bind(process_order)
    .from(source)
    .to(BrokerTarget::new(bus, "orders.processed"));

routes.install().await?;
```

`HttpPath<T>` decodes named path parameters into `T`. `HttpQuery<T>` decodes
the query string. HTTP-specific values are captured as namespaced route
metadata, so the core router and user-defined transports remain independent of
Axum. `HttpSource` rejects bodies over its configured limit with HTTP `413`
and returns HTTP `503` when its route receiver is no longer available.

`RouteSource` and `RouteTarget` are public traits. Additional transports such as
gRPC or WebSocket can integrate without changing `Router`.

## Error Routing

All bound handlers return `Result`. Add `errors_to` to send handler, codec, or
target-delivery failures to another transport-neutral target:

```rust,ignore
async fn process_order(
    order: ProcessOrder,
) -> Result<OrderProcessed, OrderError> {
    // Validate and process the order.
    # unimplemented!()
}

let mut router = Router::new()
    .bind(process_order)
    .errors_to(BrokerTarget::new(bus.clone(), "orders.errors"))
    .from(BrokerSource::new(
        bus.clone(),
        "orders.process",
        "order-workers",
    ))
    .to(BrokerTarget::new(bus, "orders.processed"));
```

The error target can be any `RouteTarget`, including `BrokerTarget`,
`HttpTarget`, or a user-defined target. It receives an encoded `RouteFailure`
containing a transport-neutral `RouteError` and the original address, headers,
metadata, and payload. Routing headers pass through to the error target.

For an HTTP source, `202 Accepted` means the request was admitted for
asynchronous processing. A later handler or delivery failure can still be sent
to the configured broker or HTTP error target. Failure of the error target
itself is terminal and is not routed recursively.

## Request/Reply Proxy

`BrokerProxy` provides typed request/reply calls over the configured bus. A
proxy is built synchronously and initializes its reply subscription lazily on
the first `call`.

```rust,no_run
use std::time::Duration;

use qanat::{
    codec::JsonCodec,
    nats_bus::NatsBus,
    router::{BrokerProxy, BrokerRoute, BrokerSource, BrokerTarget, ProxyError, Router},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct ProcessOrder {
    id: u64,
}

#[derive(Deserialize, Serialize)]
struct OrderProcessed {
    id: u64,
}

async fn process_order(
    order: ProcessOrder,
) -> Result<OrderProcessed, std::convert::Infallible> {
    Ok(OrderProcessed { id: order.id })
}

#[tokio::main]
async fn main() -> Result<(), qanat::errors::BusError> {
    let bus = NatsBus::connect(JsonCodec, "nats://localhost:4222").await?;
    let mut router = Router::new()
        .bind(process_order)
        .errors_to(BrokerTarget::reply_to(bus.clone()))
        .from(BrokerSource::new(
            bus.clone(),
            "orders.process",
            "order-workers",
        ))
        .to(BrokerTarget::reply_to(bus.clone()));
    let proxy = BrokerProxy::new(
        bus,
        BrokerRoute::new("orders.process", "order-workers"),
    )
        .timeout(Duration::from_secs(5));

    router.install().await?;

    let response: OrderProcessed = match proxy.call(&ProcessOrder { id: 42 }).await {
        Ok(response) => response,
        Err(ProxyError::Remote(failure)) => {
            eprintln!("remote handler failed: {}", failure.error);
            return Ok(());
        }
        Err(error) => return Err(qanat::errors::BusError::Internal(error.to_string())),
    };
    assert_eq!(response.id, 42);
    Ok(())
}
```

Each call gets a UUID correlation ID. By default, each proxy instance also gets
its own reply subject under `_qanat.reply`, preventing another service instance
from consuming its response. The bound handler preserves the correlation ID
and replies to the subject supplied by the caller.

Use `call_with_headers` to attach application headers. Use `.reply_to(...)` for
a fixed reply subject or `.reply_topic_prefix(...)` to replace the default
instance-specific prefix.

`BrokerProxy::new` uses `HeaderAwareCodec` with JSON as the default. To select
another outgoing format, use `BrokerProxy::with_codec`. Header-capable buses
negotiate reply decoding by content type; NNG/Redis use the configured default
for requests and replies, which must match the service's endpoints.

`BrokerProxy::call` follows Rust's `Result` model and returns `ProxyError`:

- `ProxyError::Remote` contains the callee's structured `Box<RouteFailure>`, keeping the error enum small without dynamic dispatch.
- `ProxyError::Timeout` contains the correlation ID and configured deadline.
- `ProxyError::Transport` wraps bus, connection, and codec failures.
- `ProxyError::RuntimeStopped` reports failure of the local reply dispatcher.

Configure the callee with
`.errors_to(BrokerTarget::reply_to(bus.clone()))` so handler and delivery
failures return immediately to the waiting proxy with the same correlation ID.
Without an error reply, the caller cannot distinguish a silent callee from a
lost message and eventually returns `ProxyError::Timeout`.

`Proxy` is a public trait implemented by `BrokerProxy`. Applications can accept
`P: Proxy` and consumers can implement the same abstraction for HTTP, gRPC, or
other request/reply transports while choosing their own associated error type.

## HTTP Routing

With the `axum` feature, `HttpRouter` accepts native Axum handlers and
extractors:

```rust,ignore
use qanat::router::HttpRouter;

let router = HttpRouter::new()
    .get("/health", health)
    .post("/orders/{id}", create_order)
    .into_router();
```

## Subject Routing

Qanat uses NATS-style subject patterns for locally routed backends:

| Pattern | Meaning |
| --- | --- |
| `orders.created` | Exact subject |
| `orders.*` | Exactly one token after `orders` |
| `orders.>` | One or more trailing tokens after `orders` |
| `>` | Any non-empty subject |

Fanout subscribers each receive a copy. Within a queue group, matching messages
are distributed across consumers.

NATS and RabbitMQ use broker-native routing. NNG and Redis carry the subject in
the wire frame and use Qanat's local router after receipt.

## Feature Flags

| Feature | Adds |
| --- | --- |
| `axum` | Axum `HttpRouter`, `HttpSource`, `HttpPath`, and `HttpQuery` |
| `http-client` | Reqwest-backed outbound `HttpTarget` invoker |
| `nats` | NATS backend |
| `nng` | NNG Bus0 backend |
| `rabbitmq` | RabbitMQ topic-exchange backend |
| `redis` | Redis pub/sub backend with local routing |
| `cbor` | `CborCodec` |
| `msgpack` | `MsgPackCodec` |

Features are independent and disabled by default.

## Testing

Run the broker-free, default-feature suite first:

```console
cargo test --locked --no-default-features
```

Run every feature and backend integration:

```console
cargo test --locked --all-features -- --test-threads=1
```

NATS, RabbitMQ, and Redis tests probe their standard local ports and return
early when a service is unavailable. The release workflow starts all three
services and sets `QANAT_REQUIRE_BROKERS=1`, so a failed broker connection
cannot be silently skipped during release verification. Set the same variable
to require all local broker integrations. NNG integration tests use its
in-process transport and require no service.

Before publishing, also run:

```console
cargo fmt --check
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo package --locked
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
