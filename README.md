# Qanat Router

Qanat Router is an async Rust routing library for in-process messages, external
message brokers, and HTTP handlers. It provides one `Bus` abstraction across
backends while keeping serialization outside the in-memory bus.

The crates.io package is named `qanat-router`; the Rust library name remains
`qanat`.

> This project is currently in beta. Public APIs may change before `1.0`.

## Features

- Typed, in-memory pub/sub without serialization
- Fanout subscriptions and queue-group delivery
- `*` and `>` subject wildcards
- User-selected JSON, CBOR, or MessagePack codecs
- NATS, NNG, RabbitMQ, and Redis backends
- Typed broker handlers, replies, and request/reply proxies
- Axum routing without an extra HTTP abstraction layer
- Optional dependencies for every external backend and non-JSON codec

## Installation

The default build includes the in-memory bus and JSON codec without any
external broker dependency:

```toml
[dependencies]
qanat-router = "0.1.0-beta.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures = "0.3"
```

Enable only the integrations required by the application:

```toml
[dependencies]
qanat-router = { version = "0.1.0-beta.1", features = ["nats", "axum"] }
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

## Broker Handlers

`BrokerRouter` decodes a message into the handler's argument type and encodes
its return value when a reply subject is configured.

```rust,no_run
use qanat::{
    codec::JsonCodec,
    nats_bus::NatsBus,
    router::BrokerRouter,
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

async fn process_order(order: ProcessOrder) -> OrderProcessed {
    OrderProcessed { id: order.id }
}

#[tokio::main]
async fn main() -> Result<(), qanat::errors::BusError> {
    let bus = NatsBus::connect(JsonCodec, "nats://localhost:4222").await?;
    let mut router = BrokerRouter::new(bus)
        .bind("orders.process", "order-workers", process_order)
        .reply_to("orders.processed");

    router.install().await?;
    std::future::pending::<()>().await;
    Ok(())
}
```

Handlers can extract the decoded body, complete broker envelope, all headers,
individual typed headers, or the raw message.

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
| `axum` | Axum `HttpRouter` |
| `nats` | NATS backend |
| `nng` | NNG Bus0 backend |
| `rabbitmq` | RabbitMQ topic-exchange backend |
| `redis` | Redis pub/sub backend with local routing |
| `cbor` | `CborCodec` |
| `msgpack` | `MsgPackCodec` |

Features are independent and disabled by default.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
