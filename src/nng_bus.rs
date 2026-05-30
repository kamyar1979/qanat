use crate::bus::{Bus, BusStream};
use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::local_router::LocalRouter;
use crate::message::Envelope;
use crate::raw_message::RawMessage;
use crate::wire;
use bytes::Bytes;
use nng::{Protocol, Socket};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

struct NngState<C: Codec = JsonCodec> {
    local: LocalRouter<C>,
    socket: Socket,
}

// ── NngBus ────────────────────────────────────────────────────────────────────

pub struct NngBus<C: Codec = JsonCodec> {
    inner: Arc<NngState<C>>,
}

impl<C: Codec + 'static> NngBus<C> {
    /// Bind a listening socket on `url`. Other nodes dial into this address.
    /// Must be called from within a tokio runtime context.
    pub fn listen(codec: C, url: &str) -> Result<Self, BusError> {
        Self::create(codec, url, true)
    }

    /// Connect to a listening node at `url`.
    /// Must be called from within a tokio runtime context.
    pub fn dial(codec: C, url: &str) -> Result<Self, BusError> {
        Self::create(codec, url, false)
    }

    fn create(codec: C, url: &str, listen: bool) -> Result<Self, BusError> {
        let socket =
            Socket::new(Protocol::Bus0).map_err(|e| BusError::Connection(e.to_string()))?;

        if listen {
            socket
                .listen(url)
                .map_err(|e| BusError::Connection(e.to_string()))?;
        } else {
            socket
                .dial(url)
                .map_err(|e| BusError::Connection(e.to_string()))?;
        }

        let inner = Arc::new(NngState {
            local: LocalRouter::new(codec),
            socket,
        });

        Self::start_receive_loop(Arc::clone(&inner));

        Ok(Self { inner })
    }

    fn start_receive_loop(inner: Arc<NngState<C>>) {
        // Bridge: blocking NNG recv (OS thread) → tokio channel → async local dispatch
        let (bridge_tx, mut bridge_rx) = mpsc::channel::<(String, Bytes)>(256);

        let inner_recv = Arc::clone(&inner);
        std::thread::spawn(move || {
            loop {
                match inner_recv.socket.recv() {
                    Ok(msg) => {
                        if let Some((subject, payload)) = wire::decode(&msg) {
                            if bridge_tx
                                .blocking_send((
                                    subject.to_string(),
                                    Bytes::copy_from_slice(payload),
                                ))
                                .is_err()
                            {
                                break; // tokio side dropped
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::spawn(async move {
            while let Some((subject, payload)) = bridge_rx.recv().await {
                let msg = RawMessage {
                    envelope: Envelope {
                        id: inner.local.next_message_id(),
                        subject: subject.clone(),
                        timestamp: Instant::now(),
                        headers: None,
                        attempts: 0,
                    },
                    payload,
                };
                let _ = inner.local.dispatch_local(msg).await;
            }
        });
    }

    /// Serialize `payload` with the bus codec, transmit via NNG to all connected
    /// peers, and also dispatch to local subscribers (Bus0 does not echo back).
    pub async fn publish<T: Serialize>(
        &self,
        subject: &str,
        payload: &T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError> {
        let payload_bytes = self.inner.local.codec.encode(payload)?;

        let wire = wire::encode(subject, &payload_bytes);
        self.inner
            .socket
            .send(nng::Message::from(wire.as_slice()))
            .map_err(|(_, e)| BusError::Connection(e.to_string()))?;

        let raw_msg = RawMessage {
            envelope: Envelope {
                id: self.inner.local.next_message_id(),
                subject: subject.to_string(),
                timestamp: Instant::now(),
                headers,
                attempts: 0,
            },
            payload: payload_bytes,
        };
        self.inner.local.dispatch_local(raw_msg).await?;

        Ok(())
    }
}

impl<C: Codec> Bus for NngBus<C> {
    type Message = RawMessage;
    type Subscription = BusStream<RawMessage>;

    /// Route `msg` to local subscribers only. For network delivery use `publish`.
    async fn dispatch(&self, _subject: &str, msg: RawMessage) -> Result<(), BusError> {
        self.inner.local.dispatch_local(msg).await
    }

    async fn subscribe(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        Ok(self.inner.local.subscribe(pattern).await)
    }

    async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError> {
        self.inner.local.bind_queue(pattern, queue).await
    }

    async fn consume(&self, queue: &str) -> Result<Self::Subscription, BusError> {
        self.inner.local.consume(queue).await
    }
}
