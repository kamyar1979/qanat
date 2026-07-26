use crate::bus::{BusStream, ExternalBus};
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
        let (bridge_tx, mut bridge_rx) =
            mpsc::channel::<(String, Option<HashMap<String, String>>, Bytes)>(256);

        let inner_recv = Arc::clone(&inner);
        std::thread::spawn(move || {
            while let Ok(msg) = inner_recv.socket.recv() {
                let Some(frame) = wire::decode(&msg) else {
                    continue;
                };
                if bridge_tx
                    .blocking_send((
                        frame.subject.to_string(),
                        frame.headers,
                        Bytes::copy_from_slice(frame.payload),
                    ))
                    .is_err()
                {
                    break; // tokio side dropped
                }
            }
        });

        tokio::spawn(async move {
            while let Some((subject, headers, payload)) = bridge_rx.recv().await {
                let msg = RawMessage {
                    envelope: Envelope {
                        id: inner.local.next_message_id(),
                        subject: subject.clone(),
                        timestamp: Instant::now(),
                        headers,
                        attempts: 0,
                    },
                    payload,
                };
                let _ = inner.local.dispatch_local(msg).await;
            }
        });
    }
}

impl<C: Codec> ExternalBus for NngBus<C> {
    type Codec = C;
    type Subscription = BusStream<RawMessage>;

    fn codec(&self) -> &Self::Codec {
        &self.inner.local.codec
    }

    fn publish<'a, T: Serialize>(
        &'a self,
        subject: &'a str,
        value: &T,
        headers: Option<HashMap<String, String>>,
    ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
        let payload = self.codec().encode(value);

        async move {
            let payload = payload?;
            ExternalBus::publish_bytes(self, subject, payload.clone(), headers.clone()).await?;

            let message = RawMessage {
                envelope: Envelope {
                    id: self.inner.local.next_message_id(),
                    subject: subject.to_string(),
                    timestamp: Instant::now(),
                    headers,
                    attempts: 0,
                },
                payload,
            };
            self.inner.local.dispatch_local(message).await
        }
    }

    fn publish_bytes<'a>(
        &'a self,
        subject: &'a str,
        payload: Bytes,
        headers: Option<HashMap<String, String>>,
    ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
        async move {
            let wire = wire::encode(subject, headers.as_ref(), &payload);
            self.inner
                .socket
                .send(nng::Message::from(wire.as_slice()))
                .map_err(|(_, e)| BusError::Connection(e.to_string()))?;
            Ok(())
        }
    }

    /// Publish raw bytes through NNG and route locally because Bus0 does not echo.
    fn dispatch_raw<'a>(
        &'a self,
        subject: &'a str,
        msg: RawMessage,
    ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
        async move {
            ExternalBus::publish_bytes(
                self,
                subject,
                msg.payload.clone(),
                msg.envelope.headers.clone(),
            )
            .await?;
            self.inner.local.dispatch_local(msg).await
        }
    }

    async fn subscribe_raw(&self, pattern: &str) -> Result<Self::Subscription, BusError> {
        self.inner.local.subscribe(pattern).await
    }

    async fn subscribe_group_raw(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<Self::Subscription, BusError> {
        self.inner.local.subscribe_group(pattern, group).await
    }
}
