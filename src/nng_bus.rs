use bytes::Bytes;
use nng::{Protocol, Socket};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use crate::bus::{Bus, BusStream};
use crate::codec::Codec;
use crate::errors::BusError;
use crate::message::Envelope;
use crate::raw_message::RawMessage;
use crate::routing::{ConsumerId, SubjectRouter};

// ── wire framing ─────────────────────────────────────────────────────────────
// Layout: [4 bytes BE: subject_len][subject UTF-8][payload bytes]
// The codec encodes only the user payload; framing is fixed binary.

fn encode_wire(subject: &str, payload: &[u8]) -> Vec<u8> {
    let sb = subject.as_bytes();
    let mut buf = Vec::with_capacity(4 + sb.len() + payload.len());
    buf.extend_from_slice(&(sb.len() as u32).to_be_bytes());
    buf.extend_from_slice(sb);
    buf.extend_from_slice(payload);
    buf
}

fn decode_wire(buf: &[u8]) -> Option<(&str, &[u8])> {
    if buf.len() < 4 {
        return None;
    }
    let subject_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + subject_len {
        return None;
    }
    let subject = std::str::from_utf8(&buf[4..4 + subject_len]).ok()?;
    let payload = &buf[4 + subject_len..];
    Some((subject, payload))
}

// ── shared inner state ────────────────────────────────────────────────────────

struct Inner<C: Codec> {
    codec: C,
    socket: Socket,
    router: Mutex<SubjectRouter>,
    senders: Mutex<HashMap<ConsumerId, mpsc::Sender<RawMessage>>>,
    next_msg_id: AtomicU64,
}

impl<C: Codec> Inner<C> {
    async fn dispatch_local(&self, msg: RawMessage) {
        let targets = self.router.lock().await.route(&msg.envelope.subject);

        let mut to_send: Vec<(ConsumerId, mpsc::Sender<RawMessage>)> = Vec::new();
        {
            let senders = self.senders.lock().await;
            for id in &targets {
                if let Some(tx) = senders.get(id) {
                    to_send.push((*id, tx.clone()));
                }
            }
        }

        let mut dead: Vec<ConsumerId> = Vec::new();
        for (id, tx) in to_send {
            if tx.send(msg.clone()).await.is_err() {
                dead.push(id);
            }
        }

        if !dead.is_empty() {
            let mut router = self.router.lock().await;
            let mut senders = self.senders.lock().await;
            for id in dead {
                router.remove_consumer(id);
                senders.remove(&id);
            }
        }
    }
}

// ── NngBus ────────────────────────────────────────────────────────────────────

pub struct NngBus<C: Codec> {
    inner: Arc<Inner<C>>,
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
        let socket = Socket::new(Protocol::Bus0)
            .map_err(|e| BusError::Connection(e.to_string()))?;

        if listen {
            socket.listen(url)
                .map_err(|e| BusError::Connection(e.to_string()))?;
        } else {
            socket.dial(url)
                .map_err(|e| BusError::Connection(e.to_string()))?;
        }

        let inner = Arc::new(Inner {
            codec,
            socket,
            router: Mutex::new(SubjectRouter::new()),
            senders: Mutex::new(HashMap::new()),
            next_msg_id: AtomicU64::new(1),
        });

        Self::start_receive_loop(Arc::clone(&inner));

        Ok(Self { inner })
    }

    fn start_receive_loop(inner: Arc<Inner<C>>) {
        // Bridge: blocking NNG recv (OS thread) → tokio channel → async local dispatch
        let (bridge_tx, mut bridge_rx) = mpsc::channel::<(String, Bytes)>(256);

        let inner_recv = Arc::clone(&inner);
        std::thread::spawn(move || loop {
            match inner_recv.socket.recv() {
                Ok(msg) => {
                    if let Some((subject, payload)) = decode_wire(&msg) {
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
        });

        tokio::spawn(async move {
            while let Some((subject, payload)) = bridge_rx.recv().await {
                let msg = RawMessage {
                    envelope: Envelope {
                        id: inner.next_msg_id.fetch_add(1, Ordering::Relaxed),
                        subject: subject.clone(),
                        timestamp: Instant::now(),
                        headers: None,
                        attempts: 0,
                    },
                    payload,
                };
                inner.dispatch_local(msg).await;
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
        let payload_bytes = self.inner.codec.encode(payload)?;

        let wire = encode_wire(subject, &payload_bytes);
        self.inner
            .socket
            .send(nng::Message::from(wire.as_slice()))
            .map_err(|(_, e)| BusError::Connection(e.to_string()))?;

        let raw_msg = RawMessage {
            envelope: Envelope {
                id: self.inner.next_msg_id.fetch_add(1, Ordering::Relaxed),
                subject: subject.to_string(),
                timestamp: Instant::now(),
                headers,
                attempts: 0,
            },
            payload: payload_bytes,
        };
        self.inner.dispatch_local(raw_msg).await;

        Ok(())
    }
}

impl<C: Codec> Bus for NngBus<C> {
    type Message = RawMessage;

    /// Route `msg` to local subscribers only. For network delivery use `publish`.
    async fn dispatch(&self, _subject: &str, msg: RawMessage) -> Result<(), BusError> {
        self.inner.dispatch_local(msg).await;
        Ok(())
    }

    async fn subscribe(&self, pattern: &str) -> Result<BusStream<RawMessage>, BusError> {
        let (tx, rx) = mpsc::channel(128);
        let id = self.inner.router.lock().await.add_fanout(pattern);
        self.inner.senders.lock().await.insert(id, tx);
        Ok(BusStream::new(rx))
    }

    async fn bind_queue(&self, pattern: &str, queue: &str) -> Result<(), BusError> {
        self.inner.router.lock().await.bind_queue(pattern, queue)
    }

    async fn consume(&self, queue: &str) -> Result<BusStream<RawMessage>, BusError> {
        let (tx, rx) = mpsc::channel(128);
        let id = self.inner.router.lock().await.add_consumer(queue)?;
        self.inner.senders.lock().await.insert(id, tx);
        Ok(BusStream::new(rx))
    }
}
