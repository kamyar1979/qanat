use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use crate::errors::BusError;
use crate::message::{AnyMessage, Envelope};
use crate::subscription::Subscription;

pub struct Binding {
    pub subject_pattern: String,
    pub queue: Option<String>,
    pub senders: Vec<mpsc::Sender<AnyMessage>>,
    pub rr_index: usize,
}

pub struct Router {
    pub bindings: Mutex<Vec<Binding>>,
    pub next_id: AtomicU64,
}

impl Router {
    pub fn new() -> Self {
        Self {
            bindings: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
        }
    }

    fn wildcard_match(pattern: &str, subject: &str) -> bool {
        // NATS-style: * matches one token, > matches rest
        let p: Vec<&str> = pattern.split('.').collect();
        let s: Vec<&str> = subject.split('.').collect();

        for (i, part) in p.iter().enumerate() {
            match *part {
                ">" => return true,
                "*" => continue,
                _ => {
                    if s.get(i) != Some(part) {
                        return false;
                    }
                }
            }
        }

        p.len() == s.len()
    }

    pub async fn publish<T: Send + Sync + 'static>(
        &self,
        subject: &str,
        payload: T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), BusError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let msg = AnyMessage {
            envelope: Envelope {
                subject: subject.to_string(),
                timestamp: Instant::now(),
                id,
                headers,
                attempts: 0,
            },
            payload: Arc::new(payload),
        };


        let bindings = self.bindings.lock().unwrap();

        let mut matched = false;

        for binding in bindings.iter() {
            if Self::wildcard_match(&binding.subject_pattern, subject) {
                matched = true;

                match &binding.queue {
                    None => {
                        // fanout
                        for tx in &binding.senders {
                            let _ = tx.send(msg.clone()).await;
                        }
                    }
                    Some(_) => {
                        // queue group
                        let idx = binding.rr_index % binding.senders.len();
                        let tx = &binding.senders[idx];
                        let _ = tx.send(msg.clone()).await;
                    }
                }
            }
        }

        if !matched {
            return Err(BusError::NoRoute(subject.to_string()));
        }

        Ok(())
    }

    pub async fn bind_queue(
        &self,
        pattern: &str,
        queue: &str,
    ) -> Result<(), BusError> {
        let mut bindings = self.bindings.lock().unwrap();

        // find existing binding for this queue
        if let Some(b) = bindings.iter_mut().find(|b| b.queue.as_deref() == Some(queue)) {
            b.subject_pattern = pattern.to_string();
            return Ok(());
        }

        // create new binding
        bindings.push(Binding {
            subject_pattern: pattern.to_string(),
            queue: Some(queue.to_string()),
            senders: Vec::new(),
            rr_index: 0,
        });

        Ok(())
    }

    pub async fn subscribe(
        &self,
        pattern: &str,
    ) -> Result<Subscription, BusError> {
        let (tx, rx) = mpsc::channel(128);

        let mut bindings = self.bindings.lock().unwrap();

        bindings.push(Binding {
            subject_pattern: pattern.to_string(),
            queue: None,
            senders: vec![tx],
            rr_index: 0,
        });

        Ok(Subscription::new(rx))
    }

    pub async fn consume(
        &self,
        queue: &str,
    ) -> Result<Subscription, BusError> {
        let (tx, rx) = mpsc::channel(128);

        let mut bindings = self.bindings.lock().unwrap();

        let binding = bindings
            .iter_mut()
            .find(|b| b.queue.as_deref() == Some(queue))
            .ok_or_else(|| BusError::QueueNotFound(queue.to_string()))?;

        binding.senders.push(tx);

        Ok(Subscription::new(rx))
    }
}
