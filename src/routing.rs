use crate::errors::BusError;

pub(crate) type ConsumerId = u64;

struct Binding {
    subject_pattern: String,
    queue: Option<String>,
    consumers: Vec<ConsumerId>,
    rr_index: usize,
}

/// Pure routing table: wildcard matching, fanout, and round-robin decisions.
/// Has no knowledge of channels, async, or message types.
/// Returns `ConsumerId`s; callers map those to actual senders.
pub(crate) struct SubjectRouter {
    bindings: Vec<Binding>,
    next_id: ConsumerId,
}

impl SubjectRouter {
    pub fn new() -> Self {
        Self { bindings: Vec::new(), next_id: 1 }
    }

    fn alloc_id(&mut self) -> ConsumerId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub fn wildcard_match(pattern: &str, subject: &str) -> bool {
        let p: Vec<&str> = pattern.split('.').collect();
        let s: Vec<&str> = subject.split('.').collect();

        for (i, part) in p.iter().enumerate() {
            match *part {
                ">" => return i < s.len(),
                "*" => {
                    if i >= s.len() {
                        return false;
                    }
                }
                _ => {
                    if s.get(i) != Some(part) {
                        return false;
                    }
                }
            }
        }

        p.len() == s.len()
    }

    /// Register a new fanout subscriber. Each subscriber gets its own binding
    /// so that dropping one does not affect the others.
    pub fn add_fanout(&mut self, pattern: &str) -> ConsumerId {
        let id = self.alloc_id();
        self.bindings.push(Binding {
            subject_pattern: pattern.to_string(),
            queue: None,
            consumers: vec![id],
            rr_index: 0,
        });
        id
    }

    pub fn bind_queue(&mut self, pattern: &str, queue: &str) -> Result<(), BusError> {
        if let Some(b) = self.bindings.iter_mut().find(|b| b.queue.as_deref() == Some(queue)) {
            if b.subject_pattern != pattern {
                return Err(BusError::Internal(format!(
                    "queue '{}' is already bound to pattern '{}'",
                    queue, b.subject_pattern
                )));
            }
            return Ok(());
        }

        self.bindings.push(Binding {
            subject_pattern: pattern.to_string(),
            queue: Some(queue.to_string()),
            consumers: Vec::new(),
            rr_index: 0,
        });

        Ok(())
    }

    pub fn add_consumer(&mut self, queue: &str) -> Result<ConsumerId, BusError> {
        let id = self.alloc_id();
        let binding = self.bindings
            .iter_mut()
            .find(|b| b.queue.as_deref() == Some(queue))
            .ok_or_else(|| BusError::QueueNotFound(queue.to_string()))?;
        binding.consumers.push(id);
        Ok(id)
    }

    /// Returns the consumer IDs that should receive a message for `subject`.
    /// Advances rr_index for matched queue bindings.
    pub fn route(&mut self, subject: &str) -> Vec<ConsumerId> {
        let mut targets = Vec::new();
        for binding in self.bindings.iter_mut() {
            if !Self::wildcard_match(&binding.subject_pattern, subject) {
                continue;
            }
            match &binding.queue {
                None => targets.extend_from_slice(&binding.consumers),
                Some(_) => {
                    if binding.consumers.is_empty() {
                        continue;
                    }
                    let idx = binding.rr_index % binding.consumers.len();
                    binding.rr_index += 1;
                    targets.push(binding.consumers[idx]);
                }
            }
        }
        targets
    }

    /// Remove a consumer from all bindings. Empty fanout bindings are dropped;
    /// empty queue bindings are kept so new consumers can still join.
    pub fn remove_consumer(&mut self, id: ConsumerId) {
        for binding in self.bindings.iter_mut() {
            binding.consumers.retain(|&c| c != id);
        }
        self.bindings.retain(|b| b.queue.is_some() || !b.consumers.is_empty());
    }
}
