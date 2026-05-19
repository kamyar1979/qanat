use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum BusError {
    /// In-process router error (Qanat)
    Internal(String),

    /// Subject did not match any binding
    NoRoute(String),

    /// Queue does not exist
    QueueNotFound(String),

    /// Backend-specific error (RabbitMQ, NATS, etc.)
    Backend(Box<dyn Error + Send + Sync>),

    /// Serialization / deserialization error
    Serialization(String),

    /// Connection or network failure
    Connection(String),
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BusError::Internal(msg) => write!(f, "Internal error: {}", msg),
            BusError::NoRoute(subj) => write!(f, "No route for subject: {}", subj),
            BusError::QueueNotFound(q) => write!(f, "Queue not found: {}", q),
            BusError::Backend(err) => write!(f, "Backend error: {}", err),
            BusError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            BusError::Connection(msg) => write!(f, "Connection error: {}", msg),
        }
    }
}

impl Error for BusError {}
