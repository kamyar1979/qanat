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
    Backend(BackendError),

    /// Serialization / deserialization error
    Serialization(String),

    /// Connection or network failure
    Connection(String),
}

#[derive(Debug)]
pub enum BackendError {
    Other(String),

    #[cfg(feature = "nats")]
    NatsPublish(async_nats::PublishError),

    #[cfg(feature = "nats")]
    NatsSubscribe(async_nats::SubscribeError),

    #[cfg(feature = "rabbitmq")]
    RabbitMq(lapin::Error),

    #[cfg(feature = "redis")]
    Redis(redis::RedisError),
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendError::Other(err) => write!(f, "{}", err),
            #[cfg(feature = "nats")]
            BackendError::NatsPublish(err) => write!(f, "NATS publish error: {}", err),
            #[cfg(feature = "nats")]
            BackendError::NatsSubscribe(err) => write!(f, "NATS subscribe error: {}", err),
            #[cfg(feature = "rabbitmq")]
            BackendError::RabbitMq(err) => write!(f, "RabbitMQ error: {}", err),
            #[cfg(feature = "redis")]
            BackendError::Redis(err) => write!(f, "Redis error: {}", err),
        }
    }
}

impl Error for BackendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BackendError::Other(_) => None,
            #[cfg(feature = "nats")]
            BackendError::NatsPublish(err) => Some(err),
            #[cfg(feature = "nats")]
            BackendError::NatsSubscribe(err) => Some(err),
            #[cfg(feature = "rabbitmq")]
            BackendError::RabbitMq(err) => Some(err),
            #[cfg(feature = "redis")]
            BackendError::Redis(err) => Some(err),
        }
    }
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
