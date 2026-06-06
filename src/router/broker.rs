use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DEFAULT_REDELIVERY_MESSAGE_DELAY: Duration = Duration::from_secs(5);
pub const DEFAULT_MESSAGE_RETRIES: usize = 3;

/// Configuration for a broker-backed router/server layer.
///
/// This intentionally does not choose a serialization format. Qanat external
/// buses already make that a type-level choice through `Codec`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerConfiguration {
    pub broker_url: String,
    pub binding_file: Option<PathBuf>,
    pub redelivery_message_delay: Duration,
    pub max_redelivery_retries: Option<usize>,
    pub proxy_pubsub_url: Option<String>,
    pub durability: bool,
}

impl BrokerConfiguration {
    pub fn new(broker_url: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            binding_file: None,
            redelivery_message_delay: DEFAULT_REDELIVERY_MESSAGE_DELAY,
            max_redelivery_retries: Some(DEFAULT_MESSAGE_RETRIES),
            proxy_pubsub_url: None,
            durability: false,
        }
    }

    pub fn with_binding_file(mut self, binding_file: impl Into<PathBuf>) -> Self {
        self.binding_file = Some(binding_file.into());
        self
    }

    pub fn with_redelivery_message_delay(mut self, delay: Duration) -> Self {
        self.redelivery_message_delay = delay;
        self
    }

    pub fn with_max_redelivery_retries(mut self, retries: usize) -> Self {
        self.max_redelivery_retries = Some(retries);
        self
    }

    pub fn without_redelivery_limit(mut self) -> Self {
        self.max_redelivery_retries = None;
        self
    }

    pub fn with_proxy_pubsub_url(mut self, url: impl Into<String>) -> Self {
        self.proxy_pubsub_url = Some(url.into());
        self
    }

    pub fn durable(mut self) -> Self {
        self.durability = true;
        self
    }

    pub fn binding_file(&self) -> Option<&Path> {
        self.binding_file.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_configuration_uses_python_equivalent_defaults() {
        let config = BrokerConfiguration::new("amqp://guest:guest@localhost:5672/%2f");

        assert_eq!(config.broker_url, "amqp://guest:guest@localhost:5672/%2f");
        assert_eq!(config.binding_file, None);
        assert_eq!(
            config.redelivery_message_delay,
            DEFAULT_REDELIVERY_MESSAGE_DELAY
        );
        assert_eq!(config.max_redelivery_retries, Some(DEFAULT_MESSAGE_RETRIES));
        assert_eq!(config.proxy_pubsub_url, None);
        assert!(!config.durability);
    }

    #[test]
    fn broker_configuration_builder_sets_operational_options() {
        let config = BrokerConfiguration::new("redis://127.0.0.1/")
            .with_binding_file("routes.toml")
            .with_redelivery_message_delay(Duration::from_secs(10))
            .without_redelivery_limit()
            .with_proxy_pubsub_url("redis://127.0.0.1/1")
            .durable();

        assert_eq!(config.binding_file(), Some(Path::new("routes.toml")));
        assert_eq!(config.redelivery_message_delay, Duration::from_secs(10));
        assert_eq!(config.max_redelivery_retries, None);
        assert_eq!(
            config.proxy_pubsub_url.as_deref(),
            Some("redis://127.0.0.1/1")
        );
        assert!(config.durability);
    }
}
