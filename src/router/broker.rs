use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::bus::Bus;
use crate::errors::BusError;

pub const DEFAULT_REDELIVERY_MESSAGE_DELAY: Duration = Duration::from_secs(5);
pub const DEFAULT_MESSAGE_RETRIES: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BrokerSubject {
    pub subject: String,
}

impl BrokerSubject {
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BrokerRoute {
    pub pattern: String,
    pub group: String,
}

impl BrokerRoute {
    pub fn new(pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            group: group.into(),
        }
    }
}

pub struct BrokerRouter<B: Bus> {
    bus: B,
    routes: Vec<BrokerRoute>,
    subscriptions: Vec<B::Subscription>,
}

impl<B: Bus> BrokerRouter<B> {
    pub fn new(bus: B) -> Self {
        Self {
            bus,
            routes: Vec::new(),
            subscriptions: Vec::new(),
        }
    }

    pub fn bind(mut self, pattern: impl Into<String>, group: impl Into<String>) -> Self {
        self.routes.push(BrokerRoute::new(pattern, group));
        self
    }

    pub fn bus(&self) -> &B {
        &self.bus
    }

    pub fn routes(&self) -> &[BrokerRoute] {
        &self.routes
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub async fn install(&mut self) -> Result<(), BusError> {
        for route in self.routes.iter().cloned() {
            let subscription = self
                .bus
                .subscribe_group(&route.pattern, &route.group)
                .await?;
            self.subscriptions.push(subscription);
        }
        Ok(())
    }
}

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
    use crate::bus::Bus;
    use futures::stream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct FakeBus {
        group_subscriptions: Arc<AtomicUsize>,
    }

    impl FakeBus {
        fn new() -> Self {
            Self {
                group_subscriptions: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn group_subscription_count(&self) -> usize {
            self.group_subscriptions.load(Ordering::Relaxed)
        }
    }

    impl Bus for FakeBus {
        type Message = ();
        type Subscription = stream::Empty<()>;

        async fn dispatch(&self, _subject: &str, _msg: ()) -> Result<(), BusError> {
            Ok(())
        }

        async fn subscribe(&self, _pattern: &str) -> Result<Self::Subscription, BusError> {
            Ok(stream::empty())
        }

        async fn subscribe_group(
            &self,
            _pattern: &str,
            _group: &str,
        ) -> Result<Self::Subscription, BusError> {
            self.group_subscriptions.fetch_add(1, Ordering::Relaxed);
            Ok(stream::empty())
        }
    }

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

    #[test]
    fn broker_router_builds_broker_source_endpoint() {
        let bus = FakeBus::new();
        let router = BrokerRouter::new(bus.clone()).bind("orders.created", "orders.in");

        assert_eq!(router.routes().len(), 1);
        assert_eq!(router.routes()[0].pattern, "orders.created");
        assert_eq!(router.routes()[0].group, "orders.in");
        assert_eq!(router.bus().group_subscription_count(), 0);
    }

    #[tokio::test]
    async fn broker_router_installs_routes_into_bus() {
        let bus = FakeBus::new();
        let mut router = BrokerRouter::new(bus.clone())
            .bind("orders.created", "orders.in")
            .bind("payments.created", "payments.in");

        router.install().await.unwrap();

        assert_eq!(router.subscription_count(), 2);
        assert_eq!(bus.group_subscription_count(), 2);
    }
}
