use crate::bus::Bus;
use crate::errors::BusError;
use crate::router::broker::BrokerRouter;
#[cfg(feature = "axum")]
use crate::router::http::HttpRouter;

pub struct App {
    #[cfg(feature = "axum")]
    http: Option<HttpRouter>,
}

impl App {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "axum")]
            http: None,
        }
    }

    pub fn broker<B>(self, broker: BrokerRouter<B>) -> BrokerApp<B>
    where
        B: Bus,
    {
        BrokerApp {
            broker,
            #[cfg(feature = "axum")]
            http: self.http,
        }
    }

    #[cfg(feature = "axum")]
    pub fn http(mut self, http: HttpRouter) -> Self {
        self.http = Some(http);
        self
    }

    #[cfg(feature = "axum")]
    pub fn http_router(&self) -> Option<&HttpRouter> {
        self.http.as_ref()
    }

    #[cfg(feature = "axum")]
    pub fn into_http_router(self) -> Option<HttpRouter> {
        self.http
    }

    pub fn router_count(&self) -> usize {
        #[cfg(feature = "axum")]
        {
            usize::from(self.http.is_some())
        }

        #[cfg(not(feature = "axum"))]
        {
            0
        }
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BrokerApp<B: Bus> {
    broker: BrokerRouter<B>,
    #[cfg(feature = "axum")]
    http: Option<HttpRouter>,
}

impl<B> BrokerApp<B>
where
    B: Bus,
{
    pub fn broker_router(&self) -> &BrokerRouter<B> {
        &self.broker
    }

    pub fn broker_router_mut(&mut self) -> &mut BrokerRouter<B> {
        &mut self.broker
    }

    pub async fn install(&mut self) -> Result<(), BusError> {
        self.broker.install().await
    }

    #[cfg(feature = "axum")]
    pub fn http(mut self, http: HttpRouter) -> Self {
        self.http = Some(http);
        self
    }

    #[cfg(feature = "axum")]
    pub fn http_router(&self) -> Option<&HttpRouter> {
        self.http.as_ref()
    }

    #[cfg(feature = "axum")]
    pub fn into_http_router(self) -> Option<HttpRouter> {
        self.http
    }

    pub fn router_count(&self) -> usize {
        #[cfg(feature = "axum")]
        {
            1 + usize::from(self.http.is_some())
        }

        #[cfg(not(feature = "axum"))]
        {
            1
        }
    }
}

pub fn bind<F>(handler: F) -> HandlerBinding<F> {
    HandlerBinding { handler }
}

pub struct HandlerBinding<F> {
    handler: F,
}

impl<F> HandlerBinding<F> {
    pub fn from<S>(self, source: S) -> RouteFrom<F, S> {
        RouteFrom {
            handler: self.handler,
            source,
        }
    }
}

pub struct RouteFrom<F, S> {
    handler: F,
    source: S,
}

impl<F, S> RouteFrom<F, S> {
    pub fn to<T>(self, target: T) -> RouteBinding<F, S, T> {
        RouteBinding {
            handler: self.handler,
            source: self.source,
            target,
        }
    }
}

pub struct RouteBinding<F, S, T> {
    pub(crate) handler: F,
    pub(crate) source: S,
    pub(crate) target: T,
}

impl<F, S, T> RouteBinding<F, S, T> {
    pub fn parts(&self) -> (&F, &S, &T) {
        (&self.handler, &self.source, &self.target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn app_starts_without_routers() {
        let app = App::new();

        assert_eq!(app.router_count(), 0);
    }

    #[tokio::test]
    async fn app_installs_broker_router() {
        let bus = FakeBus::new();
        let broker = BrokerRouter::new(bus.clone()).bind("orders.created", "orders.in");
        let mut app = App::new().broker(broker);

        assert_eq!(app.router_count(), 1);
        app.install().await.unwrap();

        assert_eq!(app.broker_router().subscription_count(), 1);
        assert_eq!(bus.group_subscription_count(), 1);
    }
}
