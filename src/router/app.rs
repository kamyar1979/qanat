use std::marker::PhantomData;

use crate::bus::Bus;
use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
use crate::raw_message::RawMessage;
use crate::router::broker::BrokerRouter;
#[cfg(feature = "axum")]
use crate::router::http::HttpRouter;

pub struct App<H: Codec = JsonCodec> {
    #[cfg(feature = "axum")]
    http: Option<HttpRouter<H>>,
    _http_codec: PhantomData<fn() -> H>,
}

impl App<JsonCodec> {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "axum")]
            http: None,
            _http_codec: PhantomData,
        }
    }
}

impl<H> App<H>
where
    H: Codec,
{
    pub fn broker<B, C>(self, broker: BrokerRouter<B, C>) -> BrokerApp<B, C, H>
    where
        B: Bus,
        C: Codec,
    {
        BrokerApp {
            broker,
            #[cfg(feature = "axum")]
            http: self.http,
            _http_codec: PhantomData,
        }
    }

    #[cfg(feature = "axum")]
    pub fn http<H2>(self, http: HttpRouter<H2>) -> App<H2>
    where
        H2: Codec,
    {
        App {
            http: Some(http),
            _http_codec: PhantomData,
        }
    }

    #[cfg(feature = "axum")]
    pub fn http_router(&self) -> Option<&HttpRouter<H>> {
        self.http.as_ref()
    }

    #[cfg(feature = "axum")]
    pub fn into_http_router(self) -> Option<HttpRouter<H>> {
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

impl Default for App<JsonCodec> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BrokerApp<B: Bus, C: Codec = JsonCodec, H: Codec = JsonCodec> {
    broker: BrokerRouter<B, C>,
    #[cfg(feature = "axum")]
    http: Option<HttpRouter<H>>,
    _http_codec: PhantomData<fn() -> H>,
}

impl<B, C, H> BrokerApp<B, C, H>
where
    B: Bus,
    C: Codec,
    H: Codec,
{
    pub fn broker_router(&self) -> &BrokerRouter<B, C> {
        &self.broker
    }

    pub fn broker_router_mut(&mut self) -> &mut BrokerRouter<B, C> {
        &mut self.broker
    }

    pub async fn install(&mut self) -> Result<(), BusError>
    where
        B: Bus<Message = RawMessage> + 'static,
    {
        self.broker.install().await
    }

    #[cfg(feature = "axum")]
    pub fn http<H2>(self, http: HttpRouter<H2>) -> BrokerApp<B, C, H2>
    where
        H2: Codec,
    {
        BrokerApp {
            broker: self.broker,
            http: Some(http),
            _http_codec: PhantomData,
        }
    }

    #[cfg(feature = "axum")]
    pub fn http_router(&self) -> Option<&HttpRouter<H>> {
        self.http.as_ref()
    }

    #[cfg(feature = "axum")]
    pub fn into_http_router(self) -> Option<HttpRouter<H>> {
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
    use crate::raw_message::RawMessage;
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
        type Message = RawMessage;
        type Subscription = stream::Empty<RawMessage>;

        fn dispatch<'a>(
            &'a self,
            _subject: &'a str,
            _msg: RawMessage,
        ) -> impl std::future::Future<Output = Result<(), BusError>> + Send + 'a {
            async { Ok(()) }
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

    #[derive(serde::Deserialize)]
    struct TestMessage {
        id: u64,
    }

    async fn handle_test_message(message: TestMessage) {
        let _ = message.id;
    }

    #[test]
    fn app_starts_without_routers() {
        let app = App::new();

        assert_eq!(app.router_count(), 0);
    }

    #[tokio::test]
    async fn app_installs_broker_router() {
        let bus = FakeBus::new();
        let broker =
            BrokerRouter::new(bus.clone()).bind("orders.created", "orders.in", handle_test_message);
        let mut app = App::new().broker(broker);

        assert_eq!(app.router_count(), 1);
        app.install().await.unwrap();

        assert_eq!(app.broker_router().task_count(), 1);
        assert_eq!(bus.group_subscription_count(), 1);
    }

    #[cfg(feature = "axum")]
    #[test]
    fn app_accepts_http_router_with_explicit_codec() {
        let app = App::new().http(HttpRouter::with_codec(crate::codec::JsonCodec));

        let _: &crate::codec::JsonCodec = app.http_router().unwrap().codec();
        assert_eq!(app.router_count(), 1);
    }
}
