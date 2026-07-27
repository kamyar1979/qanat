use std::marker::PhantomData;

use crate::codec::{Codec, JsonCodec};
use crate::errors::BusError;
#[cfg(feature = "axum")]
use crate::http::HttpRouter;
use crate::router::core::Router;

pub struct App<C: Codec = JsonCodec> {
    routes: Option<Router<C>>,
    #[cfg(feature = "axum")]
    http: Option<HttpRouter<C>>,
    _codec: PhantomData<fn() -> C>,
}

impl App<JsonCodec> {
    pub fn new() -> Self {
        Self {
            routes: None,
            #[cfg(feature = "axum")]
            http: None,
            _codec: PhantomData,
        }
    }
}

impl<C: Codec> App<C> {
    pub fn with_router(router: Router<C>) -> Self {
        Self {
            routes: Some(router),
            #[cfg(feature = "axum")]
            http: None,
            _codec: PhantomData,
        }
    }

    pub fn router(mut self, router: Router<C>) -> Self {
        self.routes = Some(router);
        self
    }

    pub fn routes(&self) -> Option<&Router<C>> {
        self.routes.as_ref()
    }

    pub fn routes_mut(&mut self) -> Option<&mut Router<C>> {
        self.routes.as_mut()
    }

    pub async fn install(&mut self) -> Result<(), BusError> {
        if let Some(router) = self.routes.as_mut() {
            router.install().await?;
        }
        Ok(())
    }

    #[cfg(feature = "axum")]
    pub fn http(mut self, http: HttpRouter<C>) -> Self {
        self.http = Some(http);
        self
    }

    #[cfg(feature = "axum")]
    pub fn http_router(&self) -> Option<&HttpRouter<C>> {
        self.http.as_ref()
    }

    #[cfg(feature = "axum")]
    pub fn into_http_router(self) -> Option<HttpRouter<C>> {
        self.http
    }

    pub fn router_count(&self) -> usize {
        usize::from(self.routes.is_some()) + {
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
}

impl Default for App<JsonCodec> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use crate::raw_message::RawMessage;
    use crate::router::{BrokerSource, BrokerTarget};
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

    #[derive(serde::Deserialize, serde::Serialize)]
    struct TestMessage {
        id: u64,
    }

    async fn handle_test_message(
        message: TestMessage,
    ) -> Result<TestMessage, std::convert::Infallible> {
        Ok(message)
    }

    #[test]
    fn app_starts_without_routers() {
        assert_eq!(App::new().router_count(), 0);
    }

    #[tokio::test]
    async fn app_installs_neutral_router() {
        let bus = FakeBus::new();
        let router = Router::new()
            .bind(handle_test_message)
            .from(BrokerSource::new(
                bus.clone(),
                "orders.created",
                "orders.in",
            ))
            .to(BrokerTarget::new(bus.clone(), "orders.processed"));
        let mut app = App::with_router(router);

        app.install().await.unwrap();

        assert_eq!(app.routes().unwrap().task_count(), 1);
        assert_eq!(bus.group_subscriptions.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "axum")]
    #[test]
    fn app_accepts_http_router_with_same_codec_type() {
        let app = App::new().http(HttpRouter::with_codec(crate::codec::JsonCodec));

        let _: &crate::codec::JsonCodec = app.http_router().unwrap().codec();
        assert_eq!(app.router_count(), 1);
    }
}
