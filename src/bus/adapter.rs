use std::sync::Arc;

use futures::StreamExt;
use futures::future::LocalBoxFuture;

use crate::bus::Bus;
use crate::errors::BusError;
use crate::raw_message::RawMessage;
use crate::router::broker::{BrokerRoute, route_message_from_broker};
use crate::router::{RouteSource, RouteStream};

pub struct BrokerSource<B: Bus> {
    bus: Arc<B>,
    route: BrokerRoute,
}

impl<B: Bus> BrokerSource<B> {
    pub fn new(bus: B, pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            bus: Arc::new(bus),
            route: BrokerRoute::new(pattern, group),
        }
    }

    pub fn from_shared(bus: Arc<B>, pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            bus,
            route: BrokerRoute::new(pattern, group),
        }
    }

    pub fn bus(&self) -> &B {
        self.bus.as_ref()
    }

    pub fn route(&self) -> &BrokerRoute {
        &self.route
    }

    pub fn shared_bus(&self) -> Arc<B> {
        Arc::clone(&self.bus)
    }
}

impl<B> RouteSource for BrokerSource<B>
where
    B: Bus<Message = RawMessage> + 'static,
{
    fn into_stream(self: Box<Self>) -> LocalBoxFuture<'static, Result<RouteStream, BusError>> {
        Box::pin(async move {
            let stream = self
                .bus
                .subscribe_group(&self.route.pattern, &self.route.group)
                .await?;
            Ok(Box::pin(stream.map(route_message_from_broker)) as RouteStream)
        })
    }
}

mod target_impl {
    use std::sync::Arc;

    use futures::future::BoxFuture;

    use crate::bus::Bus;
    use crate::errors::BusError;
    use crate::raw_message::RawMessage;
    use crate::router::broker::{REPLY_TO_HEADER, broker_message_from_route};
    use crate::router::{RouteMessage, RouteTarget};

    enum BrokerDestination {
        Subject(String),
        ReplyTo,
    }

    pub struct BrokerTarget<B: Bus> {
        bus: Arc<B>,
        destination: BrokerDestination,
    }

    impl<B: Bus> BrokerTarget<B> {
        pub fn new(bus: B, subject: impl Into<String>) -> Self {
            Self::from_shared(Arc::new(bus), subject)
        }

        pub fn from_shared(bus: Arc<B>, subject: impl Into<String>) -> Self {
            Self {
                bus,
                destination: BrokerDestination::Subject(subject.into()),
            }
        }

        pub fn reply_to(bus: B) -> Self {
            Self::reply_to_shared(Arc::new(bus))
        }

        pub fn reply_to_shared(bus: Arc<B>) -> Self {
            Self {
                bus,
                destination: BrokerDestination::ReplyTo,
            }
        }

        pub fn bus(&self) -> &B {
            self.bus.as_ref()
        }
    }

    impl<B> RouteTarget for BrokerTarget<B>
    where
        B: Bus<Message = RawMessage> + 'static,
    {
        fn accepts(&self, message: &RouteMessage) -> bool {
            match self.destination {
                BrokerDestination::Subject(_) => true,
                BrokerDestination::ReplyTo => message.headers.contains_key(REPLY_TO_HEADER),
            }
        }

        fn deliver(&self, mut output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async move {
                let subject = match &self.destination {
                    BrokerDestination::Subject(subject) => subject.clone(),
                    BrokerDestination::ReplyTo => output
                        .headers
                        .get(REPLY_TO_HEADER)
                        .cloned()
                        .ok_or_else(|| {
                            BusError::Internal(
                                "broker reply target requires a reply_to header".into(),
                            )
                        })?,
                };
                output.headers.remove(REPLY_TO_HEADER);
                output.address = subject.clone();
                self.bus
                    .dispatch(&subject, broker_message_from_route(output))
                    .await
            })
        }
    }
}

pub use target_impl::BrokerTarget;
