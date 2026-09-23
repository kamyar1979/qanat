use std::sync::Arc;

use futures::StreamExt;
use futures::future::LocalBoxFuture;

use crate::bus::Bus;
use crate::codec::{BuiltinCodecs, CodecCollection, HeaderAwareCodec, content_type};
use crate::errors::BusError;
use crate::raw_message::RawMessage;
use crate::router::broker::{BrokerRoute, route_message_from_broker};
use crate::router::{PayloadValue, RouteMessage, RouteSource, RouteStream};

pub struct BrokerSource<B: Bus, C: CodecCollection = BuiltinCodecs> {
    bus: Arc<B>,
    route: BrokerRoute,
    codec: HeaderAwareCodec<C>,
}

impl<B: Bus> BrokerSource<B> {
    pub fn new(bus: B, pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self::from_shared(Arc::new(bus), pattern, group)
    }

    pub fn from_shared(bus: Arc<B>, pattern: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            bus,
            route: BrokerRoute::new(pattern, group),
            codec: HeaderAwareCodec::default(),
        }
    }
}

impl<B: Bus, C: CodecCollection> BrokerSource<B, C> {
    pub fn with_codec<D: CodecCollection>(self, codec: HeaderAwareCodec<D>) -> BrokerSource<B, D> {
        BrokerSource {
            bus: self.bus,
            route: self.route,
            codec,
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

impl<B, C: CodecCollection> RouteSource for BrokerSource<B, C>
where
    B: Bus<Message = RawMessage> + 'static,
{
    fn decode(&self, message: &RouteMessage) -> Result<PayloadValue, BusError> {
        let hint = if self.bus.supports_content_type_headers() {
            content_type(&message.headers)
        } else {
            None
        };
        self.codec.decode_with_content_type(&message.payload, hint)
    }

    fn open(&mut self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>> {
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
    use crate::codec::{Codec, HeaderAwareCodec, set_content_type};
    use crate::errors::BusError;
    use crate::raw_message::RawMessage;
    use crate::router::broker::{REPLY_TO_HEADER, broker_message_from_route};
    use crate::router::{PayloadValue, RouteMessage, RouteTarget};

    enum BrokerDestination {
        Subject(String),
        ReplyTo,
    }

    pub struct BrokerTarget<B: Bus, C: Codec = HeaderAwareCodec> {
        bus: Arc<B>,
        destination: BrokerDestination,
        codec: C,
    }

    impl<B: Bus> BrokerTarget<B> {
        pub fn new(bus: B, subject: impl Into<String>) -> Self {
            Self::from_shared(Arc::new(bus), subject)
        }

        pub fn from_shared(bus: Arc<B>, subject: impl Into<String>) -> Self {
            Self {
                bus,
                destination: BrokerDestination::Subject(subject.into()),
                codec: HeaderAwareCodec::default(),
            }
        }

        pub fn reply_to(bus: B) -> Self {
            Self::reply_to_shared(Arc::new(bus))
        }

        pub fn reply_to_shared(bus: Arc<B>) -> Self {
            Self {
                bus,
                destination: BrokerDestination::ReplyTo,
                codec: HeaderAwareCodec::default(),
            }
        }
    }

    impl<B: Bus, C: Codec> BrokerTarget<B, C> {
        pub fn with_codec<D: Codec>(self, codec: D) -> BrokerTarget<B, D> {
            BrokerTarget {
                bus: self.bus,
                destination: self.destination,
                codec,
            }
        }

        pub fn bus(&self) -> &B {
            self.bus.as_ref()
        }
    }

    impl<B, C: Codec> RouteTarget for BrokerTarget<B, C>
    where
        B: Bus<Message = RawMessage> + 'static,
    {
        fn encode(&self, value: &PayloadValue, message: &mut RouteMessage) -> Result<(), BusError> {
            message.payload = self.codec.encode(value)?;
            if self.bus.supports_content_type_headers() {
                set_content_type(&mut message.headers, self.codec.content_type());
            }
            Ok(())
        }

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
