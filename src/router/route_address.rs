use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouteAddress {
    value: String,
}

impl RouteAddress {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn broker(subject: impl Into<String>) -> Self {
        Self::new(format!("broker://{}", subject.into()))
    }

    pub fn grpc(service: impl Into<String>, method: impl Into<String>) -> Self {
        Self::new(format!("grpc://{}/{}", service.into(), method.into()))
    }

    pub fn websocket(path: impl Into<String>) -> Self {
        Self::new(format!("websocket://{}", path.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for RouteAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

pub trait IntoRouteAddress {
    fn into_route_address(self) -> RouteAddress;
}

impl IntoRouteAddress for RouteAddress {
    fn into_route_address(self) -> RouteAddress {
        self
    }
}

impl IntoRouteAddress for &RouteAddress {
    fn into_route_address(self) -> RouteAddress {
        self.clone()
    }
}

impl IntoRouteAddress for String {
    fn into_route_address(self) -> RouteAddress {
        RouteAddress::new(self)
    }
}

impl IntoRouteAddress for &str {
    fn into_route_address(self) -> RouteAddress {
        RouteAddress::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct KafkaTopic(&'static str);

    impl IntoRouteAddress for KafkaTopic {
        fn into_route_address(self) -> RouteAddress {
            RouteAddress::new(format!("kafka://{}", self.0))
        }
    }

    #[test]
    fn route_address_supports_builtin_router_families() {
        assert_eq!(
            RouteAddress::broker("orders.created").as_str(),
            "broker://orders.created"
        );
        assert_eq!(
            RouteAddress::grpc("OrderService", "Process").as_str(),
            "grpc://OrderService/Process"
        );
        assert_eq!(
            RouteAddress::websocket("/orders").as_str(),
            "websocket:///orders"
        );
    }

    #[test]
    fn route_address_can_be_extended_by_user_types() {
        assert_eq!(
            KafkaTopic("orders.created").into_route_address().as_str(),
            "kafka://orders.created"
        );
    }
}
