use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteManifest {
    pub routes: Vec<RouteDefinition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteDefinition {
    /// Rust path to the function used by generated routing code.
    pub handler: String,
    pub from: EndpointDefinition,
    pub to: EndpointDefinition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "router", rename_all = "snake_case")]
pub enum EndpointDefinition {
    Http(HttpEndpointDefinition),
    Broker(BrokerEndpointDefinition),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpEndpointDefinition {
    pub method: String,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEndpointDefinition {
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_manifest_deserializes_mixed_router_binding() {
        let manifest: RouteManifest = toml::from_str(
            r#"
                [[routes]]
                handler = "crate::orders::create_order"

                [routes.from]
                router = "http"
                method = "POST"
                path = "/orders"

                [routes.to]
                router = "broker"
                subject = "orders.created"
            "#,
        )
        .unwrap();

        assert_eq!(manifest.routes.len(), 1);
        assert!(matches!(
            manifest.routes[0].from,
            EndpointDefinition::Http(HttpEndpointDefinition { ref path, .. })
                if path == "/orders"
        ));
        assert!(matches!(
            manifest.routes[0].to,
            EndpointDefinition::Broker(BrokerEndpointDefinition { queue: None, .. })
        ));
    }
}
