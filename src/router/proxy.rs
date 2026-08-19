use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::time::Duration;

use serde::{Serialize, de::DeserializeOwned};

use crate::errors::BusError;
use crate::router::RouteFailure;

#[derive(Debug)]
pub enum ProxyError {
    Remote(RouteFailure),
    Timeout {
        correlation_id: String,
        timeout: Duration,
    },
    Transport(BusError),
    RuntimeStopped(String),
}

impl fmt::Display for ProxyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote(failure) => write!(formatter, "remote route failed: {}", failure.error),
            Self::Timeout {
                correlation_id,
                timeout,
            } => write!(
                formatter,
                "proxy request '{correlation_id}' timed out after {timeout:?}"
            ),
            Self::Transport(error) => write!(formatter, "proxy transport failed: {error}"),
            Self::RuntimeStopped(message) => write!(formatter, "proxy runtime stopped: {message}"),
        }
    }
}

impl Error for ProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Remote(_) | Self::Timeout { .. } | Self::RuntimeStopped(_) => None,
        }
    }
}

impl From<BusError> for ProxyError {
    fn from(error: BusError) -> Self {
        Self::Transport(error)
    }
}

#[allow(async_fn_in_trait)]
pub trait Proxy: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    async fn call<I, O>(&self, input: &I) -> Result<O, Self::Error>
    where
        I: Serialize + Sync,
        O: DeserializeOwned,
    {
        self.call_with_headers(input, HashMap::new()).await
    }

    async fn call_with_headers<I, O>(
        &self,
        input: &I,
        headers: HashMap<String, String>,
    ) -> Result<O, Self::Error>
    where
        I: Serialize + Sync,
        O: DeserializeOwned;
}
