/// An application component whose concrete type is erased when registered.
///
/// Lifecycle methods will be added when the concrete services are implemented.
pub trait Service: Send {}

pub struct App {
    services: Vec<Box<dyn Service>>,
}

impl App {
    pub fn new() -> Self {
        Self {
            services: Vec::new(),
        }
    }

    pub fn router<S>(mut self, service: S) -> Self
    where
        S: Service + 'static,
    {
        self.services.push(Box::new(service));
        self
    }

    pub fn service_count(&self) -> usize {
        self.services.len()
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
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

    struct HttpService;
    impl Service for HttpService {}

    struct BrokerService;
    impl Service for BrokerService {}

    #[test]
    fn app_erases_heterogeneous_service_types() {
        let app = App::new().router(HttpService).router(BrokerService);

        assert_eq!(app.service_count(), 2);
    }
}
