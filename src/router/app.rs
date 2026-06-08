pub struct App<R = ()> {
    routers: R,
}

impl App<()> {
    pub fn new() -> Self {
        Self { routers: () }
    }
}

impl Default for App<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> App<R> {
    pub fn router<N>(self, router: N) -> App<(R, N)> {
        App {
            routers: (self.routers, router),
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
    use crate::router::{BrokerRouter, HttpRouter};

    async fn create_order(_: u64) -> String {
        "created".to_string()
    }

    #[derive(Clone)]
    struct FakeBus;

    #[test]
    fn app_composes_typed_route_bindings() {
        let binding = bind(create_order)
            .from(BrokerRouter::new(FakeBus).bind("orders.created", "orders.in"))
            .to(HttpRouter::post("/orders"));

        let app = App::new().router(binding);
        let _ = app;
    }
}
