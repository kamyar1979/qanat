use futures::future::LocalBoxFuture;

use crate::errors::BusError;

/// A router whose concrete family/backend has been selected and can be
/// installed by the central application composer.
pub trait InstallableRouter: Send {
    fn install<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), BusError>>;
}

pub struct App {
    routers: Vec<Box<dyn InstallableRouter>>,
}

impl App {
    pub fn new() -> Self {
        Self {
            routers: Vec::new(),
        }
    }

    pub fn router<R>(mut self, router: R) -> Self
    where
        R: InstallableRouter + 'static,
    {
        self.routers.push(Box::new(router));
        self
    }

    pub async fn install(&mut self) -> Result<(), BusError> {
        for router in &mut self.routers {
            router.install().await?;
        }
        Ok(())
    }

    pub fn router_count(&self) -> usize {
        self.routers.len()
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeRouter {
        installs: Arc<AtomicUsize>,
    }

    impl InstallableRouter for FakeRouter {
        fn install<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), BusError>> {
            Box::pin(async move {
                self.installs.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn app_erases_and_installs_heterogeneous_router_types() {
        let installs = Arc::new(AtomicUsize::new(0));
        let mut app = App::new()
            .router(FakeRouter {
                installs: Arc::clone(&installs),
            })
            .router(FakeRouter {
                installs: Arc::clone(&installs),
            });

        assert_eq!(app.router_count(), 2);
        app.install().await.unwrap();
        assert_eq!(installs.load(Ordering::Relaxed), 2);
    }
}
