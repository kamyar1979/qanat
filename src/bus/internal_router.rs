use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use crate::bus::BusStream;
use crate::bus::routing::{ConsumerId, SubjectRouter};
use crate::errors::BusError;

const ROUTER_COMMAND_BUFFER: usize = 1024;
const SUBSCRIPTION_BUFFER: usize = 128;

pub(crate) struct RouterHandle<M: Clone + Send + 'static> {
    commands: mpsc::Sender<RouterCommand<M>>,
}

impl<M: Clone + Send + 'static> RouterHandle<M> {
    pub fn new() -> Self {
        let (commands, rx) = mpsc::channel(ROUTER_COMMAND_BUFFER);
        tokio::spawn(router_actor(rx));
        Self { commands }
    }

    pub async fn dispatch(&self, subject: &str, msg: M) -> Result<(), BusError> {
        let (reply, result) = oneshot::channel();
        self.send_command(RouterCommand::Dispatch {
            subject: subject.to_string(),
            msg,
            reply,
        })
        .await?;
        result.await.map_err(actor_dropped)?
    }

    pub async fn subscribe(&self, pattern: &str) -> Result<BusStream<M>, BusError> {
        let (reply, result) = oneshot::channel();
        self.send_command(RouterCommand::Subscribe {
            pattern: pattern.to_string(),
            reply,
        })
        .await?;
        result.await.map_err(actor_dropped)?
    }

    pub async fn subscribe_group(
        &self,
        pattern: &str,
        group: &str,
    ) -> Result<BusStream<M>, BusError> {
        let (reply, result) = oneshot::channel();
        self.send_command(RouterCommand::SubscribeGroup {
            pattern: pattern.to_string(),
            group: group.to_string(),
            reply,
        })
        .await?;
        result.await.map_err(actor_dropped)?
    }

    async fn send_command(&self, command: RouterCommand<M>) -> Result<(), BusError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| BusError::Internal("router actor stopped".to_string()))
    }
}

impl<M: Clone + Send + 'static> Clone for RouterHandle<M> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
        }
    }
}

enum RouterCommand<M: Clone + Send + 'static> {
    Dispatch {
        subject: String,
        msg: M,
        reply: oneshot::Sender<Result<(), BusError>>,
    },
    Subscribe {
        pattern: String,
        reply: oneshot::Sender<Result<BusStream<M>, BusError>>,
    },
    SubscribeGroup {
        pattern: String,
        group: String,
        reply: oneshot::Sender<Result<BusStream<M>, BusError>>,
    },
}

async fn router_actor<M: Clone + Send + 'static>(mut commands: mpsc::Receiver<RouterCommand<M>>) {
    let mut router = SubjectRouter::new();
    let mut senders: HashMap<ConsumerId, mpsc::Sender<M>> = HashMap::new();

    while let Some(command) = commands.recv().await {
        match command {
            RouterCommand::Dispatch {
                subject,
                msg,
                reply,
            } => {
                let result = dispatch(&mut router, &mut senders, &subject, msg);
                let _ = reply.send(result);
            }
            RouterCommand::Subscribe { pattern, reply } => {
                let (tx, rx) = mpsc::channel(SUBSCRIPTION_BUFFER);
                let id = router.add_fanout(&pattern);
                senders.insert(id, tx);
                let _ = reply.send(Ok(BusStream::new(rx)));
            }
            RouterCommand::SubscribeGroup {
                pattern,
                group,
                reply,
            } => {
                let result = match router.bind_queue(&pattern, &group) {
                    Ok(()) => match router.add_consumer(&group) {
                        Ok(id) => {
                            let (tx, rx) = mpsc::channel(SUBSCRIPTION_BUFFER);
                            senders.insert(id, tx);
                            Ok(BusStream::new(rx))
                        }
                        Err(err) => Err(err),
                    },
                    Err(err) => Err(err),
                };
                let _ = reply.send(result);
            }
        }
    }
}

fn dispatch<M: Clone + Send + 'static>(
    router: &mut SubjectRouter,
    senders: &mut HashMap<ConsumerId, mpsc::Sender<M>>,
    subject: &str,
    msg: M,
) -> Result<(), BusError> {
    let targets = router.route(subject);
    let mut dead = Vec::new();
    let mut permits = Vec::with_capacity(targets.len());

    for id in targets {
        let Some(tx) = senders.get(&id) else {
            continue;
        };

        match tx.clone().try_reserve_owned() {
            Ok(permit) => permits.push(permit),
            Err(mpsc::error::TrySendError::Closed(_)) => dead.push(id),
            Err(mpsc::error::TrySendError::Full(_)) => {
                remove_consumers(router, senders, dead);
                return Err(BusError::Backpressure(format!(
                    "subscriber queue is full for subject '{subject}'"
                )));
            }
        }
    }

    remove_consumers(router, senders, dead);

    for permit in permits {
        permit.send(msg.clone());
    }

    Ok(())
}

fn remove_consumers<M: Clone + Send + 'static>(
    router: &mut SubjectRouter,
    senders: &mut HashMap<ConsumerId, mpsc::Sender<M>>,
    dead: Vec<ConsumerId>,
) {
    for id in dead {
        router.remove_consumer(id);
        senders.remove(&id);
    }
}

fn actor_dropped(err: oneshot::error::RecvError) -> BusError {
    BusError::Internal(format!("router actor stopped before replying: {err}"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{FutureExt, StreamExt};

    use super::*;

    #[tokio::test]
    async fn full_subscription_does_not_block_unrelated_subjects() {
        let router = RouterHandle::new();
        let _slow = router.subscribe("slow").await.unwrap();
        let mut fast = router.subscribe("fast").await.unwrap();

        for value in 0..SUBSCRIPTION_BUFFER {
            router.dispatch("slow", value).await.unwrap();
        }

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            router.dispatch("slow", SUBSCRIPTION_BUFFER),
        )
        .await
        .expect("dispatch waited for subscriber capacity");
        assert!(matches!(result, Err(BusError::Backpressure(_))));

        router.dispatch("fast", 42).await.unwrap();
        assert_eq!(fast.next().await, Some(42));
    }

    #[tokio::test]
    async fn fanout_is_not_partially_delivered_when_one_subscriber_is_full() {
        let router = RouterHandle::new();
        let _full = router.subscribe("events").await.unwrap();
        let mut available = router.subscribe("events").await.unwrap();

        for value in 0..SUBSCRIPTION_BUFFER {
            router.dispatch("events", value).await.unwrap();
            assert_eq!(available.next().await, Some(value));
        }

        let result = router.dispatch("events", SUBSCRIPTION_BUFFER).await;
        assert!(matches!(result, Err(BusError::Backpressure(_))));
        assert!(available.next().now_or_never().is_none());
    }
}
