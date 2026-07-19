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
                let result = dispatch(&mut router, &mut senders, &subject, msg).await;
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

async fn dispatch<M: Clone + Send + 'static>(
    router: &mut SubjectRouter,
    senders: &mut HashMap<ConsumerId, mpsc::Sender<M>>,
    subject: &str,
    msg: M,
) -> Result<(), BusError> {
    let targets = router.route(subject);
    let mut dead = Vec::new();

    for id in targets {
        let Some(tx) = senders.get(&id) else {
            continue;
        };

        if tx.send(msg.clone()).await.is_err() {
            dead.push(id);
        }
    }

    for id in dead {
        router.remove_consumer(id);
        senders.remove(&id);
    }

    Ok(())
}

fn actor_dropped(err: oneshot::error::RecvError) -> BusError {
    BusError::Internal(format!("router actor stopped before replying: {err}"))
}
