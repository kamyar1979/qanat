use std::collections::HashMap;

use tokio::sync::{Mutex, mpsc};

use crate::errors::BusError;
use crate::routing::{ConsumerId, SubjectRouter};

#[allow(async_fn_in_trait)]
pub(crate) trait InternalRouter {
    type Message: Clone + Send + 'static;

    async fn dispatch_internal(
        &self,
        router: &Mutex<SubjectRouter>,
        senders: &Mutex<HashMap<ConsumerId, mpsc::Sender<Self::Message>>>,
        subject: &str,
        msg: Self::Message,
    ) -> Result<(), BusError> {
        let targets = router.lock().await.route(subject);

        let mut to_send: Vec<(ConsumerId, mpsc::Sender<Self::Message>)> = Vec::new();
        {
            let senders = senders.lock().await;
            for id in &targets {
                if let Some(tx) = senders.get(id) {
                    to_send.push((*id, tx.clone()));
                }
            }
        }

        let mut dead: Vec<ConsumerId> = Vec::new();
        for (id, tx) in to_send {
            if tx.send(msg.clone()).await.is_err() {
                dead.push(id);
            }
        }

        if !dead.is_empty() {
            let mut router = router.lock().await;
            let mut senders = senders.lock().await;
            for id in dead {
                router.remove_consumer(id);
                senders.remove(&id);
            }
        }

        Ok(())
    }
}
