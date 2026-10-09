use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use futures::StreamExt;
use futures::future::{BoxFuture, LocalBoxFuture};
use futures::stream;
use tokio::sync::{mpsc, oneshot};

use crate::DeliveryDecision;
use crate::codec::{BuiltinCodecs, CodecCollection, HeaderAwareCodec, content_type};
use crate::errors::BusError;
use crate::router::{PayloadValue, RouteMessage, RouteSource, RouteStream};

pub const DEFAULT_OUTBOX_LEASE_DURATION: Duration = Duration::from_secs(30);
pub const DEFAULT_OUTBOX_POLL_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_OUTBOX_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxRecord {
    pub id: String,
    pub address: String,
    pub payload: Bytes,
    pub content_type: Option<String>,
    pub headers: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub created_at: SystemTime,
    pub attempts: u32,
}

impl OutboxRecord {
    pub fn new(
        id: impl Into<String>,
        address: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            id: id.into(),
            address: address.into(),
            payload: payload.into(),
            content_type: None,
            headers: HashMap::new(),
            metadata: HashMap::new(),
            created_at: SystemTime::now(),
            attempts: 0,
        }
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_created_at(mut self, created_at: SystemTime) -> Self {
        self.created_at = created_at;
        self
    }

    pub fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxLease {
    pub consumer_id: String,
    pub token: String,
    pub until: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeasedOutboxRecord {
    pub record: OutboxRecord,
    pub lease: OutboxLease,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxClaim {
    pub consumer_id: String,
    pub limit: usize,
    pub lease_until: SystemTime,
}

/// Durable persistence boundary used by [`LeasedOutboxSource`].
///
/// Implementations must atomically exclude active leases and reject stale
/// lease tokens. Creating the outbox record atomically with application state
/// remains the responsibility of the implementing persistence transaction.
pub trait OutboxStore: Send + Sync + 'static {
    type Error: std::fmt::Display + Send + Sync + 'static;

    fn claim(
        &self,
        claim: OutboxClaim,
    ) -> BoxFuture<'_, Result<Vec<LeasedOutboxRecord>, Self::Error>>;

    fn acknowledge<'a>(
        &'a self,
        record_id: &'a str,
        lease_token: &'a str,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    fn release<'a>(
        &'a self,
        record_id: &'a str,
        lease_token: &'a str,
        available_at: Option<SystemTime>,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// Extend an active lease. Sources call this only when lease renewal is
    /// explicitly configured. Stores that do not support renewal may retain
    /// this no-op default while leaving source renewal disabled.
    fn renew_lease<'a>(
        &'a self,
        _record_id: &'a str,
        _lease_token: &'a str,
        _lease_until: SystemTime,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

struct OutboxSourceOptions {
    lease_duration: Duration,
    poll_interval: Duration,
    retry_delay: Duration,
    renewal_interval: Option<Duration>,
}

impl Default for OutboxSourceOptions {
    fn default() -> Self {
        Self {
            lease_duration: DEFAULT_OUTBOX_LEASE_DURATION,
            poll_interval: DEFAULT_OUTBOX_POLL_INTERVAL,
            retry_delay: DEFAULT_OUTBOX_RETRY_DELAY,
            renewal_interval: None,
        }
    }
}

/// A durable [`RouteSource`] backed by leased outbox records.
///
/// Records are claimed one at a time so later records do not consume lease
/// time while an earlier handler or target is still running.
pub struct LeasedOutboxSource<S: OutboxStore + ?Sized, C: CodecCollection = BuiltinCodecs> {
    store: Arc<S>,
    consumer_id: String,
    codec: HeaderAwareCodec<C>,
    options: OutboxSourceOptions,
    next_delivery_id: Arc<AtomicU64>,
    settlements: Option<mpsc::UnboundedSender<SettlementCommand>>,
}

impl<S: OutboxStore> LeasedOutboxSource<S> {
    pub fn new(store: S, consumer_id: impl Into<String>) -> Self {
        Self::from_shared(Arc::new(store), consumer_id)
    }
}

impl<S: OutboxStore + ?Sized> LeasedOutboxSource<S> {
    pub fn from_shared(store: Arc<S>, consumer_id: impl Into<String>) -> Self {
        Self {
            store,
            consumer_id: consumer_id.into(),
            codec: HeaderAwareCodec::default(),
            options: OutboxSourceOptions::default(),
            next_delivery_id: Arc::new(AtomicU64::new(1)),
            settlements: None,
        }
    }
}

impl<S: OutboxStore + ?Sized, C: CodecCollection> LeasedOutboxSource<S, C> {
    pub fn with_codec<D: CodecCollection>(
        self,
        codec: HeaderAwareCodec<D>,
    ) -> LeasedOutboxSource<S, D> {
        LeasedOutboxSource {
            store: self.store,
            consumer_id: self.consumer_id,
            codec,
            options: self.options,
            next_delivery_id: self.next_delivery_id,
            settlements: self.settlements,
        }
    }

    pub fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.options.lease_duration = duration;
        self
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.options.poll_interval = interval;
        self
    }

    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.options.retry_delay = delay;
        self
    }

    pub fn with_lease_renewal(mut self, interval: Duration) -> Self {
        self.options.renewal_interval = Some(interval);
        self
    }

    pub fn without_lease_renewal(mut self) -> Self {
        self.options.renewal_interval = None;
        self
    }

    pub fn store(&self) -> &S {
        self.store.as_ref()
    }

    fn validate(&self) -> Result<(), BusError> {
        if self.consumer_id.is_empty() {
            return Err(BusError::Internal(
                "outbox consumer ID cannot be empty".into(),
            ));
        }
        if self.options.lease_duration.is_zero() {
            return Err(BusError::Internal(
                "outbox lease duration must be nonzero".into(),
            ));
        }
        if self.options.poll_interval.is_zero() {
            return Err(BusError::Internal(
                "outbox poll interval must be nonzero".into(),
            ));
        }
        if self
            .options
            .renewal_interval
            .is_some_and(|interval| interval.is_zero() || interval >= self.options.lease_duration)
        {
            return Err(BusError::Internal(
                "outbox renewal interval must be nonzero and shorter than the lease duration"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl<S, C> RouteSource for LeasedOutboxSource<S, C>
where
    S: OutboxStore + ?Sized,
    C: CodecCollection,
{
    fn decode(&self, message: &RouteMessage) -> Result<PayloadValue, BusError> {
        self.codec
            .decode_with_content_type(&message.payload, content_type(&message.headers))
    }

    fn open(&mut self) -> LocalBoxFuture<'_, Result<RouteStream, BusError>> {
        Box::pin(async move {
            self.validate()?;
            if self.settlements.is_some() {
                return Err(BusError::Internal("outbox source is already open".into()));
            }

            let (settlements, commands) = mpsc::unbounded_channel();
            tokio::spawn(settlement_actor(
                Arc::clone(&self.store),
                commands,
                self.options.retry_delay,
                self.options.lease_duration,
                self.options.renewal_interval,
            ));
            self.settlements = Some(settlements.clone());

            let state = ClaimState {
                store: Arc::clone(&self.store),
                consumer_id: self.consumer_id.clone(),
                lease_duration: self.options.lease_duration,
                poll_interval: self.options.poll_interval,
                retry_delay: self.options.retry_delay,
                next_delivery_id: Arc::clone(&self.next_delivery_id),
                settlements,
            };
            let source = stream::unfold(state, claim_next).boxed();
            Ok(Box::pin(source) as RouteStream)
        })
    }

    fn settle(
        &self,
        message: &RouteMessage,
        decision: DeliveryDecision,
    ) -> BoxFuture<'_, Result<(), BusError>> {
        let settlements = self.settlements.clone();
        let delivery_id = message.id;
        Box::pin(async move {
            let settlements = settlements.ok_or_else(|| {
                BusError::Internal("outbox source is not open for settlement".into())
            })?;
            let (reply, result) = oneshot::channel();
            settlements
                .send(SettlementCommand::Settle {
                    delivery_id,
                    decision,
                    reply,
                })
                .map_err(|_| BusError::Internal("outbox settlement actor stopped".into()))?;
            result
                .await
                .map_err(|_| BusError::Internal("outbox settlement reply was dropped".into()))?
        })
    }
}

struct ClaimState<S: OutboxStore + ?Sized> {
    store: Arc<S>,
    consumer_id: String,
    lease_duration: Duration,
    poll_interval: Duration,
    retry_delay: Duration,
    next_delivery_id: Arc<AtomicU64>,
    settlements: mpsc::UnboundedSender<SettlementCommand>,
}

async fn claim_next<S: OutboxStore + ?Sized>(
    state: ClaimState<S>,
) -> Option<(RouteMessage, ClaimState<S>)> {
    loop {
        let Some(lease_until) = SystemTime::now().checked_add(state.lease_duration) else {
            tracing::error!("cannot calculate outbox lease deadline");
            tokio::time::sleep(state.poll_interval).await;
            continue;
        };
        let claim = OutboxClaim {
            consumer_id: state.consumer_id.clone(),
            limit: 1,
            lease_until,
        };
        let mut claimed = match state.store.claim(claim).await {
            Ok(claimed) => claimed,
            Err(error) => {
                tracing::error!(error = %error, "cannot claim outbox record; retrying");
                tokio::time::sleep(state.poll_interval).await;
                continue;
            }
        };
        if claimed.is_empty() {
            tokio::time::sleep(state.poll_interval).await;
            continue;
        }
        if claimed.len() > 1 {
            tracing::error!(
                claimed = claimed.len(),
                "outbox store returned more records than the requested claim limit"
            );
        }
        let leased = claimed.remove(0);
        for excess in claimed {
            release_after_claim_failure(&state, &excess).await;
        }

        let delivery_id = state.next_delivery_id.fetch_add(1, Ordering::Relaxed);
        let (registered, registration) = oneshot::channel();
        if state
            .settlements
            .send(SettlementCommand::Register {
                delivery_id,
                record_id: leased.record.id.clone(),
                lease_token: leased.lease.token.clone(),
                registered,
            })
            .is_err()
            || registration.await.is_err()
        {
            release_after_claim_failure(&state, &leased).await;
            return None;
        }

        let OutboxRecord {
            id,
            address,
            payload,
            content_type: record_content_type,
            mut headers,
            metadata,
            created_at: _,
            attempts,
        } = leased.record;
        if let Some(record_content_type) = record_content_type {
            crate::codec::set_content_type(&mut headers, &record_content_type);
        }
        let message = RouteMessage {
            address,
            timestamp: Instant::now(),
            id: delivery_id,
            message_id: id,
            headers,
            metadata,
            attempts,
            payload,
        };
        return Some((message, state));
    }
}

async fn release_after_claim_failure<S: OutboxStore + ?Sized>(
    state: &ClaimState<S>,
    leased: &LeasedOutboxRecord,
) {
    let available_at = SystemTime::now().checked_add(state.retry_delay);
    if let Err(error) = state
        .store
        .release(&leased.record.id, &leased.lease.token, available_at)
        .await
    {
        tracing::error!(
            error = %error,
            record_id = %leased.record.id,
            "cannot release outbox record after source failure"
        );
    }
}

enum SettlementCommand {
    Register {
        delivery_id: u64,
        record_id: String,
        lease_token: String,
        registered: oneshot::Sender<()>,
    },
    Settle {
        delivery_id: u64,
        decision: DeliveryDecision,
        reply: oneshot::Sender<Result<(), BusError>>,
    },
}

struct PendingLease {
    record_id: String,
    lease_token: String,
    stop_renewal: Option<oneshot::Sender<()>>,
    renewal: Option<tokio::task::JoinHandle<()>>,
}

async fn settlement_actor<S: OutboxStore + ?Sized>(
    store: Arc<S>,
    mut commands: mpsc::UnboundedReceiver<SettlementCommand>,
    retry_delay: Duration,
    lease_duration: Duration,
    renewal_interval: Option<Duration>,
) {
    let mut pending = HashMap::new();
    while let Some(command) = commands.recv().await {
        match command {
            SettlementCommand::Register {
                delivery_id,
                record_id,
                lease_token,
                registered,
            } => {
                let (stop_renewal, renewal) = renewal_interval
                    .map(|interval| {
                        spawn_lease_renewal(
                            Arc::clone(&store),
                            record_id.clone(),
                            lease_token.clone(),
                            lease_duration,
                            interval,
                        )
                    })
                    .map_or((None, None), |(stop, task)| (Some(stop), Some(task)));
                pending.insert(
                    delivery_id,
                    PendingLease {
                        record_id,
                        lease_token,
                        stop_renewal,
                        renewal,
                    },
                );
                let _ = registered.send(());
            }
            SettlementCommand::Settle {
                delivery_id,
                decision,
                reply,
            } => {
                let result = match pending.remove(&delivery_id) {
                    Some(mut lease) => {
                        if let Some(stop) = lease.stop_renewal.take() {
                            let _ = stop.send(());
                        }
                        if let Some(task) = lease.renewal.take() {
                            let _ = task.await;
                        }
                        settle_record(store.as_ref(), lease, decision, retry_delay).await
                    }
                    None => Err(BusError::Internal(format!(
                        "outbox delivery {delivery_id} is not pending settlement"
                    ))),
                };
                let _ = reply.send(result);
            }
        }
    }
    for (_, mut lease) in pending {
        if let Some(stop) = lease.stop_renewal.take() {
            let _ = stop.send(());
        }
        if let Some(task) = lease.renewal.take() {
            task.abort();
        }
    }
}

fn spawn_lease_renewal<S: OutboxStore + ?Sized>(
    store: Arc<S>,
    record_id: String,
    lease_token: String,
    lease_duration: Duration,
    interval: Duration,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                _ = tokio::time::sleep(interval) => {
                    let Some(lease_until) = SystemTime::now().checked_add(lease_duration) else {
                        tracing::error!(record_id = %record_id, "cannot calculate outbox renewal deadline");
                        continue;
                    };
                    if let Err(error) = store.renew_lease(&record_id, &lease_token, lease_until).await {
                        tracing::error!(
                            error = %error,
                            record_id = %record_id,
                            "cannot renew outbox lease"
                        );
                    }
                }
            }
        }
    });
    (stop, task)
}

async fn settle_record<S: OutboxStore + ?Sized>(
    store: &S,
    lease: PendingLease,
    decision: DeliveryDecision,
    retry_delay: Duration,
) -> Result<(), BusError> {
    match decision {
        DeliveryDecision::Retry => {
            let available_at = SystemTime::now().checked_add(retry_delay).ok_or_else(|| {
                BusError::Internal("cannot calculate outbox retry deadline".into())
            })?;
            store
                .release(&lease.record_id, &lease.lease_token, Some(available_at))
                .await
                .map_err(|error| outbox_error("release", &lease.record_id, error))
        }
        DeliveryDecision::Ack | DeliveryDecision::Reject => store
            .acknowledge(&lease.record_id, &lease.lease_token)
            .await
            .map_err(|error| outbox_error("acknowledge", &lease.record_id, error)),
    }
}

fn outbox_error(operation: &str, record_id: &str, error: impl std::fmt::Display) -> BusError {
    BusError::Internal(format!(
        "cannot {operation} outbox record '{record_id}': {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Codec, JsonCodec};
    use crate::router::{RouteTarget, Router};
    use futures::StreamExt;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct MockState {
        records: VecDeque<OutboxRecord>,
        acknowledged: Vec<(String, String)>,
        released: Vec<(String, String, Option<SystemTime>)>,
        claims: Vec<OutboxClaim>,
    }

    #[derive(Default)]
    struct MockStore {
        state: Mutex<MockState>,
        renewals: AtomicUsize,
    }

    impl MockStore {
        async fn push(&self, record: OutboxRecord) {
            self.state.lock().await.records.push_back(record);
        }
    }

    impl OutboxStore for MockStore {
        type Error = String;

        fn claim(
            &self,
            claim: OutboxClaim,
        ) -> BoxFuture<'_, Result<Vec<LeasedOutboxRecord>, Self::Error>> {
            Box::pin(async move {
                let mut state = self.state.lock().await;
                state.claims.push(claim.clone());
                Ok(state
                    .records
                    .pop_front()
                    .map(|record| {
                        vec![LeasedOutboxRecord {
                            record,
                            lease: OutboxLease {
                                consumer_id: claim.consumer_id,
                                token: "lease-1".into(),
                                until: claim.lease_until,
                            },
                        }]
                    })
                    .unwrap_or_default())
            })
        }

        fn acknowledge<'a>(
            &'a self,
            record_id: &'a str,
            lease_token: &'a str,
        ) -> BoxFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                self.state
                    .lock()
                    .await
                    .acknowledged
                    .push((record_id.into(), lease_token.into()));
                Ok(())
            })
        }

        fn release<'a>(
            &'a self,
            record_id: &'a str,
            lease_token: &'a str,
            available_at: Option<SystemTime>,
        ) -> BoxFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                self.state.lock().await.released.push((
                    record_id.into(),
                    lease_token.into(),
                    available_at,
                ));
                Ok(())
            })
        }

        fn renew_lease<'a>(
            &'a self,
            _record_id: &'a str,
            _lease_token: &'a str,
            _lease_until: SystemTime,
        ) -> BoxFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                self.renewals.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    fn record() -> OutboxRecord {
        OutboxRecord::new(
            "event-1",
            "outbox.events",
            JsonCodec.encode(&42u32).unwrap(),
        )
        .with_content_type("application/json")
        .with_headers(HashMap::from([("trace-id".into(), "trace-1".into())]))
        .with_metadata(HashMap::from([("tenant".into(), "one".into())]))
        .with_attempts(2)
    }

    struct GatedTarget {
        started: mpsc::UnboundedSender<()>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl RouteTarget for GatedTarget {
        fn encode(&self, value: &PayloadValue, message: &mut RouteMessage) -> Result<(), BusError> {
            message.payload = JsonCodec.encode(value)?;
            Ok(())
        }

        fn deliver(&self, _output: RouteMessage) -> BoxFuture<'_, Result<(), BusError>> {
            Box::pin(async move {
                self.started
                    .send(())
                    .map_err(|_| BusError::Internal("target observer closed".into()))?;
                let release = self.release.lock().await.take().ok_or_else(|| {
                    BusError::Internal("target release signal was already consumed".into())
                })?;
                release
                    .await
                    .map_err(|_| BusError::Internal("target release signal was dropped".into()))
            })
        }
    }

    #[tokio::test]
    async fn source_claims_one_record_and_preserves_route_data() {
        let store = Arc::new(MockStore::default());
        store.push(record()).await;
        let erased: Arc<dyn OutboxStore<Error = String>> = store.clone();
        let mut source = LeasedOutboxSource::from_shared(erased, "worker-1")
            .with_poll_interval(Duration::from_millis(10));
        let mut messages = source.open().await.unwrap();

        let message = messages.next().await.unwrap();

        assert_eq!(message.address, "outbox.events");
        assert_eq!(message.message_id, "event-1");
        assert_eq!(message.headers["trace-id"], "trace-1");
        assert_eq!(message.headers["content-type"], "application/json");
        assert_eq!(message.metadata["tenant"], "one");
        assert_eq!(message.attempts, 2);
        assert_eq!(source.decode(&message).unwrap(), PayloadValue::U64(42));
        let state = store.state.lock().await;
        assert_eq!(state.claims.len(), 1);
        assert_eq!(state.claims[0].limit, 1);
    }

    #[tokio::test]
    async fn ack_and_reject_remove_records_while_retry_releases_them() {
        for decision in [
            DeliveryDecision::Ack,
            DeliveryDecision::Reject,
            DeliveryDecision::Retry,
        ] {
            let store = Arc::new(MockStore::default());
            store.push(record()).await;
            let mut source = LeasedOutboxSource::from_shared(Arc::clone(&store), "worker-1")
                .with_poll_interval(Duration::from_millis(10));
            let mut messages = source.open().await.unwrap();
            let message = messages.next().await.unwrap();

            source.settle(&message, decision).await.unwrap();

            let state = store.state.lock().await;
            match decision {
                DeliveryDecision::Retry => {
                    assert!(state.acknowledged.is_empty());
                    assert_eq!(state.released.len(), 1);
                    assert!(state.released[0].2.is_some());
                }
                DeliveryDecision::Ack | DeliveryDecision::Reject => {
                    assert_eq!(state.acknowledged.len(), 1);
                    assert!(state.released.is_empty());
                }
            }
        }
    }

    #[tokio::test]
    async fn configured_renewal_stops_before_settlement() {
        let store = Arc::new(MockStore::default());
        store.push(record()).await;
        let mut source = LeasedOutboxSource::from_shared(Arc::clone(&store), "worker-1")
            .with_lease_duration(Duration::from_millis(100))
            .with_lease_renewal(Duration::from_millis(10))
            .with_poll_interval(Duration::from_millis(10));
        let mut messages = source.open().await.unwrap();
        let message = messages.next().await.unwrap();
        tokio::time::timeout(Duration::from_millis(100), async {
            while store.renewals.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        source
            .settle(&message, DeliveryDecision::Ack)
            .await
            .unwrap();
        let renewals = store.renewals.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(store.renewals.load(Ordering::Relaxed), renewals);
    }

    #[tokio::test]
    async fn router_acknowledges_only_after_target_delivery() {
        let store = Arc::new(MockStore::default());
        store.push(record()).await;
        let source = LeasedOutboxSource::from_shared(Arc::clone(&store), "worker-1")
            .with_poll_interval(Duration::from_millis(10));
        let (started, mut target_started) = mpsc::unbounded_channel();
        let (release_target, released) = oneshot::channel();
        let mut router = Router::new()
            .bind(|value: u32| async move { Ok::<_, String>(value + 1) })
            .from(source)
            .to(GatedTarget {
                started,
                release: Mutex::new(Some(released)),
            });
        router.install().await.unwrap();

        target_started.recv().await.unwrap();
        assert!(store.state.lock().await.acknowledged.is_empty());
        release_target.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !store.state.lock().await.acknowledged.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.state.lock().await.acknowledged[0].0, "event-1");
    }

    #[tokio::test]
    async fn invalid_source_options_fail_before_claiming() {
        let store = Arc::new(MockStore::default());
        let mut source = LeasedOutboxSource::from_shared(store, "")
            .with_poll_interval(Duration::from_millis(10));
        assert!(source.open().await.is_err());
    }
}
