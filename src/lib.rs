pub mod bus;
pub mod codec;
pub mod errors;
pub mod internal_bus;
pub(crate) mod internal_router;
#[cfg(any(feature = "nng", feature = "redis"))]
pub(crate) mod local_router;
pub(crate) mod message;
#[cfg(feature = "nats")]
pub mod nats_bus;
#[cfg(feature = "nng")]
pub mod nng_bus;
pub mod raw_message;
#[cfg(feature = "redis")]
pub mod redis_bus;
pub(crate) mod routing;
#[cfg(any(feature = "nng", feature = "redis"))]
pub(crate) mod wire;

#[cfg(test)]
mod tests {
    use super::internal_bus::InternalBus;
    use crate::bus::Bus;
    use crate::errors::BusError;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    // ── wildcard: > ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_wildcard_gt_does_not_match_bare_prefix() {
        let bus = InternalBus::new();
        let mut sub = bus.subscribe("foo.>").await.unwrap();
        bus.publish("foo", 1u32, None).await.unwrap();
        let result = timeout(Duration::from_millis(50), sub.next()).await;
        assert!(result.is_err(), "foo.> must not match foo");
    }

    #[tokio::test]
    async fn test_wildcard_gt_matches_multiple_levels() {
        let bus = InternalBus::new();
        let mut sub = bus.subscribe("foo.>").await.unwrap();
        bus.publish("foo.bar.baz", 7u32, None).await.unwrap();
        let val = sub.next().await.unwrap().downcast::<u32>().unwrap();
        assert_eq!(*val.payload, 7);
    }

    #[tokio::test]
    async fn test_wildcard_gt_alone_matches_any_subject() {
        let bus = InternalBus::new();
        let mut sub = bus.subscribe(">").await.unwrap();
        bus.publish("a.b.c.d", 42u32, None).await.unwrap();
        let val = sub.next().await.unwrap().downcast::<u32>().unwrap();
        assert_eq!(*val.payload, 42);
    }

    // ── wildcard: * ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_wildcard_star_does_not_match_fewer_tokens() {
        let bus = InternalBus::new();
        let mut sub = bus.subscribe("foo.*").await.unwrap();
        bus.publish("foo", 1u32, None).await.unwrap();
        let result = timeout(Duration::from_millis(50), sub.next()).await;
        assert!(result.is_err(), "foo.* must not match foo");
    }

    #[tokio::test]
    async fn test_wildcard_star_does_not_match_more_tokens() {
        let bus = InternalBus::new();
        let mut sub = bus.subscribe("foo.*").await.unwrap();
        bus.publish("foo.bar.baz", 1u32, None).await.unwrap();
        let result = timeout(Duration::from_millis(50), sub.next()).await;
        assert!(result.is_err(), "foo.* must not match foo.bar.baz");
    }

    // ── queue group edge cases ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_publish_to_queue_with_no_consumers_does_not_panic() {
        let bus = InternalBus::new();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        assert!(bus.publish("jobs.run", "task", None).await.is_ok());
    }

    #[tokio::test]
    async fn test_consume_nonexistent_queue_returns_error() {
        let bus = InternalBus::new();
        assert!(matches!(
            bus.consume("ghost").await,
            Err(BusError::QueueNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_bind_queue_conflicting_pattern_returns_error() {
        let bus = InternalBus::new();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        assert!(matches!(
            bus.bind_queue("tasks.*", "workers").await,
            Err(BusError::Internal(_))
        ));
    }

    #[tokio::test]
    async fn test_bind_queue_same_pattern_is_idempotent() {
        let bus = InternalBus::new();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        assert!(bus.bind_queue("jobs.*", "workers").await.is_ok());
    }

    #[tokio::test]
    async fn test_queue_group_round_robin_cycles() {
        let bus = InternalBus::new();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        let mut c1 = bus.consume("workers").await.unwrap();
        let mut c2 = bus.consume("workers").await.unwrap();

        for i in 1u32..=4 {
            bus.publish("jobs.task", i, None).await.unwrap();
        }

        let a = *c1.next().await.unwrap().downcast::<u32>().unwrap().payload;
        let b = *c2.next().await.unwrap().downcast::<u32>().unwrap().payload;
        let c = *c1.next().await.unwrap().downcast::<u32>().unwrap().payload;
        let d = *c2.next().await.unwrap().downcast::<u32>().unwrap().payload;

        let mut c1_got = vec![a, c];
        let mut c2_got = vec![b, d];
        c1_got.sort();
        c2_got.sort();

        assert_eq!(c1_got, vec![1, 3]);
        assert_eq!(c2_got, vec![2, 4]);
    }

    // ── subscription lifecycle ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_dropped_subscription_does_not_affect_later_publishes() {
        let bus = InternalBus::new();
        {
            let _sub = bus.subscribe("foo.bar").await.unwrap();
        }
        assert!(bus.publish("foo.bar", 1u32, None).await.is_ok());
    }

    // ── fanout and wildcards ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_fanout_subscribers_receive_all_messages() {
        let bus = InternalBus::new();
        let mut sub1 = bus.subscribe("foo.bar").await.unwrap();
        let mut sub2 = bus.subscribe("foo.bar").await.unwrap();

        bus.publish("foo.bar", 123u32, None).await.unwrap();

        let v1 = *sub1
            .next()
            .await
            .unwrap()
            .downcast::<u32>()
            .unwrap()
            .payload;
        let v2 = *sub2
            .next()
            .await
            .unwrap()
            .downcast::<u32>()
            .unwrap()
            .payload;

        assert_eq!(v1, 123);
        assert_eq!(v2, 123);
    }

    #[tokio::test]
    async fn test_queue_group_round_robin() {
        let bus = InternalBus::new();
        bus.bind_queue("jobs.*", "workers").await.unwrap();
        let mut c1 = bus.consume("workers").await.unwrap();
        let mut c2 = bus.consume("workers").await.unwrap();

        bus.publish("jobs.run", "A", None).await.unwrap();
        bus.publish("jobs.execute", "B", None).await.unwrap();

        let p1 = *c1.next().await.unwrap().downcast::<&str>().unwrap().payload;
        let p2 = *c2.next().await.unwrap().downcast::<&str>().unwrap().payload;

        assert!(p1 == "A" || p1 == "B");
        assert!(p2 == "A" || p2 == "B");
        assert_ne!(p1, p2);
    }

    #[tokio::test]
    async fn test_wildcard_matching() {
        let bus = InternalBus::new();
        let mut sub = bus.subscribe("foo.*").await.unwrap();
        bus.publish("foo.bar", 999i32, None).await.unwrap();
        let val = *sub.next().await.unwrap().downcast::<i32>().unwrap().payload;
        assert_eq!(val, 999);
    }

    #[cfg(feature = "nng")]
    mod nng_tests {
        use super::*;
        use crate::codec::JsonCodec;
        use crate::nng_bus::NngBus;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NNG_URL_COUNTER: AtomicU64 = AtomicU64::new(1);

        fn nng_url() -> String {
            format!(
                "inproc://qanat-test-{}",
                NNG_URL_COUNTER.fetch_add(1, Ordering::Relaxed)
            )
        }

        #[tokio::test]
        async fn test_nng_local_pub_sub() {
            // publish + subscribe on a single node; local dispatch fires even with no peers
            let bus = NngBus::listen(JsonCodec, &nng_url()).unwrap();
            let mut sub = bus.subscribe("events.login").await.unwrap();

            bus.publish("events.login", &42u32, None).await.unwrap();

            let msg = timeout(Duration::from_millis(200), sub.next())
                .await
                .expect("timed out")
                .expect("stream ended");
            assert_eq!(msg.decode_json::<u32>().unwrap(), 42);
        }

        #[tokio::test]
        async fn test_nng_two_nodes_exchange_messages() {
            let url = nng_url();
            let listener = NngBus::listen(JsonCodec, &url).unwrap();
            let dialer = NngBus::dial(JsonCodec, &url).unwrap();

            // Let the connection establish before subscribing / publishing
            tokio::time::sleep(Duration::from_millis(20)).await;

            let mut sub = listener.subscribe("orders.>").await.unwrap();

            dialer
                .publish("orders.placed", &"order-1", None)
                .await
                .unwrap();

            let msg = timeout(Duration::from_millis(200), sub.next())
                .await
                .expect("timed out")
                .expect("stream ended");
            assert_eq!(msg.decode_json::<String>().unwrap(), "order-1");
        }

        #[tokio::test]
        async fn test_nng_wildcard_routing_across_nodes() {
            let url = nng_url();
            let listener = NngBus::listen(JsonCodec, &url).unwrap();
            let dialer = NngBus::dial(JsonCodec, &url).unwrap();

            tokio::time::sleep(Duration::from_millis(20)).await;

            let mut sub_all = listener.subscribe(">").await.unwrap();
            let mut sub_foo = listener.subscribe("foo.*").await.unwrap();
            let mut sub_bar = listener.subscribe("bar.>").await.unwrap();

            dialer.publish("foo.x", &1u32, None).await.unwrap();

            // sub_all and sub_foo match; sub_bar does not
            let v1 = timeout(Duration::from_millis(200), sub_all.next())
                .await
                .expect("timed out")
                .unwrap()
                .decode_json::<u32>()
                .unwrap();
            let v2 = timeout(Duration::from_millis(200), sub_foo.next())
                .await
                .expect("timed out")
                .unwrap()
                .decode_json::<u32>()
                .unwrap();
            assert_eq!(v1, 1);
            assert_eq!(v2, 1);

            let no_msg = timeout(Duration::from_millis(50), sub_bar.next()).await;
            assert!(no_msg.is_err(), "bar.> must not match foo.x");
        }

        #[tokio::test]
        async fn test_nng_queue_group_across_nodes() {
            let url = nng_url();
            let listener = NngBus::listen(JsonCodec, &url).unwrap();
            let dialer = NngBus::dial(JsonCodec, &url).unwrap();

            tokio::time::sleep(Duration::from_millis(20)).await;

            listener.bind_queue("jobs.*", "workers").await.unwrap();
            let mut c1 = listener.consume("workers").await.unwrap();
            let mut c2 = listener.consume("workers").await.unwrap();

            dialer.publish("jobs.a", &1u32, None).await.unwrap();
            dialer.publish("jobs.b", &2u32, None).await.unwrap();

            let m1 = timeout(Duration::from_millis(200), c1.next())
                .await
                .expect("timed out")
                .unwrap()
                .decode_json::<u32>()
                .unwrap();
            let m2 = timeout(Duration::from_millis(200), c2.next())
                .await
                .expect("timed out")
                .unwrap()
                .decode_json::<u32>()
                .unwrap();

            assert!(m1 == 1 || m1 == 2);
            assert!(m2 == 1 || m2 == 2);
            assert_ne!(m1, m2);
        }
    }
}
