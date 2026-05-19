pub mod errors;
mod message;
mod subscription;
mod message_bus;
mod router;
mod qanat_bus;

#[cfg(test)]
mod tests {
    use crate::message_bus::MessageBus;
    use super::qanat_bus::QanatBus;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_fanout_subscribers_receive_all_messages() {
        let bus = QanatBus::new();

        let mut sub1 = bus.subscribe("foo.bar").await.unwrap();
        let mut sub2 = bus.subscribe("foo.bar").await.unwrap();

        bus.publish("foo.bar", 123u32, None).await.unwrap();

        let msg1 = sub1.next().await.unwrap().downcast::<u32>().unwrap();
        let msg2 = sub2.next().await.unwrap().downcast::<u32>().unwrap();

        assert_eq!(*msg1.payload, 123);
        assert_eq!(*msg2.payload, 123);
    }

    #[tokio::test]
    async fn test_queue_group_round_robin() {
        let bus = QanatBus::new();

        // Bind queue group
        bus.bind_queue("jobs.*", "workers").await.unwrap();

        // Two consumers in the same queue group
        let mut c1 = bus.consume("workers").await.unwrap();
        let mut c2 = bus.consume("workers").await.unwrap();

        // Publish 2 messages
        bus.publish("jobs.run", "A", None).await.unwrap();
        bus.publish("jobs.run", "B", None).await.unwrap();

        // Each consumer should get exactly one message
        let m1 = c1.next().await.unwrap().downcast::<&str>().unwrap();
        let m2 = c2.next().await.unwrap().downcast::<&str>().unwrap();

        let p1 = *m1.payload;
        let p2 = *m2.payload;

        assert!(p1 == "A" || p1 == "B");
        assert!(p2 == "A" || p2 == "B");
        assert_ne!(p1, p2);
    }

    #[tokio::test]
    async fn test_wildcard_matching() {
        let bus = QanatBus::new();

        let mut sub = bus.subscribe("foo.*").await.unwrap();

        bus.publish("foo.bar", 999i32, None).await.unwrap();

        let msg = sub.next().await.unwrap().downcast::<i32>().unwrap();

        assert_eq!(*msg.payload, 999);
    }

}
