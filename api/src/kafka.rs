use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub fn create_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer")
}

pub async fn publish_payment(producer: &FutureProducer, payload: &str) -> Result<(), String> {
    producer
        .send(
            FutureRecord::to("payments")
                .payload(payload)
                .key("payment"),
            Duration::from_secs(5),
        )
        .await
        .map(|_| ())
        .map_err(|(e, _)| e.to_string())
}