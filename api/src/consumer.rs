use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use redis::Client as RedisClient;
use tokio::time::{sleep, Duration};

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 1000;

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PaymentJob {
    reference_no: String,
    partner_reference_no: String,
    transaction_date: String,
    amount: Option<serde_json::Value>,
    fee_amount: Option<serde_json::Value>,
    merchant_id: Option<String>,
    additional_info: Option<serde_json::Value>,
}

async fn call_legacy(job: &PaymentJob) -> Result<(), String> {
    // TODO: replace with real legacy system call
    // For now simulate 400-700ms delay with 20% failure rate
    let delay = rand::thread_rng().gen_range(400..=700);
    sleep(Duration::from_millis(delay)).await;

    if rand::thread_rng().gen_bool(0.20) {
        return Err("Legacy system timeout".into());
    }

    Ok(())
}

async fn process_payment(job: PaymentJob, redis: &RedisClient) {
    let status_key = format!("payment_status:{}", job.partner_reference_no);

    for attempt in 1..=MAX_RETRIES {
        println!("attempt {} for {}", attempt, job.partner_reference_no);

        match call_legacy(&job).await {
            Ok(_) => {
                println!("payment success: {}", job.partner_reference_no);
                let status = serde_json::json!({
                    "status": "00",
                    "desc": "success",
                    "referenceNo": job.reference_no,
                    "partnerReferenceNo": job.partner_reference_no,
                    "paidTime": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S+07:00").to_string(),
                    "amount": job.amount,
                });
                if let Ok(mut conn) = redis.get_async_connection().await {
                    let _: Result<(), _> = conn.set_ex(
                        &status_key,
                        status.to_string(),
                        600,
                    ).await;
                }
                return;
            }
            Err(e) => {
                println!("attempt {} failed: {}", attempt, e);
                if attempt < MAX_RETRIES {
                    sleep(Duration::from_millis(RETRY_DELAY_MS * attempt as u64)).await;
                }
            }
        }
    }

    // All retries failed — mark as failed
    println!("payment failed after {} retries: {}", MAX_RETRIES, job.partner_reference_no);
    let status = serde_json::json!({
        "status": "06",
        "desc": "failed",
        "referenceNo": job.reference_no,
        "partnerReferenceNo": job.partner_reference_no,
    });
    if let Ok(mut conn) = redis.get_async_connection().await {
        let _: Result<(), _> = conn.set_ex(
            &status_key,
            status.to_string(),
            600,
        ).await;
    }
}

pub async fn run_consumer(redis: RedisClient) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "payment-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer.subscribe(&["payments"]).expect("Failed to subscribe to payments topic");

    println!("Kafka consumer started");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload = match msg.payload_view::<str>() {
                    Some(Ok(s)) => s.to_owned(),
                    _ => {
                        println!("invalid message payload");
                        continue;
                    }
                };

                match serde_json::from_str::<PaymentJob>(&payload) {
                    Ok(job) => {
                        let redis_clone = redis.clone();
                        tokio::spawn(async move {
                            process_payment(job, &redis_clone).await;
                        });
                        consumer.commit_message(&msg, CommitMode::Async).unwrap();
                    }
                    Err(e) => println!("failed to deserialize job: {}", e),
                }
            }
            Err(e) => println!("kafka error: {}", e),
        }
    }
}