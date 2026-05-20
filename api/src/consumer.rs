use chrono::Utc;
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use redis::Client as RedisClient;
use redis::cmd as redis_cmd;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::join;
use tokio::time::{sleep, Duration};

use crate::config::Config;
use crate::models::{PaymentQueryResponse, PaymentRequest};

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 55000;

/*struct PaymentJob {
    partner_reference_no: String,
    amount: Option<serde_json::Value>,
    fee_amount: Option<serde_json::Value>,
    merchant_id: Option<String>,
    additional_info: Option<serde_json::Value>,
}*/

async fn call_legacy(job: &PaymentRequest, db: &Pool<Postgres>, config: Arc<Config>) -> Result<(), String> {

    sleep(Duration::from_millis(config.effective_latency())).await;

    if rand::thread_rng().gen_bool(config.error_rate() / 100.0) {
        return Err("Legacy system timeout".into());
    }

    Ok(())
}

async fn store_to_db(
    job: &PaymentRequest, 
    db: &Pool<Postgres>, 
    status_code: &str, 
    status_desc: &str, 
) -> Result<(), anyhow::Error> {
    let amount_value: Option<i64> = job.amount
        .as_ref()
        .map(|a| a.value.trim_end_matches(".00").parse())
        .transpose()?;
    let fee_value: Option<i64> = job.fee_amount
        .as_ref()
        .map(|a| a.value.trim_end_matches(".00").parse())
        .transpose()?;

    sqlx::query!(
        r#"
        INSERT INTO payment (
            partner_reference_no,
            merchant_id,
            sub_merchant_id,
            amount_value,
            amount_currency,
            fee_value,
            fee_currency,
            status_code,
            status_desc,
            transaction_date
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
        job.partner_reference_no.clone(),
        job.merchant_id.clone(),
        job.sub_merchant_id.clone(),
        amount_value,
        job.amount.as_ref().map(|a| a.currency.clone()),
        fee_value,
        job.fee_amount.as_ref().map(|a| a.currency.clone()),
        status_code.to_string(),
        status_desc.to_string(),
        Utc::now()
    )
    .execute(db)
    .await?;

    Ok(())
}

async fn update_redis_status(
    redis: &RedisClient, 
    key: &str, 
    status_code: &str, 
    status_desc: &str, 
    paid_time: Option<String>
) -> Result<(), anyhow::Error> {

    let mut conn = redis.get_async_connection().await?;

    let raw: String = conn.get(key).await?;

    let mut status: PaymentQueryResponse = serde_json::from_str(&raw)?;

    status.latest_transaction_status = status_code.to_string();
    status.transaction_status_desc = status_desc.to_string();
    status.paid_time = paid_time;

    redis_cmd("SET")
        .arg(key)
        .arg(serde_json::to_string(&status)?)
        .arg("KEEPTTL")
        .query_async::<_, ()>(&mut conn)
        .await?;

    Ok(())
}

async fn process_payment(job: PaymentRequest, redis: &RedisClient, db: &Pool<Postgres>, config: Arc<Config>) {
    let partner_reference_no = job.partner_reference_no.as_ref().unwrap_or(&String::from("")).clone();
    let status_key = format!("payment_status:{}", partner_reference_no);

    let _ = update_redis_status(redis, &status_key, "02", "paying", None).await;

    for attempt in 1..=MAX_RETRIES {
        println!("attempt {} for {}", attempt, partner_reference_no);

        match call_legacy(&job, db, config.clone()).await {
            Ok(_) => {
                println!("payment success: {}", partner_reference_no);
                let _ = join!(
                    update_redis_status(redis, &status_key, "00", "success", Some(Utc::now().format("%Y-%m-%dT%H:%M:%S+07:00").to_string())), 
                    store_to_db(&job, db, "00", "success")
                );
                return;
            }
            Err(e) => {
                println!("attempt {} failed: {}", attempt, e);
                let _ = update_redis_status(redis, &status_key, "03", "pending", None).await;
                if attempt < MAX_RETRIES {
                    sleep(Duration::from_millis(RETRY_DELAY_MS as u64)).await;
                }
            }
        }
    }

    // All retries failed — mark as failed
    println!("payment failed after {} retries: {}", MAX_RETRIES, partner_reference_no);

    let _ = join!(update_redis_status(redis, &status_key, "06", "failed", None), store_to_db(&job, db, "06", "failed"));
}

pub async fn run_consumer(redis: RedisClient, db: Pool<Postgres>, config: Arc<Config>) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "payment-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
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

                match serde_json::from_str::<PaymentRequest>(&payload) {
                    Ok(job) => {
                        let redis_clone = redis.clone();
                        let db_clone = db.clone();
                        let config_clone = config.clone();
                        tokio::spawn(async move {
                            process_payment(job, &redis_clone, &db_clone, config_clone).await;
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