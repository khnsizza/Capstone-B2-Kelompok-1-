use redis::AsyncCommands;
use redis::Client as RedisClient;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use rand::Rng;
use uuid::Uuid;

use crate::models::{Amount, MerchantInfo, QrDecodeRequest, QrDecodeResponse, SnapHeaders};

const CACHE_TTL: u64 = 600; // 10 minutes per proposal
const LOCK_TTL: usize = 5;  // 5 seconds per proposal

fn parse_tlv(data: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let mut i = 0;
    while i + 4 <= data.len() {
        let tag = &data[i..i+2];
        let len: usize = match data[i+2..i+4].parse() {
            Ok(n) => n,
            Err(_) => break,
        };
        if i + 4 + len > data.len() { break; }
        map.insert(tag.to_string(), data[i+4..i+4+len].to_string());
        i += 4 + len;
    }
    map
}

fn extract_amount(qr_content: &str) -> Option<Amount> {
    let fields = parse_tlv(qr_content);
    if fields.get("01").map(|s| s.as_str()) != Some("12") {
        return None;
    }
    let value = fields.get("54")?.clone();
    let currency = match fields.get("53").map(|s| s.as_str()) {
        Some("360") => "IDR",
        Some(c) => c,
        None => "IDR",
    };
    Some(Amount { value, currency: currency.into() })
}

fn qr_cache_key(qr_content: &str) -> String {
    let hash = hex::encode(Sha256::digest(qr_content.as_bytes()));
    format!("merchant:{}", hash)
}

async fn query_legacy(qr_content: &str, db: &PgPool) -> QrDecodeResponse {
    use rand::Rng;
    use std::time::Duration;
    use tokio::time::sleep;
    use uuid::Uuid;

    // Simulate legacy system delay 400-700ms
    let delay = rand::thread_rng().gen_range(400..=700);
    sleep(Duration::from_millis(delay)).await;

    // 1. Get merchant
    let merchant = sqlx::query!(
        r#"
        SELECT id, name, category, city
        FROM merchants
        WHERE qr_code = $1
        "#,
        qr_content
    )
    .fetch_optional(db)
    .await;

    match merchant {
        Ok(Some(m)) => {
            // 2. Get PANs from merchant_infos (THIS is your fix)
            let pans = sqlx::query!(
                r#"
                SELECT merchant_pan, acquirer_name
                FROM merchant_infos
                WHERE merchant_id = $1
                "#,
                m.id
            )
            .fetch_all(db)
            .await;

            let infos = match pans {
                Ok(rows) => rows
                    .into_iter()
                    .map(|r| MerchantInfo {
                        merchant_pan: r.merchant_pan,
                        acquirer_name: r.acquirer_name,
                    })
                    .collect(),
                Err(_) => vec![],
            };

            QrDecodeResponse::ok(
                Uuid::new_v4().to_string().replace("-", ""),
                None,
                infos,
                None,
                None,
                None,
            )
        }

        _ => QrDecodeResponse::ok(
            Uuid::new_v4().to_string().replace("-", ""),
            None,
            vec![],
            None,
            None,
            None,
        ),
    }
}

#[post("/v1.0/qr/qr-mpm-decode", format = "json", data = "<body>")]
pub async fn qr_decode(
    body: Json<QrDecodeRequest>,
    _headers: SnapHeaders,
    redis: &State<RedisClient>,
    db: &State<PgPool>,
) -> (Status, Json<QrDecodeResponse>) {
    if body.qr_content.is_empty() {
        return (
            Status::BadRequest,
            Json(QrDecodeResponse::err(400, "02", "Invalid Mandatory Field qrContent")),
        );
    }

    if body.scan_time.is_empty() {
        return (
            Status::BadRequest,
            Json(QrDecodeResponse::err(400, "02", "Invalid Mandatory Field scanTime")),
        );
    }

    let cache_key = qr_cache_key(&body.qr_content);
    let lock_key = format!("lock:{}", cache_key);

    let mut conn = match redis.get_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            println!("redis error: {}", e);
            let resp = query_legacy(&body.qr_content, db).await;
            return (Status::Ok, Json(resp));
        }
    };

    // ── Check cache ───────────────────────────────────────────────────────
    if let Ok(cached) = conn.get::<_, String>(&cache_key).await {
        println!("cache hit: {}", cache_key);
        if let Ok(mut resp) = serde_json::from_str::<QrDecodeResponse>(&cached) {
            resp.reference_no = Some(Uuid::new_v4().to_string().replace("-", ""));
            resp.partner_reference_no = body.partner_reference_no.clone();
            let transaction_amount = extract_amount(&body.qr_content)
                .or_else(|| body.amount.clone());
            resp.transaction_amount = transaction_amount;
            return (Status::Ok, Json(resp));
        }
    }

    println!("cache miss: {}", cache_key);

    // ── Acquire distributed lock (cache stampede prevention) ──────────────
    let lock_acquired: bool = redis::cmd("SET")
        .arg(&lock_key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(LOCK_TTL)
        .query_async(&mut conn)
        .await
        .unwrap_or(false);

    if lock_acquired {
        println!("lock acquired: {}", lock_key);

        // ── Query legacy system ───────────────────────────────────────────
        let mut resp = query_legacy(&body.qr_content, db).await;

        let transaction_amount = extract_amount(&body.qr_content)
            .or_else(|| body.amount.clone());
        resp.transaction_amount = transaction_amount;
        resp.partner_reference_no = body.partner_reference_no.clone();
        resp.additional_info = body.additional_info.clone();

        // ── Store in cache ────────────────────────────────────────────────
        if let Ok(serialized) = serde_json::to_string(&resp) {
            match conn.set_ex::<_, _, ()>(&cache_key, serialized, CACHE_TTL).await {
                Ok(_) => println!("cached: {}", cache_key),
                Err(e) => println!("cache set error: {}", e),
            }
        }

        // ── Release lock ──────────────────────────────────────────────────
        let _: Result<(), _> = conn.del(&lock_key).await;
        println!("lock released: {}", lock_key);

        (Status::Ok, Json(resp))
    } else {
        println!("waiting for lock: {}", lock_key);

        // ── Exponential backoff polling ───────────────────────────────────
        for attempt in 0..5 {
            sleep(Duration::from_millis(100 * 2_u64.pow(attempt))).await;

            if let Ok(cached) = conn.get::<_, String>(&cache_key).await {
                if let Ok(mut resp) = serde_json::from_str::<QrDecodeResponse>(&cached) {
                    resp.reference_no = Some(Uuid::new_v4().to_string().replace("-", ""));
                    resp.partner_reference_no = body.partner_reference_no.clone();
                    let transaction_amount = extract_amount(&body.qr_content)
                        .or_else(|| body.amount.clone());
                    resp.transaction_amount = transaction_amount;
                    println!("got cache after waiting (attempt {})", attempt + 1);
                    return (Status::Ok, Json(resp));
                }
            }
        }

        // ── Fallback: query legacy directly ───────────────────────────────
        println!("fallback: querying legacy directly");
        let resp = query_legacy(&body.qr_content, db).await;
        (Status::Ok, Json(resp))
    }
}