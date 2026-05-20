use std::sync::Arc;

use redis::AsyncCommands;
use redis::Client as RedisClient;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::time::{sleep, Duration};

use crate::models::ApiResponse;
use crate::models::Merchant;
use crate::models::{Amount, QrDecodeRequest, QrDecodeResponse, SnapHeaders};
use crate::config::Config;
use crate::db;

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
    merchant_key(qr_content)
}

fn merchant_key(qr_content: &str) -> String {
    //let fields = parse_tlv(qr_content);
    
    // rebuild string without amount (54) and crc (63)
    let mut stripped = String::new();
    let mut i = 0;
    while i + 4 <= qr_content.len() {
        let tag = &qr_content[i..i+2];
        let len: usize = match qr_content[i+2..i+4].parse() {
            Ok(n) => n,
            Err(_) => break,
        };
        if i + 4 + len > qr_content.len() { break; }
        
        // skip amount and crc
        if tag != "54" && tag != "63" {
            stripped.push_str(&qr_content[i..i+4+len]);
        }
        i += 4 + len;
    }
    
    let hash = hex::encode(Sha256::digest(stripped.as_bytes()));
    format!("merchant:{}", hash)
}

#[post("/v1.0/qr/qr-mpm-decode", format = "json", data = "<body>")]
pub async fn qr_decode(
    body: Json<QrDecodeRequest>,
    _headers: SnapHeaders,
    redis: &State<RedisClient>,
    db: &State<PgPool>,
    config: &State<Arc<Config>>
) -> (Status, Json<ApiResponse<QrDecodeResponse>>) {
    if body.qr_content.is_empty() {
        return (
            Status::BadRequest,
            Json(ApiResponse::err(400, "02", "Invalid Mandatory Field qrContent")),
        );
    }

    if body.scan_time.is_empty() {
        return (
            Status::BadRequest,
            Json(ApiResponse::err(400, "02", "Invalid Mandatory Field scanTime")),
        );
    }

    let config = config.inner();

    let transaction_amount = extract_amount(&body.qr_content);

    let cache_key = qr_cache_key(&body.qr_content);
    
    let lock_key = format!("lock:{}", cache_key);

    let db_key = merchant_key(&body.qr_content)[9..].to_string();

    let mut conn = match redis.get_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            println!("redis error: {}", e);
            //let resp = query_legacy(&body.qr_content, db).await;
            if let Some(merchant) = db::fetch_merchant(&db_key, db, config.clone()).await {
                return (
                    Status::Ok,
                    Json(
                        ApiResponse::success(
                            QrDecodeResponse::new(
                                body.partner_reference_no.clone(), 
                                merchant, 
                                transaction_amount, 
                                None, 
                                None
                            )
                        )
                    )
                );
            }
            return (Status::NotFound, Json(ApiResponse::err(404, "08", "Invalid Merchant")))
        }
    };

    // ── Check cache ───────────────────────────────────────────────────────
    if let Ok(cached) = conn.get::<_, String>(&cache_key).await {
        println!("cache hit: {}", cache_key);
        if let Ok(merchant) = serde_json::from_str::<Merchant>(&cached) {
            return (
                Status::Ok,
                Json(
                    ApiResponse::success(
                        QrDecodeResponse::new(
                            body.partner_reference_no.clone(),
                            merchant,
                            transaction_amount, 
                            None, 
                            None
                        )
                    )
                )
            );
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
        let merchant = match db::fetch_merchant(&db_key, db, config.clone()).await {
            Some(merchant) => {
                if let Ok(serialized) = serde_json::to_string(&merchant) {
                    match conn
                        .set_ex::<_, _, ()>(&cache_key, serialized, CACHE_TTL)
                        .await
                    {
                        Ok(_) => println!("cached: {}", cache_key),
                        Err(e) => println!("cache set error: {}", e),
                    }
                }

                merchant
            }

            None => {
                return (
                    Status::NotFound,
                    Json(ApiResponse::err(
                        404,
                        "08",
                        "Invalid Merchant",
                    )),
                )
            }
        };

        // ── Release lock ──────────────────────────────────────────────────
        let _: Result<(), _> = conn.del(&lock_key).await;
        println!("lock released: {}", lock_key);

        (
            Status::Ok, 
            Json(
                ApiResponse::success(
                    QrDecodeResponse::new(
                            body.partner_reference_no.clone(), 
                            merchant, 
                            transaction_amount, 
                            None, 
                            None
                        )
                    )
            )
        )
    } else {
        println!("waiting for lock: {}", lock_key);

        // ── Exponential backoff polling ───────────────────────────────────
        for attempt in 0..5 {
            sleep(Duration::from_millis(100 * 2_u64.pow(attempt))).await;

            if let Ok(cached) = conn.get::<_, String>(&cache_key).await {
                if let Ok(merchant) = serde_json::from_str::<Merchant>(&cached) {
                    println!("got cache after waiting (attempt {})", attempt + 1);
                    return (
                        Status::Ok,
                        Json(
                            ApiResponse::success(
                                QrDecodeResponse::new(
                                    body.partner_reference_no.clone(),
                                    merchant,
                                    transaction_amount, 
                                    None, 
                                    None
                                )
                            )
                        )
                    );
                }
            }
        }

        // ── Fallback: query legacy directly ───────────────────────────────
        println!("fallback: querying legacy directly");
        if let Some(merchant) = db::fetch_merchant(&db_key, db, config.clone()).await {
            return (
                Status::Ok,
                Json(
                    ApiResponse::success(
                        QrDecodeResponse::new(
                            body.partner_reference_no.clone(), 
                            merchant, 
                            transaction_amount, 
                            None, 
                            None
                        )
                    )
                )
            );
        }
        (Status::NotFound, Json(ApiResponse::err(404, "08", "Invalid Merchants")))
    }
}