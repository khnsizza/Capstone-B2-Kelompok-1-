/// Core inquiry handler — implements Cache Stampede prevention via
/// Distributed Lock + exponential-backoff polling.
///
/// ## Routes
///
/// | Method | Path              | Description                              |
/// |--------|-------------------|------------------------------------------|
/// | GET    | /inquiry/<id>     | Skenario 3.1 — Cache Miss dengan 1.5s delay |
/// | POST   | /inquiry          | Legacy endpoint (JSON body)              |
///
/// ## Flow (GET /inquiry/<id>)
///
/// ```text
///  Request GET /inquiry/QR_MERCHANT_001
///       │
///       ▼
///  [1] Cek Redis Cache
///       │
///       ├─ HIT  ──► Return langsung (< 5ms)
///       │
///       └─ MISS ──► [CACHE MISS] log
///                       │
///                       ▼
///                  [2] Fetch dari PostgreSQL
///                      (injected 1.5s delay)
///                       │
///                       ▼
///                  [3] Simpan ke Redis
///                      [CACHE WRITE] log
///                       │
///                       ▼
///                  Return response (≤ 2s)
/// ```
use chrono::Utc;
use redis::aio::ConnectionManager;
use rocket::serde::json::Json;
use rocket::State;
use sqlx::PgPool;
use tokio::time::{Duration, Instant};

use crate::{
    cache,
    db,
    errors::QrisError,
    lock,
    metrics::METRICS,
    models::{InquiryRequest, InquiryResponse, ResponseSource},
};

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

/// Base wait (ms) sebelum polling attempt pertama oleh lock waiters.
const BACKOFF_BASE_MS: u64 = 100;
/// Maksimum jumlah polling attempts sebelum menyerah.
const BACKOFF_MAX_ATTEMPTS: u8 = 5;

// ──────────────────────────────────────────────────────────────────────────────
// Shared application state (managed by Rocket)
// ──────────────────────────────────────────────────────────────────────────────

pub struct AppState {
    pub db: PgPool,
    pub redis: ConnectionManager,
}

// ──────────────────────────────────────────────────────────────────────────────
// GET /inquiry/<id>  — Skenario 3.1: Cache Miss dengan injected delay
// ──────────────────────────────────────────────────────────────────────────────

/// Endpoint utama Skenario 3.1.
///
/// Hit via Postman: `GET http://localhost:8000/inquiry/QR_MERCHANT_001`
///
/// Alur:
/// 1. Cek Redis → jika HIT, return langsung.
/// 2. Jika MISS → fetch dari PostgreSQL (1.5s delay) → simpan ke Redis → return.
///
/// Success criteria:
/// - Response time ≤ 2 detik (1.5s delay + overhead)
/// - Data konsisten antara PostgreSQL dan Redis
/// - Error rate < 2%
#[get("/inquiry/<qr_id>")]
pub async fn inquiry_by_id(
    qr_id: String,
    state: &State<AppState>,
) -> Result<Json<InquiryResponse>, QrisError> {
    let start = Instant::now();
    let qr_code = qr_id.trim().to_string();
    let cache_key = cache::cache_key(&qr_code);
    let mut redis = state.redis.clone();

    // ── Step 1: Cek Redis Cache ──────────────────────────────────────────────
    if let Some(cached) = cache::get(&mut redis, &cache_key).await? {
        let latency = start.elapsed().as_millis() as u64;
        METRICS.inc_cache_hit();
        METRICS.record_latency(latency);

        tracing::info!(
            qr_code = %qr_code,
            latency_ms = latency,
            "[CACHE HIT] Data tersedia di Redis — return langsung"
        );

        return Ok(Json(InquiryResponse {
            latency_ms: latency,
            source: ResponseSource::Cache,
            ..cached
        }));
    }

    // ── Step 2: Cache Miss → Fetch dari PostgreSQL ───────────────────────────
    // Log [CACHE MISS] sudah dicetak di dalam db::fetch_merchant().
    METRICS.inc_cache_miss();

    let merchant = db::fetch_merchant(&state.db, &qr_code).await?;

    // ── Step 3: Simpan hasil ke Redis ────────────────────────────────────────
    let response = InquiryResponse {
        qr_code: merchant.qr_code.clone(),
        merchant_name: merchant.name,
        merchant_category: merchant.category,
        merchant_city: merchant.city,
        merchant_pan: merchant.merchant_pan,
        acquirer_name: merchant.acquirer_name,
        source: ResponseSource::Database,
        cached_at: Utc::now(),
        latency_ms: 0, // diisi di bawah
    };

    match cache::set(&mut redis, &cache_key, &response).await {
        Ok(_) => {
            println!("[CACHE WRITE] ID: {} - Successfully cached to Redis", qr_code);
            tracing::info!(
                qr_code = %qr_code,
                ttl = cache::CACHE_TTL_SECS,
                "[CACHE WRITE] Data berhasil disimpan ke Redis (TTL: {}s)",
                cache::CACHE_TTL_SECS
            );
        }
        Err(e) => {
            // Cache write failure tidak fatal — user tetap mendapat response.
            tracing::error!(
                error = %e,
                qr_code = %qr_code,
                "Gagal menyimpan ke Redis — request berikutnya akan re-query DB"
            );
        }
    }

    let latency = start.elapsed().as_millis() as u64;
    METRICS.inc_db_queries();
    METRICS.record_latency(latency);

    tracing::info!(
        qr_code = %qr_code,
        latency_ms = latency,
        source = "database",
        "Cache Miss resolved — response dikirim ke client"
    );

    Ok(Json(InquiryResponse {
        latency_ms: latency,
        ..response
    }))
}

// ──────────────────────────────────────────────────────────────────────────────
// POST /inquiry  — Legacy endpoint (JSON body, dengan distributed lock)
// ──────────────────────────────────────────────────────────────────────────────

/// Legacy endpoint dengan distributed lock untuk mencegah cache stampede.
///
/// Hit via Postman: `POST http://localhost:8000/inquiry`
/// Body: `{ "qr_code": "QR_MERCHANT_001" }`
#[post("/inquiry", format = "json", data = "<req>")]
pub async fn inquiry(
    req: Json<InquiryRequest>,
    state: &State<AppState>,
) -> Result<Json<InquiryResponse>, QrisError> {
    let start = Instant::now();
    let qr_code = req.qr_code.trim().to_string();

    let cache_key = cache::cache_key(&qr_code);
    let lock_key = lock::lock_key(&qr_code);
    let mut redis = state.redis.clone();

    // ── Step 1: Cache check (fast path) ─────────────────────────────────────
    if let Some(cached) = cache::get(&mut redis, &cache_key).await? {
        let latency = start.elapsed().as_millis() as u64;
        METRICS.inc_cache_hit();
        METRICS.record_latency(latency);
        tracing::info!(
            qr_code = %qr_code,
            latency_ms = latency,
            "Cache HIT — fast path"
        );
        return Ok(Json(InquiryResponse {
            latency_ms: latency,
            source: ResponseSource::Cache,
            ..cached
        }));
    }

    tracing::info!(qr_code = %qr_code, "Cache MISS — attempting lock acquisition");

    // ── Step 2: Try to acquire the distributed lock ──────────────────────────
    match lock::try_acquire(&mut redis, &lock_key).await? {
        // ── Path A: Lock holder (request pertama) ────────────────────────────
        Some(guard) => {
            tracing::info!(qr_code = %qr_code, "Lock ACQUIRED — executing DB query");

            let db_result = db::fetch_merchant(&state.db, &qr_code).await;

            if let Err(release_err) = lock::release(&mut redis, guard).await {
                tracing::error!(
                    error = %release_err,
                    "Failed to release distributed lock — will expire via TTL"
                );
            }

            let merchant = db_result?;

            let response = InquiryResponse {
                qr_code: merchant.qr_code,
                merchant_name: merchant.name,
                merchant_category: merchant.category,
                merchant_city: merchant.city,
                merchant_pan: merchant.merchant_pan,
                acquirer_name: merchant.acquirer_name,
                source: ResponseSource::Database,
                cached_at: Utc::now(),
                latency_ms: 0,
            };

            if let Err(cache_err) = cache::set(&mut redis, &cache_key, &response).await {
                tracing::error!(
                    error = %cache_err,
                    qr_code = %qr_code,
                    "Failed to write to cache"
                );
            } else {
                println!("[CACHE WRITE] ID: {} - Successfully cached to Redis", qr_code);
                tracing::info!(
                    qr_code = %qr_code,
                    ttl = cache::CACHE_TTL_SECS,
                    "Merchant data cached successfully"
                );
            }

            let latency = start.elapsed().as_millis() as u64;
            METRICS.inc_cache_miss();
            METRICS.record_latency(latency);

            Ok(Json(InquiryResponse {
                latency_ms: latency,
                ..response
            }))
        }

        // ── Path B: Lock waiters (request 2–N) ───────────────────────────────
        None => {
            tracing::info!(
                qr_code = %qr_code,
                "Lock NOT acquired — entering exponential backoff polling"
            );

            let result =
                exponential_backoff_poll(&mut redis, &cache_key, &qr_code, start).await?;

            Ok(Json(result))
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Exponential back-off polling (untuk lock waiters)
// ──────────────────────────────────────────────────────────────────────────────

/// Poll cache dengan exponential back-off sampai data tersedia atau
/// semua attempt habis.
async fn exponential_backoff_poll(
    redis: &mut ConnectionManager,
    cache_key: &str,
    qr_code: &str,
    start: Instant,
) -> Result<InquiryResponse, QrisError> {
    let mut wait_ms = BACKOFF_BASE_MS;

    for attempt in 1..=BACKOFF_MAX_ATTEMPTS {
        tracing::debug!(
            qr_code = qr_code,
            attempt = attempt,
            wait_ms = wait_ms,
            "Polling attempt — sleeping before check"
        );

        tokio::time::sleep(Duration::from_millis(wait_ms)).await;

        match cache::get(redis, cache_key).await? {
            Some(cached) => {
                let latency = start.elapsed().as_millis() as u64;
                METRICS.inc_cache_hit();
                METRICS.record_latency(latency);

                tracing::info!(
                    qr_code = qr_code,
                    attempt = attempt,
                    latency_ms = latency,
                    "Cache populated by lock holder — waiter served (CacheAfterLock)"
                );

                return Ok(InquiryResponse {
                    source: ResponseSource::CacheAfterLock,
                    latency_ms: latency,
                    ..cached
                });
            }
            None => {
                tracing::debug!(
                    qr_code = qr_code,
                    attempt = attempt,
                    "Cache still empty after attempt {attempt}"
                );
            }
        }

        wait_ms *= 2;
    }

    METRICS.inc_lock_contention();

    tracing::error!(
        qr_code = qr_code,
        attempts = BACKOFF_MAX_ATTEMPTS,
        elapsed_ms = start.elapsed().as_millis(),
        "Lock contention: exhausted all polling attempts"
    );

    Err(QrisError::LockContention {
        attempts: BACKOFF_MAX_ATTEMPTS,
    })
}
