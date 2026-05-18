/// QRIS Inquiry Service — entry point.
///
/// Initialises:
/// 1. Structured tracing (JSON-compatible, env-filter driven)
/// 2. PostgreSQL connection pool (min 10, max 50, 3-second acquire timeout)
/// 3. Redis ConnectionManager (multiplexed async connection)
/// 4. Rocket with all routes mounted
#[macro_use]
extern crate rocket;

mod cache;
mod db;
mod errors;
mod handler;
mod lock;
mod metrics;
mod models;

use handler::AppState;
use metrics::{MetricsSnapshot, METRICS};
use rocket::serde::json::Json;
use sqlx::postgres::PgPoolOptions;
use tokio::time::Duration;

// ──────────────────────────────────────────────────────────────────────────────
// Additional routes
// ──────────────────────────────────────────────────────────────────────────────

/// Health-check endpoint — useful for load-balancer probes.
#[get("/health")]
fn health() -> &'static str {
    "OK"
}

/// Live metrics snapshot — shows DB query count, cache hit ratio, SLA breaches.
/// Invoke this after a load test to verify success criteria.
#[get("/metrics")]
fn get_metrics() -> Json<MetricsSnapshot> {
    Json(METRICS.snapshot())
}

/// Reset all counters — useful between test runs.
#[post("/metrics/reset")]
fn reset_metrics() -> &'static str {
    use std::sync::atomic::Ordering;
    METRICS.db_queries.store(0, Ordering::Relaxed);
    METRICS.cache_hits.store(0, Ordering::Relaxed);
    METRICS.cache_misses.store(0, Ordering::Relaxed);
    METRICS.total_latency_ms.store(0, Ordering::Relaxed);
    METRICS.total_requests.store(0, Ordering::Relaxed);
    METRICS.sla_breaches.store(0, Ordering::Relaxed);
    METRICS.lock_contentions.store(0, Ordering::Relaxed);
    "Metrics reset"
}

// ──────────────────────────────────────────────────────────────────────────────
// Rocket launch
// ──────────────────────────────────────────────────────────────────────────────

#[launch]
async fn rocket() -> _ {
    // ── Structured tracing ───────────────────────────────────────────────────
    // Respects RUST_LOG env var (e.g. RUST_LOG=capstone2=debug,rocket=warn).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .with_thread_ids(true)
        .init();

    // ── Environment variables with sensible defaults ─────────────────────────
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost/qris_db".to_string()
    });
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());

    tracing::info!(database_url = %database_url, "Connecting to PostgreSQL...");
    tracing::info!(redis_url = %redis_url, "Connecting to Redis...");

    // ── PostgreSQL connection pool ───────────────────────────────────────────
    // Min: 10 keeps warm connections ready for burst traffic.
    // Max: 50 caps resource usage and triggers PoolExhausted at high concurrency.
    // acquire_timeout: 3 s gives callers a chance to retry before surfacing an error.
    let db_pool = PgPoolOptions::new()
        .min_connections(10)
        .max_connections(50)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&database_url)
        .await
        .expect("Failed to create PostgreSQL connection pool");

    tracing::info!("PostgreSQL pool ready (min=10, max=50)");

    // ── Redis connection manager ─────────────────────────────────────────────
    // ConnectionManager provides a single multiplexed async connection with
    // automatic reconnection — no per-request connection overhead.
    let redis_client =
        redis::Client::open(redis_url).expect("Invalid Redis URL");
    let redis_conn = redis_client
        .get_connection_manager()
        .await
        .expect("Failed to connect to Redis");

    tracing::info!("Redis connection manager ready");

    // ── Rocket ───────────────────────────────────────────────────────────────
    rocket::build()
        .manage(AppState {
            db: db_pool,
            redis: redis_conn,
        })
        .mount(
            "/",
            routes![
                handler::inquiry,         // POST /inquiry (legacy, dengan distributed lock)
                handler::inquiry_by_id,   // GET  /inquiry/<id> (Skenario 3.1 — Cache Miss)
                health,
                get_metrics,
                reset_metrics,
            ],
        )
}
