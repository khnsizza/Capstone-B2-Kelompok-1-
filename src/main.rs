/// QRIS Inquiry Service — entry point.
///
/// ## Skenario Coverage
/// | Skenario | Endpoint                               | Description                                      |
/// |----------|----------------------------------------|--------------------------------------------------|
/// | 3.1      | GET  /inquiry/<id>                     | Cache Miss — legacy 1.5 s injected delay         |
/// | 4.1–4.4  | POST /inquiry/concurrent               | 10 concurrent requests, distributed lock, adaptive poll |
/// | 4.5      | POST /inquiry/concurrent/timeout       | Lock timeout simulation (>5 s TTL)               |
/// | 4.6      | GET  /metrics                          | Live monitoring: hit ratio, DB queries, p95      |
/// | —        | GET  /health                           | Load-balancer probe                              |
/// | —        | POST /metrics/reset                    | Reset counters between test runs                 |
///
/// ## Quick Postman Guide
/// 1. `POST /metrics/reset` — clear counters
/// 2. Send 10 parallel `POST /inquiry/concurrent` requests with `{"qr_code":"QR_MERCHANT_001"}`
/// 3. `GET /metrics` — verify:  db_queries=1, cache_hit_ratio≥0.9, p95_latency_ms<1600
/// 4. `POST /inquiry/concurrent/timeout` — verify HTTP 503 with scenario:"4.5"
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
// Utility routes
// ──────────────────────────────────────────────────────────────────────────────

/// Health-check endpoint — useful for load-balancer probes.
#[get("/health")]
fn health() -> &'static str {
    "OK"
}

/// Live metrics snapshot — Skenario 4.6 monitoring.
///
/// Shows DB query count, cache hit ratio, p95 latency, SLA breaches.
/// Invoke this after a load test to verify all success criteria.
#[get("/metrics")]
fn get_metrics() -> Json<MetricsSnapshot> {
    let snap = METRICS.snapshot();

    // Print a formatted report to the terminal as well.
    METRICS.print_scenario_report();

    Json(snap)
}

/// Reset all counters — use between test runs in Postman.
#[post("/metrics/reset")]
fn reset_metrics() -> &'static str {
    METRICS.reset();
    tracing::info!("All metrics reset");
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
    // Min 10 keeps warm connections ready for burst traffic.
    // Max 50 caps resource usage.
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
    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let redis_conn = redis_client
        .get_connection_manager()
        .await
        .expect("Failed to connect to Redis");

    tracing::info!("Redis connection manager ready");

    println!("\n══════════════════════════════════════════════════════════════");
    println!("  QRIS Inquiry Service — Skenario 4.1–4.6 Ready");
    println!("  Endpoints:");
    println!("    GET  /health");
    println!("    GET  /inquiry/<id>              (Skenario 3.1)");
    println!("    POST /inquiry                   (legacy)");
    println!("    POST /inquiry/concurrent        (Skenario 4.1–4.4)");
    println!("    POST /inquiry/concurrent/timeout (Skenario 4.5)");
    println!("    GET  /metrics                   (Skenario 4.6)");
    println!("    POST /metrics/reset");
    println!("══════════════════════════════════════════════════════════════\n");

    // ── Rocket ───────────────────────────────────────────────────────────────
    rocket::build()
        .manage(AppState {
            db:    db_pool,
            redis: redis_conn,
        })
        .mount(
            "/",
            routes![
                handler::inquiry,                    // POST /inquiry (legacy)
                handler::inquiry_by_id,              // GET  /inquiry/<id> (Skenario 3.1)
                handler::inquiry_concurrent,         // POST /inquiry/concurrent (4.1–4.4)
                handler::inquiry_concurrent_timeout, // POST /inquiry/concurrent/timeout (4.5)
                health,
                get_metrics,
                reset_metrics,
            ],
        )
}
