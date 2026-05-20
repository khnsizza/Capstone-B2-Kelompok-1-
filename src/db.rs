/// PostgreSQL data layer for QRIS merchant inquiry.
///
/// ### Network Condition Simulation
/// Two modes are supported via `DbLatencyMode`:
///
/// * `Normal`      — 50–100 ms baseline delay (fast-path check)
/// * `HighLatency` — 300–800 ms base delay with ±150 ms jitter
///                   (Skenario 4.1–4.4 concurrent lock holder path)
///
/// ### Query Counter
/// Every call to `fetch_merchant_with_delay` increments `METRICS.db_queries`.
/// Under full cache coverage, the counter must stay at **1** for all 10
/// concurrent requests to the same QR code.
use rand::Rng;
use sqlx::PgPool;
use tokio::time::{sleep, Duration};

use crate::{
    errors::QrisError,
    metrics::METRICS,
    models::MerchantRow,
};

// ──────────────────────────────────────────────────────────────────────────────
// Network condition enum
// ──────────────────────────────────────────────────────────────────────────────

/// Controls the simulated network / DB latency for a single query call.
#[derive(Debug, Clone, Copy)]
pub enum DbLatencyMode {
    /// Normal baseline: 50–100 ms.
    #[allow(dead_code)]
    Normal,
    /// High-latency simulation: 300–800 ms base + ±150 ms jitter.
    /// Used by the lock holder in Skenario 4.1–4.4.
    HighLatency,
}

impl DbLatencyMode {
    /// Compute a concrete delay duration for this mode.
    pub fn sample_delay(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let ms = match self {
            Self::Normal => rng.gen_range(50..=100u64),
            Self::HighLatency => {
                let base: u64 = rng.gen_range(300..=800);
                let jitter: i64 = rng.gen_range(-150..=150);
                // Clamp to a minimum of 50 ms so we never go negative.
                (base as i64 + jitter).max(50) as u64
            }
        };
        Duration::from_millis(ms)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Fetch merchant data with a configurable simulated DB delay.
///
/// This is the **primary** entry point for all scenarios.  Pass
/// `DbLatencyMode::HighLatency` for the Skenario 4.1 lock-holder path and
/// `DbLatencyMode::Normal` for health-check / verification calls.
pub async fn fetch_merchant_with_delay(
    pool: &PgPool,
    qr_code: &str,
    mode: DbLatencyMode,
) -> Result<MerchantRow, QrisError> {
    let delay = mode.sample_delay();

    tracing::warn!(
        qr_code = qr_code,
        delay_ms = delay.as_millis(),
        mode = ?mode,
        "[CACHE MISS] Fetching from PostgreSQL — injecting simulated delay ({:?}ms)",
        delay.as_millis(),
    );
    println!(
        "[CACHE MISS] ID: {} - Fetching from DB (mode={:?}, delay={}ms)",
        qr_code,
        mode,
        delay.as_millis()
    );

    sleep(delay).await;

    // Increment the global DB query counter.
    METRICS.inc_db_queries();
    let total_queries = METRICS.db_queries.load(std::sync::atomic::Ordering::Relaxed);
    tracing::info!(
        qr_code = qr_code,
        total_db_queries = total_queries,
        "DB query #{} executed",
        total_queries
    );

    // Execute SQL query — JOIN merchants + merchant_infos.
    let row = sqlx::query_as::<_, MerchantRow>(
        r#"
        SELECT
            m.qr_code,
            m.name,
            m.category,
            m.city,
            mi.merchant_pan,
            mi.acquirer_name
        FROM merchants m
        JOIN merchant_infos mi ON mi.merchant_id = m.id
        WHERE m.qr_code = $1
        LIMIT 1
        "#,
    )
    .bind(qr_code)
    .fetch_optional(pool)
    .await
    .map_err(QrisError::from)?;

    match row {
        Some(r) => Ok(r),
        None => {
            // Return a synthetic "NOT_FOUND" row so the result can still be
            // cached — prevents stampede on absent QR codes.
            tracing::warn!(qr_code = qr_code, "QR code not found in database");
            Ok(MerchantRow {
                qr_code: qr_code.to_string(),
                name: "UNKNOWN".to_string(),
                category: "N/A".to_string(),
                city: "N/A".to_string(),
                merchant_pan: "N/A".to_string(),
                acquirer_name: "N/A".to_string(),
            })
        }
    }
}

/// Backwards-compatible wrapper — uses 1.5 s fixed delay (Skenario 3.1 legacy).
pub async fn fetch_merchant(pool: &PgPool, qr_code: &str) -> Result<MerchantRow, QrisError> {
    // Preserve the original 1.5 s injected delay for the GET /inquiry/<id> endpoint.
    tracing::warn!(
        qr_code = qr_code,
        delay_ms = 1500,
        "[CACHE MISS] Fetching from PostgreSQL — injecting 1.5s simulated delay (legacy)"
    );
    println!("[CACHE MISS] ID: {} - Fetching from DB (Injected 1.5s delay)", qr_code);

    sleep(Duration::from_millis(1_500)).await;

    METRICS.inc_db_queries();
    let total_queries = METRICS.db_queries.load(std::sync::atomic::Ordering::Relaxed);
    tracing::info!(
        qr_code = qr_code,
        total_db_queries = total_queries,
        "DB query #{} executed",
        total_queries
    );

    let row = sqlx::query_as::<_, MerchantRow>(
        r#"
        SELECT
            m.qr_code,
            m.name,
            m.category,
            m.city,
            mi.merchant_pan,
            mi.acquirer_name
        FROM merchants m
        JOIN merchant_infos mi ON mi.merchant_id = m.id
        WHERE m.qr_code = $1
        LIMIT 1
        "#,
    )
    .bind(qr_code)
    .fetch_optional(pool)
    .await
    .map_err(QrisError::from)?;

    match row {
        Some(r) => Ok(r),
        None => {
            tracing::warn!(qr_code = qr_code, "QR code not found in database");
            Ok(MerchantRow {
                qr_code: qr_code.to_string(),
                name: "UNKNOWN".to_string(),
                category: "N/A".to_string(),
                city: "N/A".to_string(),
                merchant_pan: "N/A".to_string(),
                acquirer_name: "N/A".to_string(),
            })
        }
    }
}
