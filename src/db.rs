/// PostgreSQL data layer for QRIS merchant inquiry.
///
/// ### Simulated Legacy DB Delay
/// Skenario 3.1 mensyaratkan injected delay sebesar **1.5 detik** untuk
/// mensimulasikan kondisi database yang sedang terbebani berat.
/// Delay ini memastikan response time diuji pada batas SLA (≤ 2 detik).
///
/// ### Query Counter
/// Setiap panggilan ke `fetch_merchant` mengincremen `METRICS.db_queries`.
/// Dengan cache aktif, counter harus bernilai 1 untuk burst request pertama
/// pada QR code yang sama.
use sqlx::PgPool;
use tokio::time::{sleep, Duration};

use crate::{
    errors::QrisError,
    metrics::METRICS,
    models::MerchantRow,
};

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

/// Injected DB latency sesuai spesifikasi Skenario 3.1 (1.5 detik).
const DB_INJECTED_DELAY_MS: u64 = 1_500;

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Fetch merchant data untuk QR code yang diminta dari PostgreSQL.
///
/// Melakukan JOIN antara tabel `merchants` dan `merchant_infos`.
/// Menyuntikkan delay 1.5 detik untuk mensimulasikan beban DB tinggi.
/// Mengincremen global DB query counter setiap kali dipanggil.
pub async fn fetch_merchant(
    pool: &PgPool,
    qr_code: &str,
) -> Result<MerchantRow, QrisError> {
    // ── [CACHE MISS] Log & Simulated DB Latency ─────────────────────────────
    // Log format sesuai spesifikasi agar mudah diverifikasi di terminal.
    println!("[CACHE MISS] ID: {} - Fetching from DB (Injected 1.5s delay)", qr_code);
    tracing::warn!(
        qr_code = qr_code,
        delay_ms = DB_INJECTED_DELAY_MS,
        "[CACHE MISS] Fetching from PostgreSQL — injecting 1.5s simulated delay"
    );

    sleep(Duration::from_millis(DB_INJECTED_DELAY_MS)).await;

    // ── Increment the global DB query counter ───────────────────────────────
    METRICS.inc_db_queries();
    let total_queries = METRICS.db_queries.load(std::sync::atomic::Ordering::Relaxed);
    tracing::info!(
        qr_code = qr_code,
        total_db_queries = total_queries,
        "DB query #{} executed",
        total_queries
    );

    // ── Execute SQL query — JOIN merchants + merchant_infos ─────────────────
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
            // Kembalikan row sintetis "NOT_FOUND" agar response tetap
            // bisa di-cache — mencegah stampede pada QR code yang tidak ada.
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
