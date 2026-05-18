/// Domain models: request / response DTOs and the raw PostgreSQL row shape.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ──────────────────────────────────────────────────────────────────────────────
// HTTP request body
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct InquiryRequest {
    /// The QR code identifier sent by the client (e.g. "QR_MERCHANT_001").
    pub qr_code: String,
}

// ──────────────────────────────────────────────────────────────────────────────
// HTTP response body (also what we store in Redis as JSON)
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InquiryResponse {
    pub qr_code: String,
    pub merchant_name: String,
    pub merchant_category: String,
    pub merchant_city: String,
    pub merchant_pan: String,
    pub acquirer_name: String,
    /// Whether this result was served from cache or fresh from the DB.
    pub source: ResponseSource,
    /// Wall-clock time at which the response was generated / cached.
    pub cached_at: DateTime<Utc>,
    /// Round-trip latency in milliseconds (populated by the handler).
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResponseSource {
    /// Data was returned directly from Redis.
    Cache,
    /// Data was fetched fresh from PostgreSQL (lock holder path).
    Database,
    /// Data was fetched via polling after another goroutine populated the cache.
    CacheAfterLock,
}

// ──────────────────────────────────────────────────────────────────────────────
// Raw PostgreSQL row — result of JOIN between `merchants` and `merchant_infos`
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, sqlx::FromRow)]
pub struct MerchantRow {
    pub qr_code: String,
    pub name: String,
    pub category: String,
    pub city: String,
    pub merchant_pan: String,
    pub acquirer_name: String,
}
