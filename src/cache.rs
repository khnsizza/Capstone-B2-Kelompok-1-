/// Redis cache layer — get / set / invalidate helpers for QRIS inquiry data.
///
/// TTL for cached merchant data: 600 seconds.
use redis::{aio::ConnectionManager, AsyncCommands};
use serde_json;

use crate::{errors::QrisError, models::InquiryResponse};

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

/// How long merchant data stays cached in Redis (10 minutes).
pub const CACHE_TTL_SECS: u64 = 600;

/// Namespace prefix — avoids key collisions with other services on the same
/// Redis instance.
pub const CACHE_PREFIX: &str = "qris:cache:";

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Returns the namespaced cache key for a given QR code.
pub fn cache_key(qr_code: &str) -> String {
    format!("{}{}", CACHE_PREFIX, qr_code)
}

/// Attempt to fetch a cached `InquiryResponse` from Redis.
///
/// Returns:
/// * `Ok(Some(response))` — cache hit.
/// * `Ok(None)` — cache miss (key does not exist or has expired).
/// * `Err(QrisError)` — Redis connectivity or deserialization failure.
pub async fn get(
    conn: &mut ConnectionManager,
    key: &str,
) -> Result<Option<InquiryResponse>, QrisError> {
    let raw: Option<String> = conn.get(key).await.map_err(QrisError::RedisError)?;

    match raw {
        None => Ok(None),
        Some(json) => {
            let response = serde_json::from_str::<InquiryResponse>(&json)
                .map_err(|e| QrisError::CacheDeserializationError(e.to_string()))?;
            Ok(Some(response))
        }
    }
}

/// Store an `InquiryResponse` in Redis with the configured TTL.
///
/// Serialization errors are returned as `QrisError::Internal` rather than
/// being silently swallowed so that callers can decide whether to propagate
/// or degrade gracefully.
pub async fn set(
    conn: &mut ConnectionManager,
    key: &str,
    response: &InquiryResponse,
) -> Result<(), QrisError> {
    let json = serde_json::to_string(response)
        .map_err(|e| QrisError::Internal(format!("Cache serialization failed: {e}")))?;

    let _: () = conn
        .set_ex(key, json, CACHE_TTL_SECS)
        .await
        .map_err(QrisError::RedisError)?;

    tracing::debug!(
        cache_key = key,
        ttl = CACHE_TTL_SECS,
        "Merchant data written to cache"
    );

    Ok(())
}

/// Delete a cache entry — useful during testing or manual invalidation.
#[allow(dead_code)]
pub async fn invalidate(conn: &mut ConnectionManager, key: &str) -> Result<(), QrisError> {
    let _: () = conn.del(key).await.map_err(QrisError::RedisError)?;
    tracing::info!(cache_key = key, "Cache entry invalidated");
    Ok(())
}
