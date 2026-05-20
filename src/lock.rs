/// Distributed Lock implementation using Redis SET NX (Not eXists).
///
/// ### Why a unique token per lock?
/// A plain `SET lock NX EX 5` has a race condition on release: if the lock
/// holder is slow and the TTL expires, another waiter acquires the lock just
/// before the original holder calls DEL — accidentally releasing the *new*
/// owner's lock.
///
/// We prevent this by storing a per-acquisition UUID as the lock value and
/// using a Lua script for atomic compare-and-delete on release.
use redis::aio::ConnectionManager;
use uuid::Uuid;

use crate::errors::QrisError;

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

/// TTL given to the Redis lock key (seconds).
pub const LOCK_TTL_SECS: u64 = 5;

/// Lua script for atomic compare-and-delete.
/// Returns 1 if the key was deleted, 0 if the value did not match (already
/// expired or taken by another holder).
const RELEASE_LOCK_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"#;

// ──────────────────────────────────────────────────────────────────────────────
// Lock handle — returned to the caller so they can release safely
// ──────────────────────────────────────────────────────────────────────────────

/// Opaque handle representing an acquired distributed lock.
/// Drop it (or call `release`) to free the Redis key.
pub struct LockGuard {
    pub key: String,
    pub token: String,
}

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Try to acquire the lock for `key` using Redis SET NX EX.
///
/// Returns `Ok(Some(guard))` if the lock was acquired, `Ok(None)` if another
/// caller already holds it.
pub async fn try_acquire(
    conn: &mut ConnectionManager,
    key: &str,
) -> Result<Option<LockGuard>, QrisError> {
    let token = Uuid::new_v4().to_string();

    // SET key token NX EX 5  → returns "OK" on success, nil on failure
    let result: Option<String> = redis::cmd("SET")
        .arg(key)
        .arg(&token)
        .arg("NX")
        .arg("EX")
        .arg(LOCK_TTL_SECS)
        .query_async(conn)
        .await
        .map_err(QrisError::RedisError)?;

    if result.is_some() {
        tracing::debug!(lock_key = key, token = %token, "Distributed lock acquired");
        Ok(Some(LockGuard {
            key: key.to_string(),
            token,
        }))
    } else {
        tracing::debug!(lock_key = key, "Lock already held by another process");
        Ok(None)
    }
}

/// Release a previously acquired lock.
///
/// Uses a Lua script so the check-and-delete is atomic.  If the lock has
/// already expired (TTL elapsed) this is a no-op.
pub async fn release(
    conn: &mut ConnectionManager,
    guard: LockGuard,
) -> Result<(), QrisError> {
    let result: i64 = redis::Script::new(RELEASE_LOCK_SCRIPT)
        .key(&guard.key)
        .arg(&guard.token)
        .invoke_async(conn)
        .await
        .map_err(QrisError::RedisError)?;

    if result == 1 {
        tracing::debug!(lock_key = %guard.key, "Distributed lock released");
    } else {
        // TTL expired before we could release — another waiter may have taken
        // the lock. This is normal under extreme load.
        tracing::warn!(
            lock_key = %guard.key,
            "Lock release was a no-op: lock had already expired or was stolen"
        );
    }

    Ok(())
}

/// Key helper — ensures consistent key naming across the codebase.
pub fn lock_key(qr_code: &str) -> String {
    format!("qris:lock:{}", qr_code)
}
