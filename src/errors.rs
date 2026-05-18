/// Typed error enum covering all failure modes in the QRIS inquiry system.
///
/// Each variant maps to a specific HTTP error code and log message so that
/// operators can triage issues without grepping raw panics.
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{self, Responder, Response};
use rocket::serde::json::serde_json;
use serde::Serialize;

// ──────────────────────────────────────────────────────────────────────────────
// Error variants
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum QrisError {
    /// Caller could not acquire the distributed lock AND exhausted all polling
    /// retries without finding data in cache.
    LockContention { attempts: u8 },

    /// The PgPool could not hand out a connection within the configured timeout.
    /// Triggered when all 50 pool slots are busy.
    PoolExhausted(String),

    /// A Redis command failed (network partition, OOM, etc.).
    RedisError(redis::RedisError),

    /// A SQLx / PostgreSQL error.
    DatabaseError(sqlx::Error),

    /// The JSON stored in Redis could not be deserialized back into the response
    /// struct — usually means a schema migration happened mid-flight.
    CacheDeserializationError(String),

    /// Generic internal error for anything that does not fit the above.
    Internal(String),
}

// ──────────────────────────────────────────────────────────────────────────────
// Display / From impls
// ──────────────────────────────────────────────────────────────────────────────

impl std::fmt::Display for QrisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LockContention { attempts } => {
                write!(f, "Lock contention: exhausted {attempts} polling attempts")
            }
            Self::PoolExhausted(msg) => write!(f, "Connection pool exhausted: {msg}"),
            Self::RedisError(e) => write!(f, "Redis error: {e}"),
            Self::DatabaseError(e) => write!(f, "Database error: {e}"),
            Self::CacheDeserializationError(msg) => {
                write!(f, "Cache deserialization error: {msg}")
            }
            Self::Internal(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl From<redis::RedisError> for QrisError {
    fn from(e: redis::RedisError) -> Self {
        Self::RedisError(e)
    }
}

impl From<sqlx::Error> for QrisError {
    fn from(e: sqlx::Error) -> Self {
        // Map pool-timeout specifically so callers can handle it distinctly.
        if matches!(e, sqlx::Error::PoolTimedOut) {
            Self::PoolExhausted(e.to_string())
        } else {
            Self::DatabaseError(e)
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// JSON error body sent to the client
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    code: u16,
}

// ──────────────────────────────────────────────────────────────────────────────
// Rocket Responder — converts QrisError → HTTP response automatically
// ──────────────────────────────────────────────────────────────────────────────

impl<'r> Responder<'r, 'static> for QrisError {
    fn respond_to(self, _req: &'r Request<'_>) -> response::Result<'static> {
        let (status, message) = match &self {
            Self::LockContention { .. } => (Status::ServiceUnavailable, self.to_string()),
            Self::PoolExhausted(_) => (Status::ServiceUnavailable, self.to_string()),
            Self::RedisError(_) => (Status::InternalServerError, self.to_string()),
            Self::DatabaseError(_) => (Status::InternalServerError, self.to_string()),
            Self::CacheDeserializationError(_) => (Status::InternalServerError, self.to_string()),
            Self::Internal(_) => (Status::InternalServerError, self.to_string()),
        };

        let body = ErrorBody {
            error: message,
            code: status.code,
        };

        let json = serde_json::to_string(&body)
            .unwrap_or_else(|_| r#"{"error":"serialization failed","code":500}"#.to_string());

        Response::build()
            .status(status)
            .header(rocket::http::ContentType::JSON)
            .sized_body(json.len(), std::io::Cursor::new(json))
            .ok()
    }
}
