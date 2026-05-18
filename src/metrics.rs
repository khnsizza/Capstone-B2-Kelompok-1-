/// Atomic runtime metrics used to verify SLA compliance:
///
/// * Total DB queries executed  → must stay at 1 per unique QR code
/// * Cache hits                 → used to compute hit ratio
/// * Cache misses               → total lookups − hits
/// * Response time histogram    → used to estimate p95 latency
///
/// All counters use `std::sync::atomic` so they are lock-free and safe to
/// read from any async task.
use std::sync::atomic::{AtomicU64, Ordering};

// ──────────────────────────────────────────────────────────────────────────────
// Global metrics singleton
// ──────────────────────────────────────────────────────────────────────────────

pub struct Metrics {
    /// Number of times we queried PostgreSQL (target: 1 per unique QR code).
    pub db_queries: AtomicU64,

    /// Number of requests served directly from Redis cache.
    pub cache_hits: AtomicU64,

    /// Number of requests that required a DB query or polling wait.
    pub cache_misses: AtomicU64,

    /// Sum of all response latencies in milliseconds (for average calculation).
    pub total_latency_ms: AtomicU64,

    /// Total number of requests completed (denominator for average latency).
    pub total_requests: AtomicU64,

    /// Requests that exceeded 1 600 ms (SLA breach counter).
    pub sla_breaches: AtomicU64,

    /// Number of times a client exhausted exponential-backoff retries.
    pub lock_contentions: AtomicU64,
}

impl Metrics {
    pub const fn new() -> Self {
        Self {
            db_queries: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            sla_breaches: AtomicU64::new(0),
            lock_contentions: AtomicU64::new(0),
        }
    }

    // ── Increment helpers ────────────────────────────────────────────────────

    pub fn inc_db_queries(&self) {
        self.db_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_lock_contention(&self) {
        self.lock_contentions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed request latency and update SLA breach counter.
    pub fn record_latency(&self, latency_ms: u64) {
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if latency_ms > 1_600 {
            self.sla_breaches.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ── Read helpers (for the /metrics endpoint) ─────────────────────────────

    pub fn snapshot(&self) -> MetricsSnapshot {
        let total = self.total_requests.load(Ordering::Relaxed);
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total_lat = self.total_latency_ms.load(Ordering::Relaxed);

        let avg_latency_ms = if total > 0 { total_lat / total } else { 0 };
        let cache_hit_ratio = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            db_queries: self.db_queries.load(Ordering::Relaxed),
            cache_hits: hits,
            cache_misses: misses,
            cache_hit_ratio,
            avg_latency_ms,
            total_requests: total,
            sla_breaches: self.sla_breaches.load(Ordering::Relaxed),
            lock_contentions: self.lock_contentions.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time snapshot of all metrics, suitable for JSON serialisation.
#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub db_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Value between 0.0 and 1.0 (target ≥ 0.90).
    pub cache_hit_ratio: f64,
    /// Arithmetic mean of all recorded latencies in milliseconds.
    pub avg_latency_ms: u64,
    pub total_requests: u64,
    /// Requests where latency exceeded the 1 600 ms SLA.
    pub sla_breaches: u64,
    pub lock_contentions: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Global static — zero-cost on access after the first use
// ──────────────────────────────────────────────────────────────────────────────

pub static METRICS: Metrics = Metrics::new();
