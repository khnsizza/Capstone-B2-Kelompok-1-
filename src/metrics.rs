/// Atomic runtime metrics — verifies SLA compliance for Skenario 4.1–4.6.
///
/// ### Tracked Metrics
/// | Counter              | Target (10 concurrent reqs)           |
/// |----------------------|---------------------------------------|
/// | `db_queries`         | = 1 (single DB hit per unique QR)     |
/// | `cache_hits`         | ≥ 9 (90% hit ratio)                   |
/// | `cache_misses`       | = 1                                   |
/// | p95 latency          | < 1 600 ms (CIMB Niaga SLA)           |
/// | `lock_timeouts`      | 0 (Skenario 4.5 — controlled timeout) |
///
/// ### p95 Estimation
/// We use a fixed-bucket histogram (boundaries: 50, 100, 200, 500, 800, 1000,
/// 1200, 1400, 1600, 2000, ∞ ms).  The p95 is estimated as the lower bound of
/// the bucket that contains the 95th percentile request.
use std::sync::atomic::{AtomicU64, Ordering};

// ──────────────────────────────────────────────────────────────────────────────
// Histogram bucket boundaries (ms) — used for p95 estimation
// ──────────────────────────────────────────────────────────────────────────────

/// Upper bounds (ms) for each latency bucket.  The last entry is effectively ∞.
pub const HIST_BUCKETS: &[u64] = &[50, 100, 200, 500, 800, 1_000, 1_200, 1_400, 1_600, 2_000, u64::MAX];

/// Number of histogram buckets.
pub const HIST_LEN: usize = 11;

// ──────────────────────────────────────────────────────────────────────────────
// Global metrics singleton
// ──────────────────────────────────────────────────────────────────────────────

pub struct Metrics {
    /// Number of times we queried PostgreSQL (target: 1 per unique QR code).
    pub db_queries: AtomicU64,

    /// Number of requests served directly from Redis cache.
    pub cache_hits: AtomicU64,

    /// Number of requests that required a DB query (lock-holder path).
    pub cache_misses: AtomicU64,

    /// Sum of all response latencies in milliseconds (for average calculation).
    pub total_latency_ms: AtomicU64,

    /// Total number of requests completed (denominator for average latency).
    pub total_requests: AtomicU64,

    /// Requests that exceeded 1 600 ms (SLA breach counter).
    pub sla_breaches: AtomicU64,

    /// Number of times a lock waiter exhausted polling retries.
    pub lock_contentions: AtomicU64,

    /// Number of times the lock holder exceeded 5 s TTL (Skenario 4.5).
    pub lock_timeouts: AtomicU64,

    /// Fixed-bucket histogram counters for p95 estimation.
    /// Index `i` counts requests where latency ∈ (HIST_BUCKETS[i-1], HIST_BUCKETS[i]].
    pub hist: [AtomicU64; HIST_LEN],
}

// SAFETY: AtomicU64 is Send + Sync; the array is initialised with const-compatible values.
unsafe impl Sync for Metrics {}

impl Metrics {
    pub const fn new() -> Self {
        // We cannot use array initialisation with non-Copy types in const context,
        // so we list them explicitly.
        Self {
            db_queries:        AtomicU64::new(0),
            cache_hits:        AtomicU64::new(0),
            cache_misses:      AtomicU64::new(0),
            total_latency_ms:  AtomicU64::new(0),
            total_requests:    AtomicU64::new(0),
            sla_breaches:      AtomicU64::new(0),
            lock_contentions:  AtomicU64::new(0),
            lock_timeouts:     AtomicU64::new(0),
            hist: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
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

    pub fn inc_lock_timeout(&self) {
        self.lock_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed request latency, update the histogram, and check SLA.
    pub fn record_latency(&self, latency_ms: u64) {
        self.total_latency_ms.fetch_add(latency_ms, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if latency_ms > 1_600 {
            self.sla_breaches.fetch_add(1, Ordering::Relaxed);
        }

        // Find the correct histogram bucket.
        for (i, &upper) in HIST_BUCKETS.iter().enumerate() {
            if latency_ms <= upper {
                self.hist[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    // ── Read helpers ─────────────────────────────────────────────────────────

    /// Estimate the p95 latency from the histogram buckets.
    pub fn p95_latency_ms(&self) -> u64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        let p95_idx = (total as f64 * 0.95).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, _upper) in HIST_BUCKETS.iter().enumerate() {
            cumulative += self.hist[i].load(Ordering::Relaxed);
            if cumulative >= p95_idx {
                // Return lower bound of this bucket.
                return if i == 0 { 0 } else { HIST_BUCKETS[i - 1] };
            }
        }
        HIST_BUCKETS[HIST_LEN - 2] // last real upper bound
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let total   = self.total_requests.load(Ordering::Relaxed);
        let hits    = self.cache_hits.load(Ordering::Relaxed);
        let misses  = self.cache_misses.load(Ordering::Relaxed);
        let total_lat = self.total_latency_ms.load(Ordering::Relaxed);

        let avg_latency_ms = if total > 0 { total_lat / total } else { 0 };
        let cache_hit_ratio = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            db_queries:       self.db_queries.load(Ordering::Relaxed),
            cache_hits:       hits,
            cache_misses:     misses,
            cache_hit_ratio,
            avg_latency_ms,
            p95_latency_ms:   self.p95_latency_ms(),
            total_requests:   total,
            sla_breaches:     self.sla_breaches.load(Ordering::Relaxed),
            lock_contentions: self.lock_contentions.load(Ordering::Relaxed),
            lock_timeouts:    self.lock_timeouts.load(Ordering::Relaxed),
        }
    }

    /// Print a Skenario 4.6 monitoring report to stdout.
    pub fn print_scenario_report(&self) {
        let snap = self.snapshot();
        let hit_pct = snap.cache_hit_ratio * 100.0;
        let sla_ok  = snap.p95_latency_ms < 1_600;
        let db_ok   = snap.db_queries <= 1;
        let hit_ok  = snap.cache_hit_ratio >= 0.90;

        println!("\n╔══════════════════════════════════════════════════════════╗");
        println!("║          SKENARIO 4.6 — MONITORING & METRICS REPORT      ║");
        println!("╠══════════════════════════════════════════════════════════╣");
        println!("║  Total Requests      : {:>6}                             ║", snap.total_requests);
        println!("║  DB Queries          : {:>6}  (target: 1) {}              ║",
            snap.db_queries,
            if db_ok { "✅" } else { "❌" }
        );
        println!("║  Cache Hits          : {:>6}                             ║", snap.cache_hits);
        println!("║  Cache Misses        : {:>6}                             ║", snap.cache_misses);
        println!("║  Cache Hit Ratio     : {:>5.1}%  (target ≥90%) {}          ║",
            hit_pct,
            if hit_ok { "✅" } else { "❌" }
        );
        println!("║  Avg Latency         : {:>6} ms                         ║", snap.avg_latency_ms);
        println!("║  p95 Latency         : {:>6} ms  (target <1600ms) {}      ║",
            snap.p95_latency_ms,
            if sla_ok { "✅" } else { "❌" }
        );
        println!("║  SLA Breaches (>1.6s): {:>6}                             ║", snap.sla_breaches);
        println!("║  Lock Contentions    : {:>6}                             ║", snap.lock_contentions);
        println!("║  Lock Timeouts (4.5) : {:>6}                             ║", snap.lock_timeouts);
        println!("╚══════════════════════════════════════════════════════════╝\n");
    }

    /// Reset all counters — useful between test runs.
    pub fn reset(&self) {
        self.db_queries.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.total_latency_ms.store(0, Ordering::Relaxed);
        self.total_requests.store(0, Ordering::Relaxed);
        self.sla_breaches.store(0, Ordering::Relaxed);
        self.lock_contentions.store(0, Ordering::Relaxed);
        self.lock_timeouts.store(0, Ordering::Relaxed);
        for bucket in &self.hist {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

/// A point-in-time snapshot of all metrics, suitable for JSON serialisation.
#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub db_queries:       u64,
    pub cache_hits:       u64,
    pub cache_misses:     u64,
    /// Value between 0.0 and 1.0 (target ≥ 0.90 for 10 concurrent requests).
    pub cache_hit_ratio:  f64,
    /// Arithmetic mean of all recorded latencies in milliseconds.
    pub avg_latency_ms:   u64,
    /// p95 latency estimate from histogram (target < 1 600 ms).
    pub p95_latency_ms:   u64,
    pub total_requests:   u64,
    /// Requests where latency exceeded the 1 600 ms SLA.
    pub sla_breaches:     u64,
    /// Times a lock waiter exhausted all polling retries.
    pub lock_contentions: u64,
    /// Times the lock holder's 5 s TTL expired (Skenario 4.5).
    pub lock_timeouts:    u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Global static — zero-cost on access after the first use
// ──────────────────────────────────────────────────────────────────────────────

pub static METRICS: Metrics = Metrics::new();
