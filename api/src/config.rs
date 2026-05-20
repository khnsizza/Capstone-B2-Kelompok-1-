use rand::Rng;
use rand_distr::Distribution;
use serde::{Deserialize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
};

/// =======================
/// Runtime Simulation Model
/// =======================
#[derive(Debug)]
pub struct Config {
    // latency (ms)
    legacy_latency_min_ms: AtomicU64,
    legacy_latency_max_ms: AtomicU64,
    jitter_ms: AtomicU64,

    // error rates in basis points (0 - 100_000)
    error_rate: AtomicU64,
}

impl Config {
    /// Create shared config
    pub fn new() -> Self {
        Self {
            legacy_latency_min_ms: AtomicU64::new(50),
            legacy_latency_max_ms: AtomicU64::new(100),
            jitter_ms: AtomicU64::new(10),

            error_rate: AtomicU64::new(7_000),    // 7%
        }
    }

    pub fn _from_payload(update_payload: &ConfigUpdate) -> Self {
        let config = Self::new();

        if let Some(min) = update_payload.latency_min_ms {
            config.legacy_latency_min_ms.store(min, Ordering::Relaxed);
        }
        if let Some(max) = update_payload.latency_max_ms {
            config.legacy_latency_max_ms.store(max, Ordering::Relaxed);
        }
        if let Some(jitter) = update_payload.jitter_ms {
            config.jitter_ms.store(jitter, Ordering::Relaxed);
        }
        if let Some(rate) = update_payload.error_rate {
            config.set_error_rate(rate);
        }

        config
    }

    /// =======================
    /// Latency API
    /// =======================
    pub fn latency_min(&self) -> u64 {
        self.legacy_latency_min_ms.load(Ordering::Relaxed)
    }

    pub fn latency_max(&self) -> u64 {
        self.legacy_latency_max_ms.load(Ordering::Relaxed)
    }

    pub fn jitter(&self) -> u64 {
        self.jitter_ms.load(Ordering::Relaxed)
    }

    pub fn set_latency(&self, min: u64, max: u64) {
        self.legacy_latency_min_ms.store(min, Ordering::Relaxed);
        self.legacy_latency_max_ms.store(max, Ordering::Relaxed);
    }

    pub fn set_jitter(&self, jitter: u64) {
        self.jitter_ms.store(jitter, Ordering::Relaxed);
    }

    /// =======================
    /// Error model API
    /// =======================
    pub fn set_error_rate(&self, rate_percent: u64) {
        let clamped = rate_percent.clamp(0, 100_000);
        self.error_rate.store(clamped, Ordering::Relaxed);
    }

    pub fn error_rate_in_basis_points(&self) -> u64 {
        self.error_rate.load(Ordering::Relaxed)
    }

    pub fn error_rate_in_percent(&self) -> f64 {
        self.error_rate_in_basis_points() as f64 / 100_000.0 * 100.0
    }

    /// =======================
    /// Runtime behavior
    /// =======================
    pub fn should_fail(&self) -> bool {
        let rate = self.error_rate.load(Ordering::Relaxed);


        let roll = rand::thread_rng().gen_range(0..100_000);
        roll < rate
    }

    /// =======================
    /// Effective latency with jitter
    /// =======================
    pub fn effective_latency_ms(&self) -> u64 {
        let base = rand::thread_rng()
            .gen_range(self.latency_min()..=self.latency_max());

        let jitter = self.jitter();

        if jitter == 0 {
            return base;
        }

        let normal = rand_distr::Normal::new(base as f64, jitter as f64)
            .unwrap_or_else(|_| rand_distr::Normal::new(base as f64, 1.0).unwrap());

        let sample = normal.sample(&mut rand::thread_rng());
        sample.max(0.0) as u64
    }
}

/// =======================
/// Optional DTO (API layer)
/// =======================
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigUpdate {
    pub latency_min_ms: Option<u64>,
    pub latency_max_ms: Option<u64>,
    pub jitter_ms: Option<u64>,
    pub error_rate: Option<u64>,
}