use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub legacy_latency_min_ms: AtomicU64,
    pub legacy_latency_max_ms: AtomicU64,
    pub jitter_ms: AtomicU64,
    pub error_rate: AtomicU64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigUpdate {
    pub latency_min_ms: Option<u64>,
    pub latency_max_ms: Option<u64>,
    pub jitter_ms: Option<u64>,
    pub error_rate: Option<u64>,
}

impl Config {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            legacy_latency_min_ms: AtomicU64::new(50),
            legacy_latency_max_ms: AtomicU64::new(100),
            jitter_ms: AtomicU64::new(10),
            error_rate: AtomicU64::new(7),  
        })
    }

    pub fn latency_min(&self) -> u64 {
        self.legacy_latency_min_ms.load(Ordering::Relaxed)
    }

    pub fn latency_max(&self) -> u64 {
        self.legacy_latency_max_ms.load(Ordering::Relaxed)
    }

    pub fn set_latency(&self, min: u64, max: u64) {
        self.legacy_latency_min_ms.store(min, Ordering::Relaxed);
        self.legacy_latency_max_ms.store(max, Ordering::Relaxed);
    }

    pub fn error_rate(&self) -> f64 {
        self.error_rate.load(Ordering::Relaxed) as f64 / 100.0
    }

    pub fn set_error_rate(&self, rate: u64) {
        self.error_rate.store(rate, Ordering::Relaxed);
    }
    pub fn jitter(&self) -> u64 {
        self.jitter_ms.load(Ordering::Relaxed)
    }

    pub fn set_jitter(&self, jitter: u64) {
        self.jitter_ms.store(jitter, Ordering::Relaxed);
    }

    pub fn effective_latency(&self) -> u64 {
        use rand::distributions::Distribution;
        let base = rand::thread_rng().gen_range(self.latency_min()..=self.latency_max());
        if self.jitter() == 0 {
            return base;
        }
        let normal = rand_distr::Normal::new(base as f64, self.jitter() as f64).unwrap();
        let sample = normal.sample(&mut rand::thread_rng());
        sample.max(0.0) as u64
    }
}