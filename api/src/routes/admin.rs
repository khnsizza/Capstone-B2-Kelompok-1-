use rocket::serde::json::{json, Json, Value};
use rocket::State;
use std::sync::Arc;
use crate::config::{Config, ConfigUpdate};

#[get("/admin/config")]
pub fn get_config(config: &State<Arc<Config>>) -> Value {
    json!({
        "latencyMinMs": config.latency_min(),
        "latencyMaxMs": config.latency_max(),
        "jitterMs": config.jitter(),
        "errorRate": config.error_rate() * 100.0,
    })
}

#[post("/admin/config", format = "json", data = "<body>")]
pub fn update_config(body: Json<ConfigUpdate>, config: &State<Arc<Config>>) -> Value {
    if let Some(min) = body.latency_min_ms {
        let max = body.latency_max_ms.unwrap_or(config.latency_max());
        config.set_latency(min, max);
    } else if let Some(max) = body.latency_max_ms {
        config.set_latency(config.latency_min(), max);
    }
    if let Some(jitter) = body.jitter_ms {
        config.set_jitter(jitter);
    }
    if let Some(rate) = body.error_rate {
        config.set_error_rate(rate);
    }
    json!({
        "latencyMinMs": config.latency_min(),
        "latencyMaxMs": config.latency_max(),
        "jitterMs": config.jitter(),
        "errorRate": config.error_rate() * 100.0,
    })
}