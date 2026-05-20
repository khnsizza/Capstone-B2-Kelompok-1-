use rocket::serde::json::{json, Json, Value};
use rocket::State;
use std::sync::Arc;
use crate::config::ConfigUpdate;
use crate::legacy::LegacyClient;

#[get("/admin/config")]
pub fn get_config(legacy: &State<Arc<LegacyClient>>) -> Value {
    let config = &legacy.inner().clone().config;
    json!({
        "latencyMinMs": config.latency_min(),
        "latencyMaxMs": config.latency_max(),
        "jitterMs": config.jitter(),
        "errorRate": config.error_rate_in_percent(),
    })
}

#[post("/admin/config", format = "json", data = "<body>")]
pub fn update_config(body: Json<ConfigUpdate>, legacy: &State<Arc<LegacyClient>>) -> Value {
    let body = body.into_inner();
    let config = &legacy.inner().clone().config;

    // latency
    if body.latency_min_ms.is_some() || body.latency_max_ms.is_some() {
        let min = body.latency_min_ms.unwrap_or_else(|| config.latency_min());
        let max = body.latency_max_ms.unwrap_or_else(|| config.latency_max());

        config.set_latency(min, max);
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
        "errorRate": config.error_rate_in_percent(),
    })
}