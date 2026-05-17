use rocket::serde::json::{json, Value};
use rocket::State;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct NetworkConfig {
    pub bad_network: AtomicBool,
}

impl NetworkConfig {
    pub fn new() -> Self {
        Self { bad_network: AtomicBool::new(false) }
    }

    pub fn is_bad(&self) -> bool {
        self.bad_network.load(Ordering::Relaxed)
    }
}

#[post("/admin/network/bad")]
pub fn set_bad_network(config: &State<NetworkConfig>) -> Value {
    config.bad_network.store(true, Ordering::Relaxed);
    json!({ "network": "bad" })
}

#[post("/admin/network/good")]
pub fn set_good_network(config: &State<NetworkConfig>) -> Value {
    config.bad_network.store(false, Ordering::Relaxed);
    json!({ "network": "good" })
}

#[get("/admin/network")]
pub fn get_network_status(config: &State<NetworkConfig>) -> Value {
    let status = if config.bad_network.load(Ordering::Relaxed) { "bad" } else { "good" };
    json!({ "network": status })
}