use rocket::serde::json::{json, Value};

#[get("/health")]
pub fn health() -> Value {
    json!({ "status": "ok" })
}
