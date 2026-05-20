#[macro_use]
extern crate rocket;

mod models;
mod routes;
mod kafka;
mod consumer;
mod config;
mod legacy;

use redis::Client as RedisClient;
use rocket::{Build, Rocket};
use rocket::serde::json::Json;
use rocket::Request;
use sqlx::postgres::PgPoolOptions;
use crate::config::Config;
use crate::models::{ApiResponse};
use crate::legacy::LegacyClient;

#[catch(400)]
fn bad_request(_req: &Request) -> Json<ApiResponse<()>> {
    Json(ApiResponse::err(400, "00", "Bad Request"))
}

#[catch(401)]
fn unauthorized(_req: &Request) -> Json<ApiResponse<()>> {
    Json(ApiResponse::err(401, "00", "Unauthorized"))
}

#[catch(422)]
fn unprocessable(_req: &Request) -> Json<ApiResponse<()>> {
    Json(ApiResponse::err(400, "00", "Bad Request"))
}

#[catch(404)]
fn not_found(_req: &Request) -> Json<ApiResponse<()>> {
    Json(ApiResponse::err(404, "00", "Invalid Transaction Status"))
}

#[catch(500)]
fn internal_error(_req: &Request) -> Json<ApiResponse<()>> {
    Json(ApiResponse::err(500, "00", "General Error"))
}

#[launch]
async fn rocket() -> Rocket<Build> {
    let redis = RedisClient::open("redis://127.0.0.1/")
        .expect("Failed to connect to Redis");

    let kafka = crate::kafka::create_producer("localhost:9092");

    let db = PgPoolOptions::new()
        .max_connections(300)
        .connect("postgres://postgres:rahasia@localhost/api")
        .await
        .expect("Failed to connect to PostgreSQL");

    let legacy = LegacyClient::new(Config::new(), db);

    let redis_consumer = redis.clone();
    let legacy_consumer = legacy.clone();

    tokio::spawn(async move {
        consumer::run_consumer(redis_consumer, legacy_consumer).await;
    });

    rocket::build()
        .manage(redis)
        .manage(legacy)
        .manage(kafka)
        .register("/", catchers![
            bad_request,
            unauthorized,
            unprocessable,
            not_found,
            internal_error,
        ])
        .mount("/", routes![
            routes::health::health,
            routes::decode::qr_decode,
            routes::payment::qr_payment,
            routes::ott::apply_ott,
            routes::admin::get_config,
            routes::admin::update_config,
            routes::query::qr_query,
        ])
}