#[macro_use]
extern crate rocket;

mod models;
mod routes;

use redis::Client as RedisClient;
use rocket::{Build, Rocket};
use rocket::serde::json::Json;
use rocket::Request;
use sqlx::postgres::PgPoolOptions;
use crate::models::QrPaymentResponse;

#[catch(400)]
fn bad_request(_req: &Request) -> Json<QrPaymentResponse> {
    Json(QrPaymentResponse::err(400, "00", "Bad Request"))
}

#[catch(401)]
fn unauthorized(_req: &Request) -> Json<QrPaymentResponse> {
    Json(QrPaymentResponse::err(401, "00", "Unauthorized"))
}

#[catch(422)]
fn unprocessable(_req: &Request) -> Json<QrPaymentResponse> {
    Json(QrPaymentResponse::err(400, "00", "Bad Request"))
}

#[catch(404)]
fn not_found(_req: &Request) -> Json<QrPaymentResponse> {
    Json(QrPaymentResponse::err(404, "00", "Invalid Transaction Status"))
}

#[catch(500)]
fn internal_error(_req: &Request) -> Json<QrPaymentResponse> {
    Json(QrPaymentResponse::err(500, "00", "General Error"))
}

#[launch]
async fn rocket() -> Rocket<Build> {
    let redis = RedisClient::open("redis://127.0.0.1/")
        .expect("Failed to connect to Redis");

    let db = PgPoolOptions::new()
        .max_connections(50)
        .connect("postgres://postgres:rahasia@localhost/api")
        .await
        .expect("Failed to connect to PostgreSQL");

    rocket::build()
        .manage(redis)
        .manage(db)
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
        ])
}