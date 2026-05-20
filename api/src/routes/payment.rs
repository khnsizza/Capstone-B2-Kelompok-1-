use std::sync::Arc;

use rdkafka::producer::FutureProducer;
use redis::AsyncCommands;
use redis::Client as RedisClient;
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::serde::json::Json;
use rocket::State;
use crate::kafka;
use crate::models::ApiResponse;
use crate::models::PaymentQueryResponse;
use crate::models::{PaymentRequest, PaymentResponse, SnapHeaders};
use crate::config::Config;

const IDEMPOTENCY_TTL: u64 = 165;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for SnapHeaders {
    type Error = String;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let h = |name: &str| req.headers().get_one(name).map(str::to_owned);

        macro_rules! need {
            ($name:expr, $status:expr) => {
                match h($name) {
                    Some(v) => v,
                    None => return Outcome::Error(($status, format!("Missing {}", $name))),
                }
            };
        }

        Outcome::Success(SnapHeaders {
            authorization:          need!("Authorization",  Status::Unauthorized),
            timestamp:              need!("X-TIMESTAMP",    Status::BadRequest),
            signature:              need!("X-SIGNATURE",    Status::BadRequest),
            partner_id:             need!("X-PARTNER-ID",  Status::BadRequest),
            external_id:            need!("X-EXTERNAL-ID", Status::BadRequest),
            channel_id:             need!("CHANNEL-ID",    Status::BadRequest),
            authorization_customer: h("Authorization-Customer"),
        })
    }
}

#[post("/v1.0/qr/qr-mpm-payment", format = "json", data = "<body>")]
pub async fn qr_payment(
    body: Json<PaymentRequest>,
    headers: SnapHeaders,
    redis: &State<RedisClient>,
    kafka: &State<FutureProducer>,
    _network: &State<Arc<Config>>,
) -> (Status, Json<ApiResponse<PaymentResponse>>) {
    let partner_ref = match &body.partner_reference_no {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return (
            Status::BadRequest,
            Json(ApiResponse::err(400, "02", "Invalid Mandatory Field partnerReferenceNo")),
        ),
    };

    let status_key = format!("payment_status:{}", partner_ref);

    let idempotency_key = format!("payment:{}", partner_ref);

    match redis.get_async_connection().await {
        Ok(mut conn) => {
            // Duplicate check
            if conn.get::<_, String>(&idempotency_key).await.is_ok() {
                println!("duplicate payment, returning in progress: {}", partner_ref);
                return (Status::Accepted, Json(ApiResponse::in_progress()));
            }

            let resp = PaymentResponse::new(
                Some(partner_ref.clone()),
                body.amount.clone(),
                body.fee_amount.clone(),
                body.verification_id.clone(),
                body.additional_info.clone(),
            );

            let req = body.into_inner();

            match kafka::publish_payment(kafka, &serde_json::to_string(&req).unwrap()).await {
                Ok(_) => {
                    let status = PaymentQueryResponse::new(
                        Some(resp.reference_no.clone()), 
                        Some(partner_ref.clone()), 
                        Some(headers.external_id), 
                        "00".to_string(), 
                        "01", "initiated", 
                        None, 
                        req.amount.clone(), 
                        req.fee_amount.clone(), 
                        req.additional_info.clone()
                    );

                    println!("payment published to kafka: {}", partner_ref);
                    if let Ok(serialized) = serde_json::to_string(&req) {
                        let _: Result<(), _> = conn.set_ex(&idempotency_key, serialized, IDEMPOTENCY_TTL).await;
                        println!("payment stored: {}", partner_ref);
                        let _: Result<(), _> = conn.set_ex(&status_key, serde_json::to_string(&status).unwrap_or("".to_string()), IDEMPOTENCY_TTL).await;
                        println!("payment status stored: {}", partner_ref);
                    }
                    (Status::Ok, Json(ApiResponse::success(resp)))
                },
                Err(e) => {
                    println!("kafka error: {}", e);
                    (Status::Ok, Json(ApiResponse::success(resp)))
                },
            }
        }
        Err(_e) => {
            println!("connection error to the databse, returning in progress: {}", partner_ref);
            return (
                Status::Accepted,
                Json(ApiResponse::in_progress()),
            );
        }
    }
}