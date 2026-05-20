use redis::AsyncCommands;
use redis::Client as RedisClient;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;
use std::sync::Arc;

use crate::legacy::LegacyClient;
use crate::models::ApiResponse;
use crate::models::{PaymentQueryRequest, PaymentQueryResponse, SnapHeaders};

#[post("/v1.0/qr/qr-mpm-query", format = "json", data = "<body>")]
pub async fn qr_query(
    body: Json<PaymentQueryRequest>,
    _headers: SnapHeaders,
    redis: &State<RedisClient>,
    legacy: &State<Arc<LegacyClient>>,
) -> (Status, Json<ApiResponse<PaymentQueryResponse>>) {
    if body.service_code.is_empty() {
        return (
            Status::BadRequest,
            Json(ApiResponse::err(400, "02", "Invalid Mandatory Field serviceCode")),
        );
    }

    let partner_ref = match &body.original_partner_reference_no {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return (
            Status::BadRequest,
            Json(ApiResponse::err(400, "02", "Invalid Mandatory Field originalPartnerReferenceNo")),
        ),
    };

    let status_key = format!("payment_status:{}", partner_ref);

    match redis.get_async_connection().await {
        Err(e) => {
            println!("redis error: {}", e);
            (
                Status::InternalServerError,
                Json(ApiResponse::err(500, "00", "General Error")),
            )
        }
        Ok(mut conn) => {
            match conn.get::<_, String>(&status_key).await {
                Ok(cached) => {
                    if let Ok(status) = serde_json::from_str::<PaymentQueryResponse>(&cached) {
                        return (
                            Status::Ok,
                            Json(ApiResponse::success(status)),
                        );
                    }

                    // cached but couldn't deserialize — treat as pending
                    (
                        Status::Ok,
                        Json(ApiResponse::success(
                            PaymentQueryResponse::new(
                                body.original_reference_no.clone(),
                                body.original_partner_reference_no.clone(),
                                body.original_external_id.clone(),
                                body.service_code.clone(),
                                "03",
                                "pending",
                                None,
                                None,
                                None,
                                body.additional_info.clone(),
                            )
                        )),
                    )
                }
                Err(_) => {
                    // not in redis — retrieve from db
                    match legacy.query_payment(&body.original_partner_reference_no.clone().unwrap_or_default()).await {
                        Ok(Some(resp)) => (Status::Ok, Json(ApiResponse::success(resp))),
                        Ok(None) => (Status::NotFound, Json(ApiResponse::err(404, "01", "Transaction Not Found"))),
                        Err(_) => (Status::InternalServerError, Json(ApiResponse::err(500, "00", "General Error"))),
                    }
                }
            }
        }
    }
}