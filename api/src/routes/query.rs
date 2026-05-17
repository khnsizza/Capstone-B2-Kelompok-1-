use redis::AsyncCommands;
use redis::Client as RedisClient;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;

use crate::models::{QrQueryRequest, QrQueryResponse, SnapHeaders};

#[post("/v1.0/qr/qr-mpm-query", format = "json", data = "<body>")]
pub async fn qr_query(
    body: Json<QrQueryRequest>,
    _headers: SnapHeaders,
    redis: &State<RedisClient>,
) -> (Status, Json<QrQueryResponse>) {
    if body.service_code.is_empty() {
        return (
            Status::BadRequest,
            Json(QrQueryResponse::err(400, "02", "Invalid Mandatory Field serviceCode")),
        );
    }

    let partner_ref = match &body.original_partner_reference_no {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return (
            Status::BadRequest,
            Json(QrQueryResponse::err(400, "02", "Invalid Mandatory Field originalPartnerReferenceNo")),
        ),
    };

    let status_key = format!("payment_status:{}", partner_ref);

    match redis.get_async_connection().await {
        Err(e) => {
            println!("redis error: {}", e);
            (
                Status::InternalServerError,
                Json(QrQueryResponse::err(500, "00", "General Error")),
            )
        }
        Ok(mut conn) => {
            match conn.get::<_, String>(&status_key).await {
                Ok(cached) => {
                    if let Ok(status) = serde_json::from_str::<serde_json::Value>(&cached) {
                        let code = status["status"].as_str().unwrap_or("03");
                        let desc = status["desc"].as_str().unwrap_or("pending");
                        let paid_time = status["paidTime"].as_str().map(String::from);

                        return (
                            Status::Ok,
                            Json(QrQueryResponse::ok(
                                body.original_reference_no.clone(),
                                body.original_partner_reference_no.clone(),
                                body.original_external_id.clone(),
                                body.service_code.clone(),
                                code,
                                desc,
                                paid_time,
                                None,
                                None,
                                body.additional_info.clone(),
                            )),
                        );
                    }

                    // cached but couldn't deserialize — treat as pending
                    (
                        Status::Ok,
                        Json(QrQueryResponse::ok(
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
                        )),
                    )
                }
                Err(_) => {
                    // not in redis yet — still processing
                    (
                        Status::Ok,
                        Json(QrQueryResponse::ok(
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
                        )),
                    )
                }
            }
        }
    }
}