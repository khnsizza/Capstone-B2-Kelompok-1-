use chrono::Utc;
use redis::AsyncCommands;
use redis::Client as RedisClient;
use rocket::http::Status;
use rocket::response::status;
use rocket::serde::json::Json;
use rocket::State;
use sqlx::PgPool;

use crate::models::Amount;
use crate::models::ApiResponse;
use crate::models::{PaymentQueryRequest, PaymentQueryResponse, SnapHeaders};

async fn query_legacy(db: &PgPool, partner_reference: &str) -> Option<PaymentQueryResponse> {
    let row = sqlx::query!(
        r#"
        SELECT amount_value, amount_currency, fee_value, fee_currency, status_code, status_desc, transaction_date
        FROM payment
        WHERE partner_reference_no = $1
        "#,
        partner_reference
    )
    .fetch_one(db)
    .await
    .ok()?;

    Some(PaymentQueryResponse::new(
        Some("12".to_string()), 
        Some(partner_reference.to_string()), 
        Some("fdfd".to_string()), 
        "fdfd".to_string(), 
        &row.status_code, 
        &row.status_desc, 
        Some(row.transaction_date.to_rfc3339()), 
        Some(Amount {
            value: format!("{}.00", row.amount_value.unwrap_or_default().to_string()),
            currency: row.amount_currency.unwrap_or_default(),
        }), 
        Some(Amount {
            value: format!("{}.00", row.fee_value.unwrap_or_default().to_string()),
            currency: row.fee_currency.unwrap_or_default(),
        }),
        None
    ))
}

#[post("/v1.0/qr/qr-mpm-query", format = "json", data = "<body>")]
pub async fn qr_query(
    body: Json<PaymentQueryRequest>,
    _headers: SnapHeaders,
    redis: &State<RedisClient>,
    db: &State<PgPool>,
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
                    match query_legacy(db, &body.original_partner_reference_no.unwrap_or_default()).await {
                        Some(resp) => (Status::ok, Json(ApiResponse::success(resp))),
                        None => (Status::NotFound, Json(ApiResponse::err(404, "01", "Transaction Not Found")))
                    }
                }
            }
        }
    }
}