use chrono::Utc;
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::serde::json::Json;
use uuid::Uuid;

use crate::models::{QrPaymentRequest, QrPaymentResponse, SnapHeaders};

// ─── Header guard ─────────────────────────────────────────────────────────────

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

// ─── Route ────────────────────────────────────────────────────────────────────

#[post("/v1.0/qr/qr-mpm-payment", format = "json", data = "<body>")]
pub async fn qr_payment(
    body: Json<QrPaymentRequest>,
    _headers: SnapHeaders,
) -> (Status, Json<QrPaymentResponse>) {
    let partner_reference_no = match &body.partner_reference_no {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return (
            Status::BadRequest,
            Json(QrPaymentResponse::err(400, "02", "Invalid Mandatory Field partnerReferenceNo")),
        ),
    };

    let reference_no = Uuid::new_v4().to_string().replace("-", "");
    let transaction_date = Utc::now().format("%Y-%m-%dT%H:%M:%S+07:00").to_string();

    (
        Status::Ok,
        Json(QrPaymentResponse::ok(
            reference_no,
            partner_reference_no,
            transaction_date,
            body.amount.clone(),
            body.fee_amount.clone(),
            body.verification_id.clone(),
            body.additional_info.clone(),
        )),
    )
}
