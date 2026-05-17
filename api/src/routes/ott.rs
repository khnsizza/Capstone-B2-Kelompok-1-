use rocket::http::Status;
use rocket::serde::json::Json;
use uuid::Uuid;

use crate::models::{ApplyOttRequest, ApplyOttResponse, SnapHeaders, UserResource};

#[post("/v1.0/qr/apply-ott", format = "json", data = "<body>")]
pub async fn apply_ott(
    body: Json<ApplyOttRequest>,
    _headers: SnapHeaders,
) -> (Status, Json<ApplyOttResponse>) {
    if body.user_resources.is_empty() {
        return (
            Status::BadRequest,
            Json(ApplyOttResponse::err(400, "02", "Invalid Mandatory Field userResources")),
        );
    }

    let resources = body.user_resources.iter().map(|r| UserResource {
        resource_type: r.clone(),
        value: Uuid::new_v4().to_string().replace("-", ""),
    }).collect();

    (
        Status::Ok,
        Json(ApplyOttResponse::ok(resources, body.additional_info.clone())),
    )
}