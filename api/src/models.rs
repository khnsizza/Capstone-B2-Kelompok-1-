use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── Shared ───────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiResponse<T: ApiResponseContent> {
    pub response_code: String,
    pub response_message: String,
    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<T>
}

impl<T: ApiResponseContent> ApiResponse<T> {
    pub fn err(http_code: i32, case_code: &str, response_message: &str) -> Self {
        Self {
            response_code: format!("{}00{}", http_code, case_code),
            response_message: String::from(response_message),
            content: None
        }
    }
    pub fn in_progress() -> Self {
        Self {
            response_code: String::from("2020000"),
            response_message: String::from("Request In Progress"),
            content: None
        }
    }

    pub fn success(content: T) -> Self {
        Self {
            response_code: String::from("2000000"),
            response_message: String::from("Successful"),
            content: Some(content)
        }
    }
}

pub trait ApiResponseContent: Serialize + Clone {
    fn gen_reference_no() -> String {
        Uuid::new_v4().to_string().replace("-", "")
    }
}
impl ApiResponseContent for () {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Amount {
    pub value: String,
    pub currency: String,
}

// ─── QR MPM Decode ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MerchantInfo {
    pub merchant_pan: String,
    pub acquirer_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Merchant {
    #[serde(skip)]
    pub id: String,
    pub merchant_name: String,
    pub merchant_category: String, 
    pub merchant_location: String,
    pub merchant_infos: Vec<MerchantInfo>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")] // add Deserialize
pub struct QrDecodeResponse {
    pub reference_no: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partner_reference_no: Option<String>,
    pub redirect_url: String,
    #[serde(flatten)]
    pub merchant: Merchant,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_info: Option<serde_json::Value>,
}

impl QrDecodeResponse {
    pub fn new(
        partner_reference_no: Option<String>, 
        merchant: Merchant, 
        transaction_amount: Option<Amount>, 
        fee_amount: Option<Amount>,
        additional_info: Option<serde_json::Value>
    ) -> Self {
        Self {
            partner_reference_no: partner_reference_no,
            reference_no: Self::gen_reference_no(),
            redirect_url: String::new(),
            transaction_amount,
            fee_amount,
            merchant,
            additional_info
        }
    }
}

impl ApiResponseContent for QrDecodeResponse {}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QrDecodeRequest {
    pub partner_reference_no: Option<String>,
    pub qr_content: String,
    pub amount: Option<Amount>,
    pub merchant_id: Option<String>,
    pub sub_merchant_id: Option<String>,
    pub scan_time: String,
    pub additional_info: Option<serde_json::Value>,
}

// ─── QR MPM Payment ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentRequest {
    pub partner_reference_no: Option<String>,
    pub merchant_id: Option<String>,
    pub sub_merchant_id: Option<String>,
    pub amount: Option<Amount>,
    pub fee_amount: Option<Amount>,
    pub otp: Option<String>,
    pub verification_id: Option<String>,
    pub additional_info: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaymentResponse {
    pub reference_no: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partner_reference_no: Option<String>,
    pub transaction_date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_info: Option<serde_json::Value>,
}

impl ApiResponseContent for PaymentResponse {}
impl PaymentResponse {
    pub fn new(
        partner_reference_no: Option<String>, 
        amount: Option<Amount>, 
        fee_amount: Option<Amount>, 
        verification_id: Option<String>, 
        additional_info: Option<serde_json::Value>) -> Self {
        Self {
            reference_no: Self::gen_reference_no(),
            partner_reference_no,
            transaction_date: Utc::now().format("%Y-%m-%dT%H:%M:%S+07:00").to_string(),
            amount,
            fee_amount,
            verification_id,
            additional_info
        }
    }
}

// ─── Header guard ─────────────────────────────────────────────────────────────

pub struct SnapHeaders {
    pub authorization: String,
    pub authorization_customer: Option<String>,
    pub timestamp: String,
    pub signature: String,
    pub partner_id: String,
    pub external_id: String,
    pub channel_id: String,
}

// ─── Apply OTT ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyOttRequest {
    pub user_resources: Vec<String>,
    pub additional_info: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserResource {
    pub resource_type: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyOttResponse {
    pub response_code: String,
    pub response_message: String,
    pub user_resources: Vec<UserResource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_info: Option<serde_json::Value>,
}

impl ApplyOttResponse {
    pub fn ok(user_resources: Vec<UserResource>, additional_info: Option<serde_json::Value>) -> Self {
        Self {
            response_code: "2004900".into(),
            response_message: "Request has been processed successfully".into(),
            user_resources,
            additional_info,
        }
    }

    pub fn err(http: u16, case: &str, message: &str) -> Self {
        Self {
            response_code: format!("{}49{}", http, case),
            response_message: message.into(),
            user_resources: vec![],
            additional_info: None,
        }
    }
}

// ─── QR MPM Query ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentQueryRequest {
    pub original_reference_no: Option<String>,
    pub original_partner_reference_no: Option<String>,
    pub original_external_id: Option<String>,
    pub service_code: String,
    pub merchant_id: Option<String>,
    pub sub_merchant_id: Option<String>,
    pub external_store_id: Option<String>,
    pub additional_info: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaymentQueryResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_reference_no: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_partner_reference_no: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_external_id: Option<String>,
    pub service_code: String,
    pub latest_transaction_status: String,
    pub transaction_status_desc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paid_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_info: Option<serde_json::Value>,
}

impl ApiResponseContent for PaymentQueryResponse {}

impl PaymentQueryResponse {
    pub fn new(
        original_reference_no: Option<String>,
        original_partner_reference_no: Option<String>,
        original_external_id: Option<String>,
        service_code: String,
        latest_transaction_status: &str,
        transaction_status_desc: &str,
        paid_time: Option<String>,
        amount: Option<Amount>,
        fee_amount: Option<Amount>,
        additional_info: Option<serde_json::Value>,
    ) -> Self {
        Self {
            original_reference_no,
            original_partner_reference_no,
            original_external_id,
            service_code,
            latest_transaction_status: latest_transaction_status.into(),
            transaction_status_desc: transaction_status_desc.into(),
            paid_time,
            amount,
            fee_amount,
            terminal_id: None,
            additional_info,
        }
    }
}
