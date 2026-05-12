use serde::{Deserialize, Serialize};

// ─── Shared ───────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Amount {
    pub value: String,
    pub currency: String,
}

// ─── QR MPM Payment ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QrPaymentRequest {
    pub partner_reference_no: Option<String>,
    pub merchant_id: Option<String>,
    pub sub_merchant_id: Option<String>,
    pub amount: Option<Amount>,
    pub fee_amount: Option<Amount>,
    pub otp: Option<String>,
    pub verification_id: Option<String>,
    pub additional_info: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QrPaymentResponse {
    pub response_code: String,
    pub response_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_no: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partner_reference_no: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_info: Option<serde_json::Value>,
}

impl QrPaymentResponse {
    pub fn ok(
        reference_no: String,
        partner_reference_no: String,
        transaction_date: String,
        amount: Option<Amount>,
        fee_amount: Option<Amount>,
        verification_id: Option<String>,
        additional_info: Option<serde_json::Value>,
    ) -> Self {
        Self {
            response_code: "2005000".into(),
            response_message: "Request has been processed successfully".into(),
            reference_no: Some(reference_no),
            partner_reference_no: Some(partner_reference_no),
            transaction_date: Some(transaction_date),
            amount,
            fee_amount,
            verification_id,
            additional_info,
        }
    }

    pub fn err(http: u16, case: &str, message: &str) -> Self {
        Self {
            response_code: format!("{}50{}", http, case),
            response_message: message.into(),
            reference_no: None,
            partner_reference_no: None,
            transaction_date: None,
            amount: None,
            fee_amount: None,
            verification_id: None,
            additional_info: None,
        }
    }
}

// ─── QR MPM Decode ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MerchantInfo {
    pub merchant_pan: String,
    pub acquirer_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")] // add Deserialize
pub struct QrDecodeResponse {
    pub response_code: String,
    pub response_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_no: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partner_reference_no: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redirect_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merchant_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merchant_category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merchant_location: Option<String>,
    pub merchant_infos: Vec<MerchantInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_info: Option<serde_json::Value>,
}

impl QrDecodeResponse {
    pub fn ok(
        reference_no: String,
        partner_reference_no: Option<String>,
        merchant_infos: Vec<MerchantInfo>,
        transaction_amount: Option<Amount>,
        fee_amount: Option<Amount>,
        additional_info: Option<serde_json::Value>,
    ) -> Self {
        Self {
            response_code: "2004800".into(),
            response_message: "Request has been processed successfully".into(),
            reference_no: Some(reference_no),
            partner_reference_no,
            redirect_url: None,
            merchant_name: Some("Baso Malang".into()),
            merchant_category: Some("Food & Beverage".into()),
            merchant_location: Some("Jakarta".into()),
            merchant_infos,
            transaction_amount,
            fee_amount,
            additional_info,
        }
    }

    pub fn err(http: u16, case: &str, message: &str) -> Self {
        Self {
            response_code: format!("{}48{}", http, case),
            response_message: message.into(),
            reference_no: None,
            partner_reference_no: None,
            redirect_url: None,
            merchant_name: None,
            merchant_category: None,
            merchant_location: None,
            merchant_infos: vec![],
            transaction_amount: None,
            fee_amount: None,
            additional_info: None,
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

