use std::{sync::Arc, time::Duration};

use chrono::Utc;
use sqlx::{Pool, Postgres};
use tokio::time::sleep;

use crate::{config::Config, models::{Amount, Merchant, MerchantInfo, PaymentQueryResponse, PaymentRequest}};

const TIMEOUT_S: u64 = 55;

pub struct LegacyClient {
    pub config: Config,
    db: Pool<Postgres>,
}

impl LegacyClient {
    pub fn new(config: Config, db: Pool<Postgres>) -> Arc<Self> {
        Arc::new(Self { config, db })
    }

    pub async fn create_payment(&self, payment: &PaymentRequest) -> Result<(), String> {
        if self.config.should_fail() {
            println!("Legacy create_payment failed for partner_reference_no: {}", payment.partner_reference_no.as_ref().unwrap_or(&String::from("")));
            sleep(Duration::from_secs(TIMEOUT_S)).await; // Simulate timeout
            return Err("Legacy system timeout".into());
        }

        sleep(Duration::from_millis(self.config.effective_latency_ms())).await;

        Ok(())
    }

    pub async fn store_payment(
        &self,
        payment: &PaymentRequest, 
        status_code: &str, 
        status_desc: &str, 
    ) -> Result<(), anyhow::Error> {
        let amount_value: Option<i64> = payment.amount
            .as_ref()
            .map(|a| a.value.trim_end_matches(".00").parse())
            .transpose()?;
        let fee_value: Option<i64> = payment.fee_amount
            .as_ref()
            .map(|a| a.value.trim_end_matches(".00").parse())
            .transpose()?;

        sqlx::query!(
            r#"
            INSERT INTO payment (
                partner_reference_no,
                merchant_id,
                sub_merchant_id,
                amount_value,
                amount_currency,
                fee_value,
                fee_currency,
                status_code,
                status_desc,
                transaction_date
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            payment.partner_reference_no.clone(),
            payment.merchant_id.clone(),
            payment.sub_merchant_id.clone(),
            amount_value,
            payment.amount.as_ref().map(|a| a.currency.clone()),
            fee_value,
            payment.fee_amount.as_ref().map(|a| a.currency.clone()),
            status_code.to_string(),
            status_desc.to_string(),
            Utc::now()
        )
        .execute(&self.db)
        .await?;

        Ok(())
    }   
    pub async fn fetch_merchant(&self, key: &str) -> Result<Option<Merchant>, String> {
        if self.config.should_fail() {
            println!("Legacy fetch_merchant failed for key: {}", key);
            sleep(Duration::from_secs(TIMEOUT_S)).await; // Simulate timeout
            return Err("Legacy system timeout".into());
        }

        sleep(Duration::from_millis(self.config.effective_latency_ms())).await;

        // 1. Get merchant
        let row = sqlx::query!(
            r#"
            SELECT id, name, category, city, merchant_id
            FROM merchants
            WHERE qr_code = $1
            "#,
            key
        )
        .fetch_optional(&self.db)
        .await;

        println!("fetched!");
        println!("qr_content: {}", key);
        println!("fetched content: {:?}", row);

        match row {
            Ok(Some(m)) => {
                println!("matched!");
                // 2. Get PANs from merchant_infos (THIS is your fix)
                let pans: Result<Vec<_>, sqlx::Error> = sqlx::query!(
                    r#"
                    SELECT merchant_pan, acquirer_name
                    FROM merchant_infos
                    WHERE merchant_id = $1
                    "#,
                    m.id
                )
                .fetch_all(&self.db)
                .await;

                println!("fetched!!");

                let infos = match pans {
                    Ok(rows) => rows
                        .into_iter()
                        .map(|r| MerchantInfo {
                            merchant_pan: r.merchant_pan,
                            acquirer_name: r.acquirer_name,
                        })
                        .collect(),
                    Err(_) => vec![],
                };

                println!("length: {}", infos.len());

                Ok(Some(Merchant {
                    id: m.merchant_id,
                    merchant_name: m.name,
                    merchant_category: m.category,
                    merchant_location: m.city,
                    merchant_infos: infos
                }))
            }

            _ => {
                println!("Not matched!");
                Ok(None)
            },
        }
    }

    pub async fn query_payment(&self, partner_reference: &str) -> Result<Option<PaymentQueryResponse>, String> {
        if self.config.should_fail() {
            println!("Legacy query_payment failed for partner_reference: {}", partner_reference);
            sleep(Duration::from_secs(TIMEOUT_S)).await; // Simulate timeout
            return Err("Legacy system timeout".into());
        }
        sleep(Duration::from_millis(self.config.effective_latency_ms())).await;

        let row = sqlx::query!(
            r#"
            SELECT amount_value, amount_currency, fee_value, fee_currency, status_code, status_desc, transaction_date
            FROM payment
            WHERE partner_reference_no = $1
            "#,
            partner_reference
        )
        .fetch_one(&self.db)
        .await.map_err(|e| e.to_string())?;

        Ok(Some(PaymentQueryResponse::new(
            Some("12".to_string()), 
            Some(partner_reference.to_string()), 
            Some("fdfd".to_string()), 
            "00".to_string(), 
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
        )))
    }

}

