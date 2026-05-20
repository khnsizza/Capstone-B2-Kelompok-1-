use std::{sync::Arc, time::Duration};

use chrono::Utc;
use rand::Rng;
use sqlx::{PgPool, Pool, Postgres};
use tokio::time::sleep;

use crate::{config::Config, models::{Merchant, MerchantInfo, PaymentRequest}};


pub async fn create_payment(job: &PaymentRequest, db: &Pool<Postgres>, config: Arc<Config>) -> Result<(), String> {

    sleep(Duration::from_millis(config.effective_latency())).await;

    if rand::thread_rng().gen_bool(config.error_rate()) {
        return Err("Legacy system timeout".into());
    }

    Ok(())
}

pub async fn store_payment(
    job: &PaymentRequest, 
    db: &Pool<Postgres>, 
    status_code: &str, 
    status_desc: &str, 
) -> Result<(), anyhow::Error> {
    let amount_value: Option<i64> = job.amount
        .as_ref()
        .map(|a| a.value.trim_end_matches(".00").parse())
        .transpose()?;
    let fee_value: Option<i64> = job.fee_amount
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
        job.partner_reference_no.clone(),
        job.merchant_id.clone(),
        job.sub_merchant_id.clone(),
        amount_value,
        job.amount.as_ref().map(|a| a.currency.clone()),
        fee_value,
        job.fee_amount.as_ref().map(|a| a.currency.clone()),
        status_code.to_string(),
        status_desc.to_string(),
        Utc::now()
    )
    .execute(db)
    .await?;

    Ok(())
}

pub async fn fetch_merchant(key: &str, db: &PgPool, config: Arc<Config>) -> Option<Merchant> {
    sleep(Duration::from_millis(config.effective_latency())).await;

    // 1. Get merchant
    let row = sqlx::query!(
        r#"
        SELECT id, name, category, city, merchant_id
        FROM merchants
        WHERE qr_code = $1
        "#,
        key
    )
    .fetch_optional(db)
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
            .fetch_all(db)
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

            Some(Merchant {
                id: m.merchant_id,
                merchant_name: m.name,
                merchant_category: m.category,
                merchant_location: m.city,
                merchant_infos: infos
            })
        }

        _ => {
            println!("Not matched!");
            None
        },
    }
}
