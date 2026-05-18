-- Migration: 0001_qris_merchant.sql
-- Run with: sqlx migrate run  (or psql -f this file)

CREATE TABLE IF NOT EXISTS merchants (
    id       SERIAL PRIMARY KEY,
    qr_code  TEXT UNIQUE NOT NULL,
    name     TEXT NOT NULL,
    category TEXT NOT NULL,
    city     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merchant_infos (
    id           SERIAL PRIMARY KEY,
    merchant_id  INT NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
    merchant_pan TEXT NOT NULL,
    acquirer_name TEXT NOT NULL
);

-- Seed data for the stampede scenario
INSERT INTO merchants (qr_code, name, category, city)
VALUES
    ('QR_MERCHANT_001', 'Toko Maju Jaya', 'Retail',    'Jakarta'),
    ('QR_MERCHANT_002', 'Warung Bu Sari', 'Food & Bev', 'Bandung'),
    ('QR_MERCHANT_003', 'Apotek Sehat',   'Pharmacy',  'Surabaya')
ON CONFLICT (qr_code) DO NOTHING;

INSERT INTO merchant_infos (merchant_id, merchant_pan, acquirer_name)
VALUES
    (1, '1234567890123456', 'Bank BCA'),
    (2, '2345678901234567', 'Bank Mandiri'),
    (3, '3456789012345678', 'Bank BNI')
ON CONFLICT DO NOTHING;

-- Index for fast lookups by qr_code (already covered by UNIQUE constraint,
-- but explicit for clarity in query plans)
CREATE INDEX IF NOT EXISTS idx_merchants_qr_code ON merchants (qr_code);
