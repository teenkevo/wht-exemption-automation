-- feature: registration_status
-- description: Registration status code
-- dtype: text
-- source: reg
-- missing: null, no registration row
SELECT
    TIN_NO,
    cast(max(REG_STATUS) as string) AS registration_status
FROM lakehouse.reg
GROUP BY TIN_NO

-- feature: taxpayer_status
-- description: Taxpayer status
-- dtype: text
-- source: reg
-- missing: null, no registration row
SELECT
    TIN_NO,
    cast(max(taxpayer_status) as string) AS taxpayer_status
FROM lakehouse.reg
GROUP BY TIN_NO

-- feature: business_age_years
-- description: Years since the business start date
-- dtype: number
-- source: reg
-- missing: null, no registration row
SELECT
    TIN_NO,
    CASE
        WHEN min(BUSINESS_START_DT) IS NULL THEN NULL
        ELSE greatest(months_between(current_date(), cast(min(BUSINESS_START_DT) as date)) / 12.0, 0.0)
    END AS business_age_years
FROM lakehouse.reg
GROUP BY TIN_NO

-- feature: returns_filed_count
-- description: Number of filed returns
-- dtype: number
-- source: returns
-- missing: 0, no returns on record
SELECT
    TAXPAYER_TIN AS TIN_NO,
    cast(count(*) as double) AS returns_filed_count
FROM lakehouse.returns
GROUP BY TAXPAYER_TIN

-- feature: declared_tax_total
-- description: Declared tax across filed returns
-- dtype: number
-- source: returns
-- missing: 0, no returns on record
SELECT
    TAXPAYER_TIN AS TIN_NO,
    cast(sum(net_tax) as double) AS declared_tax_total
FROM lakehouse.returns
GROUP BY TAXPAYER_TIN

-- feature: tax_collected
-- description: Total tax paid
-- dtype: number
-- source: payments
-- missing: 0, no payments on record
SELECT
    TIN_NO,
    cast(sum(TOT_PAYMENT_AMT) as double) AS tax_collected
FROM lakehouse.payments
GROUP BY TIN_NO

-- feature: payments_count
-- description: Number of payments
-- dtype: number
-- source: payments
-- missing: 0, no payments on record
SELECT
    TIN_NO,
    cast(count(*) as double) AS payments_count
FROM lakehouse.payments
GROUP BY TIN_NO

-- feature: outstanding_liability
-- description: Outstanding liability across ledger entries
-- dtype: number
-- source: ledger
-- missing: 0, no ledger rows
SELECT
    TPIN AS TIN_NO,
    cast(sum(OUTSTANDING_LIABILITY) as double) AS outstanding_liability
FROM lakehouse.ledger
GROUP BY TPIN

-- feature: waiver_received
-- description: Waiver amount across ledger entries
-- dtype: number
-- source: ledger
-- missing: 0, no ledger rows
SELECT
    TPIN AS TIN_NO,
    cast(sum(TOTAL_WAIVER) as double) AS waiver_received
FROM lakehouse.ledger
GROUP BY TPIN

-- feature: efris_sales_total
-- description: EFRIS sales amount
-- dtype: number
-- source: efris
-- missing: 0, no invoices on record
SELECT
    sellertin AS TIN_NO,
    cast(sum(sales_amount) as double) AS efris_sales_total
FROM lakehouse.efris
GROUP BY sellertin

-- feature: efris_invoice_count
-- description: Number of EFRIS invoices
-- dtype: number
-- source: efris
-- missing: 0, no invoices on record
SELECT
    sellertin AS TIN_NO,
    cast(count(*) as double) AS efris_invoice_count
FROM lakehouse.efris
GROUP BY sellertin

-- feature: customs_red_lane_count
-- description: Customs declarations routed to physical inspection
-- dtype: number
-- source: customs
-- missing: 0, no customs declarations
SELECT
    taxpayer_tin AS TIN_NO,
    cast(sum(is_red_flg) as double) AS customs_red_lane_count
FROM lakehouse.customs
GROUP BY taxpayer_tin
