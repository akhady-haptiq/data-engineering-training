{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ ref('trial_balance') }}
)

SELECT
    -- Renamed columns using snake_case
    "Account_ID" as gl_number,
    "Account_Name" as gl_name,
    "Category" as category,
    left("Month", 4)::int as year,
    right("Month", 2)::int as month,
    "Debit" as debit,
    "Credit" as credit,
    ("Debit"::NUMERIC - "Credit"::NUMERIC) AS period_amount

FROM source_data