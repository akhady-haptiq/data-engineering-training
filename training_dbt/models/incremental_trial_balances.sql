{{
    config(
        materialized='incremental',
        unique_key=['year', 'month', 'gl_number']
    )
}}

SELECT
    gl_number,
    gl_name,
    category,
    year,
    month,
    debit,
    credit,
    period_amount,
    CURRENT_TIMESTAMP AS loaded_at
FROM {{ ref('staging_trial_balances') }}

{% if is_incremental() %}
    WHERE (year, month) NOT IN (
        SELECT DISTINCT year, month 
        FROM {{ this }}
    )
{% endif %}
