SELECT
    gl_number,
    year,
    month,
    COUNT(*) AS duplicate_count
FROM {{ ref('staging_trial_balances') }}
GROUP BY gl_number, year, month
HAVING COUNT(*) > 1
