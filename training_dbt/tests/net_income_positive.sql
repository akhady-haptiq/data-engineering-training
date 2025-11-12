SELECT
    year,
    month,
    net_income
FROM {{ ref('income_statement') }}
WHERE month > 3
  AND net_income <= 0

