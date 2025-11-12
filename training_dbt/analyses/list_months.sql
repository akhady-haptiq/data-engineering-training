SELECT
    year,
    month,
    (year * 100) + month AS year_month,
    total_revenue,
    total_expenses,
    net_income
FROM {{ ref('income_statement') }}
ORDER BY year, month;

