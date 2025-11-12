WITH tb AS (
    SELECT * FROM {{ ref('staging_trial_balances') }}
),

aggregated AS (
    SELECT
        year,
        month,
        SUM(CASE WHEN category = 'Revenue' THEN credit ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN category = 'Expense' THEN debit ELSE 0 END) AS total_expenses
    FROM tb
    GROUP BY year, month
),

income AS (
    SELECT
        year,
        month,
        total_revenue,
        total_expenses,
        (total_revenue - total_expenses) AS net_income,
        SUM(total_revenue - total_expenses) OVER (
            PARTITION BY year
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ytd_net_income
    FROM aggregated
)

SELECT
    year,
    month,
    total_revenue,
    total_expenses,
    net_income,
    ytd_net_income
FROM income
WHERE net_income > 0  -- Filter to show only profitable months
ORDER BY year, month