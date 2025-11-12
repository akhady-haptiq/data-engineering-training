WITH tb AS (
    SELECT * FROM {{ ref('staging_trial_balances') }}
),

aggregated AS (
    SELECT
        year,
        month,
        SUM(CASE WHEN category = 'Assets' THEN debit ELSE 0 END) AS total_assets,
        SUM(CASE WHEN category = 'Liabilities' THEN credit ELSE 0 END) AS total_liabilities,
        SUM(CASE WHEN category = 'Equity' THEN credit ELSE 0 END) AS total_equity
    FROM tb
    GROUP BY year, month
),

with_rank AS (
    SELECT
        year,
        month,
        total_assets,
        total_liabilities,
        total_equity,
        (total_liabilities + total_equity) AS total_liabilities_and_equity,
        ABS(total_assets - (total_liabilities + total_equity)) < 0.01 AS accounting_equation_valid,
        RANK() OVER (ORDER BY year DESC, month DESC) AS period_rank
    FROM aggregated
)

SELECT
    year,
    month,
    total_assets,
    total_liabilities,
    total_equity,
    total_liabilities_and_equity,
    accounting_equation_valid
FROM with_rank
WHERE period_rank = 1  -- Only show the latest month's balance sheet
ORDER BY year, month