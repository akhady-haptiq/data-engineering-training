WITH ordered_months AS (
    SELECT DISTINCT
        year,
        month,
        (year * 100) + month AS year_month,
        ROW_NUMBER() OVER (ORDER BY year, month) AS rn
    FROM {{ ref('income_statement') }}
),

month_gaps AS (
    SELECT
        o1.year AS current_year,
        o1.month AS current_month,
        o1.year_month AS current_year_month,
        o2.year AS next_year,
        o2.month AS next_month,
        o2.year_month AS next_year_month,
        o2.year_month - o1.year_month AS month_difference
    FROM ordered_months o1
    LEFT JOIN ordered_months o2
        ON o2.rn = o1.rn + 1
    WHERE o2.year_month IS NOT NULL
)

SELECT
    current_year,
    current_month,
    next_year,
    next_month,
    month_difference,
    'GAP: Missing ' || (month_difference - 1)::text || ' month(s)' AS gap_description
FROM month_gaps
WHERE month_difference > 1
ORDER BY current_year, current_month;

