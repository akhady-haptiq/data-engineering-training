{% test sequential_months(model, year_column, month_column) %}
WITH ordered_months AS (
    SELECT DISTINCT
        {{ year_column }} AS year,
        {{ month_column }} AS month,
        ROW_NUMBER() OVER (ORDER BY {{ year_column }}, {{ month_column }}) AS rn
    FROM {{ model }}
),

month_gaps AS (
    SELECT
        o1.year AS current_year,
        o1.month AS current_month,
        o2.year AS next_year,
        o2.month AS next_month,
        -- Calculate the actual month difference accounting for year boundaries
        ((o2.year - o1.year) * 12) + (o2.month - o1.month) AS month_difference
    FROM ordered_months o1
    LEFT JOIN ordered_months o2
        ON o2.rn = o1.rn + 1
    WHERE o2.year IS NOT NULL
)

SELECT
    current_year,
    current_month,
    next_year,
    next_month,
    month_difference,
    'Expected: 1, Got: ' || month_difference::text AS gap_info
FROM month_gaps
WHERE month_difference != 1  -- Gap detected: difference is not exactly 1 month

{% endtest %}

