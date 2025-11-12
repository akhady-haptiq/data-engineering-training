SELECT 'Distinct Categories' AS check_type, category AS value, COUNT(*) AS cnt
FROM {{ ref('staging_trial_balances') }}
GROUP BY category
ORDER BY category

UNION ALL

-- Check for duplicates
SELECT 'Duplicate Check' AS check_type, 
       CONCAT(gl_number::text, '-', year::text, '-', month::text) AS value,
       COUNT(*) AS cnt
FROM {{ ref('staging_trial_balances') }}
GROUP BY gl_number, year, month
HAVING COUNT(*) > 1
LIMIT 10

UNION ALL

-- Check total row count
SELECT 'Total Rows' AS check_type, '' AS value, COUNT(*) AS cnt
FROM {{ ref('staging_trial_balances') }}

UNION ALL

-- Check unique combinations
SELECT 'Unique Combinations' AS check_type, '' AS value, COUNT(DISTINCT (gl_number, year, month)) AS cnt
FROM {{ ref('staging_trial_balances') }};

