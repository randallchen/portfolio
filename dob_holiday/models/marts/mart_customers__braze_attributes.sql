{{ config(materialized='table') }}

WITH base AS (
    SELECT
        external_id,
        email,
        first_name,
        last_name,
        city,
        state,
        country,
        child_age
    FROM {{ ref('int_customers') }}
),

age_changed AS (
    SELECT external_id
    FROM {{ ref('snp_int_customers__child_age') }}
    WHERE dbt_valid_to IS NULL
      AND dbt_updated_at >= current_date - INTERVAL ({{ var('child_age_sync_window_days', 7) }}) DAY
)

SELECT
    b.external_id,
    b.email,
    b.first_name,
    b.last_name,
    b.city,
    b.state,
    b.country,
    CASE
        WHEN ac.external_id IS NOT NULL THEN b.child_age
        ELSE NULL
    END AS child_age
FROM base b
LEFT JOIN age_changed ac ON b.external_id = ac.external_id
