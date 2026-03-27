-- mart_customers__braze_attributes.sql
{{ config(materialized='table') }}

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
