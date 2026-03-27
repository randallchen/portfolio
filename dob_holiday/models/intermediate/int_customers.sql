-- int_customers.sql

WITH customers_buyflow_responses AS (

	SELECT 
		customer_id,
		email,
		city,
		state,
		country,
		child_birth_date,
		DATEDIFF('year', child_birth_date, today()::date)  AS child_age,
		created_at_utc,
		updated_at_utc
	FROM {{ ref('stg_customer__buyflow_responses')}}
)

SELECT *
FROM customers_buyflow_responses
