-- int_customers.sql

WITH customers_buyflow_responses AS (
	SELECT
		customer_id,
		external_id,
		first_name,
		last_name,
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

SELECT
	customer_id,
	external_id,
	first_name,
	last_name,
	email,
	city,
	state,
	country,
	child_birth_date,
	child_age,
	created_at_utc,
	updated_at_utc
FROM customers_buyflow_responses
