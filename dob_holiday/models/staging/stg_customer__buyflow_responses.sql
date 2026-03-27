-- stg_customer__buyflow_responses.sql



WITH source AS(
	SELECT 
		customer_id,
		external_id,
		TRIM(first_name)    		AS first_name,
		TRIM(last_name)  	   		AS last_name,
		LOWER(TRIM(email))			AS email,
		phone,
		city,
		LOWER(TRIM(state))         	AS state,
		LOWER(TRIM(country))       	AS country,
		TRIM(postal_code)		   	AS postal_code,
		child_birthday::DATE 		AS child_birth_date,
		created_at::TIMESTAMPTZ		AS created_at_utc,
		updated_at::TIMESTAMPTZ		AS updated_at_utc,
		ROW_NUMBER() 
			OVER (
				PARTITION BY external_id
				ORDER BY created_at_utc ASC
				) AS rn
	FROM {{ source('main', 'raw_customers') }}
)

SELECT 
		customer_id,
		external_id,
		first_name,
		last_name,
		email,
		phone,
		city,
		state,
		country,
		postal_code,
		child_birth_date,
		created_at_utc,
		updated_at_utc
FROM source
WHERE rn = 1 -- Using row_number() to deduplicate any customers with same external_id