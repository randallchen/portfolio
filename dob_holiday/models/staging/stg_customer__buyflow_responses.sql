-- stg_customer__buyflow_responses.sql

WITH source AS (

	SELECT *
	FROM {{source('buyflow_responses', 'customers_raw') }}

)

SELECT *
	FROM {{source('buyflow_responses', 'customers_raw') }}
