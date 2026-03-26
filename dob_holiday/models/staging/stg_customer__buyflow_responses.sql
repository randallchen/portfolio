-- stg_customer__buyflow_responses.sql

WITH source AS (

	SELECT *
	FROM read_parquet('./synth_file_generation/customers_raw.parquet')
)

SELECT *
FROM source
