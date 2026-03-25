import duckdb

conn = duckdb.connect()

# Preview rows
conn.query("SELECT * FROM read_parquet('customers_raw.parquet') LIMIT 10").df()

# # Check shape
# conn.query("SELECT COUNT(*) FROM read_parquet('customers_raw.parquet')").df()

# # Inspect schema
# conn.query("DESCRIBE SELECT * FROM read_parquet('customers_raw.parquet')").df()

# # Spot-check nulls in child_birthday
# conn.query("""
#     SELECT
#         COUNT(*) AS total,
#         COUNT(child_birthday) AS has_birthday,
#         COUNT(*) - COUNT(child_birthday) AS no_birthday
#     FROM read_parquet('customers_raw.parquet')
# """).df()