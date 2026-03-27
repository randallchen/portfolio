{% snapshot snp_int_customers__child_age %}
{{
    config(
        target_schema='snapshots',
        unique_key='external_id',
        strategy='check',
        check_cols=['child_age'],
        invalidate_hard_deletes=True,
    )
}}

SELECT
    external_id,
    child_age
FROM {{ ref('int_customers') }}

{% endsnapshot %}
