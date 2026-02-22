-- Approach 7: dbt — SCD2 snapshot for dim_customer
-- Uses the 'check' strategy to detect changes in the tracked columns.
-- Requires two snapshot runs: first with batch_1 data, then with all data.
-- dbt automatically manages dbt_valid_from / dbt_valid_to columns.

{% snapshot snap_dim_customer %}

{{ config(
    unique_key='customer_id',
    strategy='check',
    check_cols=['email', 'address', 'city', 'country', 'segment']
) }}

SELECT * FROM {{ ref('silver_customers') }}

{% endsnapshot %}
