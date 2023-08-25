
{{ config(materialized='view') }}
select

    c_Customer_sk customer_sk_id,
    c_customer_id customer_id,
    c_first_name first_name,
    c_last_name last_name,
    c_email_address email,
    sha2_binary(c_customer_id) _pk
from {{source('snowflake', 'customer')}}
LIMIT 1000