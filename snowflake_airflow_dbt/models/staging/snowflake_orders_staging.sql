{{ config(materialized='view') }}

select

    a.sold_date,
    a.ship_date,
    a.bill_cutomer_sk_id,
    a.ship_customer_sk_id,
    a.item_sk_id,
    a.order_number,
    a.quantity,
    a.cost,
    b.first_name,
    b.last_name,
    b.email,
    sha2_binary(a.order_number) _pk

from {{ref('snowflake_orders_clean')}} a
left join {{ref('snowflake_customers_clean')}} b 
on a.bill_cutomer_sk_id = b.customer_sk_id
where a.ship_date is not null