{{ config(materialized='view') }}

select
    dateadd(DAY,CS_SOLD_DATE_SK - 2415020, '1920-01-01') sold_date,
    dateadd(DAY,CS_SHIP_DATE_SK - 2415020, '1920-01-01') ship_date,
    CS_BILL_CUSTOMER_SK bill_cutomer_sk_id,
    CS_SHIP_CUSTOMER_SK ship_customer_sk_id,
    CS_ITEM_SK item_sk_id,
    CS_ORDER_NUMBER order_number,
    CS_QUANTITY quantity,
    CS_WHOLESALE_COST cost,
    sha2_binary(CS_ORDER_NUMBER) _pk
from {{source('snowflake', 'catalog_sales')}}
LIMIT 1000