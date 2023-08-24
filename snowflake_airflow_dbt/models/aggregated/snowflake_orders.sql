{{ 
    config(
        materialized='table',
        transient=false,
        post_hook=[
            "SELECT 1;
            {{clone()}}"
            ]

    )
}}
select
    first_name,
    last_name,
    email,
    ifnull(cast(bill_cutomer_sk_id as string), 'None') bill_cutomer_sk_id,
    ifnull(cast(ship_customer_sk_id as string), 'None') ship_customer_sk_id,
    ship_date,
    count(distinct order_number) orders,
    sum(quantity) quantity,
    sum(cost) cost,
    sha2_binary(concat(
        ifnull(cast( bill_cutomer_sk_id as string), 'None') ,
        ifnull(cast(ship_customer_sk_id as string), 'None'),
        ifnull(cast(ship_date as string), 'None')
        
        )) _pk


from {{ref('snowflake_orders_staging')}} a
group by 
    first_name,
    last_name,
    email,
    ifnull(cast(bill_cutomer_sk_id as string), 'None'),
    ifnull(cast(ship_customer_sk_id as string), 'None'),
    ship_date