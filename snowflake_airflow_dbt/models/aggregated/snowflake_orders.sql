select
    first_name,
    last_name,
    email,
    bill_cutomer_sk_id,
    ship_customer_sk_id
    ship_date,
    count(distinct order_number) orders,
    sum(quantity) quantity,
    sum(cost) cost,
    sha2_binary(concat(
        cast(bill_cutomer_sk_id as string),
        cast(ship_customer_sk_id as string),
        cast(ship_date as string)
        
        )) _pk


from {{ref('snowflake_orders_staging')}} a
group by 
    first_name,
    last_name,
    email,
    bill_cutomer_sk_id,
    ship_customer_sk_id
    ship_date