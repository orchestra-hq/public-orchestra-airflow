{% macro clone() %}
create table if not exists SNOWFLAKE_WORKING.PUBLIC_prod.prod_snowflake_orders_clone  clone snowflake_working.public_aggregated.snowflake_orders
{% endmacro %}