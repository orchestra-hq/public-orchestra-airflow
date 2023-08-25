{{
    config(
        materialized='incremental',
        unique_key='_pk'
    )
}}

select
    *

from {{ref('snowflake_orders')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
where ship_date >= dateadd(YEAR,-10,current_date())
{% endif %}