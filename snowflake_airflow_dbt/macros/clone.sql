{% macro clone() %}
{% set identifier_parts = this.identifier.split('.') %}
{% set table_name = identifier_parts[-1] %}
create or replace table SNOWFLAKE_WORKING.PUBLIC_prod.{{table_name}}_clone
clone snowflake_working.public_aggregated.{{table_name}}
{% endmacro %}