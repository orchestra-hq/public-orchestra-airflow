
{% test test_recent_days(model, column_name, num_days) %}
WITH max_date AS (
  SELECT MAX(cast({{column_name}} as date)) as latest_date
  FROM {{model}}
)
SELECT *
FROM max_date
WHERE latest_date < dateadd(day, -{{num_days}}, current_date())
{% endtest %}