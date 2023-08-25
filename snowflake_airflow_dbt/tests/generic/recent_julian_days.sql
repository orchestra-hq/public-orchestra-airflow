
{% test recent_julian_days(model, column_name, num_days) %}
WITH max_date AS (
  SELECT MAX(cast(dateadd(DAY,{{column_name}} - 2415020, '1920-01-01') as date)) as latest_date
  FROM {{model}}
)
SELECT *
FROM max_date
WHERE latest_date < dateadd(day, -{{num_days}}, current_date())
{% endtest %}