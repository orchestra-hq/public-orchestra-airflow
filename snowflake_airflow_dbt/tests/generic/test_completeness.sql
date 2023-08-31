
{% test completeness_hourly(model, column_name, num_days) %}
WITH staging AS (

  SELECT
    DATE_TRUNC(HOUR,{{column_name}} ) as hour_,
    sum(1) rows_
  FROM {{model}}
  group by 1 

), 

lags as (

SELECT 

  hour_,
  LAG(hour_) OVER (ORDER BY hour_ desc) as prev_hour,
  DATEDIFF(HOUR, hour_, prev_hour) hourly_diff

FROM max_date
WHERE hour_ < dateadd(day, -{{num_days}}, current_date())
and hourly_diff != 1
{% endtest %}