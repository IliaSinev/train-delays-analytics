{{ config(materialized='table') }}

SELECT 
    sum(PFPI_MINUTES)
    , datetime_trunc(EVENT_DATETIME, day)

 FROM `train-delays-analytics.dbt_isinev.stg_delays_data` 
 WHERE DATE(EVENT_DATETIME) <= "2018-05-25"
 group by datetime_trunc(EVENT_DATETIME, day)
 order by datetime_trunc(EVENT_DATETIME, day)