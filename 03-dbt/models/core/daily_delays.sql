{{ config(materialized='table') }}

SELECT 
     count(*) AS DELAY_COUNT
    ,round(sum(PFPI_MINUTES)) AS TOTAL_DELAY
    ,round(avg(PFPI_MINUTES)) AS AVG_DELAY
    ,datetime_trunc(EVENT_DATETIME, day) AS EVENT_DATE
 FROM {{ref ('stg_delays_data')}}
 GROUP BY DATETIME_TRUNC(EVENT_DATETIME, day)