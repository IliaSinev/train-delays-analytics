{{ config(materialized='table') }}

SELECT 
     delays.PERFORMANCE_EVENT
    ,delays.PERFORMANCE_EVENT_TYPE
    ,dates.Year
    ,dates.Month
    ,COUNT(*) AS TOTAL_EVENTS
    ,ROUND(SUM(PFPI_MINUTES)) AS TOTAL_DELAYS_TIME
    ,ROUND(AVG(PFPI_MINUTES)) AS AVG_DELAYS_TIME
 FROM {{ref ('stg_delays_data')}} AS delays
 LEFT OUTER JOIN {{ref ('dim_Date')}} AS dates
 ON EXTRACT(MONTH FROM delays.EVENT_DATETIME) = dates.Month AND EXTRACT(YEAR FROM delays.EVENT_DATETIME) = dates.Year
 GROUP BY    delays.PERFORMANCE_EVENT
            ,delays.PERFORMANCE_EVENT_TYPE
            ,dates.Year
            ,dates.Month
