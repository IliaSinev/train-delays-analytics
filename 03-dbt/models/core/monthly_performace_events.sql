{{ config(materialized='table') }}

WITH delays AS (
   SELECT * FROM {{ref ('stg_delays_data')}}
)
SELECT 
     delays.PERFORMANCE_EVENT
    ,delays.PERFORMANCE_EVENT_TYPE
    ,dates.Year
    ,dates.Month
    ,dates.MonthName
    ,dates.MonthName_Short
    ,dates.Quarter
    ,dates.QuarterName
    ,dates.DATE
    ,COUNT(*) AS TOTAL_EVENTS
    ,ROUND(SUM(delays.PFPI_MINUTES)) AS TOTAL_DELAYS_TIME
    ,ROUND(AVG(delays.PFPI_MINUTES)) AS AVG_DELAYS_TIME
 FROM delays
 INNER JOIN {{ref ('dim_Date')}} AS dates
 ON EXTRACT(MONTH FROM delays.EVENT_DATETIME) = dates.Month AND EXTRACT(YEAR FROM delays.EVENT_DATETIME) = dates.Year
 GROUP BY    delays.PERFORMANCE_EVENT
            ,delays.PERFORMANCE_EVENT_TYPE
            ,dates.Year
            ,dates.Month
            ,dates.MonthName
            ,dates.MonthName_Short
            ,dates.Quarter
            ,dates.QuarterName
            ,dates.DATE
