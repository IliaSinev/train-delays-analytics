{{ config(materialized='table') }}

SELECT
     COUNT(*) AS DELAY_COUNT
    ,ROUND(SUM(delays.PFPI_MINUTES)) AS TOTAL_DELAY
    ,round(avg(delays.PFPI_MINUTES)) AS AVG_DELAY
    ,CASE WHEN stanox.Region = 'NA' THEN 'non-UK'
            ELSE stanox.Region
        END AS Region
    ,dates.MonthName
    ,dates.Year
    ,dates.DATE
 FROM {{ref ('stg_delays_data')}} AS delays
 INNER JOIN {{ref ('dim_stanox_locations')}} AS stanox
 ON delays.START_STANOX = stanox.STANOX_NO
 INNER JOIN {{ref ('dim_Date')}} AS dates
 ON DATETIME_TRUNC(delays.EVENT_DATETIME, DAY) = dates.DATE
 GROUP BY 
     stanox.Region
    ,dates.MonthName
    ,dates.Year
    dates.DATE