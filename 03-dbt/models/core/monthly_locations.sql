{{ config(materialized='table') }}

SELECT
     ST_GEOGPOINT(locations.Latitude, locations.Longitude) AS Location
    ,locations.FULL_NAME
    ,dates.Year
    ,dates.Month
    ,dates.MonthName
    ,dates.MonthName_Short
    ,dates.Quarter
    ,dates.QuarterName
    ,count(*) AS DELAY_COUNT
    ,round(sum(delays.PFPI_MINUTES)) AS TOTAL_DELAY
    ,round(avg(delays.PFPI_MINUTES)) AS AVG_DELAY
FROM {{ref ('stg_delays_data')}} AS delays
INNER JOIN {{ref ('dim_Date')}} AS dates
ON DATETIME_TRUNC(delays.EVENT_DATETIME, DAY) = dates.DATE
INNER JOIN {{ref ('dim_stanox_locations')}} AS locations
ON delays.START_STANOX = locations.STANOX_NO
WHERE 1=1
 AND locations.Latitude is not null
 AND locations.Longitude is not null
GROUP BY locations.Latitude
        ,locations.Longitude
        ,locations.FULL_NAME
        ,dates.Year
        ,dates.Month
        ,dates.MonthName
        ,dates.MonthName_Short
        ,dates.Quarter
        ,dates.QuarterName
ORDER BY DELAY_COUNT DESC, TOTAL_DELAY DESC