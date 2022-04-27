{{ config(materialized='table') }}

WITH loc_delays AS (
SELECT
     ST_GEOGPOINT(locations.Longitude, locations.Latitude) AS Location
    ,locations.FULL_NAME AS Station
    ,dates.Year
    ,dates.Month
    ,dates.MonthName
    ,dates.MonthName_Short
    ,dates.Quarter
    ,dates.QuarterName
    ,dates.DATE
    ,count(*) AS DELAY_COUNT
    ,round(sum(delays.PFPI_MINUTES)) AS TOTAL_DELAY
    ,round(avg(delays.PFPI_MINUTES)) AS AVG_DELAY
    ,dense_rank() OVER (PARTITION BY dates.Year, dates.MonthName, dates.DATE ORDER BY count(*) DESC) AS COUNT_RANK
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
        ,dates.DATE
)
SELECT
     Location
    ,Station
    ,DATE
    ,Year
    ,Month
    ,MonthName
    ,MonthName_Short
    ,Quarter
    ,QuarterName
    ,DELAY_COUNT
    ,TOTAL_DELAY
    ,AVG_DELAY
FROM loc_delays
WHERE COUNT_RANK <=15