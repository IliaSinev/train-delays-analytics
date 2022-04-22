{{ config(materialized='table')}}

WITH stanox_cte AS
(
    SELECT
         STANOX_NO
        ,FULL_NAME
        ,CRS_CODE
        ,Route_Description
        ,ROW_NUMBER() OVER(PARTITION_BY
                                 STANOX_NO
                                ,FULL_NAME
                                ,CRS_CODE
                                ,Route_Description
                            ORDER BY INSERT_DATETIME DESC)
    FROM {{ source('core', 'dim_Stanox') }}
)
,
attributes AS
(
    SELECT
         TLC
        ,Region
        ,Local_authority_district_unitary
        ,Local_authority_county_unitary
        ,Constituency
        ,ITL2_subregion
        ,ITL2_subregion_code
        ,Station_group
        ,Network_Rail_Region
    FROM {{ source('core', 'dim_StationAttributes')}}
)
SELECT
     CAST(stanox.STANOX_NO AS INT64) AS STANOX_NO
    ,stanox.FULL_NAME
    ,loc.Latitude
    ,loc.Longitude
    ,stanox.CRS_CODE
    ,stanox.Route_Description
    ,stanox.INSERT_DATETIME
FROM {{ source('core', 'dim_Stanox') }} AS stanox
left outer join {{ref('Stanox-Locations')}} AS loc
on stanox.STANOX_NO = attr.TLC