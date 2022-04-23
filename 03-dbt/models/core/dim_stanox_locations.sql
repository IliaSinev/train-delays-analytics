{{ config(materialized='table')}}

WITH stanox AS
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
                            ORDER BY INSERT_DATETIME DESC) AS RW_NR
    FROM {{ source('core', 'dim_Stanox') }}
)
,
stanox_upd AS
(
    SELECT
         cast(STANOX_NO AS INT64) AS STANOX_NO
        ,FULL_NAME
        ,CRS_CODE
        ,Route_Description
    FROM stanox
    WHERE RW_NR = 1
)
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
),
locations AS
(
    SELECT
         Stanox
        ,Latitude
        ,Longitude
    FROM {{ ref ('Stanox-Locations')}}
    WHERE Latitude is not null AND Longitude is not null
)
SELECT
    
FROM {{ source('core', 'dim_Stanox') }} AS stanox
left outer join {{ref('Stanox-Locations')}} AS loc
on stanox.STANOX_NO = attr.TLC





