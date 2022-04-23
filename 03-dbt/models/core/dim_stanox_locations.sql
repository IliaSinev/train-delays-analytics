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
     st.STANOX_NO
    ,st.FULL_NAME
    ,st.CRS_CODE
    ,st.Route_Description
    ,COALESCE(att.Region, 'NA') AS Region
    ,COALESCE(att.Local_authority_district_unitary, 'NA') AS Local_authority_district_unitary
    ,COALESCE(att.Local_authority_county_unitary, 'NA') AS Local_authority_county_unitary
    ,COALESCE(att.Constituency, 'NA') AS Constituency
    ,COALESCE(att.ITL2_subregion, 'NA') AS ITL2_subregion
    ,COALESCE(att.ITL2_subregion_code, 'NA') AS ITL2_subregion_code
    ,COALESCE(att.Station_group, 'NA') AS Station_group
    ,COALESCE(att.Network_Rail_Region, 'NA') AS Network_Rail_Region
    ,loc.Latitude
    ,loc.Longitude
FROM stanox_upd AS st
LEFT OUTER JOIN attributes AS att
ON st.CRS_CODE = attr.TLC
LEFT OUTER JOIN lacations AS loc
ON st.STANOX_NO = loc.Stanox





