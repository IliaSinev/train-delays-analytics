{{ config(
    materialized='table',
    partition_by={
        "field":"EVENT_DATETIME",
        "data_type":"datetime",
        "granularity":"month"
    }
) }}

WITH raw_cte AS
(
SELECT 
    FINANCIAL_YEAR_AND_PERIOD
    ,ORIGIN_DEPARTURE_DATE
    ,TRUST_TRAIN_ID_AFFECTED
    ,PLANNED_ORIG_LOC_CODE_AFF
    ,PLANNED_ORIG_GBTT_DATETIME_AFF
    ,PLANNED_ORIG_WTT_DATETIME_AFF
    ,PLANNED_DEST_LOC_CODE_AFFECTED
    ,PLANNED_DEST_GBTT_DATETIME_AFF
    ,PLANNED_DEST_WTT_DATETIME_AFF
    ,TRAIN_SERVICE_CODE_AFFECTED
    ,SERVICE_GROUP_CODE_AFFECTED
    ,OPERATOR_AFFECTED
    ,ENGLISH_DAY_TYPE
    ,APP_TIMETABLE_FLAG_AFF
    ,TRAIN_SCHEDULE_TYPE_AFFECTED
    ,TRACTION_TYPE_AFFECTED
    ,TRAILING_LOAD_AFFECTED
    ,TIMING_LOAD_AFFECTED
    ,UNIT_CLASS_AFFECTED
    ,INCIDENT_NUMBER
    ,INCIDENT_CREATE_DATE
    ,INCIDENT_START_DATETIME
    ,INCIDENT_END_DATETIME
    ,SECTION_CODE
    ,NETWORK_RAIL_LOCATION_MANAGER
    ,RESPONSIBLE_MANAGER
    ,INCIDENT_REASON
    ,ATTRIBUTION_STATUS
    ,INCIDENT_EQUIPMENT
    ,INCIDENT_DESCRIPTION
    ,REACTIONARY_REASON_CODE
    ,INCIDENT_RESPONSIBLE_TRAIN
    ,PERFORMANCE_EVENT_CODE
    ,START_STANOX
    ,END_STANOX
    ,EVENT_DATETIME
    ,PFPI_MINUTES
    ,TRUST_TRAIN_ID_RESP
    ,TRUST_TRAIN_ID_REACT
    ,ROW_NUMBER() OVER (PARTITION BY 
                                     FINANCIAL_YEAR_AND_PERIOD
                                    ,ORIGIN_DEPARTURE_DATE
                                    ,TRUST_TRAIN_ID_AFFECTED
                                    ,PLANNED_ORIG_LOC_CODE_AFF
                                    ,PLANNED_ORIG_GBTT_DATETIME_AFF
                                    ,PLANNED_ORIG_WTT_DATETIME_AFF
                                    ,PLANNED_DEST_LOC_CODE_AFFECTED
                                    ,PLANNED_DEST_GBTT_DATETIME_AFF
                                    ,PLANNED_DEST_WTT_DATETIME_AFF
                                    ,TRAIN_SERVICE_CODE_AFFECTED
                                    ,SERVICE_GROUP_CODE_AFFECTED
                                    ,OPERATOR_AFFECTED
                                    ,ENGLISH_DAY_TYPE
                                    ,APP_TIMETABLE_FLAG_AFF
                                    ,TRAIN_SCHEDULE_TYPE_AFFECTED
                                    ,TRACTION_TYPE_AFFECTED
                                    ,TRAILING_LOAD_AFFECTED
                                    ,TIMING_LOAD_AFFECTED
                                    ,UNIT_CLASS_AFFECTED
                                    ,INCIDENT_NUMBER
                                    ,INCIDENT_CREATE_DATE
                                    ,INCIDENT_START_DATETIME
                                    ,INCIDENT_END_DATETIME
                                    ,SECTION_CODE
                                    ,NETWORK_RAIL_LOCATION_MANAGER
                                    ,RESPONSIBLE_MANAGER
                                    ,INCIDENT_REASON
                                    ,ATTRIBUTION_STATUS
                                    ,INCIDENT_EQUIPMENT
                                    ,INCIDENT_DESCRIPTION
                                    ,REACTIONARY_REASON_CODE
                                    ,INCIDENT_RESPONSIBLE_TRAIN
                                    ,PERFORMANCE_EVENT_CODE
                                    ,START_STANOX
                                    ,END_STANOX
                                    ,EVENT_DATETIME
                                    ,TRUST_TRAIN_ID_RESP
                                    ,TRUST_TRAIN_ID_REACT
                        ORDER BY insert_datetime DESC
    ) AS ROW_NR
FROM {{ source('staging', 'train_delays_all') }}
)
SELECT
{{ dbt_utils.surrogate_key(['INCIDENT_NUMBER', 'INCIDENT_CREATE_DATE']) }} AS DELAY_ID
,FINANCIAL_YEAR_AND_PERIOD
,{{ convert_date('ORIGIN_DEPARTURE_DATE')}} as ORIGIN_DEPARTURE_DATE

-- CASE 
--     WHEN ORIGIN_DEPARTURE_DATE LIKE "__.%" THEN parse_date("%d.%m.%y", LEFT(ORIGIN_DEPARTURE_DATE, 10))
--     WHEN ORIGIN_DEPARTURE_DATE LIKE "__/%" THEN parse_date("%d/%m/%Y", LEFT(ORIGIN_DEPARTURE_DATE, 10))
--     WHEN ORIGIN_DEPARTURE_DATE LIKE "__-%" THEN parse_date("%d-%b-%Y", LEFT(ORIGIN_DEPARTURE_DATE, 11))
--     ELSE NULL
--  END AS ORIGIN_DEPARTURE_DATE
,TRUST_TRAIN_ID_AFFECTED
,SAFE_CAST (PLANNED_ORIG_LOC_CODE_AFF AS INT64) AS PLANNED_ORIG_LOC_CODE_AFF
,{{ convert_datetime('PLANNED_ORIG_GBTT_DATETIME_AFF')}} AS PLANNED_ORIG_GBTT_DATETIME_AFF

-- CASE WHEN PLANNED_ORIG_GBTT_DATETIME_AFF IS NOT NULL OR PLANNED_ORIG_GBTT_DATETIME_AFF != ''
--         THEN CASE 
--             WHEN PLANNED_ORIG_GBTT_DATETIME_AFF = "" THEN NULL
--             WHEN PLANNED_ORIG_GBTT_DATETIME_AFF LIKE "%/%"
--                     THEN parse_datetime("%d/%m/%Y %H:%M", PLANNED_ORIG_GBTT_DATETIME_AFF)
--                     ELSE parse_datetime("%d-%b-%Y %H:%M", PLANNED_ORIG_GBTT_DATETIME_AFF)
--         END 
--     ELSE NULL
--  END AS PLANNED_ORIG_GBTT_DATETIME_AFF
,{{ convert_datetime('PLANNED_ORIG_WTT_DATETIME_AFF')}} AS PLANNED_ORIG_WTT_DATETIME_AFF

-- CASE WHEN PLANNED_ORIG_WTT_DATETIME_AFF IS NOT NULL OR PLANNED_ORIG_WTT_DATETIME_AFF != ''
--         THEN CASE 
--             WHEN PLANNED_ORIG_WTT_DATETIME_AFF = "" THEN NULL
--             WHEN PLANNED_ORIG_WTT_DATETIME_AFF LIKE "%/%"
--                     THEN parse_datetime("%d/%m/%Y %H:%M", PLANNED_ORIG_WTT_DATETIME_AFF)
--                     ELSE parse_datetime("%d-%b-%Y %H:%M", PLANNED_ORIG_WTT_DATETIME_AFF)
--         END 
--     ELSE NULL
--  END AS PLANNED_ORIG_WTT_DATETIME_AFF
,SAFE_CAST(PLANNED_DEST_LOC_CODE_AFFECTED AS INT64) PLANNED_DEST_LOC_CODE_AFFECTED
,{{ convert_datetime('PLANNED_DEST_GBTT_DATETIME_AFF')}} AS PLANNED_DEST_GBTT_DATETIME_AFF

-- CASE WHEN PLANNED_DEST_GBTT_DATETIME_AFF IS NOT NULL OR PLANNED_DEST_GBTT_DATETIME_AFF != ''
--         THEN CASE 
--             WHEN PLANNED_DEST_GBTT_DATETIME_AFF = "" THEN NULL
--             WHEN PLANNED_DEST_GBTT_DATETIME_AFF LIKE "%/%"
--                     THEN parse_datetime("%d/%m/%Y %H:%M", PLANNED_DEST_GBTT_DATETIME_AFF)
--                     ELSE parse_datetime("%d-%b-%Y %H:%M", PLANNED_DEST_GBTT_DATETIME_AFF)
--         END 
--     ELSE NULL
--  END AS PLANNED_DEST_GBTT_DATETIME_AFF
,{{ convert_datetime('PLANNED_DEST_WTT_DATETIME_AFF')}} AS PLANNED_DEST_WTT_DATETIME_AFF

-- CASE WHEN PLANNED_DEST_WTT_DATETIME_AFF IS NOT NULL OR PLANNED_DEST_WTT_DATETIME_AFF != ''
--         THEN CASE 
--             WHEN PLANNED_DEST_WTT_DATETIME_AFF = "" THEN NULL
--             WHEN PLANNED_DEST_WTT_DATETIME_AFF LIKE "%/%"
--                     THEN parse_datetime("%d/%m/%Y %H:%M", PLANNED_DEST_WTT_DATETIME_AFF)
--                     ELSE parse_datetime("%d-%b-%Y %H:%M", PLANNED_DEST_WTT_DATETIME_AFF)
--         END 
--     ELSE NULL
--  END AS PLANNED_DEST_WTT_DATETIME_AFF
,CAST(TRAIN_SERVICE_CODE_AFFECTED AS INT64) AS TRAIN_SERVICE_CODE_AFFECTED
,SERVICE_GROUP_CODE_AFFECTED
,OPERATOR_AFFECTED
,ENGLISH_DAY_TYPE
,APP_TIMETABLE_FLAG_AFF
,TRAIN_SCHEDULE_TYPE_AFFECTED
,TRACTION_TYPE_AFFECTED
,TRAILING_LOAD_AFFECTED
,TIMING_LOAD_AFFECTED
,UNIT_CLASS_AFFECTED
,CAST(INCIDENT_NUMBER AS INT64) AS INCIDENT_NUMBER
,{{ convert_date('INCIDENT_CREATE_DATE')}} as INCIDENT_CREATE_DATE

-- CASE
--     WHEN INCIDENT_CREATE_DATE LIKE "__.%" THEN parse_date("%d.%m.%Y", LEFT(INCIDENT_CREATE_DATE, 10))
--     WHEN INCIDENT_CREATE_DATE LIKE "__/%" THEN parse_date("%d/%m/%Y", LEFT(INCIDENT_CREATE_DATE, 10))
--     WHEN INCIDENT_CREATE_DATE LIKE "__-%" THEN parse_date("%d-%b-%Y", LEFT(INCIDENT_CREATE_DATE, 11))
--     ELSE NULL
-- END AS INCIDENT_CREATE_DATE
,{{ convert_datetime('INCIDENT_START_DATETIME')}} AS INCIDENT_START_DATETIME

-- CASE WHEN INCIDENT_START_DATETIME IS NOT NULL OR INCIDENT_START_DATETIME != ''
--         THEN CASE 
--             WHEN INCIDENT_START_DATETIME = "" THEN NULL
--             WHEN INCIDENT_START_DATETIME LIKE "%/%"
--                     THEN parse_datetime("%d/%m/%Y %H:%M", INCIDENT_START_DATETIME)
--                     ELSE parse_datetime("%d-%b-%Y %H:%M", INCIDENT_START_DATETIME)
--         END 
--     ELSE NULL
--  END AS INCIDENT_START_DATETIME
,{{ convert_datetime('INCIDENT_END_DATETIME')}} AS INCIDENT_END_DATETIME

-- CASE WHEN INCIDENT_END_DATETIME IS NOT NULL OR INCIDENT_END_DATETIME != ''
--         THEN CASE 
--             WHEN INCIDENT_END_DATETIME = "" THEN NULL
--             WHEN INCIDENT_END_DATETIME LIKE "%/%"
--                     THEN parse_datetime("%d/%m/%Y %H:%M", INCIDENT_END_DATETIME)
--                     ELSE parse_datetime("%d-%b-%Y %H:%M", INCIDENT_END_DATETIME)
--         END 
--     ELSE NULL
--  END AS INCIDENT_END_DATETIME
,CAST(SECTION_CODE AS INT64) AS SECTION_CODE
,NETWORK_RAIL_LOCATION_MANAGER
,RESPONSIBLE_MANAGER
,INCIDENT_REASON
,ATTRIBUTION_STATUS
,INCIDENT_EQUIPMENT
,INCIDENT_DESCRIPTION
,REACTIONARY_REASON_CODE
,INCIDENT_RESPONSIBLE_TRAIN
,CASE   WHEN PERFORMANCE_EVENT_CODE = 'A' or PERFORMANCE_EVENT_CODE = 'M' THEN 'Delay'
        WHEN PERFORMANCE_EVENT_CODE in ('C', 'D', 'O', 'P', 'S', 'F') THEN 'Cancellation'
        ELSE NULL END AS PERFORMANCE_EVENT_TYPE
,{{ get_performance_event('EVENT_DATETIME') }} AS EVENT_DATETIME
,CAST(START_STANOX AS INT64) AS START_STANOX
,CAST(END_STANOX AS INT64) AS END_STANOX
,{{ convert_datetime('INCIDENT_END_DATETIME')}} AS INCIDENT_END_DATETIME

-- CASE WHEN EVENT_DATETIME IS NOT NULL OR EVENT_DATETIME != ''
--         THEN CASE 
--             WHEN EVENT_DATETIME = "" THEN NULL
--             WHEN EVENT_DATETIME LIKE "%/%"
--                     THEN parse_datetime("%d/%m/%Y %H:%M", EVENT_DATETIME)
--                     ELSE parse_datetime("%d-%b-%Y %H:%M", EVENT_DATETIME)
--         END 
--     ELSE NULL
--  END AS EVENT_DATETIME
,PFPI_MINUTES
,TRUST_TRAIN_ID_RESP
,TRUST_TRAIN_ID_REACT
FROM raw_cte
WHERE ROW_NR = 1