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
WHERE INCIDENT_NUMBER IS NOT NULL and INCIDENT_CREATE_DATE IS NOT NULL
)
SELECT
{{ dbt_utils.surrogate_key(['INCIDENT_NUMBER', 'INCIDENT_CREATE_DATE']) }} AS DELAY_ID
,FINANCIAL_YEAR_AND_PERIOD
,{{ convert_date('ORIGIN_DEPARTURE_DATE')}} as ORIGIN_DEPARTURE_DATE
,TRUST_TRAIN_ID_AFFECTED
,SAFE_CAST (PLANNED_ORIG_LOC_CODE_AFF AS INT64) AS PLANNED_ORIG_LOC_CODE_AFF
,{{ convert_datetime('PLANNED_ORIG_GBTT_DATETIME_AFF')}} AS PLANNED_ORIG_GBTT_DATETIME_AFF
,{{ convert_datetime('PLANNED_ORIG_WTT_DATETIME_AFF')}} AS PLANNED_ORIG_WTT_DATETIME_AFF
,SAFE_CAST(PLANNED_DEST_LOC_CODE_AFFECTED AS INT64) PLANNED_DEST_LOC_CODE_AFFECTED
,{{ convert_datetime('PLANNED_DEST_GBTT_DATETIME_AFF')}} AS PLANNED_DEST_GBTT_DATETIME_AFF
,{{ convert_datetime('PLANNED_DEST_WTT_DATETIME_AFF')}} AS PLANNED_DEST_WTT_DATETIME_AFF
,SAFE_CAST(TRAIN_SERVICE_CODE_AFFECTED AS INT64) AS TRAIN_SERVICE_CODE_AFFECTED
,SERVICE_GROUP_CODE_AFFECTED
,OPERATOR_AFFECTED
,ENGLISH_DAY_TYPE
,APP_TIMETABLE_FLAG_AFF
,TRAIN_SCHEDULE_TYPE_AFFECTED
,TRACTION_TYPE_AFFECTED
,TRAILING_LOAD_AFFECTED
,TIMING_LOAD_AFFECTED
,UNIT_CLASS_AFFECTED
,SAFE_CAST(INCIDENT_NUMBER AS INT64) AS INCIDENT_NUMBER
,{{ convert_date('INCIDENT_CREATE_DATE')}} as INCIDENT_CREATE_DATE
,{{ convert_datetime('INCIDENT_START_DATETIME') }} AS INCIDENT_START_DATETIME
,{{ convert_datetime('INCIDENT_END_DATETIME') }} AS INCIDENT_END_DATETIME
,SECTION_CODE
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
,{{ get_performance_event ('PERFORMANCE_EVENT_CODE') }} AS PERFORMANCE_EVENT
,{{ convert_datetime('EVENT_DATETIME') }} AS EVENT_DATETIME
,SAFE_CAST(START_STANOX AS INT64) AS START_STANOX
,SAFE_CAST(END_STANOX AS INT64) AS END_STANOX
,PFPI_MINUTES
,TRUST_TRAIN_ID_RESP
,TRUST_TRAIN_ID_REACT
FROM raw_cte
WHERE ROW_NR = 1