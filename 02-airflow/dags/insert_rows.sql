INSERT INTO `train-delays-analytics.delays_data_all.train_delays_all`
    (
    FINANCIAL_YEAR_AND_PERIOD,
    ORIGIN_DEPARTURE_DATE,
    TRUST_TRAIN_ID_AFFECTED,
    PLANNED_ORIG_LOC_CODE_AFF,
    PLANNED_ORIG_GBTT_DATETIME_AFF,
    PLANNED_ORIG_WTT_DATETIME_AFF,
    PLANNED_DEST_LOC_CODE_AFFECTED,
    PLANNED_DEST_GBTT_DATETIME_AFF,
    PLANNED_DEST_WTT_DATETIME_AFF,
    TRAIN_SERVICE_CODE_AFFECTED,
    SERVICE_GROUP_CODE_AFFECTED,
    OPERATOR_AFFECTED,
    ENGLISH_DAY_TYPE,
    APP_TIMETABLE_FLAG_AFF,
    TRAIN_SCHEDULE_TYPE_AFFECTED,
    TRACTION_TYPE_AFFECTED,
    TRAILING_LOAD_AFFECTED,
    TIMING_LOAD_AFFECTED,
    UNIT_CLASS_AFFECTED,
    INCIDENT_NUMBER,
    INCIDENT_CREATE_DATE,
    INCIDENT_START_DATETIME,
    INCIDENT_END_DATETIME,
    SECTION_CODE,
    NETWORK_RAIL_LOCATION_MANAGER,
    RESPONSIBLE_MANAGER,
    INCIDENT_REASON,
    ATTRIBUTION_STATUS,
    INCIDENT_EQUIPMENT,
    INCIDENT_DESCRIPTION,
    REACTIONARY_REASON_CODE,
    INCIDENT_RESPONSIBLE_TRAIN,
    PERFORMANCE_EVENT_CODE,
    START_STANOX,
    END_STANOX,
    EVENT_DATETIME,
    PFPI_MINUTES,
    TRUST_TRAIN_ID_RESP,
    TRUST_TRAIN_ID_REACT,
    insert_datetime
    )
SELECT  
    FINANCIAL_YEAR_AND_PERIOD,
    ORIGIN_DEPARTURE_DATE,
    TRUST_TRAIN_ID_AFFECTED,
    PLANNED_ORIG_LOC_CODE_AFF,
    PLANNED_ORIG_GBTT_DATETIME_AFF,
    PLANNED_ORIG_WTT_DATETIME_AFF,
    PLANNED_DEST_LOC_CODE_AFFECTED,
    PLANNED_DEST_GBTT_DATETIME_AFF,
    PLANNED_DEST_WTT_DATETIME_AFF,
    TRAIN_SERVICE_CODE_AFFECTED,
    SERVICE_GROUP_CODE_AFFECTED,
    OPERATOR_AFFECTED,
    ENGLISH_DAY_TYPE,
    APP_TIMETABLE_FLAG_AFF,
    TRAIN_SCHEDULE_TYPE_AFFECTED,
    TRACTION_TYPE_AFFECTED,
    TRAILING_LOAD_AFFECTED,
    TIMING_LOAD_AFFECTED,
    UNIT_CLASS_AFFECTED,
    INCIDENT_NUMBER,
    INCIDENT_CREATE_DATE,
    INCIDENT_START_DATETIME,
    INCIDENT_END_DATETIME,
    SECTION_CODE,
    NETWORK_RAIL_LOCATION_MANAGER,
    RESPONSIBLE_MANAGER,
    INCIDENT_REASON,
    ATTRIBUTION_STATUS,
    INCIDENT_EQUIPMENT,
    INCIDENT_DESCRIPTION,
    REACTIONARY_REASON_CODE,
    INCIDENT_RESPONSIBLE_TRAIN,
    PERFORMANCE_EVENT_CODE,
    START_STANOX,
    END_STANOX,
    EVENT_DATETIME,
    PFPI_MINUTES,
    TRUST_TRAIN_ID_RESP,
    TRUST_TRAIN_ID_REACT,
    current_datetime
FROM `train-delays-analytics.delays_data_all.external_tmp`