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
    TRUST_TRAIN_ID_REACT
    )
SELECT  
    financial_year_period,
    origin_departure_date,
    trust_train_id,
    planned_origin_location_code,
    case when planned_origin_gbtt_datetime is null or planned_origin_gbtt_datetime = ""
            then null
            else PARSE_DATETIME("%d/%m/%Y %H:%M", planned_origin_gbtt_datetime) 
        end AS planned_origin_gbtt_datetime,
    case when planned_origin_wtt_datetime is null or planned_origin_wtt_datetime = ""
            then null
            else PARSE_DATETIME("%d/%m/%Y %H:%M", planned_origin_wtt_datetime) 
        end AS planned_origin_wtt_datetime,
    planned_dest_location_code,
    case when planned_dest_gbtt_datetime is null or planned_dest_gbtt_datetime = ""
            then null
            else PARSE_DATETIME("%d/%m/%Y %H:%M", planned_dest_gbtt_datetime) 
        end AS planned_dest_gbtt_datetime,
    case when planned_dest_wtt_datetime is null or planned_dest_wtt_datetime = ""
            then null
            else PARSE_DATETIME("%d/%m/%Y %H:%M", planned_dest_wtt_datetime) 
        end AS planned_dest_wtt_datetime,
    cast(train_service_code as string) as train_service_code,
    service_group_code,
    toc_code,
    english_day_type,
    applicable_timetable_flag,
    train_schedule_type,
    traction_type,
    cast(trailing_load as string),
    timing_load,
    cast(unit_class as string),
    incident_number,
    case when incident_create_date is null or incident_create_date = ""
            then null
            else PARSE_DATE("%d/%m/%Y", LEFT(incident_create_date, 11) )
        end AS incident_create_date,
    case when incident_start_datetime is null or incident_start_datetime = ""
            then null
            else PARSE_DATETIME("%d/%m/%Y %H:%M", incident_start_datetime)
        end AS incident_start_datetime,
    case when incident_end_datetime is null or incident_end_datetime = ""
            then null
            else PARSE_DATETIME("%d/%m/%Y %H:%M", incident_end_datetime) 
        end AS incident_end_datetime,
    section_code,
    nr_location_manager,
    responsible_manager,
    incident_reason,
    attribution_status,
    incident_equipment,
    incident_description,
    react_reason,
    incident_resp_train,
    event_type,
    start_stanox,
    end_stanox,
    case when event_datetime is null or event_datetime = ""
            then null
            else PARSE_DATETIME("%d/%m/%Y %H:%M", event_datetime) 
        end AS event_datetime,
    pfpi_minutes,
    resp_train,
    react_train
FROM `train-delays-analytics.delays_data_all.external_tmp`