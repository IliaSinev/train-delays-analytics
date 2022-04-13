from numpy import source
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd
import json

# ToDO: move file schemas to separate json file
schema_pre201909 = {
    "FINANCIAL_YEAR_AND_PERIOD": "string",
    "ORIGIN_DEPARTURE_DATE": "string",
    "TRUST_TRAIN_ID_AFFECTED": "string",
    "PLANNED_ORIG_LOC_CODE_AFF": "string",
    "PLANNED_ORIG_GBTT_DATETIME_AFF": "string",
    "PLANNED_ORIG_WTT_DATETIME_AFF": "string",
    "PLANNED_DEST_LOC_CODE_AFFECTED": "string",
    "PLANNED_DEST_GBTT_DATETIME_AFF": "string",
    "PLANNED_DEST_WTT_DATETIME_AFF": "string",
    "TRAIN_SERVICE_CODE_AFFECTED": "string",
    "SERVICE_GROUP_CODE_AFFECTED": "string",
    "OPERATOR_AFFECTED": "string",
    "ENGLISH_DAY_TYPE": "string",
    "APP_TIMETABLE_FLAG_AFF": "string",
    "TRAIN_SCHEDULE_TYPE_AFFECTED": "string",
    "TRACTION_TYPE_AFFECTED": "string",
    "TRAILING_LOAD_AFFECTED": "string",
    "TIMING_LOAD_AFFECTED": "string",
    "UNIT_CLASS_AFFECTED": "string",
    "INCIDENT_NUMBER": "string",
    "INCIDENT_CREATE_DATE": "string",
    "INCIDENT_START_DATETIME": "string",
    "INCIDENT_END_DATETIME": "string",
    "SECTION_CODE": "string",
    "NETWORK_RAIL_LOCATION_MANAGER": "string",
    "RESPONSIBLE_MANAGER": "string",
    "INCIDENT_REASON": "string",
    "ATTRIBUTION_STATUS": "string",
    "INCIDENT_EQUIPMENT": "string",
    "INCIDENT_DESCRIPTION": "string",
    "REACTIONARY_REASON_CODE": "string",
    "INCIDENT_RESPONSIBLE_TRAIN": "string",
    "PERFORMANCE_EVENT_CODE": "string",
    "START_STANOX": "string",
    "END_STANOX": "string",
    "EVENT_DATETIME": "string",
    "PFPI_MINUTES": "double",
    "TRUST_TRAIN_ID_RESP": "string",
    "TRUST_TRAIN_ID_REACT": "string"}

schema_201910_202004 = {
    "financial_year_period": "string",
    "origin_departure_date": "string",
    "trust_train_id": "string",
    "planned_origin_location_code": "string",
    "planned_origin_wtt_datetime": "string",
    "planned_origin_gbtt_datetime": "string",
    "planned_dest_location_code": "string",
    "planned_dest_wtt_datetime": "string",
    "planned_dest_gbtt_datetime": "string",
    "train_service_code": "string",
    "service_group_code": "string",
    "toc_code": "string",
    "english_day_type": "string",
    "applicable_timetable_flag": "string",
    "train_schedule_type": "string",
    "traction_type": "string",
    "trailing_load": "string",
    "timing_load": "string",
    "unit_class": "string",
    "incident_number": "string",
    "incident_create_date": "string",
    "incident_start_datetime": "string",
    "incident_end_datetime": "string",
    "section_code": "string",
    "nr_location_manager": "string",
    "resp_manager": "string",
    "incident_reason": "string",
    "attribution_status": "string",
    "incident_equipment": "string",
    "incident_description": "string",
    "react_reason": "string",
    "incident_resp_train": "string",
    "resp_train": "string",
    "react_train": "string",
    "event_type": "string",
    "start_stanox": "string",
    "end_stanox": "string",
    "event_datetime": "string",
    "pfpi_minutes": "double",
    "non_pfpi_minutes": "double"
}
schema_202005_202105 = {
    "FINANCIAL_YEAR_PERIOD": "string",
    "ORIGIN_DEPARTURE_DATE": "string",
    "TRUST_TRAIN_ID": "string",
    "PLANNED_ORIGIN_LOCATION_CODE": "string",
    "PLANNED_ORIGIN_WTT_DATETIME": "string",
    "PLANNED_ORIGIN_GBTT_DATETIME": "string",
    "PLANNED_DEST_LOCATION_CODE": "string",
    "PLANNED_DEST_WTT_DATETIME": "string",
    "PLANNED_DEST_GBTT_DATETIME": "string",
    "TRAIN_SERVICE_CODE": "string",
    "SERVICE_GROUP_CODE": "string",
    "TOC_CODE": "string",
    "ENGLISH_DAY_TYPE": "string",
    "APPLICABLE_TIMETABLE_FLAG": "string",
    "TRAIN_SCHEDULE_TYPE": "string",
    "TRACTION_TYPE": "string",
    "TRAILING_LOAD": "string",
    "TIMING_LOAD": "string",
    "UNIT_CLASS": "string",
    "INCIDENT_NUMBER": "string",
    "INCIDENT_CREATE_DATE": "string",
    "INCIDENT_START_DATETIME": "string",
    "INCIDENT_END_DATETIME": "string",
    "SECTION_CODE": "string",
    "NR_LOCATION_MANAGER": "string",
    "RESP_MANAGER": "string",
    "INCIDENT_REASON": "string",
    "ATTRIBUTION_STATUS": "string",
    "INCIDENT_EQUIPMENT": "string",
    "INCIDENT_DESCRIPTION": "string",
    "REACT_REASON": "string",
    "INCIDENT_RESP_TRAIN": "string",
    "RESP_TRAIN": "string",
    "REACT_TRAIN": "string",
    "EVENT_TYPE": "string",
    "START_STANOX": "string",
    "END_STANOX": "string",
    "EVENT_DATETIME": "string",
    "PFPI_MINUTES": "double",
    "NON_PFPI_MINUTES": "double"
}
schema_post202106 = {
    "FINANCIAL_YEAR_PERIOD": "string",
    "ORIGIN_DEPARTURE_DATE": "string",
    "TRUST_TRAIN_ID": "string",
    "PLANNED_ORIGIN_LOCATION_CODE": "string",
    "PLANNED_ORIGIN_WTT_DATETIME": "string",
    "PLANNED_ORIGIN_GBTT_DATETIME": "string",
    "PLANNED_DEST_LOCATION_CODE": "string",
    "PLANNED_DEST_WTT_DATETIME": "string",
    "PLANNED_DEST_GBTT_DATETIME": "string",
    "TRAIN_SERVICE_CODE": "string",
    "SERVICE_GROUP_CODE": "string",
    "TOC_CODE": "string",
    "ENGLISH_DAY_TYPE": "string",
    "APPLICABLE_TIMETABLE_FLAG": "string",
    "TRAIN_SCHEDULE_TYPE": "string",
    "TRACTION_TYPE": "string",
    "TRAILING_LOAD": "string",
    "TIMING_LOAD": "string",
    "UNIT_CLASS": "string",
    "INCIDENT_NUMBER": "string",
    "INCIDENT_CREATE_DATE": "string",
    "INCIDENT_START_DATETIME": "string",
    "INCIDENT_END_DATETIME": "string",
    "SECTION_CODE": "string",
    "NR_LOCATION_MANAGER": "string",
    "RESPONSIBLE_MANAGER": "string",
    "INCIDENT_REASON": "string",
    "ATTRIBUTION_STATUS": "string",
    "INCIDENT_EQUIPMENT": "string",
    "INCIDENT_DESCRIPTION": "string",
    "REACT_REASON": "string",
    "INCIDENT_RESP_TRAIN": "string",
    "RESP_TRAIN": "string",
    "REACT_TRAIN": "string",
    "EVENT_TYPE": "string",
    "START_STANOX": "string",
    "END_STANOX": "string",
    "EVENT_DATETIME": "string",
    "PFPI_MINUTES": "double",
    "NON_PFPI_MINUTES": "double"
}

col_names = [
    "FINANCIAL_YEAR_AND_PERIOD",
    "ORIGIN_DEPARTURE_DATE",
    "TRUST_TRAIN_ID_AFFECTED",
    "PLANNED_ORIG_LOC_CODE_AFF",
    "PLANNED_ORIG_WTT_DATETIME_AFF",
    "PLANNED_ORIG_GBTT_DATETIME_AFF",
    "PLANNED_DEST_LOC_CODE_AFFECTED",
    "PLANNED_DEST_WTT_DATETIME_AFF",
    "PLANNED_DEST_GBTT_DATETIME_AFF",
    "TRAIN_SERVICE_CODE_AFFECTED",
    "SERVICE_GROUP_CODE_AFFECTED",
    "OPERATOR_AFFECTED",
    "ENGLISH_DAY_TYPE",
    "APP_TIMETABLE_FLAG_AFF",
    "TRAIN_SCHEDULE_TYPE_AFFECTED",
    "TRACTION_TYPE_AFFECTED",
    "TRAILING_LOAD_AFFECTED",
    "TIMING_LOAD_AFFECTED",
    "UNIT_CLASS_AFFECTED",
    "INCIDENT_NUMBER",
    "INCIDENT_CREATE_DATE",
    "INCIDENT_START_DATETIME",
    "INCIDENT_END_DATETIME",
    "SECTION_CODE",
    "NETWORK_RAIL_LOCATION_MANAGER",
    "RESPONSIBLE_MANAGER",
    "INCIDENT_REASON",
    "ATTRIBUTION_STATUS",
    "INCIDENT_EQUIPMENT",
    "INCIDENT_DESCRIPTION",
    "REACTIONARY_REASON_CODE",
    "INCIDENT_RESPONSIBLE_TRAIN",
    "TRUST_TRAIN_ID_RESP",
    "TRUST_TRAIN_ID_REACT",
    "PERFORMANCE_EVENT_CODE",
    "START_STANOX",
    "END_STANOX",
    "EVENT_DATETIME",
    "PFPI_MINUTES"
]

dim_schemas = {
    "Stanox Codes": {
        'STANOX NO.':'string',
        'FULL NAME':'string', 
        'CRS CODE':'string', 
        'Route Description':'string'},
    "Incident Reason": {
        "Incident Category": "string",
         "Incident Reason": "string",
         "Incident Reason Name": "string",
         "Incident Category Description": "string",
         "Incident Reason Description": "string",
         "Incident JPIP Category": "string",
         "Incident Category Super Group Code": "string"},
    "Responsible Manager": {
        "Responsible Manager": "string",
        "Responsible Manager Name": "string",
        "Responsible Organisation": "string",
        "Responsible Organisation Full Name": "string",
        "Responsible Organisation Name": "string",
        "Responsible Org NR-TOC/FOC Others": "string"},
    "Reactionary Reasons":{
        "Reactionary Reason Code" : "string",
        "Reactionary Reason Description": "string",
        "Reactionary Reason Name": "string"},
    "Performance Event":{
        "Performance Event Types": "string",
        "Performance Event Group": "string",
        "Performance Event Name": "string"},
    "Service Group": {
        "Service Group Code": "string",
        "Service Group Description": "string",
        "TSC": "string",
        "TSC Description": "string"},
    "Operator": {
        "Operator Code": "string",
        "Operator Name": "string"},
    "Train Service": {
        "Service Group Code": "string",
        "Service Group Description": "string",
        "Train Service Code": "string",
        "TSC Description": "string"}
}

def csv_to_parquet(src_file, execcalmonth):
#
    if int(execcalmonth) <= 201909:
        print(f'Applying schema_pre201909 for {src_file}')
        ConvertOptions = pv.ConvertOptions(column_types = schema_pre201909)
        table = pv.read_csv(src_file, 
                    convert_options=ConvertOptions)
    elif int(execcalmonth) >= 201910 and int(execcalmonth) <= 202004:
        print(f'Applying schema_201910_202004 for {src_file}')
        ConvertOptions = pv.ConvertOptions(column_types = schema_201910_202004)
        table = pv.read_csv(src_file, 
                    convert_options=ConvertOptions)
        for i in range(table.num_columns, 39, -1):
            table = table.remove_column(i-1)
        table = table.rename_columns(col_names)
    elif int(execcalmonth) >= 202005 and int(execcalmonth) <= 202105:
        print(f'Applying schema_202005_202105 for {src_file}')
        ConvertOptions = pv.ConvertOptions(column_types = schema_202005_202105)
        table = pv.read_csv(src_file, 
                        convert_options=ConvertOptions)
        for i in range(table.num_columns, 39, -1):
            table = table.remove_column(i-1)
        table = table.rename_columns(col_names)
    elif int(execcalmonth) >=202106:
        print(f'Applying schema_post202106 for {src_file}')
        ConvertOptions = pv.ConvertOptions(column_types = schema_post202106)
        table = pv.read_csv(src_file, 
                        convert_options=ConvertOptions)
        for i in range(table.num_columns, 39, -1):
            table = table.remove_column(i-1)
        table = table.rename_columns(col_names)
    
    print(f'Saved table has schema: \n {table.schema} \n Saved table has {table.num_columns} columns')
    pq.write_table(table, src_file.replace('.csv', '.parquet').replace('raw', 'pq'))

def xlsx_to_parquet(inpath: str, outpath: str, dim_name: str, sheet_no: str, schema: dict):
    print(f'Converting {dim_name} dimension table')
    df_dim = pd.read_excel(inpath
                        # , engine='openpyxl'
                        , sheet_name = sheet_no
                        , names = [*schema.keys()]
                        , dtype = schema
                        , skipfooter=2)
    df_dim.to_parquet(outpath)
