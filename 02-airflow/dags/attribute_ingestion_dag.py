import os
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator, BigQueryDeleteTableOperator

from convert_to_parquet import xlsx_to_parquet
# from local_to_gcs import upload

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Google cloud parameters
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'delays_data_all')

SOURCE_LINK = r'https://sacuksprodnrdigital0001.blob.core.windows.net/historic-delay-attribution/Reference%20Files/Historic-Delay-Attribution-Glossary.xlsx'
SOURCE_FILE = SOURCE_LINK.split('/')[-1]

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    # "end_date": datetime(2022,5,1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id = "ingest_dim_data",
    schedule_interval = "@once",
    default_args = default_args,
    catchup = True,
    max_active_runs = 1,
) as dag:

    def upload_to_gcs(bucket, object_name, local_file):        
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(f"dim/{object_name}")
        blob.upload_from_filename(local_file)
    
    # wget_task = BashOperator(
    #     task_id = 'download_source',
    #     bash_command=f"ls {AIRFLOW_HOME}/raw/{SOURCE_FILE} >> /dev/null 2>&1 && echo \"Target file is already downloaded -> skipping\" \
    #         || curl -sSf {SOURCE_LINK}",
    #     do_xcom_push=False
    # )

    with open(f'{AIRFLOW_HOME}/dags/dim_info.json') as json_file:
            source_dict = json.load(json_file)

    for dim in [*source_dict.keys()]:

        format_to_parquet_task = PythonOperator(
            task_id=f"format_{dim}_to_parquet_task",
            python_callable=xlsx_to_parquet,
            op_kwargs={
                "inpath" : f"{AIRFLOW_HOME}/dags/{SOURCE_FILE}",
                "outpath": f"{AIRFLOW_HOME}/pq/{dim}.parquet",
                # outpath = f'{inpath[:inpath.rfind("/")].replace("raw", "pq")}/{dim_name}.parquet'
                "dim_name"  : dim,
                "sheet_no"  : source_dict[dim]["sheetNo"],
                "schema"    : source_dict[dim]["schema"],
            },
        )
        
        local_to_gcs_task = PythonOperator(
        task_id=f"upload_{dim}_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "execdate": "{{ ds_nodash }}",
            "bucket": BUCKET,
            "object_name": f"{dim}.parquet",
            "local_file": f"{AIRFLOW_HOME}/pq/{dim}.parquet",
        },
        )

        delete_external_table_task = BigQueryDeleteTableOperator(
        task_id=f"delete_external_{dim}_table_task",
        deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.dim_{dim}_tmp",
        ) 
        
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bigquery_external_{dim}_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"dim_{dim}_tmp",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/dim/{dim}.parquet"],
            },
        },
        )   

        INSERT_BQ_TBL_QUERY = (
        f"INSERT INTO {BIGQUERY_DATASET}.dim_{dim}\
        SELECT *    \
            , CURRENT_DATETIME() AS INSERT_DATETIME\
        FROM {BIGQUERY_DATASET}.dim_{dim}_tmp;"
        )   

        bq_insert_rows_task = BigQueryInsertJobOperator(
            task_id=f"bq_insert_new_{dim}_rows_task",
            configuration={
                "query": {
                    "query": INSERT_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

# ToDo: create external table from the new dim file -> insert new rows to the native BQ table with insert_datetime column
        # create_dim_table_task = 

    # wget_task >> 
        format_to_parquet_task >> local_to_gcs_task >> delete_external_table_task >> bigquery_external_table_task >> bq_insert_rows_task