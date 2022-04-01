import os
from datetime import datetime
import json

import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator, BigQueryDeleteTableOperator

# from format_to_parquet import format_to_parquet
# from local_to_gcs import upload

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Google cloud parameters
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'delays_data_all')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2018,5,1),
    "end_date": datetime(2019,1,1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id = "ingest_fact_data",
    schedule_interval = "0 6 2 * *",
    default_args = default_args,
    catchup = True,
    max_active_runs = 2,
) as dag:

    def get_source_link(execdate: str, **context) -> tuple:
        execution_calmonth = execdate[:6]
        execution_year =  execdate[:4]      
        with open(f'{AIRFLOW_HOME}/raw/source_files.json') as json_file:
            source_dict = json.load(json_file)
        source_link = source_dict[execution_calmonth]
        outfile = f'train-delays-{execution_calmonth}'
        context['ti'].xcom_push(key='source_link', value=source_link)
        context['ti'].xcom_push(key='output_file', value=outfile)
        context['ti'].xcom_push(key='calmonth', value=execution_calmonth)
        context['ti'].xcom_push(key='calyear', value=execution_year)
        print(f'found link {source_link} for execution calmonth {execution_calmonth}')

    def format_to_parquet(src_file):
        table = pv.read_csv(src_file)
        pq.write_table(table, src_file.replace('.csv', '.parquet').replace('raw', 'pq'))

    def upload_to_gcs(execdate: str, bucket, object_name, local_file):        
        exec_year = execdate[:4]
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(f"raw/{exec_year}/{object_name}")
        blob.upload_from_filename(local_file)

    get_download_link_task = PythonOperator(
        task_id="get_download_link",
        python_callable=get_source_link,
        provide_context=True,
        retries=1,
        op_kwargs={
            "execdate": "{{ ds_nodash }}",
            },

    )

    SOURCE_LINK = "{{ ti.xcom_pull(key=\"source_link\") }}"
    OUTPUT_FILE = "{{ ti.xcom_pull(key=\"output_file\") }}"
    CALMONTH = "{{ ti.xcom_pull(key=\"calmonth\") }}"
    CALYEAR = "{{ ti.xcom_pull(key=\"calyear\") }}"

    wget_task = BashOperator(
        task_id = 'download_source',
        bash_command=f"curl -sSf {SOURCE_LINK} > {AIRFLOW_HOME}/raw/{OUTPUT_FILE}.zip \
            && unzip -p {AIRFLOW_HOME}/raw/{OUTPUT_FILE}.zip > {AIRFLOW_HOME}/raw/{OUTPUT_FILE}.csv",
        do_xcom_push=False
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/raw/{OUTPUT_FILE}.csv",
            },

    )
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "execdate": "{{ ds_nodash }}",
            "bucket": BUCKET,
            "object_name": f"{OUTPUT_FILE}.parquet",
            "local_file": f"{AIRFLOW_HOME}/pq/{OUTPUT_FILE}.parquet",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_tmp",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{CALYEAR}/{OUTPUT_FILE}.parquet"],
            },
        },
    )

    bq_insert_rows_task = BigQueryInsertJobOperator(
        task_id=f"bq_insert_new_rows_task",
        configuration={
            "query": {
                "query": "{% include 'insert_rows.sql' %}",
                "useLegacySql": False,
            }
        }
    )

    delete_external_table_task = BigQueryDeleteTableOperator(
        task_id="delete_external_table_task",
        deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.external_tmp",
    )

    get_download_link_task >> wget_task >> format_to_parquet_task >> local_to_gcs_task >> \
        bigquery_external_table_task >> bq_insert_rows_task >> delete_external_table_task

