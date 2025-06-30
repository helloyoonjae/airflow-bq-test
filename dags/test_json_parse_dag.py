from airflow import DAG
from datetime import datetime
import pendulum
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# GCP 연결 ID
gcp_conn_id = "google_cloud_default"
table_id = {
    "projectId": "hyperconnect-464313",
    "datasetId": "dataset01",
    "tableId": "table_a"
}
source_uri = "gs://my-hyper-bucket/input_data.json"
# https://cloud.google.com/bigquery/docs/external-table-definition

with DAG(
    dag_id="test_json_parse_dag",
    #schedule=None,  # 수동 실행
    schedule="0 10 * * *",  # 매일 오전 10시 (KST)
    start_date=datetime(2025, 6, 30, tzinfo=pendulum.timezone("Asia/Seoul")),
    catchup=False,
    tags=["example"],
) as dag:


    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="my-hyper-bucket",
        source_objects=["input_data.json"],
        destination_project_dataset_table="hyperconnect-464313.dataset01.table_b",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="google_cloud_default"
    )

    load_to_bq  # DAG에 태스크 등록
