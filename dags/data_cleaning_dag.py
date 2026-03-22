"""DAG: data_cleaning
Daily automated data cleaning pipeline.
"""

import uuid
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

YC_S3_CONNECTION = Connection(
    conn_id="yc-s3", conn_type="s3", host=S3_ENDPOINT_URL,
    extra={"aws_access_key_id": S3_ACCESS_KEY, "aws_secret_access_key": S3_SECRET_KEY, "host": S3_ENDPOINT_URL})
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa", conn_type="yandexcloud",
    extra={"extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY, "extra__yandexcloud__service_account_json": DP_SA_JSON})

def setup_airflow_connections(**kwargs):
    session = Session()
    try:
        for conn in [YC_S3_CONNECTION, YC_SA_CONNECTION]:
            if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
                session.add(conn)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def detect_new_files(**kwargs):
    import boto3
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="", Delimiter="/")
    all_txt = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".txt")]
    manifest_key = "cleaned/_processed_files.json"
    processed = set()
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=manifest_key)
        processed = set(json.loads(obj["Body"].read().decode()))
    except Exception:
        pass
    new_files = [f for f in all_txt if f not in processed]
    if new_files:
        kwargs["ti"].xcom_push(key="new_files", value=",".join(new_files))
        kwargs["ti"].xcom_push(key="all_processed", value=json.dumps(list(processed | set(new_files))))
    return len(new_files) > 0

def update_manifest(**kwargs):
    import boto3
    all_processed = kwargs["ti"].xcom_pull(task_ids="detect_new_files", key="all_processed")
    if not all_processed:
        return
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
    s3.put_object(Bucket=S3_BUCKET_NAME, Key="cleaned/_processed_files.json", Body=all_processed.encode())

with DAG(dag_id="data_cleaning", start_date=datetime(2026, 3, 15), schedule_interval="@daily",
         catchup=False, max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}) as dag:
    setup_connections = PythonOperator(task_id="setup_connections", python_callable=setup_airflow_connections)
    detect_new = ShortCircuitOperator(task_id="detect_new_files", python_callable=detect_new_files)
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster", folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-cleaning-{uuid.uuid4().hex[:8]}",
        cluster_description="Ephemeral Spark cluster for data cleaning",
        subnet_id=YC_SUBNET_ID, s3_bucket=S3_BUCKET_NAME, service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY, zone=YC_ZONE, cluster_image_version="2.0",
        masternode_resource_preset="s3-c2-m8", masternode_disk_type="network-ssd", masternode_disk_size=40,
        datanode_resource_preset="s3-c4-m16", datanode_disk_type="network-ssd", datanode_disk_size=128,
        datanode_count=3, computenode_count=0, services=["YARN", "SPARK", "HDFS"],
        connection_id=YC_SA_CONNECTION.conn_id, security_group_ids=[DP_SECURITY_GROUP_ID])
    run_cleaning = DataprocCreatePysparkJobOperator(
        task_id="run_data_cleaning", main_python_file_uri=f"s3a://{S3_BUCKET_NAME}/src/data_cleaning.py",
        connection_id=YC_SA_CONNECTION.conn_id, args=["--bucket", S3_BUCKET_NAME])
    update_processed = PythonOperator(task_id="update_manifest", python_callable=update_manifest)
    delete_cluster = DataprocDeleteClusterOperator(task_id="delete_dataproc_cluster", trigger_rule=TriggerRule.ALL_DONE)
    setup_connections >> detect_new >> create_cluster >> run_cleaning >> update_processed >> delete_cluster
