"""DAG: data_cleaning — Daily automated data cleaning pipeline."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="data_cleaning", start_date=datetime(2026, 3, 15),
         schedule_interval="@daily", catchup=False, max_active_runs=1,
         default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
         tags=["cleaning"]) as dag:
    start = EmptyOperator(task_id="start")
    detect = EmptyOperator(task_id="detect_new_files")
    create = EmptyOperator(task_id="create_dataproc_cluster")
    clean = EmptyOperator(task_id="run_data_cleaning")
    update = EmptyOperator(task_id="update_manifest")
    delete = EmptyOperator(task_id="delete_dataproc_cluster")
    start >> detect >> create >> clean >> update >> delete
