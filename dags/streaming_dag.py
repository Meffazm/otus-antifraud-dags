"""DAG: streaming_inference — Kafka producer + Spark Structured Streaming."""
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="streaming_inference", start_date=datetime(2026, 3, 21),
         schedule_interval=None, catchup=False, max_active_runs=1,
         default_args={"retries": 0}, tags=["streaming", "kafka"]) as dag:
    setup = EmptyOperator(task_id="setup_connections")
    create = EmptyOperator(task_id="create_dataproc_cluster")
    producer = EmptyOperator(task_id="run_kafka_producer")
    streaming = EmptyOperator(task_id="run_streaming_inference")
    delete = EmptyOperator(task_id="delete_dataproc_cluster")
    setup >> create >> producer >> streaming >> delete
