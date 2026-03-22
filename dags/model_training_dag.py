"""DAG: model_training — Weekly model retraining with MLflow."""
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="model_training", start_date=datetime(2026, 3, 15),
         schedule_interval="@weekly", catchup=False, max_active_runs=1,
         default_args={"retries": 0}, tags=["training", "mlflow"]) as dag:
    setup = EmptyOperator(task_id="setup_connections")
    create = EmptyOperator(task_id="create_dataproc_cluster")
    train = EmptyOperator(task_id="train_model")
    validate = EmptyOperator(task_id="validate_model")
    delete = EmptyOperator(task_id="delete_dataproc_cluster")
    setup >> create >> train >> validate >> delete
