# otus-antifraud-dags

Airflow DAGs for the otus-antifraud project.

## DAGs

- **data_cleaning** — daily data cleaning pipeline (S3 CSV -> Parquet)
- **model_training** — weekly model retraining + A/B validation with MLflow
- **streaming_inference** — manual streaming inference (Kafka + Spark Structured Streaming)

## Usage

This repo is synced by Airflow deployed in K8s via `gitSync`.
