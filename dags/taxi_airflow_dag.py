
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
import os

# --- Configurations ---
PROJECT_ID = "taxi-talks"
REGION = "us-central1"
CLUSTER_NAME = "taxi-talks-cluster"
BUCKET_NAME = "taxi-talks-bucket"

# PySpark Job Configurations
PYSPARK_JOB_BRONZE_TO_SILVER = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/2_process_bronze_to_silver.py",
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
    },
}

PYSPARK_JOB_SILVER_TO_GOLD = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/3_process_silver_to_gold.py",
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
    },
}

PYSPARK_JOB_TRAIN_MODEL = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/4_train_model.py",
    },
}

# --- DAG Definition ---
with DAG(
    "nyc_taxi_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task 1: Process Bronze to Silver
    process_bronze_to_silver = DataprocSubmitJobOperator(
        task_id="process_bronze_to_silver",
        job=PYSPARK_JOB_BRONZE_TO_SILVER,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Task 2: Process Silver to Gold
    process_silver_to_gold = DataprocSubmitJobOperator(
        task_id="process_silver_to_gold",
        job=PYSPARK_JOB_SILVER_TO_GOLD,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Task 3: Train ML Model
    train_model = DataprocSubmitJobOperator(
        task_id="train_model",
        job=PYSPARK_JOB_TRAIN_MODEL,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Dependencies
    process_bronze_to_silver >> process_silver_to_gold >> train_model
