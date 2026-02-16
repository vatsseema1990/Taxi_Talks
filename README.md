
# Taxi Talks - GCP Data Engineering Migration

This project demonstrates a Data Engineering pipeline migrated to Google Cloud Platform (GCP).

## Architecture

1.  **Ingestion**: Bronze Layer (GCS Bucket)
2.  **Processing**: Silver Layer (Parquet/Delta on GCS) using data cleaning and transformation.
3.  **Warehousing**: Gold Layer (BigQuery Tables) using aggregated views.
4.  **AI/ML**: Taxi Fare Prediction using Spark MLlib.
5.  **Orchestration**: Cloud Composer (Airflow) or Local Pipeline Runner.

## Local Execution (Demo)

You can run the entire pipeline locally to verify the logic.

1.  **Prerequisites**:
    - Python 3.8+
    - Java 8/11 (for PySpark)
    - Data in `Sample Data` folder (should be at the project root)

2.  **Setup**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Run Pipeline**:
    ```bash
    python pipeline/pipeline_runner.py
    ```
    This script will sequentially run ingestion, silver processing, gold aggregation, and model training.

4.  **Check Output**:
    - `Taxi_Talks/data/bronze`: Raw data
    - `Taxi_Talks/data/silver`: Processed parquet files
    - `Taxi_Talks/data/gold`: Aggregated results
    - `Taxi_Talks/models`: Trained ML model

## GCP Deployment

To deploy to GCP:

1.  **Configuration**:
    - Edit `config.py` and set `IS_LOCAL = False`.
    - Update `PROJECT_ID`, `BUCKET_NAME`, etc.

2.  **Infrastructure**:
    - Use `0_setup_infrastructure.sh` (create this based on your needs) or Terraform to provision Buckets, Dataproc Cluster, and BigQuery Dataset.

3.  **Deploy Scripts**:
    - Upload scripts to GCS: `gsutil cp *.py gs://<your-bucket>/scripts/`

4.  **Deploy DAG**:
    - Upload `dags/taxi_airflow_dag.py` to your Cloud Composer DAGs bucket.

