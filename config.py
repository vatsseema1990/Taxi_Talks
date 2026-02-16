
import os

# --- Configuration ---

# Set this to False when deploying to GCP
IS_LOCAL = True

# Project ID (Replace with your actual GCP Project ID)
PROJECT_ID = "taxi-talks"

# GCS Bucket Name (Replace with your actual bucket name)
BUCKET_NAME = "taxi-talks-bucket"

# BigQuery Dataset Name
BQ_DATASET = "taxi_talks_gold"

# Paths
if IS_LOCAL:
    # Local paths for testing
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "Sample Data") # Updated to point to existing Sample Data
    BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
    SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
    GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
    MODEL_DIR = os.path.join(BASE_DIR, "models")
else:
    # GCS paths for production
    BASE_DIR = f"gs://{BUCKET_NAME}"
    DATA_DIR = None # Not used in cloud, source is GCS
    BRONZE_DIR = f"gs://{BUCKET_NAME}/bronze"
    SILVER_DIR = f"gs://{BUCKET_NAME}/silver"
    GOLD_DIR = f"gs://{BUCKET_NAME}/gold"
    MODEL_DIR = f"gs://{BUCKET_NAME}/models"

# Ensure local directories exist if running locally
if IS_LOCAL:
    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)
    os.makedirs(MODEL_DIR, exist_ok=True)

print(f"Running in {'LOCAL' if IS_LOCAL else 'CLOUD'} mode.")
print(f"Bronze Path: {BRONZE_DIR}")
print(f"Silver Path: {SILVER_DIR}")
print(f"Gold Path: {GOLD_DIR}")
