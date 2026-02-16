
import os
import shutil
from google.cloud import storage
import config

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def ingest_data():
    if config.IS_LOCAL:
        print("--- Local Ingestion ---")
        # Ensure bronze directory exists
        os.makedirs(config.BRONZE_DIR, exist_ok=True)
        
        # Copy files from Sample Data to Bronze
        source_dir = config.DATA_DIR
        if not os.path.exists(source_dir):
             # Fallback if DATA_DIR is not set correctly or relative path issue
             source_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Sample Data")

        print(f"Source Directory: {source_dir}")
        
        for filename in os.listdir(source_dir):
            if filename.endswith(".csv") or filename.endswith(".parquet"):
                source_file = os.path.join(source_dir, filename)
                dest_file = os.path.join(config.BRONZE_DIR, filename)
                shutil.copy2(source_file, dest_file)
                print(f"Copied {filename} to {dest_file}")
    else:
        print("--- Cloud Ingestion ---")
        # Initialize GCS client
        # In a real scenario, you might list files from a source bucket or local directory
        # Here we assume we are uploading local sample files to GCS Bronze bucket
        source_dir = "Sample Data" # Assuming script is run from project root
        
        for filename in os.listdir(source_dir):
            if filename.endswith(".csv") or filename.endswith(".parquet"):
                source_file = os.path.join(source_dir, filename)
                destination_blob_name = f"bronze/{filename}"
                upload_to_gcs(config.BUCKET_NAME, source_file, destination_blob_name)

if __name__ == "__main__":
    ingest_data()
