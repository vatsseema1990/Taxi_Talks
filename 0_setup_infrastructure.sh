
#!/bin/bash

# Configuration
PROJECT_ID="taxi-talks"
REGION="us-central1"
BUCKET_NAME="taxi-talks-bucket"
CLUSTER_NAME="taxi-talks-cluster"
BQ_DATASET="taxi_talks_gold"

echo "Setting up infrastructure for $PROJECT_ID..."

# 1. Enable APIs
gcloud services enable dataproc.googleapis.com \
    compute.googleapis.com \
    storage-component.googleapis.com \
    bigquery.googleapis.com

# 2. Create GCS Bucket
echo "Creating GCS Bucket: gs://$BUCKET_NAME"
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME/

# 3. Create BigQuery Dataset
echo "Creating BigQuery Dataset: $BQ_DATASET"
bq --location=$REGION mk -d --description "Taxi Talks Gold Data" $PROJECT_ID:$BQ_DATASET

# 4. Create Dataproc Cluster
echo "Creating Dataproc Cluster: $CLUSTER_NAME"
gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --zone $REGION-a \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size 50GB \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 50GB \
    --image-version 2.0-debian10 \
    --project $PROJECT_ID

echo "Infrastructure setup complete."
