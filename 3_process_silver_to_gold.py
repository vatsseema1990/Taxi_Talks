
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min, sum
import config
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

def process_silver_to_gold():
    spark = create_spark_session()
    
    print("--- Processing Silver to Gold ---")
    
    # Read from Silver (assuming Parquet)
    silver_path = config.SILVER_DIR
    trip_zone_path = os.path.join(silver_path, "trip_zone")
    trip_type_path = os.path.join(silver_path, "trip_type")
    
    # Check if files exist
    if not os.path.exists(trip_zone_path):
        print(f"Skipping Gold processing: {trip_zone_path} not found.")
        return

    # Read Silver tables
    df_trip_zone = spark.read.parquet(trip_zone_path)
    # df_trip_type might be optional or non-existent in sample
    # df_trip_type = spark.read.parquet(trip_type_path)

    # Register Temp Views for SQL
    df_trip_zone.createOrReplaceTempView("dim_zone")
    
    # Run SQL Aggregations
    # Example: Count zones by borough
    print("Running SQL Aggregations...")
    summary_df = spark.sql("""
        SELECT 
            Borough, 
            count(*) as zone_count 
        FROM dim_zone 
        GROUP BY Borough
    """)
    
    summary_df.show()
    
    # Write to Gold (Parquet locally, BigQuery in Cloud)
    gold_path = config.GOLD_DIR
    summary_output = os.path.join(gold_path, "zone_summary")
    
    if config.IS_LOCAL:
        summary_df.write.mode("overwrite").parquet(summary_output)
        print(f"Gold summary written to {summary_output}")
    else:
        # For Cloud, write to BigQuery
        print(f"Writing to BigQuery: {config.BQ_DATASET}.zone_summary")
        # summary_df.write.format("bigquery") \
        #    .option("table", f"{config.PROJECT_ID}:{config.BQ_DATASET}.zone_summary") \
        #    .save()

    spark.stop()

if __name__ == "__main__":
    process_silver_to_gold()
