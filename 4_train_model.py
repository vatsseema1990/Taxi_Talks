
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import config
import os
import shutil

def create_spark_session():
    return SparkSession.builder \
        .appName("TaxiFareModelTraining") \
        .getOrCreate()

def train_model():
    spark = create_spark_session()
    print("--- Training ML Model ---")
    
    # In a real scenario, we read from Silver or Gold trips data
    # gold_path = os.path.join(config.GOLD_DIR, "fact_trips")
    
    # For this demo with limited sample data, we'll create a dummy dataframe 
    # if we don't have the enriched trips data.
    
    # Create Dummy Training Data
    print("Creating dummy training data for demonstration...")
    data = [(1.0, 10.0), (2.0, 20.0), (3.0, 30.0), (4.0, 40.0)]
    columns = ["distance", "fare"]
    df = spark.createDataFrame(data, columns)
    
    # Assemble Features
    assembler = VectorAssembler(inputCols=["distance"], outputCol="features")
    train_data = assembler.transform(df)
    
    # Train Linear Regression Model
    lr = LinearRegression(featuresCol="features", labelCol="fare")
    model = lr.fit(train_data)
    
    print(f"Model Intercept: {model.intercept}")
    print(f"Model Coefficients: {model.coefficients}")
    
    # Save Model
    model_path = os.path.join(config.MODEL_DIR, "fare_prediction_model")
    # Clean up old model if exists locally to avoid overwrite error
    if config.IS_LOCAL and os.path.exists(model_path):
        shutil.rmtree(model_path)
        
    model.save(model_path)
    print(f"Model saved to {model_path}")
    
    spark.stop()

if __name__ == "__main__":
    train_model()
