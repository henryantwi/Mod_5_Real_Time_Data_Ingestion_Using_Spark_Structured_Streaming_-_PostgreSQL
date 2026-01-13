"""
Spark Structured Streaming to PostgreSQL
=========================================
This script monitors a directory for new CSV files, processes them using
Spark Structured Streaming, and writes the data to PostgreSQL in real-time.

Usage:
    spark-submit --jars /opt/spark/jars/postgresql-42.7.1.jar spark_streaming_to_postgres.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# Configuration from environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce_events")
POSTGRES_USER = os.getenv("POSTGRES_USER", "lab_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lab_password")

# Paths
INPUT_DIR = "/opt/spark/work-dir/data"
CHECKPOINT_DIR = "/opt/spark/work-dir/output/checkpoints"

# JDBC URL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Define the schema for CSV files
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("product_category", StringType(), True),
        StructField("product_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("event_timestamp", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("device_type", StringType(), True),
    ]
)


def create_spark_session():
    """Create and configure the Spark session."""
    return (
        SparkSession.builder.appName("EcommerceEventsStreaming")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .getOrCreate()
    )


def write_to_postgres(batch_df, batch_id):
    """
    Write each micro-batch to PostgreSQL.
    This function is called for each batch in the streaming query.
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No data to write")
        return

    # Add created_at timestamp
    df_with_timestamp = batch_df.withColumn("created_at", current_timestamp())

    # Connection properties
    connection_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    try:
        # Write to PostgreSQL
        df_with_timestamp.write.jdbc(
            url=JDBC_URL,
            table="user_events",
            mode="append",
            properties=connection_properties,
        )

        record_count = batch_df.count()
        print(
            f"Batch {batch_id}: Successfully wrote {record_count} records to PostgreSQL"
        )

    except Exception as e:
        print(f"Batch {batch_id}: Error writing to PostgreSQL - {str(e)}")
        raise


def main():
    print("=" * 70)
    print("Spark Structured Streaming - E-Commerce Events to PostgreSQL")
    print("=" * 70)
    print(f"Input Directory: {INPUT_DIR}")
    print(f"PostgreSQL URL: {JDBC_URL}")
    print(f"Checkpoint Directory: {CHECKPOINT_DIR}")
    print("=" * 70)

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created successfully")

    # Create checkpoint directory
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    # Read streaming data from CSV files
    print(f"Starting to monitor directory: {INPUT_DIR}")

    streaming_df = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .csv(INPUT_DIR)
    )

    # Transform: Convert event_timestamp string to timestamp type
    transformed_df = streaming_df.withColumn(
        "event_timestamp", col("event_timestamp").cast(TimestampType())
    )

    # Write stream to PostgreSQL using foreachBatch
    query = (
        transformed_df.writeStream.foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Streaming query started. Waiting for data...")
    print("Press Ctrl+C to stop the streaming job")
    print("-" * 70)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming query...")
        query.stop()
        print("Streaming query stopped")
    finally:
        spark.stop()
        print("Spark session closed")


if __name__ == "__main__":
    main()
