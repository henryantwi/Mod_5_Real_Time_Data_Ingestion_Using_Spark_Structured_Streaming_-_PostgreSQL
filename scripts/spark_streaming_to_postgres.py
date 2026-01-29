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

# Import Pydantic validation functions
from models import validate_batch, log_validation_summary, get_log_file_paths

# Import performance tracking
from performance_tracker import get_tracker, PerformanceTracker

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
    Write each micro-batch to PostgreSQL after Pydantic validation.
    Invalid rows are logged and skipped; valid rows are written.
    Performance metrics are tracked for each batch.
    """
    # Get performance tracker
    tracker = get_tracker()
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No data to write")
        return

    # Convert Spark DataFrame rows to dictionaries for validation
    rows = [row.asDict() for row in batch_df.collect()]
    
    # Start tracking this batch
    tracker.start_batch(batch_id, len(rows))
    
    # Validate using Pydantic (pass batch_id for logging)
    valid_rows, invalid_rows = validate_batch(rows, batch_id=batch_id)
    
    # Record validation metrics
    tracker.record_validation(len(valid_rows), len(invalid_rows))
    
    # Log validation summary
    log_validation_summary(batch_id, len(valid_rows), len(invalid_rows))
    
    # If no valid rows, exit early
    if not valid_rows:
        print(f"Batch {batch_id}: All records failed validation, nothing to write")
        tracker.end_batch()
        return

    # Create DataFrame from valid rows
    spark = batch_df.sparkSession
    valid_df = spark.createDataFrame(valid_rows)
    
    # Add created_at timestamp
    df_with_timestamp = valid_df.withColumn("created_at", current_timestamp())

    # Connection properties
    connection_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    try:
        # Track database write time
        tracker.start_db_write()
        
        # Write valid records to PostgreSQL
        df_with_timestamp.write.jdbc(
            url=JDBC_URL,
            table="user_events",
            mode="append",
            properties=connection_properties,
        )
        
        # Record DB write completion
        tracker.record_db_write()

        print(
            f"Batch {batch_id}: Successfully wrote {len(valid_rows)} records to PostgreSQL"
        )
        if invalid_rows:
            print(f"Batch {batch_id}: Skipped {len(invalid_rows)} invalid records")

    except Exception as e:
        print(f"Batch {batch_id}: Error writing to PostgreSQL - {str(e)}")
        raise
    finally:
        # End batch tracking
        batch_metrics = tracker.end_batch()
        print(f"Batch {batch_id}: Processing time {batch_metrics.total_processing_time_ms:.2f}ms, "
              f"Throughput: {batch_metrics.records_per_second:.2f} records/sec")


def main():
    print("=" * 70)
    print("Spark Structured Streaming - E-Commerce Events to PostgreSQL")
    print("=" * 70)
    print(f"Input Directory: {INPUT_DIR}")
    print(f"PostgreSQL URL: {JDBC_URL}")
    print(f"Checkpoint Directory: {CHECKPOINT_DIR}")
    
    # Print log file locations
    log_paths = get_log_file_paths()
    print("-" * 70)
    print("Validation Error Logs:")
    print(f"  Log Directory: {log_paths['log_directory']}")
    print(f"  Error Log: {log_paths['validation_errors_log']}")
    print(f"  Detailed JSON: {log_paths['validation_errors_json']}")
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
        # Print performance summary
        tracker = get_tracker()
        tracker.print_summary()
        
        # Save summary to file
        summary_file = tracker.save_summary_to_file()
        print(f"Performance summary saved to: {summary_file}")
        
        spark.stop()
        print("Spark session closed")


if __name__ == "__main__":
    main()
