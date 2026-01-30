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
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# Import psycopg2 for direct PostgreSQL upsert operations
import psycopg2
from psycopg2.extras import execute_values

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


def upsert_to_postgres(valid_rows: list[dict], batch_id: int) -> dict:
    """
    Upsert records to PostgreSQL using ON CONFLICT DO UPDATE.
    
    This handles:
    - Duplicate event_id: Updates existing record with new values
    - Corrected data: Re-sent records replace errored ones
    - Data deduplication: Same event won't create duplicate rows
    
    Returns:
        dict with 'inserted' and 'updated' counts
    """
    # Upsert SQL with ON CONFLICT DO UPDATE
    # When event_id already exists, update all fields with new values
    upsert_sql = """
        INSERT INTO user_events (
            event_id, user_id, event_type, product_id, product_name,
            product_category, product_price, quantity, event_timestamp,
            session_id, device_type, created_at
        ) VALUES (
            %(event_id)s, %(user_id)s, %(event_type)s, %(product_id)s, %(product_name)s,
            %(product_category)s, %(product_price)s, %(quantity)s, %(event_timestamp)s,
            %(session_id)s, %(device_type)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (event_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            event_type = EXCLUDED.event_type,
            product_id = EXCLUDED.product_id,
            product_name = EXCLUDED.product_name,
            product_category = EXCLUDED.product_category,
            product_price = EXCLUDED.product_price,
            quantity = EXCLUDED.quantity,
            event_timestamp = EXCLUDED.event_timestamp,
            session_id = EXCLUDED.session_id,
            device_type = EXCLUDED.device_type,
            created_at = CURRENT_TIMESTAMP
        RETURNING (xmax = 0) AS inserted;
    """
    # Note: xmax = 0 means INSERT, xmax != 0 means UPDATE
    
    conn = None
    inserted_count = 0
    updated_count = 0
    
    try:
        # Connect to PostgreSQL directly
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        
        with conn.cursor() as cursor:
            for row in valid_rows:
                cursor.execute(upsert_sql, row)
                result = cursor.fetchone()
                if result and result[0]:  # inserted = True
                    inserted_count += 1
                else:
                    updated_count += 1
        
        conn.commit()
        
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()
    
    return {"inserted": inserted_count, "updated": updated_count}


def write_to_postgres(batch_df, batch_id):
    """
    Write each micro-batch to PostgreSQL after Pydantic validation using UPSERT.
    
    This function:
    1. Validates rows using Pydantic
    2. Upserts valid rows using ON CONFLICT DO UPDATE
    3. Logs invalid rows and tracks performance metrics
    
    Upsert handles:
    - Duplicate records: Same event_id won't cause errors
    - Corrected data: Updated values replace old ones
    - Data deduplication: Prevents duplicate inserts
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

    try:
        # Track database write time
        tracker.start_db_write()
        
        # Upsert valid records to PostgreSQL (INSERT or UPDATE)
        upsert_result = upsert_to_postgres(valid_rows, batch_id)
        
        # Record DB write completion
        tracker.record_db_write()
        
        inserted = upsert_result["inserted"]
        updated = upsert_result["updated"]

        print(
            f"Batch {batch_id}: Successfully upserted {len(valid_rows)} records "
            f"({inserted} inserted, {updated} updated)"
        )
        if invalid_rows:
            print(f"Batch {batch_id}: Skipped {len(invalid_rows)} invalid records")

    except Exception as e:
        print(f"Batch {batch_id}: Error upserting to PostgreSQL - {str(e)}")
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
