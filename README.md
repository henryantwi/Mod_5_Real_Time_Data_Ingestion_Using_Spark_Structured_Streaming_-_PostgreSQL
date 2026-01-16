# Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

A real-time data pipeline that simulates an e-commerce platform tracking user activity. This project demonstrates how to generate fake user events, stream them using Apache Spark Structured Streaming, and store processed data in a PostgreSQL database.

## Table of Contents

- [Project Summary](#project-summary)
- [Learning Objectives](#learning-objectives)
- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Usage](#detailed-usage)
- [Components](#components)
- [Deliverables](#deliverables)
- [Troubleshooting](#troubleshooting)

## Project Summary

This project builds a complete real-time data ingestion pipeline that:

1. **Simulates Data**: Generates fake e-commerce events (product views, purchases, cart actions) as CSV files
2. **Streams with Spark**: Uses Spark Structured Streaming to monitor and process new CSV files in real-time
3. **Stores in PostgreSQL**: Writes processed data to a PostgreSQL database for querying and analytics

### Key Features

- Real-time data streaming and processing
- Data transformation using Spark SQL
- Relational database integration
- Continuous data ingestion handling
- Docker-based containerized environment

## Learning Objectives

By completing this project, you will:

- Simulate and ingest streaming data
- Use Spark Structured Streaming to process data in real-time
- Store and verify processed data in a PostgreSQL database
- Understand the architecture of a real-time data pipeline
- Measure and evaluate system performance

## Architecture Overview

```
┌─────────────────┐
│  Data Generator │
│   (Python)      │
└────────┬────────┘
         │ CSV Files
         ▼
┌─────────────────┐
│   Data Folder   │
│   (Monitored)   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Spark Structured Streaming     │
│  - Monitors directory           │
│  - Processes new files          │
│  - Transforms data              │
└────────┬────────────────────────┘
         │ JDBC
         ▼
┌─────────────────┐
│   PostgreSQL    │
│   Database      │
└─────────────────┘
```

### Data Flow

1. **Data Generation**: `data_generator.py` creates CSV files with e-commerce events
2. **File Monitoring**: Spark Structured Streaming watches the `data/` directory
3. **Processing**: New CSV files are automatically detected and processed
4. **Transformation**: Data is cleaned, validated, and transformed
5. **Storage**: Processed data is written to PostgreSQL `user_events` table

## Project Structure

```
.
├── docker-compose.yml              # Docker orchestration configuration
├── Dockerfile.spark                # Spark container definition
├── README.md                       # This file
├── user_guide.md                   # Detailed usage instructions
├── lab_work.md                     # Lab requirements and objectives
├── postgres_connection_details.txt # Database connection information
│
├── scripts/
│   ├── data_generator.py          # Generates fake e-commerce events
│   └── spark_streaming_to_postgres.py  # Spark streaming job
│
├── sql/
│   └── postgres_setup.sql         # Database schema and setup
│
├── data/                           # Input directory (monitored by Spark)
├── output/                         # Spark checkpoints and output
└── .gitignore                      # Git ignore rules
```

## Prerequisites

Before starting, ensure you have:

- **Docker Desktop** installed and running
- **Docker Compose** (included with Docker Desktop)
- **Terminal/PowerShell** access
- **At least 4GB RAM** available for Docker
- **Internet connection** (for downloading Docker images)

## Quick Start

### 1. Start the Environment

```bash
# Navigate to project directory
cd "path/to/project"

# Build and start containers
docker-compose up -d --build
```

Wait for both containers to be healthy (about 30-60 seconds).

### 2. Verify Containers

```bash
docker-compose ps
```

You should see both `realtime_postgres` and `realtime_spark` running.

### 3. Start Spark Streaming

```bash
# Enter Spark container
docker exec -it realtime_spark bash

# Run the streaming job
spark-submit --jars /opt/spark/jars/postgresql-42.7.1.jar scripts/spark_streaming_to_postgres.py
```

Keep this terminal running - it monitors for new CSV files.

### 4. Generate Data

In a **new terminal**:

```bash
# Enter Spark container
docker exec -it realtime_spark bash

# Generate test data
python3 scripts/data_generator.py --events 10 --batches 5 --interval 3
```

### 5. Verify Data in PostgreSQL

```bash
# Enter PostgreSQL container
docker exec -it realtime_postgres psql -U lab_user -d ecommerce_events

# Check records
SELECT COUNT(*) FROM user_events;

# View recent events
SELECT * FROM user_events ORDER BY event_timestamp DESC LIMIT 10;

# Exit
\q
```

## Detailed Usage

### Data Generator Options

```bash
# Generate a single batch of 10 events
python3 scripts/data_generator.py

# Generate 5 batches of 20 events each, 3 seconds apart
python3 scripts/data_generator.py --events 20 --batches 5 --interval 3

# Run continuously until interrupted
python3 scripts/data_generator.py --events 10 --interval 5 --continuous
```

**Parameters:**
- `--events N`: Number of events per batch (default: 10)
- `--batches N`: Number of batches to generate (default: 1)
- `--interval SECONDS`: Seconds between batches (default: 5.0)
- `--continuous`: Run continuously until Ctrl+C

### Spark Streaming Job

The streaming job:
- Monitors `/opt/spark/work-dir/data` for new CSV files
- Processes files in micro-batches (every 10 seconds)
- Transforms and validates data
- Writes to PostgreSQL `user_events` table
- Maintains checkpoints in `/opt/spark/work-dir/output/checkpoints`

### Accessing Spark Web UIs

- **Spark Master UI**: http://localhost:8080
- **Spark Application UI**: http://localhost:4040

### Stopping the Environment

```bash
# Stop containers (preserves data)
docker-compose stop

# Stop and remove containers (data persists in volume)
docker-compose down

# Stop and remove everything including data
docker-compose down -v
```

## Components

### 1. Data Generator (`scripts/data_generator.py`)

Generates realistic e-commerce events including:
- Event types: view, add_to_cart, remove_from_cart, purchase, wishlist
- Product information: ID, name, category, price
- User data: user ID, session ID, device type
- Timestamps: event and creation timestamps

### 2. Spark Streaming Job (`scripts/spark_streaming_to_postgres.py`)

- Monitors directory for new CSV files
- Reads and validates data using defined schema
- Transforms timestamps and data types
- Writes to PostgreSQL using JDBC
- Handles errors and provides logging

### 3. PostgreSQL Setup (`sql/postgres_setup.sql`)

Creates:
- `user_events` table with proper schema
- Indexes for performance optimization
- `event_summary` view for analytics
- User permissions

### 4. Docker Configuration

- **PostgreSQL**: Database server with automatic schema initialization
- **Spark**: Custom image with Python dependencies and JDBC driver
- **Networking**: Bridge network for container communication
- **Volumes**: Persistent data storage

## Deliverables

| Deliverable | Description | Status |
|-------------|-------------|--------|
| `data_generator.py` | Python script to generate CSV event data | Complete |
| `spark_streaming_to_postgres.py` | Spark Structured Streaming job | Complete |
| `postgres_setup.sql` | SQL script to create database and table | Complete |
| `postgres_connection_details.txt` | Connection information | Complete |
| `user_guide.md` | Step-by-step instructions | Complete |
| `project_overview.md` | System components and flow explanation | Pending |
| `test_cases.md` | Manual test plan with expected outcomes | Pending |
| `performance_metrics.md` | System performance report | Pending |
| `system_architecture.png` | Data flow diagram | Pending |

## Testing Checklist

- [ ] CSV files are being generated correctly
- [ ] Spark detects and processes new files
- [ ] Data transformations are correct
- [ ] Data is written to PostgreSQL without errors
- [ ] Performance metrics are within expected limits
- [ ] Checkpoints are created and maintained
- [ ] Error handling works correctly
- [ ] Database indexes improve query performance

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Container won't start | Run `docker-compose logs <service>` to check errors |
| Spark can't connect to PostgreSQL | Ensure postgres container is healthy first: `docker-compose ps` |
| No data appearing in database | Check that CSV files exist in `data/` folder |
| JDBC driver not found | Verify JAR exists: `docker exec realtime_spark ls /opt/spark/jars/` |
| Permission denied errors | Check file permissions in mounted volumes |
| Port already in use | Stop other services using ports 5432, 8080, 7077, or 4040 |

### Useful Debugging Commands

```bash
# View container logs
docker-compose logs spark
docker-compose logs postgres

# Check container status
docker-compose ps

# Inspect container
docker exec -it realtime_spark bash
docker exec -it realtime_postgres bash

# Check disk usage
docker system df

# Restart a service
docker-compose restart spark
```

## Database Schema

### user_events Table

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key (auto-increment) |
| event_id | VARCHAR(50) | Unique event identifier |
| user_id | VARCHAR(50) | User identifier |
| event_type | VARCHAR(20) | Type of event (view, purchase, etc.) |
| product_id | VARCHAR(50) | Product identifier |
| product_name | VARCHAR(255) | Product name |
| product_category | VARCHAR(100) | Product category |
| product_price | DECIMAL(10,2) | Product price |
| quantity | INTEGER | Quantity (default: 1) |
| event_timestamp | TIMESTAMP | When the event occurred |
| session_id | VARCHAR(50) | User session identifier |
| device_type | VARCHAR(20) | Device type (mobile, desktop, tablet) |
| created_at | TIMESTAMP | When record was created in database |

## Tools & Technologies

- **Apache Spark Structured Streaming**: Real-time data processing
- **PostgreSQL 15**: Relational database
- **Python 3**: Data generation and scripting
- **Docker & Docker Compose**: Containerization
- **Faker**: Fake data generation library
- **psycopg2**: PostgreSQL Python adapter
- **Pandas**: Data manipulation (for future enhancements)

## Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## License

This project is for educational purposes as part of the Data Engineering course.

## Author

Created as part of Module 5: Real-Time Data Ingestion Lab

---

**Note**: For detailed step-by-step instructions, see [user_guide.md](user_guide.md)
