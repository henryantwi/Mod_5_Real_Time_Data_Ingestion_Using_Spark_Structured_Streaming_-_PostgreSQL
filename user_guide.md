# User Guide: Real-Time Data Pipeline

## Prerequisites
- Docker Desktop installed and running
- Terminal/PowerShell access

## Quick Start

### Step 1: Start the Docker Environment

```bash
# Navigate to the project directory (eg.)
cd "c:\Users\HenryNanaAntwi\Development\Data Engineering Stuff\DE05 Big Data\Lab-2"

# Build and start containers
docker-compose up -d --build
```

Wait for both containers to be healthy (about 30-60 seconds).

### Step 2: Verify Containers are Running

```bash
docker-compose ps
```

You should see both `realtime_postgres` and `realtime_spark` running.

---

## Running the Pipeline

### Terminal 1: Start the Spark Streaming Job

```bash
# Enter the Spark container
docker exec -it realtime_spark bash

# Run the streaming job
spark-submit --jars /opt/spark/jars/postgresql-42.7.1.jar scripts/spark_streaming_to_postgres.py
```

Keep this running - it will monitor for new CSV files.

### Terminal 2: Generate Data

```bash
# Enter the Spark container (new terminal)
docker exec -it realtime_spark bash

# Generate a single batch of 10 events
python3 scripts/data_generator.py

# Generate 5 batches of 20 events each, 3 seconds apart
python3 scripts/data_generator.py --events 20 --batches 5 --interval 3

# Run continuously until you press Ctrl+C
python3 scripts/data_generator.py --events 10 --interval 5 --continuous
```

---

## Verify Data in PostgreSQL

```bash
# Enter the PostgreSQL container
docker exec -it realtime_postgres psql -U lab_user -d ecommerce_events

# Check record count
SELECT COUNT(*) FROM user_events;

# View recent events
SELECT event_id, user_id, event_type, product_name, event_timestamp 
FROM user_events 
ORDER BY event_timestamp DESC 
LIMIT 10;

# View event summary
SELECT * FROM event_summary;

# Exit psql
\q
```

---

## Stopping the Environment

```bash
# Stop containers (preserves data)
docker-compose stop

# Stop and remove containers (data persists in volume)
docker-compose down

# Stop and remove everything including data
docker-compose down -v
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Container won't start | Run `docker-compose logs <service>` to check errors |
| Spark can't connect to PostgreSQL | Ensure postgres container is healthy first |
| No data appearing in database | Check that CSV files exist in `data/` folder |
| JDBC driver not found | Verify the JAR exists at `/opt/spark/jars/postgresql-42.7.1.jar` |

---

## Useful Commands

```bash
# View Spark container logs
docker-compose logs spark

# View PostgreSQL logs
docker-compose logs postgres

# Check disk usage
docker system df

# Restart a specific service
docker-compose restart spark
```
