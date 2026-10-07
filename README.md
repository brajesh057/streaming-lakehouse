# Real-Time Streaming Lakehouse

A production-style real-time data engineering project demonstrating
stream ingestion, stream processing, data quality, lakehouse architecture,
orchestration, analytics, and visualization.

## Planned Architecture

Data Producer
    |
    v
Kafka
    |
    v
Spark Structured Streaming
    |
    v
Bronze Layer
    |
    v
Silver Layer
    |
    v
Gold Layer
    |
    v
Airflow + Dashboard

## Technologies

- Python
- Apache Kafka
- Apache Spark
- Apache Airflow
- PostgreSQL
- Docker / Docker Compose
- SQL
- AWS concepts
- Git / GitHub

## Project Goals

1. Generate realistic streaming data.
2. Ingest events through Kafka.
3. Process streams using Spark Structured Streaming.
4. Implement Bronze, Silver, and Gold data layers.
5. Add data-quality checks and a dead-letter/quarantine flow.
6. Build real-time analytics.
7. Orchestrate supporting workflows with Airflow.
8. Build a dashboard for business insights.
9. Document architecture, decisions, problems, and solutions.
10. Demonstrate AWS-compatible data-engineering concepts.

## Repository Structure

streaming-lakehouse/
|-- producer/
|-- spark/
|-- airflow/
|-- dashboard/
|-- data/
|-- docs/
|-- README.md
|-- schema.md
-- .gitignore
