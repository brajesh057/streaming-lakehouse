# Data Schema

## Initial Event: Trade Event

The streaming pipeline will initially process simulated financial trade
events.

### Fields

| Field | Type | Description |
|---|---|---|
| event_id | string | Unique identifier for the event |
| event_time | timestamp | Time at which the event occurred |
| symbol | string | Trading symbol |
| price | double | Trade price |
| quantity | integer | Number of units traded |
| side | string | BUY or SELL |
| source | string | Event producer/source |
| ingestion_time | timestamp | Time received by the pipeline |

## Data Layers

### Bronze

Raw events captured from Kafka with minimal transformation.

### Silver

Validated and cleaned events with standardized types and data-quality
rules applied.

### Gold

Business-level aggregations and analytics optimized for consumption by
dashboards and downstream users.

## Data Quality Rules

Initial rules will include:

- event_id must not be null.
- event_time must be a valid timestamp.
- symbol must not be null or empty.
- price must be greater than zero.
- quantity must be greater than zero.
- side must be either BUY or SELL.

Invalid events will be separated from valid events using a
dead-letter/quarantine approach.
