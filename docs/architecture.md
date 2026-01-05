# Architecture Overview

This project implements an incremental Medallion architecture using Databricks SQL and Delta Lake.

---

## Ingestion
- Input data consists of manually created CSV batch files
- Files are stored in a Databricks Volume acting as a landing zone
- All files in the folder are processed on each pipeline run

---

## Bronze Layer
- Append-only Delta table
- Stores raw records exactly as ingested
- Tracks ingestion metadata:
  - `ingest_time`
  - `source_file`

---

## Silver Layer
- Cleans and validates data
- Uses MERGE to upsert records by business key
- Maintains a quarantine table for invalid records
- Ensures idempotent pipeline execution

---

## Gold Layer
- Rebuilds analytical aggregates on each run
- Produces KPI-focused tables
- Enforces revenue consistency between Silver and Gold

---

## Design Goals
- Idempotency
- Traceability
- Data quality enforcement
- Clear separation of responsibilities across layers
