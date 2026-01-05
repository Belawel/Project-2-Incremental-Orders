# Project 2 – Incremental Medallion Orders Pipeline (Databricks SQL)

## Overview
This project implements an **incremental Medallion architecture pipeline (Bronze → Silver → Gold)** using **Databricks SQL and Delta Lake**.

The pipeline processes **manually created CSV batch files** as raw input and is designed to be **idempotent**, **repeatable**, and **production-oriented**.

Raw data is ingested in append-only mode, cleaned and upserted using `MERGE`, and transformed into analytics-ready Gold tables with built-in validation checks.

---

## Architecture Overview
![Incremental Medallion Orders Pipeline](images/project2_incremental_medallion_architecture.png)

---

## Input Data
- Manually created CSV batch files (simulating real batch arrivals)
- Example files:
  - `orders_p2_batch1.csv`
  - `orders_p2_batch2.csv`
- Stored in a Databricks Volume:
