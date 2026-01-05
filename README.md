## Overview
This project implements an **incremental Medallion architecture pipeline (Bronze → Silver → Gold)** using **Databricks SQL and Delta Lake**.  
The pipeline processes **manually created CSV batch files** as raw input and is designed to be **idempotent, repeatable, and production-oriented**.

Raw data is ingested in append-only mode, cleaned and upserted using `MERGE`, and transformed into analytics-ready Gold tables with validation checks to ensure data consistency.
