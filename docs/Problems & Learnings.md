# Problems & Learnings

This project intentionally documents realistic challenges encountered while building an incremental Medallion pipeline.  
The goal was not only to make the pipeline work, but to understand *why* certain architectural decisions are necessary in real-world data engineering.

---

## 1. Schema drift across incremental batch files

### Problem
As new CSV batches were introduced, subtle schema differences appeared:
- Numeric fields (e.g. `price`) arrived as strings
- Decimal formatting varied between batches (comma vs dot)
- Timestamp formats were inconsistent

This caused ingestion failures and schema mismatch errors when writing to Delta tables.

### Resolution
- Disabled automatic schema inference
- Explicitly casted all columns during Bronze ingestion
- Normalized numeric formats before persistence

### Learning
Incremental pipelines must enforce schema explicitly.  
Relying on schema inference introduces instability and runtime failures as data evolves.

---

## 2. Duplicate records caused by reprocessing input files

### Problem
The Bronze ingestion step reprocessed all files in the landing folder on each pipeline run, causing duplicate raw records.

### Resolution
- Accepted duplicates in Bronze by design (append-only)
- Implemented deduplication logic in Silver using `MERGE`
- Used `order_id` as the business key and `ingest_time` to resolve conflicts

### Learning
Bronze should preserve raw history and lineage.  
Correctness belongs in Silver, not in the ingestion layer.

---

## 3. Invalid business data breaking downstream aggregates

### Problem
Some records contained invalid business values:
- Missing customer identifiers
- Zero or negative prices
- Invalid timestamps

These records caused incorrect KPIs in Gold-level aggregations.

### Resolution
- Introduced a Silver quarantine table
- Routed invalid records with explicit rejection reasons
- Prevented bad data from reaching analytics tables

### Learning
Data quality enforcement must occur before analytical aggregation.  
Quarantine tables provide traceability without data loss.

---

## 4. Confusion between INSERT and MERGE semantics

### Problem
Early versions of the Silver layer used `INSERT` instead of `MERGE`, resulting in multiple versions of the same logical order.

### Resolution
- Replaced INSERT logic with MERGE
- Defined clear business keys (`order_id`)
- Enforced “latest record wins” logic based on ingestion timestamp

### Learning
Incremental pipelines require clear ownership of business keys and deterministic update rules.

---

## 5. Gold aggregates drifting from Silver totals

### Problem
Initial Gold revenue totals did not match Silver-level revenue due to grouping and aggregation errors.

### Resolution
- Rebuilt Gold tables from Silver on each run
- Added revenue consistency validation checks
- Ensured Gold tables are fully derived from Silver

### Learning
Gold layers should be deterministic, reproducible, and validated against their upstream sources.

---

## 6. Managing multiple projects in the same Databricks workspace

### Problem
Using shared schemas across multiple projects created confusion and risk of accidental table overwrites.

### Resolution
- Created a dedicated schema per project (`lakehouse_p2`)
- Adopted strict naming conventions
- Isolated tables and volumes per project

### Learning
Schema isolation is essential for maintainability and safe experimentation.

---

## 7. Manual file handling vs pipeline-driven ingestion

### Problem
Early experimentation relied on manually creating directories and file paths, which did not scale and was error-prone.

### Resolution
- Switched to folder-based ingestion
- Removed hard-coded file names
- Allowed the pipeline to process any new files placed in the landing directory

### Learning
Production pipelines should never depend on manual file naming or operator intervention.

---

## Summary
These challenges significantly shaped the final architecture and reinforced best practices around incremental ingestion, data quality enforcement, and Medallion-based design.
