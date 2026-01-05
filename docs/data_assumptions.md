# Data Assumptions

The following assumptions were made to keep the pipeline deterministic and production-oriented.

---

## Business Keys
- `order_id` uniquely identifies a logical order
- Multiple records with the same `order_id` may arrive over time

---

## Incremental Behavior
- Raw CSV files are batch-delivered
- Late-arriving data is allowed
- Reprocessing input files is expected

---

## Conflict Resolution
- When multiple records exist for the same `order_id`, the **latest ingestion timestamp wins**
- Silver layer enforces this rule using `MERGE`

---

## Data Quality Rules
Records are considered invalid if:
- `customer_id` is missing or empty
- `price` is null or ≤ 0
- `quantity` is null or ≤ 0
- `order_time` cannot be cast to a timestamp

Invalid records are quarantined and excluded from analytics.

---

## Gold Layer Assumptions
- Gold tables are always rebuilt from Silver
- Gold aggregates must match Silver totals
- Gold tables are analytics-ready and contain no raw or invalid data

---

## Scope Limitations
- Streaming ingestion is out of scope
- File-level idempotency is not enforced in Bronze
- CSV files are assumed to be UTF-8 encoded
