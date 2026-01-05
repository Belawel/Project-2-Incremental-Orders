-- Project 2: Incremental Orders
-- Layer: Silver (clean + MERGE)

USE CATALOG workspace;
USE SCHEMA lakehouse_p2;

-- 1) Create quarantine table (stores bad rows)
CREATE TABLE IF NOT EXISTS silver_orders_quarantine (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  price DOUBLE,
  quantity INT,
  order_time STRING,
  ingest_time TIMESTAMP,
  source_file STRING,
  quarantine_reason STRING
)
USING DELTA;

-- 2) Insert bad rows into quarantine (append-only)
INSERT INTO silver_orders_quarantine
SELECT
  order_id,
  customer_id,
  product_id,
  price,
  quantity,
  order_time,
  ingest_time,
  source_file,
  CASE
    WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'missing_customer_id'
    WHEN price IS NULL THEN 'missing_price'
    WHEN price <= 0 THEN 'invalid_price'
    WHEN quantity IS NULL THEN 'missing_quantity'
    WHEN quantity <= 0 THEN 'invalid_quantity'
    WHEN TRY_CAST(order_time AS TIMESTAMP) IS NULL THEN 'invalid_order_time'
    ELSE 'unknown_reason'
  END AS quarantine_reason
FROM bronze_orders
WHERE
  customer_id IS NULL OR TRIM(customer_id) = ''
  OR price IS NULL OR price <= 0
  OR quantity IS NULL OR quantity <= 0
  OR TRY_CAST(order_time AS TIMESTAMP) IS NULL;

-- 3) Create Silver table (clean, deduped by MERGE key)
CREATE TABLE IF NOT EXISTS silver_orders (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  price DOUBLE,
  quantity INT,
  order_time TIMESTAMP,
  ingest_time TIMESTAMP,
  source_file STRING,
  updated_at TIMESTAMP
)
USING DELTA;

-- 4) MERGE clean rows into Silver (latest ingest_time wins)
MERGE INTO silver_orders AS tgt
USING (
  SELECT
    order_id,
    customer_id,
    product_id,
    price,
    quantity,
    CAST(order_time AS TIMESTAMP) AS order_time,
    ingest_time,
    source_file,
    current_timestamp() AS updated_at
  FROM bronze_orders
  WHERE
    customer_id IS NOT NULL AND TRIM(customer_id) <> ''
    AND price IS NOT NULL AND price > 0
    AND quantity IS NOT NULL AND quantity > 0
    AND TRY_CAST(order_time AS TIMESTAMP) IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingest_time DESC) = 1
) AS src
ON tgt.order_id = src.order_id

WHEN MATCHED AND src.ingest_time >= tgt.ingest_time THEN
  UPDATE SET
    tgt.customer_id = src.customer_id,
    tgt.product_id  = src.product_id,
    tgt.price       = src.price,
    tgt.quantity    = src.quantity,
    tgt.order_time  = src.order_time,
    tgt.ingest_time = src.ingest_time,
    tgt.source_file = src.source_file,
    tgt.updated_at  = src.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    order_id, customer_id, product_id, price, quantity,
    order_time, ingest_time, source_file, updated_at
  )
  VALUES (
    src.order_id, src.customer_id, src.product_id, src.price, src.quantity,
    src.order_time, src.ingest_time, src.source_file, src.updated_at
  );

-- 5) Verify results
SELECT COUNT(*) AS silver_rows FROM silver_orders;
SELECT * FROM silver_orders ORDER BY updated_at DESC;

SELECT quarantine_reason, COUNT(*) AS rows
FROM silver_orders_quarantine
GROUP BY quarantine_reason
ORDER BY rows DESC;
