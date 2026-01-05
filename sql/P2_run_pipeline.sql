-- Project 2: Incremental Orders Pipeline (RUN)
-- Bronze: append-only
-- Silver: MERGE (latest record wins)
-- Gold: refresh aggregates

USE CATALOG workspace;
USE SCHEMA lakehouse_p2;

-- BRONZE (append-only)


CREATE TABLE IF NOT EXISTS bronze_orders (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  price DOUBLE,
  quantity INT,
  order_time STRING,
  ingest_time TIMESTAMP,
  source_file STRING
)
USING DELTA;

INSERT INTO bronze_orders
SELECT
  CAST(order_id AS STRING) AS order_id,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(product_id AS STRING) AS product_id,
  CAST(REPLACE(TRIM(price), ',', '.') AS DOUBLE) AS price,
  CAST(TRIM(quantity) AS INT) AS quantity,
  CAST(order_time AS STRING) AS order_time,
  current_timestamp() AS ingest_time,
  _metadata.file_path AS source_file
FROM read_files(
  '/Volumes/workspace/lakehouse_demo/raw_data/project2/',
  format => 'csv',
  header => true,
  inferSchema => false
);

-- SILVER – QUARANTINE (append-only)

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

-- SILVER – MERGE (clean + latest wins)

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
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY ingest_time DESC
  ) = 1
) src
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

-- GOLD – REFRESH AGGREGATES

DROP TABLE IF EXISTS gold_daily_revenue;

CREATE TABLE gold_daily_revenue AS
SELECT
  DATE(order_time) AS order_date,
  COUNT(*) AS orders_count,
  SUM(price * quantity) AS revenue,
  AVG(price * quantity) AS avg_order_value
FROM silver_orders
GROUP BY DATE(order_time)
ORDER BY order_date DESC;

DROP TABLE IF EXISTS gold_top_products;

CREATE TABLE gold_top_products AS
SELECT
  product_id,
  SUM(quantity) AS total_quantity,
  SUM(price * quantity) AS total_revenue,
  COUNT(*) AS order_lines
FROM silver_orders
GROUP BY product_id
ORDER BY total_revenue DESC;

-- FINAL CHECKS

SELECT
  (SELECT COUNT(*) FROM bronze_orders) AS bronze_rows,
  (SELECT COUNT(*) FROM silver_orders) AS silver_rows,
  (SELECT COUNT(*) FROM silver_orders_quarantine) AS quarantine_rows;

SELECT
  (SELECT SUM(price * quantity) FROM silver_orders) AS silver_total_revenue,
  (SELECT SUM(revenue) FROM gold_daily_revenue) AS gold_total_revenue;
