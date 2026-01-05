-- Project 2: Incremental Orders
-- Layer: Bronze (append-only)

USE CATALOG workspace;
USE SCHEMA lakehouse_p2;

-- Create Bronze table if it does not exist
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

-- Append new raw records
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

-- Verify Bronze load
SELECT COUNT(*) AS bronze_rows FROM bronze_orders;
SELECT * FROM bronze_orders ORDER BY ingest_time DESC;
