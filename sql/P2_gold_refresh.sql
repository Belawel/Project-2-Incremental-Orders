-- Project 2: Incremental Orders
-- Layer: Gold (refresh aggregates from Silver)

USE CATALOG workspace;
USE SCHEMA lakehouse_p2;

-- 1) Daily revenue KPIs
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

-- Verify
SELECT * FROM gold_daily_revenue ORDER BY order_date DESC;


-- 2) Top products by revenue
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

-- Verify
SELECT * FROM gold_top_products LIMIT 10;


-- 3) Consistency check (Silver revenue must equal Gold revenue)
SELECT
  (SELECT SUM(price * quantity) FROM silver_orders) AS silver_total_revenue,
  (SELECT SUM(revenue) FROM gold_daily_revenue) AS gold_total_revenue;
