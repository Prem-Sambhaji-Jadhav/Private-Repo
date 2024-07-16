-- Databricks notebook source
-- CREATE OR REPLACE TABLE sandbox.pj_adhoc_hypermarket_vs_supermarket AS (
--     WITH cte AS (
--         SELECT
--             YEAR(business_day) AS year,
--             REGION,
--             Type,
--             store_name,
--             transaction_id,
--             ROUND(SUM(amount),0) AS sales
--         FROM dashboard.tbl_lulu_uae_store_classification AS t1
--         JOIN gold.pos_transactions AS t2 ON t1.store_id = t2.store_id
--         JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
--         WHERE (business_day BETWEEN "2024-01-01" AND "2024-03-10"
--                 OR business_day BETWEEN "2023-01-01" AND "2023-03-10")
--         AND amount > 0
--         AND quantity > 0
--         GROUP BY year, REGION, Type, store_name, transaction_id
--     ),

--     cte2 AS (
--         SELECT
--             year,
--             REGION,
--             Type,
--             store_name,
--             CASE WHEN sales >= 0 AND sales < 40 THEN "0-40"
--             WHEN sales >= 40 AND sales < 80 THEN "40-80"
--             WHEN sales >= 80 AND sales < 120 THEN "80-120"
--             WHEN sales >= 120 AND sales < 160 THEN "120-160"
--             WHEN sales >= 160 AND sales < 200 THEN "160-200"
--             WHEN sales >= 200 AND sales < 300 THEN "200-300"
--             WHEN sales >= 300 AND sales < 400 THEN "300-400"
--             WHEN sales >= 400 AND sales < 800 THEN "400-800"
--             ELSE "800+" END AS atv_bucket,
--             COUNT(DISTINCT transaction_id) AS transactions
--         FROM cte
--         GROUP BY year, REGION, Type, store_name, atv_bucket
--     )

--     SELECT
--         REGION,
--         Type,
--         store_name,
--         atv_bucket,
--         MAX(CASE WHEN year = 2023 THEN transactions ELSE 0 END) AS 2023_transactions,
--         MAX(CASE WHEN year = 2024 THEN transactions ELSE 0 END) AS 2024_transactions
--     FROM cte2
--     GROUP BY REGION, Type, store_name, atv_bucket
-- )

-- COMMAND ----------

SELECT * FROM sandbox.pj_adhoc_hypermarket_vs_supermarket

-- COMMAND ----------

SELECT
    Type,
    store_name,
    ROUND(SUM(amount),0) AS sales
FROM dashboard.tbl_lulu_uae_store_classification AS t1
JOIN gold.pos_transactions AS t2 ON t1.store_id = t2.store_id
JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
WHERE business_day BETWEEN "2024-01-01" AND "2024-03-10"
AND amount > 0
AND quantity > 0
GROUP BY Type, store_name

-- COMMAND ----------



-- COMMAND ----------


