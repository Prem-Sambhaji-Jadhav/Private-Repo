-- Databricks notebook source
SELECT material_id, unit_price, COUNT(*) AS count
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2
ON t1.product_id = t2.material_id
WHERE business_day BETWEEN '2023-10-01' AND '2023-12-31'
AND category_name = 'WATER'
GROUP BY material_id, unit_price

-- COMMAND ----------

WITH CountRank AS (
    SELECT material_id, unit_price,
            COUNT(*) AS count,
            RANK() OVER (PARTITION BY material_id ORDER BY COUNT(*) DESC) AS rnk
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2
    ON t1.product_id = t2.material_id
    WHERE business_day BETWEEN '2023-10-01' AND '2023-12-31'
    AND category_name = 'WATER'
    GROUP BY material_id, unit_price
)

SELECT material_id, unit_price
FROM CountRank
WHERE rnk = 1

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


