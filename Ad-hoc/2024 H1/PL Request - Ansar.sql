-- Databricks notebook source
WITH lulu_pl_sales AS (SELECT t2.category_id, t2.category_name, t2.material_id, t2.material_name, t3.nationality_group, t4.segment,
                                         CASE WHEN t1.LHPRDate IS NOT NULL THEN 1 ELSE 0 END AS happiness,
                                         ROUND(SUM(t1.amount),0) AS sales
                                 FROM gold.pos_transactions AS t1
                                 JOIN gold.material_master AS t2
                                 ON t1.product_id = t2.material_id
                                 JOIN gold.customer_profile AS t3
                                 ON t1.customer_id = t3.account_key
                                 JOIN analytics.customer_segments AS t4
                                 ON t1.customer_id = t4.customer_id
                                 WHERE t1.business_day BETWEEN "2023-01-01" AND "2023-12-17"
                                 AND t2.brand IN ("LULU PRIVATE LABEL", "LULU PL COVID DSP")
                                 AND round(t1.amount, 0) > 0
                                 AND t1.quantity > 0
                                 AND t3.nationality_group NOT IN ("NA", "OTHERS")
                                 AND t4.key = 'rfm'
                                 AND t4.channel = 'pos'
                                 AND t4.country = 'uae'
                                 AND t4.month_year = '202311'
                                 GROUP BY t2.category_id, t2.category_name, t2.material_id, t2.material_name, t3.nationality_group, t4.segment, happiness)

SELECT category_id, category_name, material_id, material_name, nationality_group, segment,
        MAX(CASE WHEN happiness = 1 THEN sales ELSE 0 END) AS happiness_sales,
        MAX(CASE WHEN happiness = 0 THEN sales ELSE 0 END) AS non_happiness_sales
FROM lulu_pl_sales
GROUP BY category_id, category_name, material_id, material_name, nationality_group, segment

-- COMMAND ----------

WITH lulu_pl_sales AS (SELECT t3.region_name, t3.store_id, t3.store_name, t2.category_id, t2.category_name,
                              CASE WHEN t1.LHPRDate IS NOT NULL THEN 1 ELSE 0 END AS happiness,
                              ROUND(SUM(t1.amount),0) AS sales
                      FROM gold.pos_transactions AS t1
                      JOIN gold.material_master AS t2
                            ON t1.product_id = t2.material_id
                      JOIN gold.store_master AS t3
                            ON t1.store_id = t3.store_id
                      WHERE t1.business_day BETWEEN "2023-01-01" AND "2023-12-17"
                            AND t2.brand IN ("LULU PRIVATE LABEL", "LULU PL COVID DSP")
                            AND round(t1.amount, 0) > 0
                            AND t1.quantity > 0
                      GROUP BY t3.region_name, t3.store_id, t3.store_name,
                                t2.category_id, t2.category_name, happiness)

SELECT region_name, store_id, store_name, category_id, category_name,
        MAX(CASE WHEN happiness = 1 THEN sales ELSE 0 END) AS happiness_sales,
        MAX(CASE WHEN happiness = 0 THEN sales ELSE 0 END) AS non_happiness_sales
FROM lulu_pl_sales
GROUP BY region_name, store_id, store_name, category_id, category_name

-- COMMAND ----------

SELECT brand, category_id,
        ROUND(SUM(t1.amount),0) AS sales
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2
ON t1.product_id = t2.material_id
WHERE business_day BETWEEN "2023-01-01" AND "2023-12-17"
AND brand IN ("LULU PRIVATE LABEL", "LULU PL COVID DSP")
AND round(amount, 0) > 0
AND quantity > 0
GROUP BY brand, category_id

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


