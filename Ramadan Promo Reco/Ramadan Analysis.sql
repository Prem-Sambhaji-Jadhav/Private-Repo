-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Testing Query

-- COMMAND ----------

-- WITH cte AS (
--   SELECT CASE WHEN business_day BETWEEN "2023-03-08" AND "2023-04-21" THEN "Ramadan"
--               ELSE "Not Ramadan"
--           END AS period_type,
          
--           segment,
--           ROUND(SUM(amount),0) AS total_sales,
--           COUNT(DISTINCT mobile) AS total_customers
--   FROM gold.pos_transactions AS t1
--   JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id

--   WHERE business_day BETWEEN "2023-03-08" AND "2023-12-31"
--   AND mobile IS NOT NULL
--   AND t2.country = 'uae'
--   AND month_year = '202401'
--   AND key = 'rfm'
--   AND channel = 'pos'

--   GROUP BY period_type, segment
-- ),

-- cte2 AS (
--   SELECT CASE WHEN business_day BETWEEN "2023-03-08" AND "2023-03-14" THEN "-w2"
--               WHEN business_day BETWEEN "2023-03-15" AND "2023-03-21" THEN "-w1"
--               WHEN business_day BETWEEN "2023-03-22" AND "2023-03-28" THEN "w1"
--               WHEN business_day BETWEEN "2023-03-29" AND "2023-04-04" THEN "w2"
--               WHEN business_day BETWEEN "2023-04-05" AND "2023-04-11" THEN "w3"
--               WHEN business_day BETWEEN "2023-04-12" AND "2023-04-21" THEN "w4"
--               ELSE "General"
--           END AS week_number,

--           CASE WHEN week_number = 'General' THEN 'Non Ramadan'
--           ELSE 'Ramadan' END AS period_type,

--           category_name,
--           t3.segment,
--           ROUND(SUM(amount),0) AS sales,
--           COUNT(DISTINCT mobile) AS customers

--   FROM gold.pos_transactions AS t1
--   JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
--   JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
--   WHERE business_day BETWEEN "2023-03-08" AND "2023-12-31"
--   AND ROUND(amount,0) > 0
--   AND quantity > 0
--   AND category_name IS NOT NULL
--   AND t3.country = 'uae'
--   AND month_year = '202401'
--   AND key = 'rfm'
--   AND channel = 'pos'
--   AND category_name IS NOT NULL

--   GROUP BY week_number, category_name, t3.segment
-- )

-- SELECT 
--         t2.week_number,
--         t2.period_type,
--         t2.category_name,
--         t2.segment,
--         sales,
--         customers,
--         total_sales AS total_segment_sales,
--         total_customers AS total_segment_customers
-- FROM cte AS t1
-- JOIN cte2 AS t2 ON t1.period_type = t2.period_type AND t1.segment = t2.segment
-- ORDER BY t2.week_number, t2.category_name, t2.segment

-- COMMAND ----------

WITH ramadan_period AS (
  SELECT "Ramadan" AS period_type,
          segment,
          ROUND(SUM(amount),0) AS total_period_sales,
          COUNT(DISTINCT mobile) AS total_period_customers
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  WHERE business_day BETWEEN "2023-03-08" AND "2023-04-21"
  AND mobile IS NOT NULL
  AND t2.country = 'uae'
  AND month_year = '202304'
  AND key = 'rfm'
  AND channel = 'pos'
  GROUP BY period_type, segment
),

ramadan_weeks AS (
  SELECT "Ramadan" AS period_type,
          CASE WHEN business_day BETWEEN "2023-03-08" AND "2023-03-14" THEN "-2"
              WHEN business_day BETWEEN "2023-03-15" AND "2023-03-21" THEN "-1"
              WHEN business_day BETWEEN "2023-03-22" AND "2023-03-28" THEN "1"
              WHEN business_day BETWEEN "2023-03-29" AND "2023-04-04" THEN "2"
              WHEN business_day BETWEEN "2023-04-05" AND "2023-04-11" THEN "3"
              ELSE "4"
          END AS week_number,
          category_name,
          t3.segment,
          ROUND(SUM(amount),0) AS sales,
          COUNT(DISTINCT mobile) AS customers

  FROM gold.pos_transactions AS t1
  JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
  JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id

  WHERE business_day BETWEEN "2023-03-08" AND "2023-04-21"
  AND ROUND(amount,0) > 0
  AND quantity > 0
  AND category_name IS NOT NULL
  AND t3.country = 'uae'
  AND month_year = '202304'
  AND key = 'rfm'
  AND channel = 'pos'
  AND category_name IS NOT NULL

  GROUP BY week_number, category_name, t3.segment
),

ramadan_table AS (
  SELECT 
          t2.week_number,
          t2.period_type,
          t2.category_name,
          t2.segment,
          sales,
          customers,
          total_period_sales AS total_period_segment_sales,
          total_period_customers AS total_period_segment_customers
  FROM ramadan_period AS t1
  JOIN ramadan_weeks AS t2 ON t1.period_type = t2.period_type AND t1.segment = t2.segment
),

non_ramadan_period AS (
  SELECT "Non Ramadan" AS period_type,
          segment,
          ROUND(SUM(amount),0) AS total_period_sales,
          COUNT(DISTINCT mobile) AS total_period_customers
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  WHERE business_day BETWEEN "2023-04-22" AND "2023-12-31"
  AND mobile IS NOT NULL
  AND t2.country = 'uae'
  AND month_year = '202312'
  AND key = 'rfm'
  AND channel = 'pos'
  GROUP BY period_type, segment
),

non_ramadan_weeks AS (
  SELECT "Non Ramadan" AS period_type,
          "General" AS week_number,
          category_name,
          t3.segment,
          ROUND(SUM(amount),0) AS sales,
          COUNT(DISTINCT mobile) AS customers

  FROM gold.pos_transactions AS t1
  JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
  JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id

  WHERE business_day BETWEEN "2023-04-22" AND "2023-12-31"
  AND ROUND(amount,0) > 0
  AND quantity > 0
  AND category_name IS NOT NULL
  AND t3.country = 'uae'
  AND month_year = '202312'
  AND key = 'rfm'
  AND channel = 'pos'
  AND category_name IS NOT NULL

  GROUP BY week_number, category_name, t3.segment
),

non_ramadan_table AS (
  SELECT 
          t2.week_number,
          t2.period_type,
          t2.category_name,
          t2.segment,
          sales,
          customers,
          total_period_sales AS total_period_segment_sales,
          total_period_customers AS total_period_segment_customers
  FROM non_ramadan_period AS t1
  JOIN non_ramadan_weeks AS t2 ON t1.period_type = t2.period_type AND t1.segment = t2.segment
)

SELECT *
FROM ramadan_table

UNION ALL

SELECT *
FROM non_ramadan_table
ORDER BY period_type DESC, week_number, category_name, segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Creating Sandbox Table

-- COMMAND ----------

DROP TABLE IF EXISTS sandbox.pj_ramadan_promo_base_table;

CREATE TABLE sandbox.pj_ramadan_promo_base_table AS (
  WITH ramadan_period AS (
    SELECT "Ramadan" AS period_type,
            segment,
            ROUND(SUM(amount),0) AS total_period_sales,
            COUNT(DISTINCT mobile) AS total_period_customers
    FROM gold.pos_transactions AS t1
    JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
    WHERE business_day BETWEEN "2023-03-08" AND "2023-04-21"
    AND mobile IS NOT NULL
    AND t2.country = 'uae'
    AND month_year = '202304'
    AND key = 'rfm'
    AND channel = 'pos'
    GROUP BY period_type, segment
  ),

  ramadan_weeks AS (
    SELECT "Ramadan" AS period_type,
            CASE WHEN business_day BETWEEN "2023-03-08" AND "2023-03-14" THEN "-2"
                WHEN business_day BETWEEN "2023-03-15" AND "2023-03-21" THEN "-1"
                WHEN business_day BETWEEN "2023-03-22" AND "2023-03-28" THEN "1"
                WHEN business_day BETWEEN "2023-03-29" AND "2023-04-04" THEN "2"
                WHEN business_day BETWEEN "2023-04-05" AND "2023-04-11" THEN "3"
                ELSE "4"
            END AS week_number,
            category_name,
            t3.segment,
            ROUND(SUM(amount),0) AS sales,
            COUNT(DISTINCT mobile) AS customers

    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id

    WHERE business_day BETWEEN "2023-03-08" AND "2023-04-21"
    AND ROUND(amount,0) > 0
    AND quantity > 0
    AND category_name IS NOT NULL
    AND t3.country = 'uae'
    AND month_year = '202304'
    AND key = 'rfm'
    AND channel = 'pos'
    AND category_name IS NOT NULL

    GROUP BY week_number, category_name, t3.segment
  ),

  ramadan_table AS (
    SELECT 
            t2.period_type,
            t2.week_number,
            t2.category_name,
            t2.segment,
            sales,
            customers,
            total_period_sales AS total_period_segment_sales,
            total_period_customers AS total_period_segment_customers
    FROM ramadan_period AS t1
    JOIN ramadan_weeks AS t2 ON t1.period_type = t2.period_type AND t1.segment = t2.segment
  ),

  non_ramadan_period AS (
    SELECT "Non Ramadan" AS period_type,
            segment,
            ROUND(SUM(amount),0) AS total_period_sales,
            COUNT(DISTINCT mobile) AS total_period_customers
    FROM gold.pos_transactions AS t1
    JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
    WHERE business_day BETWEEN "2023-04-22" AND "2023-12-31"
    AND mobile IS NOT NULL
    AND t2.country = 'uae'
    AND month_year = '202312'
    AND key = 'rfm'
    AND channel = 'pos'
    GROUP BY period_type, segment
  ),

  non_ramadan_weeks AS (
    SELECT "Non Ramadan" AS period_type,
            "General" AS week_number,
            category_name,
            t3.segment,
            ROUND(SUM(amount),0) AS sales,
            COUNT(DISTINCT mobile) AS customers

    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id

    WHERE business_day BETWEEN "2023-04-22" AND "2023-12-31"
    AND ROUND(amount,0) > 0
    AND quantity > 0
    AND category_name IS NOT NULL
    AND t3.country = 'uae'
    AND month_year = '202312'
    AND key = 'rfm'
    AND channel = 'pos'
    AND category_name IS NOT NULL

    GROUP BY week_number, category_name, t3.segment
  ),

  non_ramadan_table AS (
    SELECT 
            t2.period_type,
            t2.week_number,
            t2.category_name,
            t2.segment,
            sales,
            customers,
            total_period_sales AS total_period_segment_sales,
            total_period_customers AS total_period_segment_customers
    FROM non_ramadan_period AS t1
    JOIN non_ramadan_weeks AS t2 ON t1.period_type = t2.period_type AND t1.segment = t2.segment
  )

  SELECT *
  FROM ramadan_table

  UNION ALL

  SELECT *
  FROM non_ramadan_table
  ORDER BY period_type DESC, week_number, category_name, segment
)

-- COMMAND ----------

SELECT * FROM sandbox.pj_ramadan_promo_base_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Data Support

-- COMMAND ----------

SELECT YEAR(business_day) AS year_info,
        COUNT(DISTINCT mobile) AS num_mobile,
        COUNT(DISTINCT customer_id) AS num_cust_id

FROM gold.pos_transactions
WHERE (business_day BETWEEN "2023-03-08" AND "2023-04-21"
    OR business_day BETWEEN "2022-03-18" AND "2022-05-01")
AND ROUND(amount,0) > 0
AND quantity > 0
GROUP BY year_info

-- 1,688,126

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


