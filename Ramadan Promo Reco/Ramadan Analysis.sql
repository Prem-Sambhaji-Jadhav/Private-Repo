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

-- MAGIC %md
-- MAGIC ###Ramadan Customer Count

-- COMMAND ----------

SELECT COUNT(DISTINCT mobile) AS num_mobile,
        COUNT(DISTINCT customer_id) AS num_cust_id

FROM gold.pos_transactions
WHERE business_day BETWEEN "2023-03-22" AND "2023-04-21"
AND LHPRDATE IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Dept Lookup Table

-- COMMAND ----------

SELECT
    DISTINCT department_name,
    category_name,
    material_group_name
FROM gold.material_master
WHERE department_name NOT IN ('016', '018')
AND department_name IS NOT NULL
ORDER BY 1, 2, 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TMS Segment Mapping

-- COMMAND ----------

SELECT
    category_name,
    mission_name,
    segment_name,
    sales_perc
FROM (
    SELECT
        category_name,
        mission_name,
        segment_name,
        sales_perc,
        ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY sales_perc DESC) AS row_num
    FROM analytics.category_mission_mapping AS t1
    JOIN analytics.tms_segment_mission_affinity AS t2 ON t1.mission_label = t2.mission_label
    JOIN analytics.tms_segment_label_mapping AS t3 ON t2.segment_label = t3.segment_label
    WHERE UPPER(category_name) IN ('MENS JEANS', 'LADIES DRESSES', 'MENS TROUSERS', 'OTHER MENSWEAR', 'LADIES FOOTWEAR', 'WEARABLE GADGETS')
    AND t2.month_year = "202310"
)
WHERE row_num = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Calculating Population for Campaigns

-- COMMAND ----------

WITH grocery AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('grocery & household needs')
  AND UPPER(category_name) IN ('FROZEN PASTRY')
  GROUP BY mobile, transaction_id
),

allaround AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('all around affluent shoppers')
  AND UPPER(category_name) IN ('SOUPS', 'COOKING SAUCES & AIDS', 'CHEESE & MARGARINES')
  GROUP BY mobile, transaction_id
),

baking AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('baking enthusiasts')
  AND UPPER(category_name) IN ('JUICE & DRINKS (LONG LIFE)')
  GROUP BY mobile, transaction_id
),

cooking AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('cooking enthusiasts')
  AND UPPER(category_name) IN ('HOME BAKING')
  GROUP BY mobile, transaction_id
)

SELECT
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM (
  SELECT mobile, transaction_id, sales
  FROM grocery
  UNION ALL
  SELECT mobile, transaction_id, sales
  FROM allaround
  UNION ALL
  SELECT mobile, transaction_id, sales
  FROM baking
  UNION ALL
  SELECT mobile, transaction_id, sales
  FROM cooking
)

-- COMMAND ----------

WITH grocery AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('grocery & household needs')
  AND UPPER(category_name) IN ('FROZEN PASTRY')
  GROUP BY mobile, transaction_id
),

allaround AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('all around affluent shoppers')
  AND UPPER(category_name) IN ('SOUPS', 'COOKING SAUCES & AIDS', 'CHEESE & MARGARINES')
  GROUP BY mobile, transaction_id
),

baking AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('baking enthusiasts')
  AND UPPER(category_name) IN ('JUICE & DRINKS (LONG LIFE)')
  GROUP BY mobile, transaction_id
),

cooking AS (
  SELECT
      mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  WHERE business_day >= "2023-01-06"
  AND mobile IS NOT NULL
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('cooking enthusiasts')
  AND UPPER(category_name) IN ('HOME BAKING')
  GROUP BY mobile, transaction_id
)

SELECT "Grocery" AS tms_segment, COUNT(DISTINCT mobile) AS customer_count,
  SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
  MEDIAN(sales) AS MTV
FROM grocery

UNION ALL

SELECT "All Around", COUNT(DISTINCT mobile) AS customer_count,
  SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
  MEDIAN(sales) AS MTV
FROM allaround

UNION ALL

SELECT "Baking", COUNT(DISTINCT mobile) AS customer_count,
  SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
  MEDIAN(sales) AS MTV
FROM baking

UNION ALL

SELECT "Cooking", COUNT(DISTINCT mobile) AS customer_count,
  SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
  MEDIAN(sales) AS MTV
FROM cooking

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###1. Loyalty Customers TMS Population

-- COMMAND ----------

WITH customers AS (
  SELECT
      DISTINCT mobile
  FROM gold.pos_transactions
  WHERE business_day >= "2023-01-06"
  AND loyalty_points IS NOT NULL
),

grocery AS (
  SELECT
      t1.mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  JOIN customers AS t4 ON t1.mobile = t4.mobile
  WHERE business_day >= "2023-01-06"
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('grocery & household needs')
  AND UPPER(category_name) IN ('FROZEN PASTRY')
  GROUP BY t1.mobile, transaction_id
),

allaround AS (
  SELECT
      t1.mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  JOIN customers AS t4 ON t1.mobile = t4.mobile
  WHERE business_day >= "2023-01-06"
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('all around affluent shoppers')
  AND UPPER(category_name) IN ('SOUPS', 'COOKING SAUCES & AIDS', 'CHEESE & MARGARINES')
  GROUP BY t1.mobile, transaction_id
),

baking AS (
  SELECT
      t1.mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  JOIN customers AS t4 ON t1.mobile = t4.mobile
  WHERE business_day >= "2023-01-06"
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('baking enthusiasts')
  AND UPPER(category_name) IN ('JUICE & DRINKS (LONG LIFE)')
  GROUP BY t1.mobile, transaction_id
),

cooking AS (
  SELECT
      t1.mobile,
      transaction_id,
      ROUND(SUM(amount),0) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
  JOIN customers AS t4 ON t1.mobile = t4.mobile
  WHERE business_day >= "2023-01-06"
  AND amount > 0
  AND quantity > 0
  AND key = 'tms'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = '202310'
  AND LOWER(segment) IN ('cooking enthusiasts')
  AND UPPER(category_name) IN ('HOME BAKING')
  GROUP BY t1.mobile, transaction_id
)

SELECT
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM (
  SELECT mobile, transaction_id, sales
  FROM grocery
  UNION ALL
  SELECT mobile, transaction_id, sales
  FROM allaround
  UNION ALL
  SELECT mobile, transaction_id, sales
  FROM baking
  UNION ALL
  SELECT mobile, transaction_id, sales
  FROM cooking
)

-- COMMAND ----------

WITH customers AS (
    SELECT
        DISTINCT mobile
    FROM gold.pos_transactions
    WHERE business_day >= "2023-01-06"
    AND loyalty_points IS NOT NULL
),

cte AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND key = 'tms'
    AND channel = 'pos'
    AND t2.country = 'uae'
    AND month_year = '202310'
    AND LOWER(segment) IN ('grocery & household needs', 'all around affluent shoppers', 'baking enthusiasts', 'cooking enthusiasts')
    AND UPPER(category_name) IN ('FROZEN PASTRY', 'SOUPS', 'COOKING SAUCES & AIDS', 'CHEESE & MARGARINES', 'JUICE & DRINKS (LONG LIFE)', 'HOME BAKING')
    GROUP BY t1.mobile, transaction_id
)

SELECT
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM cte

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2. Customer Count, ATV and MTV in Categories

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2.1 All Categories & Segments in one Query

-- COMMAND ----------

WITH customers AS (
    SELECT
        DISTINCT mobile
    FROM gold.pos_transactions
    WHERE business_day >= "2023-01-06"
    AND loyalty_points IS NOT NULL
),

cte AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('FROZEN PASTRY', 'SOUPS', 'COOKING SAUCES & AIDS', 'CHEESE & MARGARINES', 'JUICE & DRINKS (LONG LIFE)', 'HOME BAKING')
    GROUP BY t1.mobile, transaction_id
)

SELECT
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM cte  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2.2 All Categories & Segments Segregated in Queries

-- COMMAND ----------

WITH customers AS (
    SELECT
        DISTINCT mobile
    FROM gold.pos_transactions
    WHERE business_day >= "2023-01-06"
    AND loyalty_points IS NOT NULL
),

frozen_pastry AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('FROZEN PASTRY')
    GROUP BY t1.mobile, transaction_id
),

soups AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('SOUPS')
    GROUP BY t1.mobile, transaction_id
),

cooking AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('COOKING SAUCES & AIDS')
    GROUP BY t1.mobile, transaction_id
),

cheese AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('CHEESE & MARGARINES')
    GROUP BY t1.mobile, transaction_id
),

juice AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('JUICE & DRINKS (LONG LIFE)')
    GROUP BY t1.mobile, transaction_id
),

baking AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('HOME BAKING')
    GROUP BY t1.mobile, transaction_id
)

SELECT "FROZEN PASTRY" AS category_name,
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM frozen_pastry

UNION

SELECT "SOUPS",
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM soups

UNION

SELECT "COOKING SAUCES & AIDS",
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM cooking

UNION

SELECT "CHEESE & MARGARINES",
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM cheese

UNION

SELECT "JUICE & DRINKS (LONG LIFE)",
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM juice

UNION

SELECT "HOME BAKING",
    COUNT(DISTINCT mobile) AS customer_count,
    SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
    MEDIAN(sales) AS MTV
FROM baking

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2.3 80-90% percentile Transaction Check

-- COMMAND ----------

WITH customers AS (
    SELECT
        DISTINCT mobile
    FROM gold.pos_transactions
    WHERE business_day >= "2023-01-06"
    AND loyalty_points IS NOT NULL
),

cte AS (
    SELECT
        t1.mobile,
        transaction_id,
        ROUND(SUM(amount),0) AS sales
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN customers AS t4 ON t1.mobile = t4.mobile
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('FROZEN PASTRY', 'SOUPS', 'COOKING SAUCES & AIDS', 'CHEESE & MARGARINES', 'JUICE & DRINKS (LONG LIFE)', 'HOME BAKING')
    GROUP BY t1.mobile, transaction_id
)

SELECT
    PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY sales DESC) OVER() AS 100th_percentile_sales,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sales DESC) OVER() AS 95th_percentile_sales,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY sales DESC) OVER() AS 90th_percentile_sales,
    PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY sales DESC) OVER() AS 85th_percentile_sales,
    PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY sales DESC) OVER() AS 80th_percentile_sales,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sales DESC) OVER() AS 75th_percentile_sales
FROM cte
LIMIT 1

-- COMMAND ----------

-- WITH customers AS (
--     SELECT
--         DISTINCT mobile
--     FROM gold.pos_transactions
--     WHERE business_day >= "2023-01-06"
--     AND loyalty_points IS NOT NULL
-- ),

WITH cte AS (
    SELECT
        t1.mobile,
        ROUND(SUM(amount),0) / COUNT(DISTINCT transaction_id) AS ATV
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE business_day >= "2023-01-06"
    AND amount > 0
    AND quantity > 0
    AND UPPER(category_name) IN ('FROZEN PASTRY', 'SOUPS', 'COOKING SAUCES & AIDS', 'CHEESE & MARGARINES', 'JUICE & DRINKS (LONG LIFE)', 'HOME BAKING')
    GROUP BY t1.mobile
)

SELECT
    percentile(ATV,0.20) as p_20,
    percentile(ATV,0.25) as p_25,
    percentile(ATV,0.35) as p_35,
    percentile(ATV,0.40) as p_40,
    percentile(ATV,0.50) as p_50,
    percentile(ATV,0.75) as p_75,
    percentile(ATV,0.80) as p_80,
    percentile(ATV,0.85) as p_85,
    percentile(ATV,0.90) as p_90,
    percentile(ATV,0.95) as p_95
FROM cte

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Segment-wise RPC, ATV, Customers

-- COMMAND ----------

WITH cte AS (
    SELECT
        mobile,
        segment,
        ROUND(SUM(amount),0) AS sales,
        COUNT(DISTINCT transaction_id) AS num_trans
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE business_day BETWEEN "2023-03-08" AND "2023-04-21"
    AND mobile IS NOT NULL
    AND amount > 0
    AND quantity > 0
    AND key = 'rfm'
    AND channel = 'pos'
    AND t3.country = 'uae'
    AND month_year = '202304'
    -- AND UPPER(category_name) IN ('TABLEWARE', 'KITCHENWARE')
    -- AND UPPER(department_name) IN ('FASHION JEWELLERY & ACCESSORIES', 'FASHION LIFESTYLE', 'FASHION RETAIL', 'FOOTWEAR', 'LADIES BAGS & ACCESSORIES', 'TEXTILES')
    AND UPPER(department_name) IN ('ELECTRICALS', 'COMPUTERS & GAMING', 'MOBILE PHONES', 'ELECTRONICS', 'CONSUMER ELECTRONICS ACCESSORIES')
    GROUP BY mobile, segment
)

SELECT
    segment,
    ROUND(SUM(sales) / COUNT(DISTINCT mobile),2) AS RPC,
    ROUND(SUM(sales) / SUM(num_trans),2) AS ATV,
    COUNT(DISTINCT mobile) AS customers
FROM cte
GROUP BY segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Never Transacted in Category

-- COMMAND ----------

WITH cte AS (
    SELECT
        mobile,
        segment,
        COUNT(DISTINCT CASE WHEN UPPER(category_name) IN ('TABLEWARE', 'KITCHENWARE') THEN transaction_id END) AS num_trans
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE business_day BETWEEN "2022-01-01" AND "2023-12-31"
    AND mobile IS NOT NULL
    AND amount > 0
    AND quantity > 0
    AND key = 'rfm'
    AND channel = 'pos'
    AND t3.country = 'uae'
    AND month_year = '202401'
    GROUP BY mobile, segment
    HAVING num_trans = 0
)

SELECT
    segment,
    COUNT(DISTINCT mobile) AS customers
FROM cte
GROUP BY segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Code-Piyush

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TMS Segment Numbers (PG)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df_tms = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/piyush@loyalytics.in/tms_info_data_exp.csv")
-- MAGIC
-- MAGIC pdf_tms_master = df_tms.toPandas()
-- MAGIC campaign_list = pdf_tms_master.campaign_id.unique().tolist()
-- MAGIC
-- MAGIC campaign_id_test = 'W-2C4'
-- MAGIC
-- MAGIC segment_tuple = tuple(i.lower() for i in set(pdf_tms_master[pdf_tms_master.campaign_id == campaign_id_test]['tms_segment'].values))
-- MAGIC category_tuple = tuple(i.lower() for i in set(pdf_tms_master[pdf_tms_master.campaign_id == campaign_id_test]['category'].values))
-- MAGIC
-- MAGIC print('campaign_list: ', campaign_list)
-- MAGIC print('test segment list: ', segment_tuple)
-- MAGIC print('test category list: ', category_tuple)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC customer_counts_list = []
-- MAGIC
-- MAGIC def get_customer_count(campaign_id, segment_tuple, category_tuple):
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     with lhp_info as (   
-- MAGIC         select account_key as customer_id, email, LHRDATE, accept_email_flg, mobile
-- MAGIC         from gold.customer_profile
-- MAGIC         where LHRDATE is not null and mobile is not null
-- MAGIC         ),
-- MAGIC
-- MAGIC     trans_info AS (
-- MAGIC         SELECT
-- MAGIC             t1.mobile,
-- MAGIC             t1.transaction_id,
-- MAGIC             min(t4.email) as email,
-- MAGIC             min(t4.accept_email_flg) as accept_email_flg,
-- MAGIC             ROUND(SUM(t1.amount),0) AS sales
-- MAGIC         FROM gold.pos_transactions AS t1
-- MAGIC         JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
-- MAGIC         JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
-- MAGIC         JOIN lhp_info AS t4 ON t1.mobile = t4.mobile
-- MAGIC         WHERE t1.business_day >= "2023-01-06"
-- MAGIC         AND t1.amount > 0
-- MAGIC         AND t1.quantity > 0
-- MAGIC         AND t2.key = 'tms'
-- MAGIC         AND t2.channel = 'pos'
-- MAGIC         AND t2.country = 'uae'
-- MAGIC         AND t2.month_year = '202310'
-- MAGIC         AND LOWER(t2.segment) IN {segment_tuple}
-- MAGIC         AND LOWER(t3.category_name) IN {category_tuple}
-- MAGIC         GROUP BY t1.mobile, t1.transaction_id
-- MAGIC     )
-- MAGIC
-- MAGIC     SELECT '{campaign_id}' as campaign_id,
-- MAGIC         COUNT(DISTINCT mobile) AS customer_count,
-- MAGIC         COUNT(DISTINCT email) AS num_emails,
-- MAGIC         COUNT(DISTINCT email) filter(where accept_email_flg=1) as num_email_accept,
-- MAGIC         SUM(sales)/COUNT(DISTINCT transaction_id) AS ATV,
-- MAGIC         MEDIAN(sales) AS MTV,
-- MAGIC         percentile(sales, 0.25) as p_25,
-- MAGIC         percentile(sales, 0.75) as p_75,
-- MAGIC         percentile(sales, 0.90) as p_95
-- MAGIC     FROM trans_info
-- MAGIC     """
-- MAGIC
-- MAGIC     df = spark.sql(query)
-- MAGIC
-- MAGIC     return df
-- MAGIC
-- MAGIC for campaign_id in campaign_list:
-- MAGIC
-- MAGIC     segment_tuple = tuple(i.lower() for i in set(pdf_tms_master[pdf_tms_master.campaign_id == campaign_id]['tms_segment'].values))
-- MAGIC     category_tuple = tuple(i.lower() for i in set(pdf_tms_master[pdf_tms_master.campaign_id == campaign_id]['category'].values))
-- MAGIC     
-- MAGIC     cust_count_temp = get_customer_count(campaign_id, segment_tuple, category_tuple)
-- MAGIC     customer_counts_list.append(cust_count_temp)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from functools import reduce
-- MAGIC
-- MAGIC
-- MAGIC def unionAll(*dfs):
-- MAGIC     return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
-- MAGIC
-- MAGIC # Concatenate all DataFrames in the list
-- MAGIC customer_metrics_raw = unionAll(*customer_counts_list)
-- MAGIC pdf_customer_metrics_raw = customer_metrics_raw.toPandas()
-- MAGIC customer_metrics_raw.display()
-- MAGIC
-- MAGIC # W-2C4	398213	140510	22472

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Expected Conversion Rate
-- MAGIC already idenfied customers <br>
-- MAGIC evaluated in 2024

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from functools import reduce
-- MAGIC
-- MAGIC exp_conv_dict = {}
-- MAGIC weekly_customer_counts_list = []
-- MAGIC
-- MAGIC def get_expected_conversion(campaign_id, segment_tuple, category_tuple):
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     with lhp_info as (   
-- MAGIC         select account_key as customer_id, email, LHRDATE, accept_email_flg, mobile
-- MAGIC         from gold.customer_profile
-- MAGIC         where LHRDATE is not null and mobile is not null
-- MAGIC         ),
-- MAGIC
-- MAGIC     trans_info AS (
-- MAGIC         SELECT
-- MAGIC         YEAR(t1.business_day) as year,
-- MAGIC         weekofyear(t1.business_day) as week_number,
-- MAGIC         t1.mobile,
-- MAGIC         t1.transaction_id,
-- MAGIC         min(t4.email) as email,
-- MAGIC         min(t4.accept_email_flg) as accept_email_flg,
-- MAGIC         ROUND(SUM(t1.amount),0) AS sales
-- MAGIC         FROM gold.pos_transactions AS t1
-- MAGIC         JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
-- MAGIC         JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
-- MAGIC         JOIN lhp_info AS t4 ON t1.mobile = t4.mobile
-- MAGIC         WHERE t1.business_day >= "2023-01-01" and t1.business_day <= "2023-12-31"
-- MAGIC         AND t1.amount > 0
-- MAGIC         AND t1.quantity > 0
-- MAGIC         AND t2.key = 'tms'
-- MAGIC         AND t2.channel = 'pos'
-- MAGIC         AND t2.country = 'uae'
-- MAGIC         AND t2.month_year = '202310'
-- MAGIC         AND LOWER(t2.segment) IN {segment_tuple}
-- MAGIC         AND LOWER(t3.category_name) IN {category_tuple}
-- MAGIC         GROUP BY year, week_number, t1.mobile, t1.transaction_id
-- MAGIC     ),
-- MAGIC
-- MAGIC     weekly_counts as (
-- MAGIC     SELECT '{campaign_id}' as campaign_id,
-- MAGIC         week_number,
-- MAGIC         COUNT(DISTINCT mobile) AS customer_count
-- MAGIC     FROM trans_info
-- MAGIC     WHERE year = 2024
-- MAGIC     GROUP BY 1,2
-- MAGIC     )
-- MAGIC     
-- MAGIC     select campaign_id, round(avg(customer_count),0) as avg_weekly_customers
-- MAGIC     from weekly_counts
-- MAGIC     group by campaign_id
-- MAGIC     """
-- MAGIC
-- MAGIC     df = spark.sql(query)
-- MAGIC
-- MAGIC     return df
-- MAGIC
-- MAGIC for campaign_id in campaign_list:
-- MAGIC
-- MAGIC     segment_tuple = tuple(i.lower() for i in set(pdf_tms_master[pdf_tms_master.campaign_id == campaign_id]['tms_segment'].values))
-- MAGIC     category_tuple = tuple(i.lower() for i in set(pdf_tms_master[pdf_tms_master.campaign_id == campaign_id]['category'].values))
-- MAGIC     
-- MAGIC     cust_count_temp = get_expected_conversion(campaign_id, segment_tuple, category_tuple)
-- MAGIC     weekly_customer_counts_list.append(cust_count_temp)
-- MAGIC
-- MAGIC
-- MAGIC def unionAll(*dfs):
-- MAGIC     return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
-- MAGIC
-- MAGIC # Concatenate all DataFrames in the list
-- MAGIC weekly_customer_counts_merged = unionAll(*weekly_customer_counts_list)
-- MAGIC pdf_weekly_customers = weekly_customer_counts_merged.toPandas()
-- MAGIC
-- MAGIC weekly_customer_counts_merged.display()
-- MAGIC
-- MAGIC # W-2C4 63485

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # final dataframe with conversion numbers
-- MAGIC
-- MAGIC pdf_final_merged = pdf_customer_metrics_raw.merge(pdf_weekly_customers, on="campaign_id")
-- MAGIC spark.createDataFrame(pdf_final_merged).display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## RFM-Dept activation metrics (2022)
-- MAGIC ramadan 2022

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TCG

-- COMMAND ----------

with rfm_segment_info as (
    select customer_id, segment
    from analytics.customer_segments
    where country = 'uae'
    AND month_year = '202304'
    AND key = 'rfm'
    AND channel = 'pos'
    ),


pre_ramadan_trans_data as (
  select t1.*, t2.*, t3.segment,
  case when business_day between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as is_ramadan_trans
  from gold.pos_transactions t1
  join gold.material_master t2
  on t1.product_id = t2.material_id
  join rfm_segment_info t3
  on t1.customer_id = t3.customer_id
  where t1.business_day between '2022-01-01' and date_add('2023-03-22',30)
  and round(t1.amount,2) > 0
  and t1.quantity > 0
  and t1.mobile is not null
  and t1.transaction_type in ('SALE','SELL_MEDIA')
  and upper(t2.department_name) in ('ELECTRICALS',
                                    'COMPUTERS & GAMING',
                                    'MOBILE PHONES',
                                    'ELECTRONICS',
                                    'CONSUMER ELECTRONICS ACCESSORIES'
                                    )
)



select
t1.segment,
t2.total_segment_customers,
-- t1.total_dept_customers,
-- t1.total_ramadan_customers,
t1.total_ramadan_activated
-- t1.ramadan_activation_perc
from(
    select
    segment,
    count(customer_id) as total_dept_customers,
    sum(ramadan_trans_flag) as total_ramadan_customers,
    sum(ramadan_activated_flag) as total_ramadan_activated,
    sum(ramadan_activated_flag)/count(customer_id) as ramadan_activation_perc
    from(
        select
        customer_id,
        min(segment) as segment,
        min(business_day) as first_transaction_date,
        case when max(is_ramadan_trans) = 1 then 1 else 0 end as ramadan_trans_flag,
        case when min(business_day) between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as ramadan_activated_flag
        from pre_ramadan_trans_data
        group by customer_id
    )
    group by segment) t1

join (select segment, count(customer_id) as total_segment_customers
      from rfm_segment_info
      group by segment) t2

on t1.segment = t2.segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fashion

-- COMMAND ----------

with rfm_segment_info as (
    select customer_id, segment
    from analytics.customer_segments
    where country = 'uae'
    AND month_year = '202304'
    AND key = 'rfm'
    AND channel = 'pos'
    ),


pre_ramadan_trans_data as (
  select t1.*, t2.*, t3.segment,
  case when transaction_date between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as is_ramadan_trans
  from gold.pos_transactions t1
  join gold.material_master t2
  on t1.product_id = t2.material_id
  join rfm_segment_info t3
  on t1.customer_id = t3.customer_id
  where t1.business_day between '2022-01-01' and '2023-04-21'
  and round(t1.amount,2) > 0
  and t1.quantity > 0
  and t1.mobile is not null
  and t1.transaction_type in ('SALE','SELL_MEDIA')
  and upper(t2.department_name) in ('FASHION JEWELLERY & ACCESSORIES',
                                    'FASHION LIFESTYLE',
                                    'FASHION RETAIL',
                                    'FOOTWEAR',
                                    'LADIES BAGS & ACCESSORIES',
                                    'TEXTILES'
                                    )
)



select
t1.segment,
t2.total_segment_customers,
-- t1.total_dept_customers,
-- t1.total_ramadan_customers,
t1.total_ramadan_activated
-- t1.ramadan_activation_perc
from(
    select
    segment,
    count(customer_id) as total_dept_customers,
    sum(ramadan_trans_flag) as total_ramadan_customers,
    sum(ramadan_activated_flag) as total_ramadan_activated,
    sum(ramadan_activated_flag)/count(customer_id) as ramadan_activation_perc
    from(
        select
        customer_id,
        min(segment) as segment,
        min(transaction_date) as first_transaction_date,
        case when max(is_ramadan_trans) = 1 then 1 else 0 end as ramadan_trans_flag,
        case when min(transaction_date) between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as ramadan_activated_flag
        from pre_ramadan_trans_data
        group by customer_id
    )
    group by segment) t1

join (select segment, count(customer_id) as total_segment_customers
      from rfm_segment_info
      group by segment) t2
      
on t1.segment = t2.segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Home

-- COMMAND ----------

with rfm_segment_info as (
    select customer_id, segment
    from analytics.customer_segments
    where country = 'uae'
    AND month_year = '202304'
    AND key = 'rfm'
    AND channel = 'pos'
    ),


pre_ramadan_trans_data as (
  select t1.*, t2.*, t3.segment,
  case when transaction_date between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as is_ramadan_trans
  from gold.pos_transactions t1
  join gold.material_master t2
  on t1.product_id = t2.material_id
  join rfm_segment_info t3
  on t1.customer_id = t3.customer_id
  where t1.business_day between '2022-01-01' and '2023-04-21'
  and round(t1.amount,2) > 0
  and t1.quantity > 0
  and t1.mobile is not null
  and t1.transaction_type in ('SALE','SELL_MEDIA')
  and lower(t2.category_name) in ('kitchenware', 'tableware')
)



select
t1.segment,
t2.total_segment_customers,
-- t1.total_dept_customers,
-- t1.total_ramadan_customers,
t1.total_ramadan_activated
-- t1.ramadan_activation_perc
from(
    select
    segment,
    count(customer_id) as total_dept_customers,
    sum(ramadan_trans_flag) as total_ramadan_customers,
    sum(ramadan_activated_flag) as total_ramadan_activated,
    sum(ramadan_activated_flag)/count(customer_id) as ramadan_activation_perc
    from(
        select
        customer_id,
        min(segment) as segment,
        min(transaction_date) as first_transaction_date,
        case when max(is_ramadan_trans) = 1 then 1 else 0 end as ramadan_trans_flag,
        case when min(transaction_date) between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as ramadan_activated_flag
        from pre_ramadan_trans_data
        group by customer_id
    )
    group by segment) t1

join (select segment, count(customer_id) as total_segment_customers
      from rfm_segment_info
      group by segment) t2
      
on t1.segment = t2.segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## activation code (updated)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # list of dept.
-- MAGIC fashion_list = ('FASHION JEWELLERY & ACCESSORIES', 'FASHION LIFESTYLE','FASHION RETAIL', 'FOOTWEAR', 'LADIES BAGS & ACCESSORIES', 'TEXTILES')
-- MAGIC # list of dept.
-- MAGIC tcg_list = ('ELECTRICALS','COMPUTERS & GAMING','MOBILE PHONES','ELECTRONICS','CONSUMER ELECTRONICS ACCESSORIES')
-- MAGIC # list of categories.
-- MAGIC home_list = ('KITCHENWARE', 'TABLEWARE')
-- MAGIC
-- MAGIC master_list = [fashion_list, tcg_list, home_list]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def ramandan_dept_activations(entity_list, filter_type):
-- MAGIC
-- MAGIC     entity_type = {'department': 'department_name', 'category': 'category_name'}.get(filter_type, None)
-- MAGIC
-- MAGIC     if entity_type is None:
-- MAGIC         print("invalid parameter: 'filter_type'")
-- MAGIC         return None
-- MAGIC
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     with rfm_segment_info as (
-- MAGIC         select customer_id, segment
-- MAGIC         from analytics.customer_segments
-- MAGIC         where country = 'uae'
-- MAGIC         AND month_year = '202304'
-- MAGIC         AND key = 'rfm'
-- MAGIC         AND channel = 'pos'
-- MAGIC         ),
-- MAGIC
-- MAGIC     non_transacting_before_ramadan as (
-- MAGIC         SELECT
-- MAGIC         t1.customer_id,
-- MAGIC         t3.segment,
-- MAGIC         COUNT(DISTINCT CASE WHEN UPPER(t2.{entity_type}) IN {entity_list} THEN transaction_id END) AS num_trans
-- MAGIC         FROM gold.pos_transactions AS t1
-- MAGIC         JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC         JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
-- MAGIC         WHERE t1.business_day BETWEEN "2022-01-01" AND "2023-03-07"
-- MAGIC         AND t1.transaction_type in ('SALE','SELL_MEDIA')
-- MAGIC         AND t1.mobile IS NOT NULL
-- MAGIC         AND t1.amount > 0
-- MAGIC         AND t1.quantity > 0
-- MAGIC         AND key = 'rfm'
-- MAGIC         AND t3.channel = 'pos'
-- MAGIC         AND t3.country = 'uae'
-- MAGIC         AND t3.month_year = '202304'
-- MAGIC         AND t3.segment in ('Frequentist','Moderate','Slipping Loyalist','VIP')
-- MAGIC         GROUP BY t1.customer_id, t3.segment
-- MAGIC         HAVING num_trans = 0
-- MAGIC     ),
-- MAGIC
-- MAGIC     non_transacting_seg_agg as (
-- MAGIC     select segment, count(distinct customer_id) as num_non_activated
-- MAGIC     from non_transacting_before_ramadan
-- MAGIC     group by segment
-- MAGIC     ),
-- MAGIC
-- MAGIC
-- MAGIC     pre_ramadan_trans_data as (
-- MAGIC     select t1.*, t2.*, t3.segment,
-- MAGIC     case when transaction_date between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as is_ramadan_trans
-- MAGIC     from gold.pos_transactions t1
-- MAGIC     join gold.material_master t2
-- MAGIC     on t1.product_id = t2.material_id
-- MAGIC     join non_transacting_before_ramadan t3
-- MAGIC     on t1.customer_id = t3.customer_id
-- MAGIC     where t1.business_day between '2022-01-01' and '2023-04-21'
-- MAGIC     and round(t1.amount,2) > 0
-- MAGIC     and t1.quantity > 0
-- MAGIC     and t1.mobile is not null
-- MAGIC     and t1.transaction_type in ('SALE','SELL_MEDIA')
-- MAGIC     and upper(t2.{entity_type}) in {entity_list}
-- MAGIC     )
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC     select
-- MAGIC     t1.segment,
-- MAGIC     t2.total_segment_customers,
-- MAGIC     t3.num_non_activated,
-- MAGIC     -- t1.total_dept_customers,
-- MAGIC     t1.total_ramadan_customers,
-- MAGIC     t1.total_ramadan_activated,
-- MAGIC     t1.total_ramadan_activated/t3.num_non_activated as activation_perc
-- MAGIC     from(
-- MAGIC         select
-- MAGIC         segment,
-- MAGIC         count(customer_id) as total_dept_customers,
-- MAGIC         sum(ramadan_trans_flag) as total_ramadan_customers,
-- MAGIC         sum(ramadan_activated_flag) as total_ramadan_activated
-- MAGIC         from(
-- MAGIC             select
-- MAGIC             customer_id,
-- MAGIC             min(segment) as segment,
-- MAGIC             min(transaction_date) as first_transaction_date,
-- MAGIC             case when max(is_ramadan_trans) = 1 then 1 else 0 end as ramadan_trans_flag,
-- MAGIC             case when min(transaction_date) between date_add('2023-03-22',-14) and date_add('2023-03-22',30) then 1 else 0 end as ramadan_activated_flag
-- MAGIC             from pre_ramadan_trans_data
-- MAGIC             group by customer_id
-- MAGIC         )
-- MAGIC         group by segment) t1
-- MAGIC
-- MAGIC     join (select segment, count(customer_id) as total_segment_customers
-- MAGIC         from rfm_segment_info
-- MAGIC         group by segment) t2
-- MAGIC     on t1.segment = t2.segment
-- MAGIC
-- MAGIC     join non_transacting_seg_agg t3
-- MAGIC     on t1.segment = t3.segment
-- MAGIC
-- MAGIC     """
-- MAGIC
-- MAGIC     df = spark.sql(query)
-- MAGIC
-- MAGIC     return df
-- MAGIC
-- MAGIC df_activation_counts = ramandan_dept_activations(home_list, filter_type='category')
-- MAGIC df_activation_counts.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customer Attribute diff.
-- MAGIC (activated vs non activated)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## difference in attributes of those activated vs non activated
-- MAGIC
-- MAGIC def ramandan_dept_activations(entity_list, filter_type):
-- MAGIC
-- MAGIC     entity_type = {'department': 'department_name', 'category': 'category_name'}.get(filter_type, None)
-- MAGIC
-- MAGIC     if entity_type is None:
-- MAGIC         print("invalid parameter: 'filter_type'")
-- MAGIC         return None
-- MAGIC
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     with rfm_segment_info as (
-- MAGIC         select customer_id, segment, average_order_value,
-- MAGIC         inter_purchase_time, total_orders, total_spend, recency
-- MAGIC         from analytics.customer_segments
-- MAGIC         where country = 'uae'
-- MAGIC         AND month_year = '202304'
-- MAGIC         AND key = 'rfm'
-- MAGIC         AND channel = 'pos'
-- MAGIC         AND segment in ('Frequentist','Moderate','Slipping Loyalist','VIP')
-- MAGIC         ),
-- MAGIC
-- MAGIC     inactive_customers as (
-- MAGIC     SELECT
-- MAGIC     t1.customer_id,
-- MAGIC     COUNT(DISTINCT CASE WHEN UPPER(t2.{entity_type}) IN {entity_list} THEN transaction_id END) AS num_trans
-- MAGIC     FROM gold.pos_transactions AS t1
-- MAGIC     JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC     WHERE t1.business_day BETWEEN "2022-01-01" AND date_add('2023-03-22',-15)
-- MAGIC     --- till the date just before ramadan extended period starts
-- MAGIC     AND t1.transaction_type in ('SALE','SELL_MEDIA')
-- MAGIC     AND t1.mobile IS NOT NULL
-- MAGIC     AND t1.amount > 0
-- MAGIC     AND t1.quantity > 0
-- MAGIC     GROUP BY t1.customer_id
-- MAGIC     HAVING num_trans = 0
-- MAGIC     ),
-- MAGIC
-- MAGIC     ramadan_info_customer_agg as (
-- MAGIC     select customer_id, count(distinct transaction_id) as num_trans,
-- MAGIC     sum(amount) as total_sales, count(distinct category_name) as num_categories,
-- MAGIC     count(distinct product_id) as num_products
-- MAGIC     from(
-- MAGIC         select t1.*, t2.*
-- MAGIC         from gold.pos_transactions t1
-- MAGIC         join gold.material_master t2
-- MAGIC         on t1.product_id = t2.material_id
-- MAGIC         join inactive_customers t3
-- MAGIC         on t1.customer_id = t3.customer_id
-- MAGIC         where t1.business_day between date_add('2023-03-22',-14) and date_add('2023-03-22',30)
-- MAGIC         and round(t1.amount,2) > 0
-- MAGIC         and t1.quantity > 0
-- MAGIC         and t1.mobile is not null
-- MAGIC         and t1.transaction_type in ('SALE','SELL_MEDIA')
-- MAGIC         and upper(t2.{entity_type}) in {entity_list}
-- MAGIC         )
-- MAGIC     group by customer_id
-- MAGIC     )
-- MAGIC
-- MAGIC
-- MAGIC     select segment, ramadan_activated_flag, count(customer_id) as num_customers,
-- MAGIC     sum(total_spend)/sum(total_orders) as ATV, avg(inter_purchase_time) as IPT,
-- MAGIC     avg(recency) as recency, avg(total_orders) as frequency, avg(total_spend) as RPC
-- MAGIC
-- MAGIC     from(
-- MAGIC         select t1.*, t2.segment,
-- MAGIC         t2.average_order_value,
-- MAGIC         t2.inter_purchase_time,
-- MAGIC         t2.total_orders,
-- MAGIC         t2.total_spend,
-- MAGIC         t2.recency
-- MAGIC
-- MAGIC         from(
-- MAGIC             select a.customer_id, case when b.customer_id is null then 0 else 1 end as ramadan_activated_flag
-- MAGIC             from inactive_customers a left join ramadan_info_customer_agg b
-- MAGIC             on a.customer_id = b.customer_id
-- MAGIC             ) t1
-- MAGIC         join rfm_segment_info t2
-- MAGIC         on t1.customer_id = t2.customer_id
-- MAGIC     )
-- MAGIC     group by segment, ramadan_activated_flag
-- MAGIC  
-- MAGIC
-- MAGIC     """
-- MAGIC
-- MAGIC     df = spark.sql(query)
-- MAGIC
-- MAGIC     return df
-- MAGIC
-- MAGIC
-- MAGIC item_dict = {'fashion':fashion_list, 'tcg': tcg_list, 'home': home_list}
-- MAGIC
-- MAGIC
-- MAGIC df_attribute_diff = ramandan_dept_activations(tcg_list, filter_type='department')
-- MAGIC df_attribute_diff.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## customer campaign touches

-- COMMAND ----------

with customer_campaign_opens as (

    select customer_key, campaign_channel, count(*) as num_campaigns,
    sum(count(*)) over(partition by customer_key) as total_campaigns,
    sum(case when delivery_status = 'opened' then 1 else 0 end) as num_opens,
    sum(sum(case when delivery_status = 'opened' then 1 else 0 end)) over(partition by customer_key) as total_opens,
    sum(sum(case when delivery_status = 'opened' then 1 else 0 end)) over(partition by customer_key)/sum(count(*)) over(partition by customer_key) as overall_open_rate
    from analytics.campaign_delivery
    group by customer_key, campaign_channel
    order by customer_key
    )



select distinct customer_key, total_campaigns, total_opens, overall_open_rate,
case when total_campaigns >=3 and overall_open_rate = 0 then 1 else 0
     end as to_exclude_flag -- flexible definition
from customer_campaign_opens

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## email availibility

-- COMMAND ----------

 with rfm_segment_info as (
        select customer_id, segment
        from analytics.customer_segments
        where country = 'uae'
        AND month_year = '202401'
        AND key = 'rfm'
        AND channel = 'pos'
        ),
  
  lhp_info as (
    select account_key as customer_id, email, mobile
    from gold.customer_profile
    where LHRDATE is not null
  )


select segment,
count(email) as num_emails,
count(*) as num_customers,
count(email)/count(*) as email_availability_perc
from rfm_segment_info t1
join lhp_info t2 on t1.customer_id = t2.customer_id
group by segment
order by email_availability_perc desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## activation campaign product list

-- COMMAND ----------

drop table if exists sandbox.pg_ramandan_acquisition_items;

create table sandbox.pg_ramandan_acquisition_items as (

WITH input_ids AS (
  SELECT EXPLODE(ARRAY(
    18, 20, 22, 43, 51, 52, 53, 54, -- Fashion
    19004, 19003, 19001, 19002, 19010, -- Home
    36003017, 36003001, 36009002, 36007004, 16005018, 36003002, 36003003, 16005001, 
    16004001, 16004002, -- Updated Glassware IDs for TCG
    17013006, 17013007, 17013008, 17013009, 17014 -- TCG
  )) AS input_id
),


id_theme_mapping AS (
  SELECT id, theme
  FROM VALUES
    (18, 'Fashion'), (20, 'Fashion'), (22, 'Fashion'), (43, 'Fashion'), (51, 'Fashion'), (52, 'Fashion'), (53, 'Fashion'), (54, 'Fashion'),
    (19004, 'Home'), (19003, 'Home'), (19001, 'Home'), (19002, 'Home'), (19010, 'Home'),
    (36003017, 'TCG'), (36003001, 'TCG'), (36009002, 'TCG'), (36007004, 'TCG'), (16005018, 'TCG'), (36003002, 'TCG'), (36003003, 'TCG'), (16005001, 'TCG'), 
    (16004001, 'TCG'), (16004002, 'TCG'), -- Updated Glassware IDs for TCG
    (17013006, 'TCG'), (17013007, 'TCG'), (17013008, 'TCG'), (17013009, 'TCG'), (17014, 'TCG')
  mapping(id, theme)
),

id_groupings as (
SELECT
DISTINCT
  i.input_id,
  m.theme,
  CASE
    WHEN d.material_id IS NOT NULL THEN 'Department'
    WHEN c.material_id IS NOT NULL THEN 'Category'
    WHEN mm.material_id IS NOT NULL THEN 'Material Group'
  END AS mapped_entity
FROM input_ids i
JOIN id_theme_mapping m ON i.input_id = m.id
LEFT JOIN gold.material_master d ON i.input_id = d.department_id
LEFT JOIN gold.material_master c ON i.input_id = c.category_id
LEFT JOIN gold.material_master mm ON i.input_id = mm.material_group_id
WHERE d.material_id IS NOT NULL OR c.material_id IS NOT NULL OR mm.material_id IS NOT NULL)


SELECT DISTINCT
   CASE
    WHEN s.theme = 'TCG' AND s.mapped_entity = 'Category' THEN m.material_group_id
    ELSE  s.input_id
  END AS  input_id,
  lower(s.theme) as campaign_theme,
  lower(CASE
    WHEN s.theme = 'TCG' AND s.mapped_entity = 'Category' THEN 'Material Group'
    ELSE s.mapped_entity
  END) AS mapped_entity
FROM id_groupings s
LEFT JOIN gold.material_master m ON s.input_id = m.category_id
ORDER BY campaign_theme, input_id

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ATV, RPC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def get_entity_ids (theme_name):
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     select distinct input_id
-- MAGIC     from sandbox.pg_ramandan_acquisition_items
-- MAGIC     where campaign_theme = '{theme_name}'
-- MAGIC     """
-- MAGIC
-- MAGIC     id_tuple = tuple(row['input_id'] for row in spark.sql(query).collect())
-- MAGIC     return id_tuple

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import DataFrame
-- MAGIC
-- MAGIC def get_customer_metrics(theme_name, filter_type):
-- MAGIC
-- MAGIC     entity_type = {'department': 'department_id', 'category': 'category_id', 'material group': 'material_group_id'}.get(filter_type, None)
-- MAGIC
-- MAGIC     if entity_type is None:
-- MAGIC         print("invalid parameter: 'filter_type'")
-- MAGIC         return None
-- MAGIC     
-- MAGIC     entity_list = get_entity_ids(theme_name)
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     WITH cte AS (
-- MAGIC         SELECT
-- MAGIC             mobile,
-- MAGIC             segment,
-- MAGIC             ROUND(SUM(amount),0) AS sales,
-- MAGIC             COUNT(DISTINCT transaction_id) AS num_trans
-- MAGIC         FROM gold.pos_transactions AS t1
-- MAGIC         JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC         JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
-- MAGIC         WHERE business_day BETWEEN "2023-03-08" AND "2023-04-21"
-- MAGIC         AND mobile IS NOT NULL
-- MAGIC         AND amount > 0
-- MAGIC         AND quantity > 0
-- MAGIC         AND key = 'rfm'
-- MAGIC         AND channel = 'pos'
-- MAGIC         AND t3.country = 'uae'
-- MAGIC         AND month_year = '202304'
-- MAGIC         AND {entity_type} IN {entity_list}
-- MAGIC         GROUP BY mobile, segment
-- MAGIC     )
-- MAGIC
-- MAGIC     SELECT
-- MAGIC         '{theme_name}' as theme,
-- MAGIC         segment,
-- MAGIC         ROUND(SUM(sales) / COUNT(DISTINCT mobile),2) AS RPC,
-- MAGIC         ROUND(SUM(sales) / SUM(num_trans),2) AS ATV,
-- MAGIC         COUNT(DISTINCT mobile) AS customers
-- MAGIC     FROM cte
-- MAGIC     GROUP BY segment
-- MAGIC     """
-- MAGIC
-- MAGIC     output_df = spark.sql(query)
-- MAGIC     return output_df
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC mapping_dict = {'tcg': 'material group', 'fashion': 'department', 'home': 'category'}
-- MAGIC
-- MAGIC dataframes = [get_customer_metrics(theme_name, entity_type) for theme_name, entity_type in mapping_dict.items()]
-- MAGIC
-- MAGIC final_merged_dataframe = dataframes[0]
-- MAGIC
-- MAGIC for dataframe in dataframes[1:]:
-- MAGIC     final_merged_dataframe = final_merged_dataframe.union(dataframe)
-- MAGIC
-- MAGIC final_merged_dataframe.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## activation code [update-2]
-- MAGIC updated code <br>
-- MAGIC updated list

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## difference in attributes of those activated vs non activated
-- MAGIC
-- MAGIC def ramandan_dept_activations(theme_name, entity_type):
-- MAGIC
-- MAGIC     entity_name = {'department': 'department_id', 'category': 'category_id', 'material group': 'material_group_id'}.get(entity_type, None)
-- MAGIC
-- MAGIC     if entity_name is None:
-- MAGIC         print("invalid parameter: 'entity_type'")
-- MAGIC         return None
-- MAGIC     
-- MAGIC     entity_list = get_entity_ids(theme_name)
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     with rfm_segment_info as (
-- MAGIC         select customer_id, segment, average_order_value,
-- MAGIC         inter_purchase_time, total_orders, total_spend, recency
-- MAGIC         from analytics.customer_segments
-- MAGIC         where country = 'uae'
-- MAGIC         AND month_year = '202304'
-- MAGIC         AND key = 'rfm'
-- MAGIC         AND channel = 'pos'
-- MAGIC         AND segment in ('Frequentist','Moderate','Slipping Loyalist','VIP')
-- MAGIC         ),
-- MAGIC
-- MAGIC     inactive_customers as (
-- MAGIC     SELECT
-- MAGIC     t1.customer_id,
-- MAGIC     COUNT(DISTINCT CASE WHEN t2.{entity_name} IN {entity_list} THEN transaction_id END) AS num_trans
-- MAGIC     FROM gold.pos_transactions AS t1
-- MAGIC     JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC     WHERE t1.business_day BETWEEN "2022-01-01" AND date_add('2023-03-22',-15)
-- MAGIC     --- till the date just before ramadan extended period starts
-- MAGIC     AND t1.transaction_type in ('SALE','SELL_MEDIA')
-- MAGIC     AND t1.mobile IS NOT NULL
-- MAGIC     AND t1.amount > 0
-- MAGIC     AND t1.quantity > 0
-- MAGIC     GROUP BY t1.customer_id
-- MAGIC     HAVING num_trans = 0
-- MAGIC     ),
-- MAGIC
-- MAGIC     ramadan_info_customer_agg as (
-- MAGIC     select customer_id, count(distinct transaction_id) as num_trans,
-- MAGIC     sum(amount) as total_sales, count(distinct category_name) as num_categories,
-- MAGIC     count(distinct product_id) as num_products
-- MAGIC     from(
-- MAGIC         select t1.*, t2.*
-- MAGIC         from gold.pos_transactions t1
-- MAGIC         join gold.material_master t2
-- MAGIC         on t1.product_id = t2.material_id
-- MAGIC         where t1.business_day between date_add('2023-03-22',-14) and date_add('2023-03-22',30)
-- MAGIC         and round(t1.amount,2) > 0
-- MAGIC         and t1.quantity > 0
-- MAGIC         and t1.mobile is not null
-- MAGIC         and t1.transaction_type in ('SALE','SELL_MEDIA')
-- MAGIC         and t2.{entity_name} in {entity_list}
-- MAGIC         )
-- MAGIC     group by customer_id
-- MAGIC     )
-- MAGIC
-- MAGIC
-- MAGIC     select '{theme_name}' as theme, segment, ramadan_activated_flag,
-- MAGIC     count(customer_id) as num_customers,
-- MAGIC     sum(total_spend)/sum(total_orders) as ATV, avg(inter_purchase_time) as IPT,
-- MAGIC     avg(recency) as recency, avg(total_orders) as frequency, avg(total_spend) as RPC
-- MAGIC
-- MAGIC     from(
-- MAGIC         select t1.*, t2.segment,
-- MAGIC         t2.average_order_value,
-- MAGIC         t2.inter_purchase_time,
-- MAGIC         t2.total_orders,
-- MAGIC         t2.total_spend,
-- MAGIC         t2.recency
-- MAGIC
-- MAGIC         from(
-- MAGIC             select a.customer_id, case when b.customer_id is null then 0 else 1 end as ramadan_activated_flag
-- MAGIC             from inactive_customers a left join ramadan_info_customer_agg b
-- MAGIC             on a.customer_id = b.customer_id
-- MAGIC             ) t1
-- MAGIC         join rfm_segment_info t2
-- MAGIC         on t1.customer_id = t2.customer_id
-- MAGIC     )
-- MAGIC     group by segment, ramadan_activated_flag
-- MAGIC  
-- MAGIC
-- MAGIC     """
-- MAGIC
-- MAGIC     df = spark.sql(query)
-- MAGIC
-- MAGIC     return df
-- MAGIC
-- MAGIC
-- MAGIC mapping_dict = {'tcg': 'material group', 'fashion': 'department', 'home': 'category'}
-- MAGIC
-- MAGIC dataframes = [ramandan_dept_activations(theme_name, entity_type) for theme_name, entity_type in mapping_dict.items()]
-- MAGIC
-- MAGIC final_merged_dataframe = dataframes[0]
-- MAGIC
-- MAGIC for dataframe in dataframes[1:]:
-- MAGIC     final_merged_dataframe = final_merged_dataframe.union(dataframe)
-- MAGIC
-- MAGIC final_merged_dataframe.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## current inactive
-- MAGIC acquisition candidates

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## difference in attributes of those activated vs non activated
-- MAGIC
-- MAGIC def get_current_inactive_base(theme_name, entity_type, period_start_date, period_end_date):
-- MAGIC
-- MAGIC     entity_name = {'department': 'department_id', 'category': 'category_id', 'material group': 'material_group_id'}.get(entity_type, None)
-- MAGIC
-- MAGIC     if entity_name is None:
-- MAGIC         print("invalid parameter: 'entity_type'")
-- MAGIC         return None
-- MAGIC     
-- MAGIC     entity_list = get_entity_ids(theme_name)
-- MAGIC
-- MAGIC     query = f"""
-- MAGIC     with rfm_segment_info as (
-- MAGIC         select customer_id, segment, average_order_value,
-- MAGIC         inter_purchase_time, total_orders, total_spend, recency
-- MAGIC         from analytics.customer_segments
-- MAGIC         where country = 'uae'
-- MAGIC         AND month_year = '202401'
-- MAGIC         AND key = 'rfm'
-- MAGIC         AND channel = 'pos'
-- MAGIC         AND segment in ('Frequentist','Moderate','Slipping Loyalist','VIP')
-- MAGIC         ),
-- MAGIC
-- MAGIC     inactive_customers as (
-- MAGIC     SELECT
-- MAGIC     t1.customer_id,
-- MAGIC     COUNT(DISTINCT CASE WHEN t2.{entity_name} IN {entity_list} THEN transaction_id END) AS num_trans
-- MAGIC     FROM gold.pos_transactions AS t1
-- MAGIC     JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC     WHERE t1.business_day BETWEEN '{period_start_date}' AND '{period_end_date}'
-- MAGIC     AND t1.transaction_type in ('SALE','SELL_MEDIA')
-- MAGIC     AND t1.mobile IS NOT NULL
-- MAGIC     AND t1.amount > 0
-- MAGIC     AND t1.quantity > 0
-- MAGIC     GROUP BY t1.customer_id
-- MAGIC     HAVING num_trans = 0
-- MAGIC     )
-- MAGIC
-- MAGIC
-- MAGIC     select '{theme_name}' as theme, t2.segment,
-- MAGIC     count(t1.customer_id) as num_customers
-- MAGIC     from inactive_customers t1
-- MAGIC     join rfm_segment_info t2
-- MAGIC     on t1.customer_id = t2.customer_id
-- MAGIC     group by t2.segment
-- MAGIC  
-- MAGIC     """
-- MAGIC
-- MAGIC     df = spark.sql(query)
-- MAGIC
-- MAGIC     return df
-- MAGIC
-- MAGIC
-- MAGIC period_start_date_1, period_end_date_1 = '2023-01-01', '2024-02-28' # from 2023
-- MAGIC period_start_date_2, period_end_date_2 = '2022-01-01', '2024-02-28' # extended period (inclsive 22)
-- MAGIC
-- MAGIC period_type = 2
-- MAGIC
-- MAGIC if period_type == 1:
-- MAGIC     period_start_date, period_end_date = period_start_date_1, period_end_date_1
-- MAGIC
-- MAGIC if period_type == 2:
-- MAGIC     period_start_date, period_end_date = period_start_date_2, period_end_date_2
-- MAGIC
-- MAGIC
-- MAGIC mapping_dict = {'tcg': 'material group', 'fashion': 'department', 'home': 'category'}
-- MAGIC dataframes = [get_current_inactive_base(theme_name, entity_type, period_start_date, period_end_date) for theme_name, entity_type in mapping_dict.items()]
-- MAGIC
-- MAGIC final_merged_dataframe = dataframes[0]
-- MAGIC
-- MAGIC for dataframe in dataframes[1:]:
-- MAGIC     final_merged_dataframe = final_merged_dataframe.union(dataframe)
-- MAGIC
-- MAGIC final_merged_dataframe.display()

-- COMMAND ----------


