-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Sandbox Table

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_basmati_analysis AS (
  SELECT DISTINCT
      transaction_id,
      material_id,
      material_name,
      brand,
      customer_id,
      nationality_group,
      quantity,
      unit_price,
      regular_unit_price,
      actual_unit_price,
      amount,
      mocd_flag
  FROM gold.pos_transactions AS t1
  JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
  JOIN gold.customer_profile AS t3 ON t1.customer_id = t3.account_key
  WHERE business_day BETWEEN "2023-03-01" AND "2024-02-29"
  AND nationality_group NOT IN ("NA", "OTHERS")
  AND amount > 0
  AND quantity > 0
  AND material_group_name = "BASMATI"
  -- AND material_id IN (769822, 769823, 998605, 1995943, 1001554,
  --                     1699150, 1999515, 1204799, 2139524, 1677285,
  --                     430465, 2090174, 2093503, 2073576, 791385,
  --                     430464, 2012372, 1380377, 1913551, 2166888,
  --                     1380376, 1535690, 1535719, 1535688, 1380378,
  --                     1397412, 4898, 4900, 2008309, 4899, 4897, 4903, 4902)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Sales

-- COMMAND ----------

WITH cte AS (
  SELECT
      brand,
      material_name,
      nationality_group,
      ROUND(SUM(amount)) AS sales,
      COUNT(DISTINCT transaction_id) AS transactions,
      ROUND(SUM(quantity)) AS quantity_sold
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("GAUTAM", "SINNARA")
  GROUP BY brand, material_name, nationality_group
)

SELECT
    brand,
    material_name,
    MAX(CASE WHEN nationality_group = "AFRICAN" THEN sales ELSE 0 END) AS African_sales,
    MAX(CASE WHEN nationality_group = "ARABS" THEN sales ELSE 0 END) AS Arabs_sales,
    MAX(CASE WHEN nationality_group = "ASIANS" THEN sales ELSE 0 END) AS Asians_sales,
    MAX(CASE WHEN nationality_group = "BANGLADESHI" THEN sales ELSE 0 END) AS Bangladeshi_sales,
    MAX(CASE WHEN nationality_group = "EGYPTIAN" THEN sales ELSE 0 END) AS Egyptian_sales,
    MAX(CASE WHEN nationality_group = "EMIRATI" THEN sales ELSE 0 END) AS Emirati_sales,
    MAX(CASE WHEN nationality_group = "EUROPEAN" THEN sales ELSE 0 END) AS European_sales,
    MAX(CASE WHEN nationality_group = "FILIPINO" THEN sales ELSE 0 END) AS Filipino_sales,
    MAX(CASE WHEN nationality_group = "INDIAN" THEN sales ELSE 0 END) AS Indian_sales,
    MAX(CASE WHEN nationality_group = "PAKISTANI" THEN sales ELSE 0 END) AS Pakistani_sales,
    MAX(CASE WHEN nationality_group = "SRI-LANKAN" THEN sales ELSE 0 END) AS Srilankan_sales
FROM cte
GROUP BY brand, material_name

-- COMMAND ----------

WITH cte AS (
  SELECT
      brand,
      nationality_group,
      ROUND(SUM(amount)) AS sales,
      COUNT(DISTINCT transaction_id) AS transactions,
      ROUND(SUM(quantity)) AS quantity_sold
  FROM sandbox.pj_basmati_analysis
  GROUP BY brand, nationality_group
)

SELECT
    brand,
    MAX(CASE WHEN nationality_group = "AFRICAN" THEN sales ELSE 0 END) AS African_sales,
    MAX(CASE WHEN nationality_group = "ARABS" THEN sales ELSE 0 END) AS Arabs_sales,
    MAX(CASE WHEN nationality_group = "ASIANS" THEN sales ELSE 0 END) AS Asians_sales,
    MAX(CASE WHEN nationality_group = "BANGLADESHI" THEN sales ELSE 0 END) AS Bangladeshi_sales,
    MAX(CASE WHEN nationality_group = "EGYPTIAN" THEN sales ELSE 0 END) AS Egyptian_sales,
    MAX(CASE WHEN nationality_group = "EMIRATI" THEN sales ELSE 0 END) AS Emirati_sales,
    MAX(CASE WHEN nationality_group = "EUROPEAN" THEN sales ELSE 0 END) AS European_sales,
    MAX(CASE WHEN nationality_group = "FILIPINO" THEN sales ELSE 0 END) AS Filipino_sales,
    MAX(CASE WHEN nationality_group = "INDIAN" THEN sales ELSE 0 END) AS Indian_sales,
    MAX(CASE WHEN nationality_group = "PAKISTANI" THEN sales ELSE 0 END) AS Pakistani_sales,
    MAX(CASE WHEN nationality_group = "SRI-LANKAN" THEN sales ELSE 0 END) AS Srilankan_sales
FROM cte
GROUP BY brand

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Transactions

-- COMMAND ----------

WITH cte AS (
  SELECT
      brand,
      material_name,
      nationality_group,
      ROUND(SUM(amount)) AS sales,
      COUNT(DISTINCT transaction_id) AS transactions,
      ROUND(SUM(quantity)) AS quantity_sold
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("GAUTAM", "SINNARA")
  GROUP BY brand, material_name, nationality_group
)

SELECT
    brand,
    material_name,
    MAX(CASE WHEN nationality_group = "AFRICAN" THEN transactions ELSE 0 END) AS African_transactions,
    MAX(CASE WHEN nationality_group = "ARABS" THEN transactions ELSE 0 END) AS Arabs_transactions,
    MAX(CASE WHEN nationality_group = "ASIANS" THEN transactions ELSE 0 END) AS Asians_transactions,
    MAX(CASE WHEN nationality_group = "BANGLADESHI" THEN transactions ELSE 0 END) AS Bangladeshi_transactions,
    MAX(CASE WHEN nationality_group = "EGYPTIAN" THEN transactions ELSE 0 END) AS Egyptian_transactions,
    MAX(CASE WHEN nationality_group = "EMIRATI" THEN transactions ELSE 0 END) AS Emirati_transactions,
    MAX(CASE WHEN nationality_group = "EUROPEAN" THEN transactions ELSE 0 END) AS European_transactions,
    MAX(CASE WHEN nationality_group = "FILIPINO" THEN transactions ELSE 0 END) AS Filipino_transactions,
    MAX(CASE WHEN nationality_group = "INDIAN" THEN transactions ELSE 0 END) AS Indian_transactions,
    MAX(CASE WHEN nationality_group = "PAKISTANI" THEN transactions ELSE 0 END) AS Pakistani_transactions,
    MAX(CASE WHEN nationality_group = "SRI-LANKAN" THEN transactions ELSE 0 END) AS Srilankan_transactions
FROM cte
GROUP BY brand, material_name

-- COMMAND ----------

WITH cte AS (
  SELECT
      brand,
      nationality_group,
      ROUND(SUM(amount)) AS sales,
      COUNT(DISTINCT transaction_id) AS transactions,
      ROUND(SUM(quantity)) AS quantity_sold
  FROM sandbox.pj_basmati_analysis
  GROUP BY brand, nationality_group
)

SELECT
    brand,
    MAX(CASE WHEN nationality_group = "AFRICAN" THEN transactions ELSE 0 END) AS African_transactions,
    MAX(CASE WHEN nationality_group = "ARABS" THEN transactions ELSE 0 END) AS Arabs_transactions,
    MAX(CASE WHEN nationality_group = "ASIANS" THEN transactions ELSE 0 END) AS Asians_transactions,
    MAX(CASE WHEN nationality_group = "BANGLADESHI" THEN transactions ELSE 0 END) AS Bangladeshi_transactions,
    MAX(CASE WHEN nationality_group = "EGYPTIAN" THEN transactions ELSE 0 END) AS Egyptian_transactions,
    MAX(CASE WHEN nationality_group = "EMIRATI" THEN transactions ELSE 0 END) AS Emirati_transactions,
    MAX(CASE WHEN nationality_group = "EUROPEAN" THEN transactions ELSE 0 END) AS European_transactions,
    MAX(CASE WHEN nationality_group = "FILIPINO" THEN transactions ELSE 0 END) AS Filipino_transactions,
    MAX(CASE WHEN nationality_group = "INDIAN" THEN transactions ELSE 0 END) AS Indian_transactions,
    MAX(CASE WHEN nationality_group = "PAKISTANI" THEN transactions ELSE 0 END) AS Pakistani_transactions,
    MAX(CASE WHEN nationality_group = "SRI-LANKAN" THEN transactions ELSE 0 END) AS Srilankan_transactions
FROM cte
GROUP BY brand

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Quantity Sold

-- COMMAND ----------

WITH cte AS (
  SELECT
      brand,
      material_name,
      nationality_group,
      ROUND(SUM(amount)) AS sales,
      COUNT(DISTINCT transaction_id) AS transactions,
      ROUND(SUM(quantity)) AS quantity_sold
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("GAUTAM", "SINNARA")
  GROUP BY brand, material_name, nationality_group
)

SELECT
    brand,
    material_name,
    MAX(CASE WHEN nationality_group = "AFRICAN" THEN quantity_sold ELSE 0 END) AS African_quantity,
    MAX(CASE WHEN nationality_group = "ARABS" THEN quantity_sold ELSE 0 END) AS Arabs_quantity,
    MAX(CASE WHEN nationality_group = "ASIANS" THEN quantity_sold ELSE 0 END) AS Asians_quantity,
    MAX(CASE WHEN nationality_group = "BANGLADESHI" THEN quantity_sold ELSE 0 END) AS Bangladeshi_quantity,
    MAX(CASE WHEN nationality_group = "EGYPTIAN" THEN quantity_sold ELSE 0 END) AS Egyptian_quantity,
    MAX(CASE WHEN nationality_group = "EMIRATI" THEN quantity_sold ELSE 0 END) AS Emirati_quantity,
    MAX(CASE WHEN nationality_group = "EUROPEAN" THEN quantity_sold ELSE 0 END) AS European_quantity,
    MAX(CASE WHEN nationality_group = "FILIPINO" THEN quantity_sold ELSE 0 END) AS Filipino_quantity,
    MAX(CASE WHEN nationality_group = "INDIAN" THEN quantity_sold ELSE 0 END) AS Indian_quantity,
    MAX(CASE WHEN nationality_group = "PAKISTANI" THEN quantity_sold ELSE 0 END) AS Pakistani_quantity,
    MAX(CASE WHEN nationality_group = "SRI-LANKAN" THEN quantity_sold ELSE 0 END) AS Srilankan_quantity
FROM cte
GROUP BY brand, material_name

-- COMMAND ----------

WITH cte AS (
  SELECT
      brand,
      nationality_group,
      ROUND(SUM(amount)) AS sales,
      COUNT(DISTINCT transaction_id) AS transactions,
      ROUND(SUM(quantity)) AS quantity_sold
  FROM sandbox.pj_basmati_analysis
  GROUP BY brand, nationality_group
)

SELECT
    brand,
    MAX(CASE WHEN nationality_group = "AFRICAN" THEN quantity_sold ELSE 0 END) AS African_quantity,
    MAX(CASE WHEN nationality_group = "ARABS" THEN quantity_sold ELSE 0 END) AS Arabs_quantity,
    MAX(CASE WHEN nationality_group = "ASIANS" THEN quantity_sold ELSE 0 END) AS Asians_quantity,
    MAX(CASE WHEN nationality_group = "BANGLADESHI" THEN quantity_sold ELSE 0 END) AS Bangladeshi_quantity,
    MAX(CASE WHEN nationality_group = "EGYPTIAN" THEN quantity_sold ELSE 0 END) AS Egyptian_quantity,
    MAX(CASE WHEN nationality_group = "EMIRATI" THEN quantity_sold ELSE 0 END) AS Emirati_quantity,
    MAX(CASE WHEN nationality_group = "EUROPEAN" THEN quantity_sold ELSE 0 END) AS European_quantity,
    MAX(CASE WHEN nationality_group = "FILIPINO" THEN quantity_sold ELSE 0 END) AS Filipino_quantity,
    MAX(CASE WHEN nationality_group = "INDIAN" THEN quantity_sold ELSE 0 END) AS Indian_quantity,
    MAX(CASE WHEN nationality_group = "PAKISTANI" THEN quantity_sold ELSE 0 END) AS Pakistani_quantity,
    MAX(CASE WHEN nationality_group = "SRI-LANKAN" THEN quantity_sold ELSE 0 END) AS Srilankan_quantity
FROM cte
GROUP BY brand

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #MOCD vs Non-MOCD

-- COMMAND ----------

SELECT
    brand,
    material_name,
    ROUND(SUM(CASE WHEN mocd_flag = 1 THEN amount ELSE 0 END)) AS mocd_sales,
    ROUND(SUM(CASE WHEN mocd_flag IS NULL THEN amount ELSE 0 END)) AS non_mocd_sales,
    COUNT(DISTINCT CASE WHEN mocd_flag = 1 THEN transaction_id END) AS mocd_transactions,
    COUNT(DISTINCT CASE WHEN mocd_flag IS NULL THEN transaction_id END) AS non_mocd_transactions,
    ROUND(SUM(CASE WHEN mocd_flag = 1 THEN quantity ELSE 0 END)) AS mocd_quantity,
    ROUND(SUM(CASE WHEN mocd_flag IS NULL THEN quantity ELSE 0 END)) AS non_mocd_quantity
FROM sandbox.pj_basmati_analysis
WHERE nationality_group = "EMIRATI"
AND brand IN ("GAUTAM", "SINNARA")
GROUP BY brand, material_name

-- COMMAND ----------

SELECT
    brand,
    ROUND(SUM(CASE WHEN mocd_flag = 1 THEN amount ELSE 0 END)) AS mocd_sales,
    ROUND(SUM(CASE WHEN mocd_flag IS NULL THEN amount ELSE 0 END)) AS non_mocd_sales,
    COUNT(DISTINCT CASE WHEN mocd_flag = 1 THEN transaction_id END) AS mocd_transactions,
    COUNT(DISTINCT CASE WHEN mocd_flag IS NULL THEN transaction_id END) AS non_mocd_transactions,
    ROUND(SUM(CASE WHEN mocd_flag = 1 THEN quantity ELSE 0 END)) AS mocd_quantity,
    ROUND(SUM(CASE WHEN mocd_flag IS NULL THEN quantity ELSE 0 END)) AS non_mocd_quantity
FROM sandbox.pj_basmati_analysis
WHERE nationality_group = "EMIRATI"
GROUP BY brand

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Transaction Level Price

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##GAUTAM Products

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS GAUTAM_BASMATI_XXL_CLASSIC_5KG
FROM cte
WHERE material_name = "GAUTAM BASMATI XXL CLASSIC 5KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS GAUTAM_BASMATI_XXL_1121_3KG
FROM cte
WHERE material_name = "GAUTAM BASMATI XXL 1121 3KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS GAUTAM_BASMATI_XXL_1121_20KG
FROM cte
WHERE material_name = "GAUTAM BASMATI XXL 1121 20KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS GAUTAM_BASMATI_XXL_1121_39KG
FROM cte
WHERE material_name = "GAUTAM BASMATI XXL 1121 39KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS GAUTM_BASMATI_XXL_1121_5KG_1KG
FROM cte
WHERE material_name = "GAUTM BASMATI XXL 1121 5KG+1KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS GAUTAM_BASMATI_XXL_1121_10KG
FROM cte
WHERE material_name = "GAUTAM BASMATI XXL 1121 10KG"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##SINNARA Products

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINA_GOLD_IND_BASMATI_XXL_38KG
FROM cte
WHERE material_name = "SINA.GOLD IND.BASMATI XXL 38KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINA_TTNM_PEM_BASMTIXXXXL20KG
FROM cte
WHERE material_name = "SINA.TTNM PEM.BASMTIXXXXL20KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNARA_BASMATI_2KG
FROM cte
WHERE material_name = "SINNARA BASMATI 2KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNARA_BASMATI_L_G_20KG
FROM cte
WHERE material_name = "SINNARA BASMATI L.G 20KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNARA_BASMATI_5KG
FROM cte
WHERE material_name = "SINNARA BASMATI 5KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNA_GOLD_BASMATI_1121_20KG
FROM cte
WHERE material_name = "SINNA.GOLD BASMATI 1121 20KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNARA_BASMATI_10KG
FROM cte
WHERE material_name = "SINNARA BASMATI 10KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNARA_BASMATI_GRN_RICE_39KG
FROM cte
WHERE material_name = "SINNARA BASMATI GRN.RICE 39KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNARA_GOLD_BASMATI_1121_5KG
FROM cte
WHERE material_name = "SINNARA GOLD BASMATI 1121 5KG"

-- COMMAND ----------

WITH cte AS (
  SELECT DISTINCT
      transaction_id,
      material_name,
      actual_unit_price
  FROM sandbox.pj_basmati_analysis
  WHERE brand IN ("SINNARA", "GAUTAM")
)

SELECT DISTINCT
    transaction_id,
    actual_unit_price AS SINNARA_BASMATI_3KG
FROM cte
WHERE material_name = "SINNARA BASMATI 3KG"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Data Validation

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_basmati_analysis_temp AS (
  SELECT DISTINCT
      transaction_id,
      product_id,
      material_name,
      brand,
      customer_id,
      nationality_group,
      quantity,
      unit_price,
      regular_unit_price,
      actual_unit_price,
      amount,
      mocd_flag
  FROM gold.pos_transactions AS t1
  JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
  JOIN gold.customer_profile AS t3 ON t1.customer_id = t3.account_key
  WHERE business_day BETWEEN "2023-01-01" AND "2023-12-31"
  AND amount > 0
  AND quantity > 0
  AND category_name = "RICE"
)

-- COMMAND ----------

SELECT
    ROUND(SUM(amount)) AS sales,
    COUNT(DISTINCT transaction_id) AS trans
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
WHERE business_day BETWEEN "2023-01-01" AND "2023-12-31"
AND amount > 0
AND quantity > 0
AND category_name = "RICE"

-- COMMAND ----------

SELECT
    nationality_group,
    COUNT(DISTINCT transaction_id) AS transactions
FROM gold.pos_transactions AS t1
JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
WHERE mocd_flag = 1
AND business_day BETWEEN "2023-03-01" AND "2024-02-29"
AND amount > 0
AND quantity > 0
GROUP BY nationality_group

-- COMMAND ----------

SELECT COUNT(DISTINCT t1.store_id)
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.customer_profile AS t3 ON t1.customer_id = t3.account_key
JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
WHERE business_day BETWEEN "2023-03-01" AND "2024-02-29"
AND nationality_group NOT IN ("NA", "OTHERS")
AND amount > 0
AND quantity > 0
-- AND material_id IN (769822, 769823, 998605, 1995943, 1001554,
--                     1699150, 1999515, 1204799, 2139524, 1677285,
--                     430465, 2090174, 2093503, 2073576, 791385,
--                     430464, 2012372, 1380377, 1913551, 2166888,
--                     1380376, 1535690, 1535719, 1535688, 1380378,
--                     1397412, 4898, 4900, 2008309, 4899, 4897, 4903, 4902)
AND material_group_name = "BASMATI"

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


