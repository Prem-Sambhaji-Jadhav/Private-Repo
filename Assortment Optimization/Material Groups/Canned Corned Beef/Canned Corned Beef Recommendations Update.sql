-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Monthly Growth - 384585

-- COMMAND ----------

WITH sales_data AS (
    SELECT
        INT(CONCAT(YEAR(t1.business_day), LPAD(MONTH(t1.business_day), 2, 0))) AS year_month,
        t3.region_name,
        t2.material_id,
        SUM(t1.amount) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t1.business_day BETWEEN "2023-10-01" AND "2024-09-28"
        AND t2.category_name = 'CANNED MEATS'
        AND t2.material_group_name = 'CANNED CORNED BEEF'
        AND t1.transaction_type IN ('SALE', 'SELL_MEDIA')
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3
),

gp_data AS (
    SELECT
        CASE WHEN region = 'AUH' THEN 'ABU DHABI'
            WHEN region = 'ALN' THEN 'AL AIN'
            WHEN region = 'DXB' THEN 'DUBAI'
            WHEN region = 'SHJ' THEN 'SHARJAH'
            ELSE 'none' END AS region_name,
        year_month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE country = 'AE'
    AND year_month BETWEEN 202310 AND 202409
),

combined AS (
    SELECT
        t1.year_month,
        t1.region_name,
        t1.material_id,
        t1.sales,
        COALESCE(t1.sales * t2.gp_wth_chargeback / 100, 0) AS gp
    FROM sales_data AS t1
    LEFT JOIN gp_data AS t2
        ON t1.year_month = t2.year_month
        AND t1.region_name = t2.region_name
        AND t1.material_id = t2.material_id
),

monthly_sales AS (
    SELECT
        year_month,
        ROUND(SUM(sales)) AS total_sales,
        ROUND(SUM(gp)) AS total_gp
    FROM combined
    GROUP BY 1
)

SELECT
    year_month,
    total_sales,
    COALESCE(ROUND((total_sales - LAG(total_sales, 1, 0) OVER(ORDER BY year_month)) / LAG(total_sales, 1, 0) OVER(ORDER BY year_month) * 100, 2), 0) AS growth
FROM monthly_sales
ORDER BY year_month

-- COMMAND ----------

WITH sales_data AS (
    SELECT
        INT(CONCAT(YEAR(t1.business_day), LPAD(MONTH(t1.business_day), 2, 0))) AS year_month,
        t3.region_name,
        t2.material_id,
        SUM(t1.amount) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t1.business_day BETWEEN "2023-10-01" AND "2024-09-28"
        AND t2.material_id = 384585
        AND t1.transaction_type IN ('SALE', 'SELL_MEDIA')
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3
),

gp_data AS (
    SELECT
        CASE WHEN region = 'AUH' THEN 'ABU DHABI'
            WHEN region = 'ALN' THEN 'AL AIN'
            WHEN region = 'DXB' THEN 'DUBAI'
            WHEN region = 'SHJ' THEN 'SHARJAH'
            ELSE 'none' END AS region_name,
        year_month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE country = 'AE'
    AND year_month BETWEEN 202310 AND 202409
),

combined AS (
    SELECT
        t1.year_month,
        t1.region_name,
        t1.material_id,
        t1.sales,
        COALESCE(t1.sales * t2.gp_wth_chargeback / 100, 0) AS gp
    FROM sales_data AS t1
    LEFT JOIN gp_data AS t2
        ON t1.year_month = t2.year_month
        AND t1.region_name = t2.region_name
        AND t1.material_id = t2.material_id
),

monthly_sales AS (
    SELECT
        year_month,
        ROUND(SUM(sales)) AS total_sales,
        ROUND(SUM(gp)) AS total_gp
    FROM combined
    GROUP BY 1
)

SELECT
    year_month,
    total_sales,
    COALESCE(ROUND((total_sales - LAG(total_sales, 1, 0) OVER(ORDER BY year_month)) / LAG(total_sales, 1, 0) OVER(ORDER BY year_month) * 100, 2), 0) AS growth
FROM monthly_sales
ORDER BY year_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #ABU DHABI (Updated)

-- COMMAND ----------

SELECT
    material_id,
    material_name,
    type,
    ROUND(sales_contri_Q4 * 100, 2) AS sales_contri_Q4,
    ROUND(gp_contri_Q4 * 100, 2) AS gp_contri_Q4
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'CANNED CORNED BEEF'
    AND region_name = 'ABU DHABI'
ORDER BY gp_contri_Q4

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 707736
    AND region_name = 'ABU DHABI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AL AIN (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 534136
    AND region_name = 'AL AIN'
