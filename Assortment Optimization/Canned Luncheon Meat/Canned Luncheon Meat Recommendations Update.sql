-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Attribute-wise Check

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Beef Hot & Spicy" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Beef Hot & Spicy" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'CANNED LUNCHEON MEAT'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE "%beef%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AL AIN (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1698
    AND region_name = 'AL AIN'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #SHARJAH (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 2227304
    AND region_name = 'SHARJAH'

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


