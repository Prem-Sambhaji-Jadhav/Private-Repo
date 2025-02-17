-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Beetroot (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Beetroot Cut" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Beetroot Cut" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PICKLES'
    AND region_name = 'AL AIN'
    AND LOWER(type) LIKE '%beetroot%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 488934
    AND region_name = 'AL AIN'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AL AIN (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 425269
    AND region_name = 'AL AIN'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #SHARJAH (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (726298, 4505, 1167126)
    AND region_name = 'SHARJAH'

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


