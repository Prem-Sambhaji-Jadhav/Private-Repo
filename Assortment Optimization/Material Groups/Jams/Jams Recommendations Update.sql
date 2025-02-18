-- Databricks notebook source
-- SELECT
--     ROUND(SUM(CASE WHEN type = "Cherry Cheesecake" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
--     ROUND(SUM(CASE WHEN type = "Cherry Cheesecake" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
-- FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
-- WHERE
--     material_group_name = 'JAMS'
--     AND region_name = 'ABU DHABI'
--     AND LOWER(type) LIKE "%%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #ABU DHABI (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (431386, 135382)
    AND region_name = 'ABU DHABI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DUBAI (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (2170412, 135382)
    AND region_name = 'DUBAI'

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


