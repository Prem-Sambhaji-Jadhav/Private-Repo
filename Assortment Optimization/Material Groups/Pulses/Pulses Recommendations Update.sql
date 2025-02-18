-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Urad Dal

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Urad White Dal Split" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS urad_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Urad White Dal Split" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS urad_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'DUBAI'
    AND type LIKE "%Urad%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Sorghum

-- COMMAND ----------

SELECT *
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'DUBAI'
    AND type LIKE "%Sorghum%"

-- COMMAND ----------

SELECT *
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'DUBAI'
    AND type LIKE "%%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Sesame Seeds (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Roasted Sesame Seeds" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS roasted_sesame_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Roasted Sesame Seeds" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS roasted_sesame_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'DUBAI'
    AND type LIKE "%Sesame%"

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 705606
    AND region_name = 'DUBAI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Chana Dal

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Roasted Chana Dal" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS roasted_chana_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Roasted Chana Dal" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS roasted_chana_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'ABU DHABI'
    AND type LIKE "%Chana Dal%"

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Green Chana Dal" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS green_chana_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Green Chana Dal" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS green_chana_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'ABU DHABI'
    AND type LIKE "%Chana Dal%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Green Peas

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Green Split Peas" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS green_split_peas_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Green Split Peas" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS green_split_peas_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'DUBAI'
    AND type LIKE "%Green%Pea%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Freekeh

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Green Freekeh Coarse" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS green_freekeh_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Green Freekeh Coarse" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS green_freekeh_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'AL AIN'
    AND type LIKE "%Freekeh%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Flax (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Flax Seeds Golden" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS flax_golden_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Flax Seeds Golden" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS flax_golden_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'DUBAI'
    AND type LIKE "%Flax%"

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Flax Seeds Powder" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS flax_powder_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Flax Seeds Powder" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS flax_powder_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'SHARJAH'
    AND type LIKE "%Flax%"

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1472338
    AND region_name = 'DUBAI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Organic Brands

-- COMMAND ----------

SELECT
    region_name,
    COUNT(DISTINCT material_id) AS total_skus,
    COUNT(DISTINCT CASE WHEN Delist_Priority > 0 THEN material_id END) AS delisted_skus,
    ROUND(delisted_skus / total_skus * 100, 2) AS delist_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND brand IN ('24 MANTRA', 'ARROWHEAD MILLS', 'BAYARA', 'BOBS RED MILL', 'EARTHLY CHOICE', 'GREEN FARM', 'HARITHAM', 'INFINITY FOODS', 'JUST ORGANIK', 'KITCHEN & LOVE', 'NATURELAND', 'NUMALS', 'ORGANIC INDIA', 'ORGANIC TATTVA', 'REEMA', 'RK') -- organic brands
GROUP BY region_name
ORDER BY region_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Organic SKUs (Updated)

-- COMMAND ----------

-- Checking the contribution of organic products within organic brands
SELECT
    ROUND(SUM(CASE WHEN type LIKE "%Sabja Seed%" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type LIKE "%Sabja Seed%" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'PULSES'
    AND region_name = 'ABU DHABI'
    AND brand IN ('24 MANTRA', 'ARROWHEAD MILLS', 'BAYARA', 'BOBS RED MILL', 'EARTHLY CHOICE', 'GREEN FARM', 'HARITHAM', 'INFINITY FOODS', 'JUST ORGANIK', 'KITCHEN & LOVE', 'NATURELAND', 'NUMALS', 'ORGANIC INDIA', 'ORGANIC TATTVA', 'REEMA', 'RK') -- organic brands

-- COMMAND ----------

-- Checking if any type being delisted is associated with one of the top 10 SKUs
WITH top_materials AS (
    SELECT
        material_id,
        material_name,
        type
    FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
    WHERE
        material_group_name = 'PULSES'
        AND region_name = 'DUBAI'
    ORDER BY gp_contri_Q4 DESC
    LIMIT 10
)

SELECT DISTINCT type
FROM top_materials
ORDER BY 1

-- COMMAND ----------

-- Updating recommendation for Green Chana Dal & Roasted Chana Dal
UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (1333100, 26585)
    AND region_name = 'ABU DHABI'

-- COMMAND ----------

-- Updating recommendation for Urad White Dal Split
UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (1333126)
    AND region_name = 'DUBAI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AL AIN (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1697421
    AND region_name = 'AL AIN'

-- COMMAND ----------



-- COMMAND ----------


