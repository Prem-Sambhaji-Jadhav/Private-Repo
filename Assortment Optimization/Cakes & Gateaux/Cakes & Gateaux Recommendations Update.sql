-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Attribute-wise Check

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Cherry Cheesecake" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Cherry Cheesecake" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'CAKES & GATEAUX'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE "%cheesecake%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #ABU DHABI (Updated)

-- COMMAND ----------

SELECT type, sales_contri_Q4, gp_contri_Q4
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'CAKES & GATEAUX'
    AND region_name = 'ABU DHABI'
    AND type IN ('Cheery Cheesecake', 'Chocolate Donut', 'Cinnamon Donut', 'Glazed Chocolate Donut', 'Glazed Donut', 'Jam Cake', 'Powdered Sugar Donut', 'Raspberry Cheesecake', 'Vanilla Ice Cream Cake')

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (2243574, 2243572)
    AND region_name = 'ABU DHABI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AL AIN (Updated)

-- COMMAND ----------

SELECT type, sales_contri_Q4, gp_contri_Q4
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'CAKES & GATEAUX'
    AND region_name = 'AL AIN'
    AND type IN ('Cheery Cheesecake', 'Chocolate Donut', 'Cinnamon Donut', 'Glazed Chocolate Donut', 'Glazed Donut', 'Powdered Sugar Donut', 'Raspberry Cheesecake', 'Vanilla Ice Cream Cake', 'Chocolate & Hazelnut Cake')

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1791856
    AND region_name = 'AL AIN'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DUBAI

-- COMMAND ----------

SELECT type, sales_contri_Q4, gp_contri_Q4
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'CAKES & GATEAUX'
    AND region_name = 'DUBAI'
    AND type IN ('Chocolate Donut', 'Cinnamon Donut', 'Glazed Chocolate Donut', 'Glazed Donut', 'Powdered Sugar Donut', 'Raspberry Cheesecake', 'Vanilla Ice Cream Cake', 'Butter Pound Cake', 'Praline Ice Cream Cake')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #SHARJAH

-- COMMAND ----------

SELECT type, sales_contri_Q4, gp_contri_Q4
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'CAKES & GATEAUX'
    AND region_name = 'SHARJAH'
    AND type IN ('Chocolate Donut', 'Cinnamon Donut', 'Raspberry Cheesecake', 'Butter Pound Cake', 'Praline Ice Cream Cake')

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


