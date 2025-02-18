-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Berry & White Chocolate Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Berry & White Chocolate Stick" THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = "Berry & White Chocolate Stick" THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = "Berry & White Chocolate Stick" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Berry & White Chocolate Stick" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE "%white chocolate%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Chocolate Brownie Bar (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Chocolate Brownie Bar" THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = "Chocolate Brownie Bar" THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = "Chocolate Brownie Bar" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Chocolate Brownie Bar" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI' -- DUBAI, SHARJAH
    AND LOWER(type) LIKE "%brownie%"

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 2119018
    AND region_name IN ('DUBAI', 'SHARJAH')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Cocoa Stick (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Cocoa Stick" THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = "Cocoa Stick" THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = "Cocoa Stick" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Cocoa Stick" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI' -- DUBAI, SHARJAH
    AND LOWER(type) LIKE "%cocoa%"

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1727617
    AND region_name IN ('DUBAI', 'SHARJAH')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Coconut Cone

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = "Coconut Cone" THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = "Coconut Cone" THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = "Coconut Cone" THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = "Coconut Cone" THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE "%coconut%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Coffee & Caramel Dough (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Coffee & Caramel Dough' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Coffee & Caramel Dough' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Coffee & Caramel Dough' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Coffee & Caramel Dough' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE '%coffee%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1611422
    AND region_name = 'DUBAI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Mango Stick (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Mango Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Mango Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Mango Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Mango Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE '%mango%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1965923
    AND region_name = 'DUBAI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Oreo Chocolate Sandwich (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Oreo Chocolate Sandwich' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Oreo Chocolate Sandwich' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Oreo Chocolate Sandwich' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Oreo Chocolate Sandwich' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI' -- DUBAI, SHARJAH
    AND LOWER(type) LIKE '%oreo%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1909828
    AND region_name IN ('DUBAI', 'SHARJAH')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Oreo Cone

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Oreo Cone' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Oreo Cone' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Oreo Cone' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Oreo Cone' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE '%oreo%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Oreo Sandwich (Update)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Oreo Sandwich' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Oreo Sandwich' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Oreo Sandwich' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Oreo Sandwich' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE '%oreo%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 2125648
    AND region_name = 'DUBAI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Strawberry Cheesecake Tub

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Strawberry Cheesecake Tub' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Strawberry Cheesecake Tub' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Strawberry Cheesecake Tub' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Strawberry Cheesecake Tub' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE '%cheesecake%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Vanilla & Caramel Bar

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Vanilla & Caramel Bar' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Caramel Bar' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Caramel Bar' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Caramel Bar' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE '%caramel%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Vanilla & Chocolate Caramel Sandwich

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Vanilla & Chocolate Caramel Sandwich' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Chocolate Caramel Sandwich' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Chocolate Caramel Sandwich' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Chocolate Caramel Sandwich' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'DUBAI'
    AND LOWER(type) LIKE '%caramel%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Vanilla Oreo Cup (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Vanilla Oreo Cup' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla Oreo Cup' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla Oreo Cup' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Vanilla Oreo Cup' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'SHARJAH' -- ABU DHABI, DUBAI, SHARJAH
    AND LOWER(type) LIKE '%oreo%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1401201
    AND region_name IN ('DUBAI', 'SHARJAH', 'ABU DHABI')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Chocolate & Caramel Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Chocolate & Caramel Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Chocolate & Caramel Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Chocolate & Caramel Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Chocolate & Caramel Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'SHARJAH'
    AND LOWER(type) LIKE '%caramel%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Chocolate & Peanut Butter Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Chocolate & Peanut Butter Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Chocolate & Peanut Butter Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Chocolate & Peanut Butter Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Chocolate & Peanut Butter Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'SHARJAH'
    AND LOWER(type) LIKE '%peanut butter%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Coconut & Milk Bar (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Coconut & Milk Bar' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Coconut & Milk Bar' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Coconut & Milk Bar' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Coconut & Milk Bar' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'SHARJAH'
    AND LOWER(type) LIKE '%milk%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1054690
    AND region_name = 'SHARJAH'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Vanilla & Cocoa Stick (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Vanilla & Cocoa Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Cocoa Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Cocoa Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Cocoa Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'SHARJAH'
    AND LOWER(type) LIKE '%cocoa%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1727613
    AND region_name = 'SHARJAH'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Vanilla & Peanut Butter Cone

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Vanilla & Peanut Butter Cone' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Peanut Butter Cone' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Peanut Butter Cone' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Vanilla & Peanut Butter Cone' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'SHARJAH'
    AND LOWER(type) LIKE '%peanut butter%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #White Chocolate & Cookies Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'White Chocolate & Cookies Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'White Chocolate & Cookies Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'White Chocolate & Cookies Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'White Chocolate & Cookies Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'SHARJAH'
    AND LOWER(type) LIKE '%white chocolate%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Caramel & Popcorn Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Caramel & Popcorn Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Caramel & Popcorn Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Caramel & Popcorn Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Caramel & Popcorn Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE '%caramel%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Chocolate Brownie Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Chocolate Brownie Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Chocolate Brownie Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Chocolate Brownie Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Chocolate Brownie Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE '%brownie%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Mango & Coconut Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Mango & Coconut Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Mango & Coconut Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Mango & Coconut Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Mango & Coconut Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE '%coconut%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Mango Kulfi

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Mango Kulfi' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Mango Kulfi' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Mango Kulfi' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Mango Kulfi' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE '%kulfi%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Mulberry & Blackberry Stick (Updated)

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Mulberry & Blackberry Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Mulberry & Blackberry Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Mulberry & Blackberry Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Mulberry & Blackberry Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE '%mulberry%'

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id = 1446263
    AND region_name = 'ABU DHABI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Salted Caramel & Macadamia Stick

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN type = 'Salted Caramel & Macadamia Stick' THEN sales_contri_Q4 END) * 100, 2) AS sales_contri,
    ROUND(SUM(CASE WHEN type = 'Salted Caramel & Macadamia Stick' THEN gp_contri_Q4 END) * 100, 2) AS gp_contri,
    ROUND(SUM(CASE WHEN type = 'Salted Caramel & Macadamia Stick' THEN sales_contri_Q4 END)/SUM(sales_contri_Q4) * 100, 2) AS type_delist_sales_perc,
    ROUND(SUM(CASE WHEN type = 'Salted Caramel & Macadamia Stick' THEN gp_contri_Q4 END)/SUM(gp_contri_Q4) * 100, 2) AS type_delist_gp_perc
FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis
WHERE
    material_group_name = 'ICE CREAM IMPULSE'
    AND region_name = 'ABU DHABI'
    AND LOWER(type) LIKE '%macadamia%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #ABU DHABI (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (2083957, 1227531, 1523929)
    AND region_name = 'ABU DHABI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DUBAI (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (2083957, 1789048, 2154106, 699963, 2031371)
    AND region_name = 'DUBAI'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #SHARJAH (Updated)

-- COMMAND ----------

UPDATE dev.sandbox.pj_ao_gp_12months
SET new_buckets = 'Observe'
WHERE
    material_id IN (2059008, 1727612, 1511378, 699961, 118173, 2045725)
    AND region_name = 'SHARJAH'
