-- Databricks notebook source
SELECT
    t2.category_id,
    t2.category_name,
    COUNT(DISTINCT t1.ean) AS ean_count,
    ROUND(COUNT(DISTINCT CASE WHEN t4.material_description IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_description,
    ROUND(COUNT(DISTINCT CASE WHEN t4.material_description_long IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_description_long,
    ROUND(COUNT(DISTINCT CASE WHEN t4.ean IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS ean,
    ROUND(COUNT(DISTINCT CASE WHEN t4.main_ean IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS main_ean,
    ROUND(COUNT(DISTINCT CASE WHEN t4.ean_category IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS ean_category,
    ROUND(COUNT(DISTINCT CASE WHEN t4.alt_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS alt_unit,
    ROUND(COUNT(DISTINCT CASE WHEN t4.conversion_numerator IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS conversion_numerator,
    ROUND(COUNT(DISTINCT CASE WHEN t4.base_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS base_unit,
    ROUND(COUNT(DISTINCT CASE WHEN t4.pacakaging_material IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS pacakaging_material,
    ROUND(COUNT(DISTINCT CASE WHEN t4.pacakaging_material_desc IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS pacakaging_material_desc,
    ROUND(COUNT(DISTINCT CASE WHEN t4.material_group IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_group,
    ROUND(COUNT(DISTINCT CASE WHEN t4.old_material_number IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS old_material_number,
    ROUND(COUNT(DISTINCT CASE WHEN t4.product_heirarchy IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS product_heirarchy,
    ROUND(COUNT(DISTINCT CASE WHEN t4.product_heirarchy_desc IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS product_heirarchy_desc,
    ROUND(COUNT(DISTINCT CASE WHEN t4.manufacturer_brand IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS manufacturer_brand,
    ROUND(COUNT(DISTINCT CASE WHEN t4.brand IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS brand,
    ROUND(COUNT(DISTINCT CASE WHEN t4.country_of_origin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS country_of_origin,
    ROUND(COUNT(DISTINCT CASE WHEN t4.status IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS status,
    ROUND(COUNT(DISTINCT CASE WHEN t4.created_on IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS created_on,
    ROUND(COUNT(DISTINCT CASE WHEN t4.last_change IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS last_change,
    ROUND(COUNT(DISTINCT CASE WHEN t4.organic IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS organic,
    ROUND(COUNT(DISTINCT CASE WHEN t4.gluten_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS gluten_free,
    ROUND(COUNT(DISTINCT CASE WHEN t4.vegan IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS vegan,
    ROUND(COUNT(DISTINCT CASE WHEN t4.sugar_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS sugar_free,
    ROUND(COUNT(DISTINCT CASE WHEN t4.fat_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS fat_free,
    ROUND(COUNT(DISTINCT CASE WHEN t4.low_sugar IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS low_sugar,
    ROUND(COUNT(DISTINCT CASE WHEN t4.lactose_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS lactose_free,
    ROUND(COUNT(DISTINCT CASE WHEN t4.vegetarian IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS vegetarian,
    ROUND(COUNT(DISTINCT CASE WHEN t4.source_of_protien IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS source_of_protien,
    ROUND(COUNT(DISTINCT CASE WHEN t4.heart_healthy_omega_3 IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS heart_healthy_omega_3,
    ROUND(COUNT(DISTINCT CASE WHEN t4.diary_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS diary_free,
    ROUND(COUNT(DISTINCT CASE WHEN t4.egg_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS egg_free,
    ROUND(COUNT(DISTINCT CASE WHEN t4.no_cholesterol IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS no_cholesterol,
    ROUND(COUNT(DISTINCT CASE WHEN t4.no_added_sugar IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS no_added_sugar,
    ROUND(COUNT(DISTINCT CASE WHEN t4.made_from_100_real_fruit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS made_from_100_real_fruit,
    ROUND(COUNT(DISTINCT CASE WHEN t4.source_of_vitamin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS source_of_vitamin,
    ROUND(COUNT(DISTINCT CASE WHEN t4.peanut_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS peanut_free,
    ROUND(COUNT(DISTINCT CASE WHEN t4.virgin_extra_virgin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS virgin_extra_virgin,
    ROUND(COUNT(DISTINCT CASE WHEN t4.content IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS content,
    ROUND(COUNT(DISTINCT CASE WHEN t4.content_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS content_unit,
    ROUND(COUNT(DISTINCT CASE WHEN t4.length IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS length,
    ROUND(COUNT(DISTINCT CASE WHEN t4.width IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS width,
    ROUND(COUNT(DISTINCT CASE WHEN t4.height IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS height,
    ROUND(COUNT(DISTINCT CASE WHEN t4.unit_of_dimension IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS unit_of_dimension,
    ROUND(COUNT(DISTINCT CASE WHEN t4.volume IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS volume,
    ROUND(COUNT(DISTINCT CASE WHEN t4.volume_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS volume_unit,
    ROUND(COUNT(DISTINCT CASE WHEN t4.gross_weight IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS gross_weight,
    ROUND(COUNT(DISTINCT CASE WHEN t4.net_weight IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS net_weight,
    ROUND(COUNT(DISTINCT CASE WHEN t4.weight_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS weight_unit,
    ROUND(COUNT(DISTINCT CASE WHEN t4.total_shelf_life IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS total_shelf_life,
    ROUND(COUNT(DISTINCT CASE WHEN t4.remaining_shelf_life IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS remaining_shelf_life
FROM gold.transaction.uae_pos_transactions AS t1
JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
LEFT JOIN gold.material.material_attributes AS t4 ON t1.ean = t4.ean
WHERE
    t1.business_day BETWEEN "2023-08-01" AND "2024-07-31"
    AND t2.department_class_id IN (1, 2)
    AND t3.tayeb_flag = 0
    AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
    AND t1.quantity > 0
    AND t1.amount > 0
GROUP BY 1, 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Data Completeness Table

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_mat_attr_check_comp_sales AS (
    SELECT
        category_id,
        category_name,
        "Data Completeness" AS Data_Quality_Attribute,
        "% of total sales" AS Measure,
        COUNT(DISTINCT t1.ean) AS ean_count,
        ROUND(SUM(CASE WHEN material_description IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS material_description,
        ROUND(SUM(CASE WHEN material_description_long IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS material_description_long,
        ROUND(SUM(CASE WHEN t4.ean IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS ean,
        ROUND(SUM(CASE WHEN main_ean IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS main_ean,
        ROUND(SUM(CASE WHEN ean_category IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS ean_category,
        ROUND(SUM(CASE WHEN alt_unit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS alt_unit,
        ROUND(SUM(CASE WHEN conversion_numerator IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS conversion_numerator,
        ROUND(SUM(CASE WHEN base_unit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS base_unit,
        ROUND(SUM(CASE WHEN pacakaging_material IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS pacakaging_material,
        ROUND(SUM(CASE WHEN pacakaging_material_desc IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS pacakaging_material_desc,
        ROUND(SUM(CASE WHEN material_group IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS material_group,
        ROUND(SUM(CASE WHEN old_material_number IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS old_material_number,
        ROUND(SUM(CASE WHEN product_heirarchy IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS product_heirarchy,
        ROUND(SUM(CASE WHEN product_heirarchy_desc IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS product_heirarchy_desc,
        ROUND(SUM(CASE WHEN manufacturer_brand IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS manufacturer_brand,
        ROUND(SUM(CASE WHEN t4.brand IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS brand,
        ROUND(SUM(CASE WHEN country_of_origin IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS country_of_origin,
        ROUND(SUM(CASE WHEN status IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS status,
        ROUND(SUM(CASE WHEN created_on IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS created_on,
        ROUND(SUM(CASE WHEN last_change IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS last_change,
        ROUND(SUM(CASE WHEN organic IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS organic,
        ROUND(SUM(CASE WHEN gluten_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS gluten_free,
        ROUND(SUM(CASE WHEN vegan IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS vegan,
        ROUND(SUM(CASE WHEN sugar_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS sugar_free,
        ROUND(SUM(CASE WHEN fat_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS fat_free,
        ROUND(SUM(CASE WHEN low_sugar IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS low_sugar,
        ROUND(SUM(CASE WHEN lactose_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS lactose_free,
        ROUND(SUM(CASE WHEN vegetarian IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS vegetarian,
        ROUND(SUM(CASE WHEN source_of_protien IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS source_of_protien,
        ROUND(SUM(CASE WHEN heart_healthy_omega_3 IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS heart_healthy_omega_3,
        ROUND(SUM(CASE WHEN diary_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS diary_free,
        ROUND(SUM(CASE WHEN egg_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS egg_free,
        ROUND(SUM(CASE WHEN no_cholesterol IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS no_cholesterol,
        ROUND(SUM(CASE WHEN no_added_sugar IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS no_added_sugar,
        ROUND(SUM(CASE WHEN made_from_100_real_fruit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS made_from_100_real_fruit,
        ROUND(SUM(CASE WHEN source_of_vitamin IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS source_of_vitamin,
        ROUND(SUM(CASE WHEN peanut_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS peanut_free,
        ROUND(SUM(CASE WHEN virgin_extra_virgin IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS virgin_extra_virgin,
        ROUND(SUM(CASE WHEN content IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS content,
        ROUND(SUM(CASE WHEN content_unit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS content_unit,
        ROUND(SUM(CASE WHEN length IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS length,
        ROUND(SUM(CASE WHEN width IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS width,
        ROUND(SUM(CASE WHEN height IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS height,
        ROUND(SUM(CASE WHEN unit_of_dimension IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS unit_of_dimension,
        ROUND(SUM(CASE WHEN volume IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS volume,
        ROUND(SUM(CASE WHEN volume_unit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS volume_unit,
        ROUND(SUM(CASE WHEN gross_weight IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS gross_weight,
        ROUND(SUM(CASE WHEN net_weight IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS net_weight,
        ROUND(SUM(CASE WHEN weight_unit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS weight_unit,
        ROUND(SUM(CASE WHEN total_shelf_life IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS total_shelf_life,
        ROUND(SUM(CASE WHEN remaining_shelf_life IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS remaining_shelf_life
    FROM gold.transaction.uae_pos_transactions AS t1
    LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    LEFT JOIN dev.sandbox.ag_mat_attr_content_fix AS t4 ON t1.ean = t4.ean
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_class_id IN (1, 2)
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        AND tayeb_flag = 0
    GROUP BY 1, 2, 3, 4
)

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_mat_attr_check_comp_ean AS (
    SELECT
        category_id,
        category_name,
        "Data Completeness" AS Data_Quality_Attribute,
        "% of total EAN" AS Measure,
        COUNT(DISTINCT t1.ean) AS ean_count,
        ROUND(COUNT(DISTINCT CASE WHEN material_description IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_description,
        ROUND(COUNT(DISTINCT CASE WHEN material_description_long IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_description_long,
        ROUND(COUNT(DISTINCT CASE WHEN t4.ean IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS ean,
        ROUND(COUNT(DISTINCT CASE WHEN main_ean IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS main_ean,
        ROUND(COUNT(DISTINCT CASE WHEN ean_category IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS ean_category,
        ROUND(COUNT(DISTINCT CASE WHEN alt_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS alt_unit,
        ROUND(COUNT(DISTINCT CASE WHEN conversion_numerator IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS conversion_numerator,
        ROUND(COUNT(DISTINCT CASE WHEN base_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS base_unit,
        ROUND(COUNT(DISTINCT CASE WHEN pacakaging_material IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS pacakaging_material,
        ROUND(COUNT(DISTINCT CASE WHEN pacakaging_material_desc IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS pacakaging_material_desc,
        ROUND(COUNT(DISTINCT CASE WHEN material_group IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_group,
        ROUND(COUNT(DISTINCT CASE WHEN old_material_number IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS old_material_number,
        ROUND(COUNT(DISTINCT CASE WHEN product_heirarchy IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS product_heirarchy,
        ROUND(COUNT(DISTINCT CASE WHEN product_heirarchy_desc IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS product_heirarchy_desc,
        ROUND(COUNT(DISTINCT CASE WHEN manufacturer_brand IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS manufacturer_brand,
        ROUND(COUNT(DISTINCT CASE WHEN t4.brand IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS brand,
        ROUND(COUNT(DISTINCT CASE WHEN country_of_origin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS country_of_origin,
        ROUND(COUNT(DISTINCT CASE WHEN status IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS status,
        ROUND(COUNT(DISTINCT CASE WHEN created_on IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS created_on,
        ROUND(COUNT(DISTINCT CASE WHEN last_change IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS last_change,
        ROUND(COUNT(DISTINCT CASE WHEN organic IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS organic,
        ROUND(COUNT(DISTINCT CASE WHEN gluten_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS gluten_free,
        ROUND(COUNT(DISTINCT CASE WHEN vegan IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS vegan,
        ROUND(COUNT(DISTINCT CASE WHEN sugar_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS sugar_free,
        ROUND(COUNT(DISTINCT CASE WHEN fat_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS fat_free,
        ROUND(COUNT(DISTINCT CASE WHEN low_sugar IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS low_sugar,
        ROUND(COUNT(DISTINCT CASE WHEN lactose_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS lactose_free,
        ROUND(COUNT(DISTINCT CASE WHEN vegetarian IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS vegetarian,
        ROUND(COUNT(DISTINCT CASE WHEN source_of_protien IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS source_of_protien,
        ROUND(COUNT(DISTINCT CASE WHEN heart_healthy_omega_3 IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS heart_healthy_omega_3,
        ROUND(COUNT(DISTINCT CASE WHEN diary_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS diary_free,
        ROUND(COUNT(DISTINCT CASE WHEN egg_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS egg_free,
        ROUND(COUNT(DISTINCT CASE WHEN no_cholesterol IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS no_cholesterol,
        ROUND(COUNT(DISTINCT CASE WHEN no_added_sugar IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS no_added_sugar,
        ROUND(COUNT(DISTINCT CASE WHEN made_from_100_real_fruit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS made_from_100_real_fruit,
        ROUND(COUNT(DISTINCT CASE WHEN source_of_vitamin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS source_of_vitamin,
        ROUND(COUNT(DISTINCT CASE WHEN peanut_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS peanut_free,
        ROUND(COUNT(DISTINCT CASE WHEN virgin_extra_virgin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS virgin_extra_virgin,
        ROUND(COUNT(DISTINCT CASE WHEN content IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS content,
        ROUND(COUNT(DISTINCT CASE WHEN content_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS content_unit,
        ROUND(COUNT(DISTINCT CASE WHEN length IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS length,
        ROUND(COUNT(DISTINCT CASE WHEN width IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS width,
        ROUND(COUNT(DISTINCT CASE WHEN height IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS height,
        ROUND(COUNT(DISTINCT CASE WHEN unit_of_dimension IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS unit_of_dimension,
        ROUND(COUNT(DISTINCT CASE WHEN volume IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS volume,
        ROUND(COUNT(DISTINCT CASE WHEN volume_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS volume_unit,
        ROUND(COUNT(DISTINCT CASE WHEN gross_weight IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS gross_weight,
        ROUND(COUNT(DISTINCT CASE WHEN net_weight IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS net_weight,
        ROUND(COUNT(DISTINCT CASE WHEN weight_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS weight_unit,
        ROUND(COUNT(DISTINCT CASE WHEN total_shelf_life IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS total_shelf_life,
        ROUND(COUNT(DISTINCT CASE WHEN remaining_shelf_life IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS remaining_shelf_life
    FROM gold.transaction.uae_pos_transactions AS t1
    LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    LEFT JOIN dev.sandbox.ag_mat_attr_content_fix AS t4 ON t1.ean = t4.ean
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_class_id IN (1, 2)
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        AND tayeb_flag = 0
    GROUP BY 1, 2, 3, 4
)

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_mat_attr_check_comp AS (
    WITH joined AS (
        SELECT * FROM dev.sandbox.pj_mat_attr_check_comp_sales
        UNION
        SELECT * FROM dev.sandbox.pj_mat_attr_check_comp_ean
    )

    SELECT *
    FROM joined
    ORDER BY 1, 4
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Data Consistency Table

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_mat_attr_check_cons_sales AS (
    SELECT
        category_id,
        category_name,
        "Data Consistency" AS Data_Quality_Attribute,
        "% of total sales" AS Measure,
        COUNT(DISTINCT t1.ean) AS ean_count,
        ROUND(SUM(CASE WHEN material_description IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS material_description,
        ROUND(SUM(CASE WHEN material_description_long IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS material_description_long,
        ROUND(SUM(CASE WHEN t4.ean IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS ean,
        ROUND(SUM(CASE WHEN main_ean IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS main_ean,
        ROUND(SUM(CASE WHEN ean_category IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS ean_category,
        ROUND(SUM(CASE WHEN alt_unit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS alt_unit,
        ROUND(SUM(CASE WHEN conversion_numerator IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS conversion_numerator,
        ROUND(SUM(CASE WHEN base_unit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS base_unit,
        ROUND(SUM(CASE WHEN pacakaging_material IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS pacakaging_material,
        ROUND(SUM(CASE WHEN pacakaging_material_desc IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS pacakaging_material_desc,
        ROUND(SUM(CASE WHEN material_group IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS material_group,
        ROUND(SUM(CASE WHEN old_material_number IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS old_material_number,
        ROUND(SUM(CASE WHEN product_heirarchy IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS product_heirarchy,
        ROUND(SUM(CASE WHEN product_heirarchy_desc IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS product_heirarchy_desc,
        ROUND(SUM(CASE WHEN manufacturer_brand IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS manufacturer_brand,
        ROUND(SUM(CASE WHEN t4.brand IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS brand,
        ROUND(SUM(CASE WHEN country_of_origin IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS country_of_origin,
        ROUND(SUM(CASE WHEN status IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS status,
        ROUND(SUM(CASE WHEN created_on IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS created_on,
        ROUND(SUM(CASE WHEN last_change IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS last_change,
        ROUND(SUM(CASE WHEN organic IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS organic,
        ROUND(SUM(CASE WHEN gluten_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS gluten_free,
        ROUND(SUM(CASE WHEN vegan IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS vegan,
        ROUND(SUM(CASE WHEN sugar_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS sugar_free,
        ROUND(SUM(CASE WHEN fat_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS fat_free,
        ROUND(SUM(CASE WHEN low_sugar IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS low_sugar,
        ROUND(SUM(CASE WHEN lactose_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS lactose_free,
        ROUND(SUM(CASE WHEN vegetarian IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS vegetarian,
        ROUND(SUM(CASE WHEN source_of_protien IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS source_of_protien,
        ROUND(SUM(CASE WHEN heart_healthy_omega_3 IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS heart_healthy_omega_3,
        ROUND(SUM(CASE WHEN diary_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS diary_free,
        ROUND(SUM(CASE WHEN egg_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS egg_free,
        ROUND(SUM(CASE WHEN no_cholesterol IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS no_cholesterol,
        ROUND(SUM(CASE WHEN no_added_sugar IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS no_added_sugar,
        ROUND(SUM(CASE WHEN made_from_100_real_fruit IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS made_from_100_real_fruit,
        ROUND(SUM(CASE WHEN source_of_vitamin IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS source_of_vitamin,
        ROUND(SUM(CASE WHEN peanut_free IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS peanut_free,
        ROUND(SUM(CASE WHEN virgin_extra_virgin IS NOT NULL THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS virgin_extra_virgin,
        ROUND(SUM(CASE WHEN content > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS content,
        ROUND(SUM(CASE WHEN content_unit IS NOT NULL AND content_unit NOT IN ('EA', 'FOZ', 'PAC', 'CH1', 'CAR', 'BAG') THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS content_unit,
        ROUND(SUM(CASE WHEN length > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS length,
        ROUND(SUM(CASE WHEN width > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS width,
        ROUND(SUM(CASE WHEN height > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS height,
        ROUND(SUM(CASE WHEN unit_of_dimension IN ("CM", "M", "MM") THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS unit_of_dimension,
        ROUND(SUM(CASE WHEN volume = (length * width * height) THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS volume,
        ROUND(SUM(CASE WHEN volume_unit IN ("CCM", "CD3", "CL", "GAL", "L", "M3", "ML", "HL") THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS volume_unit,
        ROUND(SUM(CASE WHEN gross_weight > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS gross_weight,
        ROUND(SUM(CASE WHEN net_weight > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS net_weight,
        ROUND(SUM(CASE WHEN weight_unit IN ("G", "KG", "LB", "MG", "OZ") THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS weight_unit,
        ROUND(SUM(CASE WHEN total_shelf_life > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS total_shelf_life,
        ROUND(SUM(CASE WHEN remaining_shelf_life > 0 THEN amount ELSE 0 END)/ROUND(SUM(amount), 1), 4) AS remaining_shelf_life
    FROM gold.transaction.uae_pos_transactions AS t1
    LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    LEFT JOIN dev.sandbox.ag_mat_attr_content_fix AS t4 ON t1.ean = t4.ean
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_class_id IN (1, 2)
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        AND tayeb_flag = 0
    GROUP BY 1, 2, 3, 4
)

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_mat_attr_check_cons_ean AS (
    SELECT
        category_id,
        category_name,
        "Data Consistency" AS Data_Quality_Attribute,
        "% of total EAN" AS Measure,
        COUNT(DISTINCT t1.ean) AS ean_count,
        ROUND(COUNT(DISTINCT CASE WHEN material_description IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_description,
        ROUND(COUNT(DISTINCT CASE WHEN material_description_long IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_description_long,
        ROUND(COUNT(DISTINCT CASE WHEN t4.ean IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS ean,
        ROUND(COUNT(DISTINCT CASE WHEN main_ean IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS main_ean,
        ROUND(COUNT(DISTINCT CASE WHEN ean_category IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS ean_category,
        ROUND(COUNT(DISTINCT CASE WHEN alt_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS alt_unit,
        ROUND(COUNT(DISTINCT CASE WHEN conversion_numerator IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS conversion_numerator,
        ROUND(COUNT(DISTINCT CASE WHEN base_unit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS base_unit,
        ROUND(COUNT(DISTINCT CASE WHEN pacakaging_material IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS pacakaging_material,
        ROUND(COUNT(DISTINCT CASE WHEN pacakaging_material_desc IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS pacakaging_material_desc,
        ROUND(COUNT(DISTINCT CASE WHEN material_group IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS material_group,
        ROUND(COUNT(DISTINCT CASE WHEN old_material_number IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS old_material_number,
        ROUND(COUNT(DISTINCT CASE WHEN product_heirarchy IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS product_heirarchy,
        ROUND(COUNT(DISTINCT CASE WHEN product_heirarchy_desc IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS product_heirarchy_desc,
        ROUND(COUNT(DISTINCT CASE WHEN manufacturer_brand IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS manufacturer_brand,
        ROUND(COUNT(DISTINCT CASE WHEN t4.brand IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS brand,
        ROUND(COUNT(DISTINCT CASE WHEN country_of_origin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS country_of_origin,
        ROUND(COUNT(DISTINCT CASE WHEN status IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS status,
        ROUND(COUNT(DISTINCT CASE WHEN created_on IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS created_on,
        ROUND(COUNT(DISTINCT CASE WHEN last_change IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS last_change,
        ROUND(COUNT(DISTINCT CASE WHEN organic IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS organic,
        ROUND(COUNT(DISTINCT CASE WHEN gluten_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS gluten_free,
        ROUND(COUNT(DISTINCT CASE WHEN vegan IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS vegan,
        ROUND(COUNT(DISTINCT CASE WHEN sugar_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS sugar_free,
        ROUND(COUNT(DISTINCT CASE WHEN fat_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS fat_free,
        ROUND(COUNT(DISTINCT CASE WHEN low_sugar IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS low_sugar,
        ROUND(COUNT(DISTINCT CASE WHEN lactose_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS lactose_free,
        ROUND(COUNT(DISTINCT CASE WHEN vegetarian IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS vegetarian,
        ROUND(COUNT(DISTINCT CASE WHEN source_of_protien IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS source_of_protien,
        ROUND(COUNT(DISTINCT CASE WHEN heart_healthy_omega_3 IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS heart_healthy_omega_3,
        ROUND(COUNT(DISTINCT CASE WHEN diary_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS diary_free,
        ROUND(COUNT(DISTINCT CASE WHEN egg_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS egg_free,
        ROUND(COUNT(DISTINCT CASE WHEN no_cholesterol IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS no_cholesterol,
        ROUND(COUNT(DISTINCT CASE WHEN no_added_sugar IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS no_added_sugar,
        ROUND(COUNT(DISTINCT CASE WHEN made_from_100_real_fruit IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS made_from_100_real_fruit,
        ROUND(COUNT(DISTINCT CASE WHEN source_of_vitamin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS source_of_vitamin,
        ROUND(COUNT(DISTINCT CASE WHEN peanut_free IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS peanut_free,
        ROUND(COUNT(DISTINCT CASE WHEN virgin_extra_virgin IS NOT NULL THEN t4.ean ELSE 0 END)/ean_count, 4) AS virgin_extra_virgin,
        ROUND(COUNT(DISTINCT CASE WHEN content > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS content,
        ROUND(COUNT(DISTINCT CASE WHEN content_unit IS NOT NULL AND content_unit NOT IN ('EA', 'FOZ', 'PAC', 'CH1', 'CAR', 'BAG') THEN t4.ean ELSE 0 END)/ean_count, 4) AS content_unit,
        ROUND(COUNT(DISTINCT CASE WHEN length > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS length,
        ROUND(COUNT(DISTINCT CASE WHEN width > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS width,
        ROUND(COUNT(DISTINCT CASE WHEN height > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS height,
        ROUND(COUNT(DISTINCT CASE WHEN unit_of_dimension IN ("CM", "M", "MM") THEN t4.ean ELSE 0 END)/ean_count, 4) AS unit_of_dimension,
        ROUND(COUNT(DISTINCT CASE WHEN volume = (length * width * height) THEN t4.ean ELSE 0 END)/ean_count, 4) AS volume,
        ROUND(COUNT(DISTINCT CASE WHEN volume_unit IN ("CCM", "CD3", "CL", "GAL", "L", "M3", "ML", "HL") THEN t4.ean ELSE 0 END)/ean_count, 4) AS volume_unit,
        ROUND(COUNT(DISTINCT CASE WHEN gross_weight > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS gross_weight,
        ROUND(COUNT(DISTINCT CASE WHEN net_weight > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS net_weight,
        ROUND(COUNT(DISTINCT CASE WHEN weight_unit IN ("G", "KG", "LB", "MG", "OZ") THEN t4.ean ELSE 0 END)/ean_count, 4) AS weight_unit,
        ROUND(COUNT(DISTINCT CASE WHEN total_shelf_life > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS total_shelf_life,
        ROUND(COUNT(DISTINCT CASE WHEN remaining_shelf_life > 0 THEN t4.ean ELSE 0 END)/ean_count, 4) AS remaining_shelf_life
    FROM gold.transaction.uae_pos_transactions AS t1
    LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    LEFT JOIN dev.sandbox.ag_mat_attr_content_fix AS t4 ON t1.ean = t4.ean
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_class_id IN (1, 2)
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        AND tayeb_flag = 0
    GROUP BY 1, 2, 3, 4
)

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_mat_attr_check_cons AS (
    WITH joined AS (
        SELECT * FROM dev.sandbox.pj_mat_attr_check_cons_sales
        UNION
        SELECT * FROM dev.sandbox.pj_mat_attr_check_cons_ean
    )

    SELECT *
    FROM joined
    ORDER BY 1, 4
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Final Table

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_mat_attr_check AS (
    WITH joined AS (
        SELECT * FROM dev.sandbox.pj_mat_attr_check_comp
        UNION
        SELECT * FROM dev.sandbox.pj_mat_attr_check_cons
    )

    SELECT *
    FROM joined
    WHERE category_id IS NOT NULL
    ORDER BY 1, 3, 4
)

-- COMMAND ----------

SELECT *
FROM dev.sandbox.pj_mat_attr_check
