-- Databricks notebook source


-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #PO Master Sandbox Creation

-- COMMAND ----------

-- %sql
-- CREATE OR REPLACE TABLE dev.sandbox.pj_hhc_final_elasticities AS (
--     WITH all_mgs AS (
--         SELECT
--             material_group_name,
--             CASE WHEN LEFT(demand_group, 7) = "hhc_apc"
--                     THEN CONCAT("hhc_all_purpose_cleaner_", RIGHT(demand_group, 4))
--                 WHEN LEFT(demand_group, 7) = "hhc_wup"
--                     THEN CONCAT("hhc_washing_up_", RIGHT(demand_group, 4))
--                 WHEN LEFT(demand_group, 7) = "hhc_dis"
--                     THEN CONCAT("hhc_disinfectants_", RIGHT(demand_group, 4))
--                 ELSE "none" END AS demand_group,
--             CASE WHEN RIGHT(demand_group, 4) = "dg_1" THEN "DG-1"
--                 WHEN RIGHT(demand_group, 4) = "dg_2" THEN "DG-2"
--                 WHEN RIGHT(demand_group, 4) = "dg_3" THEN "DG-3"
--                 WHEN RIGHT(demand_group, 4) = "dg_4" THEN "DG-4"
--                 ELSE "none" END AS mg_demand_group,
--             region_name,
--             material_id,
--             CASE WHEN elasticity_criteria = "accepted set" THEN "accepted_set"
--                 WHEN elasticity_criteria = "imputed: case-1" THEN "imputed: case 1"
--                 WHEN elasticity_criteria = "imputed: case-2" THEN "imputed: case 2"
--                 WHEN elasticity_criteria = "imputed: case-3" THEN "imputed: case 3"
--                 ELSE "none" END AS elasticity_criteria,
--             ROUND(price_elasticity_top_3_avg, 2) AS price_elasticity,
--             ROUND(promo_elasticity_top_3_avg, 2) AS promo_elasticity
--         FROM dev.sandbox.pj_po_final_elasticities_promo_hhc_master

--         UNION

--         SELECT * FROM analytics.pricing.hhc_final_elasticities
--     )

--     SELECT *
--     FROM all_mgs
--     ORDER BY material_group_name, demand_group, region_name, elasticity_criteria, material_id
-- )

-- COMMAND ----------

-- CREATE OR REPLACE TABLE dev.sandbox.pj_hhc_imputation_metrics AS (
--     WITH all_mgs AS (
--         SELECT
--             *,
--             CASE WHEN LEFT(demand_group, 7) = "hhc_apc" THEN "ALL PURPOSE CLEANER"
--                 WHEN LEFT(demand_group, 7) = "hhc_wup" THEN "WASHING UP"
--                 WHEN LEFT(demand_group, 7) = "hhc_dis" THEN "DISINFECTANTS"
--                 ELSE "none" END AS material_group_name,
--             CASE WHEN RIGHT(demand_group, 4) = "dg_1" THEN "DG-1"
--                 WHEN RIGHT(demand_group, 4) = "dg_2" THEN "DG-2"
--                 WHEN RIGHT(demand_group, 4) = "dg_3" THEN "DG-3"
--                 WHEN RIGHT(demand_group, 4) = "dg_4" THEN "DG-4"
--                 ELSE "none" END AS mg_demand_group
--         FROM dev.sandbox.pj_po_imputed_metrics_promo_master_hhc

--         UNION

--         SELECT * FROM analytics.pricing.hhc_imputation_metrics
--     )

--     SELECT *
--     FROM all_mgs
--     ORDER BY material_group_name, demand_group, region_name, material_id, reference_material_id, metric
-- )

-- COMMAND ----------

-- %py
-- df = spark.sql("SELECT * FROM dev.sandbox.pj_hhc_imputation_metrics").toPandas()
-- df.to_csv("/Workspace/Users/prem@loyalytics.in/hhc_imputation_metrics.csv", index = False)

-- COMMAND ----------

-- %py
-- query = """
-- SELECT
--     DISTINCT t1.material_id,
--     t2.material_name,
--     t2.brand,
--     t2.material_group_name
-- FROM dev.sandbox.pj_hhc_final_elasticities AS t1
-- JOIN gold.material.material_master AS t2 ON t1.material_id = t2.material_id
-- """
-- df = spark.sql(query).toPandas()
-- df.to_csv("/Workspace/Users/prem@loyalytics.in/hhc_material_lookup.csv", index = False)

-- COMMAND ----------

-- %py
-- query = """
-- SELECT
--     t1.material_group_name,
--     t1.demand_group,
--     t1.region_name,
--     t1.material_id,
--     t1.week_number,
--     t1.quantity_actual,
--     t1.quantity_pred,
--     t1.model_set,
--     t3.avg_unit_price,
--     GREATEST(LEAST((1 - t3.avg_unit_price / (t3.avg_unit_price + (t3.discount_amount / t3.quantity))), 1), 0) AS discount_perc_adj
-- FROM analytics.pricing.hhc_all_model_test_preds AS t1
-- JOIN analytics.pricing.hhc_all_model_results_best_model AS t2
--     ON t1.material_id = t2.material_id
--     AND t1.region_name = t2.region_name
--     AND t1.model = t2.best_model
-- JOIN analytics.pricing.hhc_mds_master AS t3
--     ON t1.material_id = t3.material_id
--     AND t1.region_name = t3.region_name
--     AND t1.week_number = t3.week_number
-- WHERE
--     t1.material_group_name IN ("BLEACH", "TOILET CLEANERS", "INSECTICIDES")
--     AND t2.model = t2.best_model
-- ORDER BY 1, 2, 3, 4, 5
-- """
-- df = spark.sql(query).toPandas()
-- df.to_csv("/Workspace/Users/prem@loyalytics.in/hhc_weekly_sales.csv", index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from sklearn.metrics.pairwise import cosine_similarity
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC file_name = 'hhc_cluster_sku_info_final_master.csv'
-- MAGIC file_path = f'/Workspace/Repos/piyush@loyalytics.in/lulu_notebooks/temp_data_files/{file_name}'
-- MAGIC df_sku_cluster_info = pd.read_csv(file_path)
-- MAGIC spark.createDataFrame(df_sku_cluster_info).createOrReplaceTempView("sku_cluster_view")
-- MAGIC
-- MAGIC query = f"""
-- MAGIC WITH attributes AS (
-- MAGIC     SELECT
-- MAGIC         t1.region_name,
-- MAGIC         CAST(t1.material_id AS INT) AS material_id,
-- MAGIC         t1.price_elasticity,
-- MAGIC         t1.promo_elasticity,
-- MAGIC         t2.content,
-- MAGIC         t2.brand,
-- MAGIC         t2.pack_type,
-- MAGIC         t2.regular_pack_flag,
-- MAGIC         t2.strength,
-- MAGIC         t2.target_surface,
-- MAGIC         t2.environment,
-- MAGIC         t2.form,
-- MAGIC         CASE WHEN t1.r2 >= 0.6 AND t1.price_elasticity < 0 THEN 1 ELSE 0 END AS acceptable_model_flag,
-- MAGIC         CASE WHEN t1.r2 >= 0.6 AND t1.price_elasticity < 0 AND t1.promo_elasticity > 0 THEN 1 ELSE 0 END AS acceptable_model_flag_promo
-- MAGIC     FROM analytics.pricing.hhc_all_model_results_best_model AS t1
-- MAGIC     JOIN sku_cluster_view AS t2 ON t1.material_id = t2.material_id
-- MAGIC     WHERE
-- MAGIC         t1.demand_group = 'hhc_bath_room_cleaners_dg_1'
-- MAGIC         AND t1.model = t1.best_model
-- MAGIC         AND t2.weight_scheme = 'w1'
-- MAGIC         AND t1.region_name = 'ABU DHABI'
-- MAGIC ),
-- MAGIC
-- MAGIC sales AS (
-- MAGIC     SELECT
-- MAGIC         region_name,
-- MAGIC         CAST(material_id AS INT) AS material_id,
-- MAGIC         week_number,
-- MAGIC         quantity,
-- MAGIC         (avg_unit_price * quantity) AS sales
-- MAGIC     FROM analytics.pricing.hhc_mds_master
-- MAGIC     WHERE
-- MAGIC         material_group_name = 'BATH ROOM CLEANERS'
-- MAGIC         AND demand_group = 'DG-1'
-- MAGIC ),
-- MAGIC
-- MAGIC unit_price AS (
-- MAGIC     SELECT
-- MAGIC         region_name,
-- MAGIC         material_id,
-- MAGIC         ROUND(SUM(sales)/ SUM(quantity), 2) AS avg_unit_price
-- MAGIC     FROM sales
-- MAGIC     GROUP BY 1, 2
-- MAGIC )
-- MAGIC
-- MAGIC SELECT
-- MAGIC     t1.*,
-- MAGIC     t2.avg_unit_price
-- MAGIC FROM attributes AS t1
-- MAGIC JOIN unit_price AS t2
-- MAGIC     ON t1.region_name = t2.region_name
-- MAGIC     AND t1.material_id = t2.material_id
-- MAGIC ORDER BY t1.region_name, t1.acceptable_model_flag, t1.material_id
-- MAGIC """
-- MAGIC attributes_df = spark.sql(query).toPandas()
-- MAGIC
-- MAGIC query = f"""
-- MAGIC SELECT
-- MAGIC     t1.region_name,
-- MAGIC     CAST(t1.material_id AS INT) AS material_id,
-- MAGIC     t1.week_number,
-- MAGIC     t1.quantity,
-- MAGIC     CASE WHEN t2.r2 >= 0.6 AND t2.price_elasticity < 0 THEN 1 ELSE 0 END AS acceptable_model_flag,
-- MAGIC     CASE WHEN t2.r2 >= 0.6 AND t2.price_elasticity < 0 AND t2.promo_elasticity > 0 THEN 1 ELSE 0 END AS acceptable_model_flag_promo
-- MAGIC FROM analytics.pricing.hhc_mds_master AS t1
-- MAGIC JOIN analytics.pricing.hhc_all_model_results_best_model AS t2
-- MAGIC     ON t1.region_name = t2.region_name
-- MAGIC     AND t1.material_id = t2.material_id
-- MAGIC WHERE t2.model = t2.best_model
-- MAGIC AND t1.demand_group = 'hhc_bath_room_cleaners_dg_1'
-- MAGIC AND t1.region_name = 'ABU DHABI'
-- MAGIC ORDER BY 1, 2, 3
-- MAGIC """
-- MAGIC qty_df = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from scipy.spatial.distance import cdist
-- MAGIC
-- MAGIC def content_price_dist(attributes_df, region, acceptable_flag = 'acceptable_model_flag'):
-- MAGIC     # Create ordinal values of content column
-- MAGIC
-- MAGIC     df_region = attributes_df[attributes_df['region_name'] == region].reset_index(drop = True)
-- MAGIC     df_region.drop(columns = 'region_name', inplace = True)
-- MAGIC
-- MAGIC     def create_ordinal(df, column_name):
-- MAGIC         # Get distinct values and assign ordinal values
-- MAGIC         unique_vals = df[column_name].unique()
-- MAGIC         ordinal_mapping = {val: idx + 1 for idx, val in enumerate(sorted(unique_vals))}
-- MAGIC         
-- MAGIC         # Set ordinal values in the df
-- MAGIC         df[column_name + '_ordinal'] = df[column_name].map(ordinal_mapping)
-- MAGIC
-- MAGIC         return df
-- MAGIC
-- MAGIC     df_region = create_ordinal(df_region, 'content')
-- MAGIC
-- MAGIC
-- MAGIC     # Create ordinal values of price column
-- MAGIC     query = f"""
-- MAGIC     SELECT
-- MAGIC         CAST(material_id AS INT) AS material_id,
-- MAGIC         material_group_name
-- MAGIC     FROM analytics.pricing.hhc_cluster_sku_info_remapped
-- MAGIC     WHERE
-- MAGIC         material_group_name = 'BATH ROOM CLEANERS'
-- MAGIC         AND cluster_idx = '1'
-- MAGIC         AND weight_scheme = 'w1'
-- MAGIC     """
-- MAGIC     df_sku_attrs_base = spark.sql(query).toPandas()
-- MAGIC     
-- MAGIC     df_region['avg_unit_price_int'] = round(df_region['avg_unit_price'])
-- MAGIC     df_sku_attrs_base = df_sku_attrs_base.merge(df_region[['material_id', 'avg_unit_price_int']], on='material_id', how='left')
-- MAGIC
-- MAGIC     def create_ordinal_category(group):
-- MAGIC         col = 'avg_unit_price_int'
-- MAGIC         unique_values = group[col].nunique()
-- MAGIC         if unique_values == 1:
-- MAGIC             group[f'{col}_ordinal'] = 1
-- MAGIC         else:
-- MAGIC             try:
-- MAGIC                 group[f'{col}_ordinal'] = pd.qcut(group[col], q=min(unique_values, 4), labels=False, duplicates='drop') + 1
-- MAGIC             except ValueError:
-- MAGIC                 group[f'{col}_ordinal'] = group[col].rank(method='dense', ascending=True).astype(int)
-- MAGIC                 group[f'{col}_ordinal'] = pd.cut(group[f'{col}_ordinal'], bins=min(4, unique_values), labels=False) + 1
-- MAGIC         return group
-- MAGIC     
-- MAGIC     df_sku_attrs_base_trans = df_sku_attrs_base.groupby('material_group_name', group_keys=True).apply(create_ordinal_category)
-- MAGIC     df_sku_attrs_base_trans = df_sku_attrs_base_trans.reset_index(drop=True)
-- MAGIC     
-- MAGIC     df_region = df_region.merge(df_sku_attrs_base_trans[['material_id', 'avg_unit_price_int_ordinal']], on='material_id', how='left')
-- MAGIC
-- MAGIC     # Calculate euclidean distance
-- MAGIC     def euclidean_distance(df, feature_column):
-- MAGIC         df_0 = df[df[acceptable_flag] == 0].set_index('material_id')
-- MAGIC         df_1 = df[df[acceptable_flag] == 1].set_index('material_id')
-- MAGIC         
-- MAGIC         distances = cdist(df_0[[feature_column]], df_1[[feature_column]], metric='euclidean')
-- MAGIC         
-- MAGIC         distance_df = pd.DataFrame(distances, index=df_0.index, columns=df_1.index)
-- MAGIC         
-- MAGIC         return distance_df
-- MAGIC
-- MAGIC     content_distance_df = euclidean_distance(df_region, 'content_ordinal')
-- MAGIC     price_distance_df = euclidean_distance(df_region, 'avg_unit_price_int_ordinal')
-- MAGIC
-- MAGIC     # Calculate weighted average of content and price distance
-- MAGIC     weight_content = 0.2
-- MAGIC     weight_price = 0.8
-- MAGIC     content_price_df = (content_distance_df * weight_content + price_distance_df * weight_price) / 2
-- MAGIC
-- MAGIC     return df_region, content_price_df
-- MAGIC
-- MAGIC content_price_df = content_price_dist(attributes_df, 'ABU DHABI', acceptable_flag = 'acceptable_model_flag')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC pd.set_option('display.max_columns', None)
-- MAGIC content_price_df

-- COMMAND ----------

-- MAGIC %py
-- MAGIC def cosine_sim(df_region, categorical_cols, acceptable_flag = 'acceptable_model_flag'):
-- MAGIC     # Calculate the cosine similarity of the categorical attributes
-- MAGIC
-- MAGIC     catg_df = df_region[['material_id', acceptable_flag] + categorical_cols]
-- MAGIC
-- MAGIC     # Drop columns having only 1 distinct value
-- MAGIC     catg_df = catg_df.loc[:, (catg_df != catg_df.iloc[0]).any()]
-- MAGIC     
-- MAGIC     common_cols = list(set(categorical_cols) & set(catg_df.columns))
-- MAGIC
-- MAGIC     # If acceptable_model_flag gets dropped, then re-include it
-- MAGIC     if acceptable_flag not in catg_df.columns:
-- MAGIC         catg_df = catg_df.merge(df_region[['material_id', acceptable_flag]], on = 'material_id', how = 'left')
-- MAGIC
-- MAGIC     # Dummification of attribute columns
-- MAGIC     catg_df_encoded = pd.get_dummies(catg_df, columns=common_cols)
-- MAGIC
-- MAGIC     catg_df_encoded_0 = catg_df_encoded[catg_df_encoded[acceptable_flag] == 0]
-- MAGIC     catg_df_encoded_1 = catg_df_encoded[catg_df_encoded[acceptable_flag] == 1]
-- MAGIC
-- MAGIC     X_0 = catg_df_encoded_0.drop(columns = ['material_id', acceptable_flag])
-- MAGIC     X_1 = catg_df_encoded_1.drop(columns = ['material_id', acceptable_flag])
-- MAGIC
-- MAGIC     # Compute the cosine similarity between each row of df1 and each row of df2
-- MAGIC     df1_array = X_0.values
-- MAGIC     df2_array = X_1.values
-- MAGIC     cosine_sim_matrix = cosine_similarity(df1_array, df2_array)
-- MAGIC     cosine_sim_df = pd.DataFrame(cosine_sim_matrix, index=catg_df_encoded_0['material_id'], columns=catg_df_encoded_1['material_id'])
-- MAGIC
-- MAGIC     # Subtract the cosine similarity values from 1 so that we can convert it into a distance metric
-- MAGIC     cosine_dist_df = cosine_sim_df.copy()
-- MAGIC     cosine_dist_df.iloc[:, :] = 1 - cosine_dist_df.iloc[:, :]
-- MAGIC
-- MAGIC     # Normalize the values
-- MAGIC     # cosine_dist_df_norm = normalize_dataframe(cosine_dist_df)
-- MAGIC
-- MAGIC     return cosine_dist_df
-- MAGIC
-- MAGIC categorical_cols = ['brand', 'pack_type', 'regular_pack_flag', 'strength', 'target_surface', 'environment', 'form']
-- MAGIC cosine_dist_df = cosine_sim(df, categorical_cols)
-- MAGIC cosine_dist_df

-- COMMAND ----------

SELECT *
FROM analytics.pricing.hhc_imputation_metrics
WHERE value IS NULL
ORDER BY metric

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Material Group Checks - AO

-- COMMAND ----------

SELECT
    department_id,
    department_name,
    category_id,
    category_name,
    material_group_id,
    material_group_name
FROM gold.transaction.uae_pos_transactions AS t1
JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
WHERE
    business_day BETWEEN "2023-08-01" AND "2024-07-29"
    AND material_group_name = "SPICES"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1, 2, 3, 4, 5, 6

-- COMMAND ----------

SELECT
    category_name,
    material_group_name,
    COUNT(DISTINCT material_id) AS SKUs
FROM gold.transaction.uae_pos_transactions AS t1
JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
WHERE
    business_day BETWEEN "2024-01-01" AND "2024-12-29"
    AND (CONCAT(t2.category_name, t2.material_group_name) IN ("ICE CREAM & DESSERTSFRUIT JUICES", "PULSES & SPICES & HERBSPULSES", "BISCUITS & CAKESRICE & OAT CAKE", "CANNED MEATSOTHER CANNED MEAT", "SAUCES & PICKLESVINEGAR", "ICE CREAM & DESSERTSFRUITS", "PULSES & SPICES & HERBSSPICES", "ICE CREAM & DESSERTSFROZEN YOGHURT", "ICE CREAM & DESSERTSICE CREAM IMPULSE", "SAUCES & PICKLESPICKLES", "ICE CREAM & DESSERTSCAKES & GATEAUX", "PRESERVATIVES & SPREADSPEANUT BUTTER", "ICE CREAM & DESSERTSSORBETS", "PASTA & NOODLEPASTA", "COOKING OILS & GHEECOCONUT OIL", "PAPER GOODSTRAVEL TISSUE &WIPES", "CANNED MEATSCANNED CORNED BEEF", "PAPER GOODSKITCHEN ROLLS", "PRESERVATIVES & SPREADSJAMS", "CANNED MEATSCANNED LUNCHEON MEAT", "BISCUITS & CAKESSAVOURY", "PAPER GOODSTOILET ROLLS", "PASTA & NOODLECUP NOODLE", "BISCUITS & CAKESCOOKIES", "PAPER GOODSFACIAL TISSUES", "CANNED MEATSCANNED SAUSAGES", "BISCUITS & CAKESCREAM FILLED BISCUIT", "BISCUITS & CAKESMAMOUL", "PRESERVATIVES & SPREADSHONEY", "COOKING OILS & GHEEOLIVE OIL", "BISCUITS & CAKESCHOCOLATE COATED", "BISCUITS & CAKESWAFER BISCUITS", "BISCUITS & CAKESPLAIN BISCUITS", "ICE CREAM & DESSERTSICE CREAM TAKE HOME", "COOKING OILS & GHEEVEGETABLE OIL", "BISCUITS & CAKESKIDS BISCUITS", "COOKING OILS & GHEESUNFLOWER OIL", "BISCUITS & CAKESRUSKS", "BISCUITS & CAKESCAKES", "BISCUITS & CAKESFIBER BISCUITS", "ICE CREAM & DESSERTSICECREAM IMPULSEPACK", "PASTA & NOODLEINSTANT NOODLE", "CONFECTIONERYCHOCOLATE BAGS", "PRESERVATIVES & SPREADSCHOCO SPREAD", "BISCUITS & CAKESSHARING PACKS")
    OR t2.category_name = "WATER")
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND tayeb_flag = 0
    AND amount > 0
    AND quantity > 0
GROUP BY 1, 2
ORDER BY 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AO Summary

-- COMMAND ----------


WITH sales_data AS (
SELECT
    t3.region_name,
    INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
    t2.category_name,
    t2.material_group_name,
    t2.material_id,
    SUM(t1.amount) AS sales
FROM gold.transaction.uae_pos_transactions AS t1
JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
WHERE
    t1.business_day BETWEEN "2023-09-01" AND "2024-08-31"
    AND (CONCAT(t2.category_name, t2.material_group_name) IN ("ICE CREAM & DESSERTSFRUIT JUICES", "PULSES & SPICES & HERBSPULSES", "BISCUITS & CAKESRICE & OAT CAKE", "CANNED MEATSOTHER CANNED MEAT", "SAUCES & PICKLESVINEGAR", "ICE CREAM & DESSERTSFRUITS", "PULSES & SPICES & HERBSSPICES", "ICE CREAM & DESSERTSFROZEN YOGHURT", "ICE CREAM & DESSERTSICE CREAM IMPULSE", "SAUCES & PICKLESPICKLES", "ICE CREAM & DESSERTSCAKES & GATEAUX", "PRESERVATIVES & SPREADSPEANUT BUTTER", "ICE CREAM & DESSERTSSORBETS", "PASTA & NOODLEPASTA", "COOKING OILS & GHEECOCONUT OIL", "PAPER GOODSTRAVEL TISSUE &WIPES", "CANNED MEATSCANNED CORNED BEEF", "PAPER GOODSKITCHEN ROLLS", "PRESERVATIVES & SPREADSJAMS", "CANNED MEATSCANNED LUNCHEON MEAT", "BISCUITS & CAKESSAVOURY", "PAPER GOODSTOILET ROLLS", "PASTA & NOODLECUP NOODLE", "BISCUITS & CAKESCOOKIES", "PAPER GOODSFACIAL TISSUES", "CANNED MEATSCANNED SAUSAGES", "BISCUITS & CAKESCREAM FILLED BISCUIT", "BISCUITS & CAKESMAMOUL", "PRESERVATIVES & SPREADSHONEY", "COOKING OILS & GHEEOLIVE OIL", "BISCUITS & CAKESCHOCOLATE COATED", "BISCUITS & CAKESWAFER BISCUITS", "BISCUITS & CAKESPLAIN BISCUITS", "ICE CREAM & DESSERTSICE CREAM TAKE HOME", "COOKING OILS & GHEEVEGETABLE OIL", "BISCUITS & CAKESKIDS BISCUITS", "COOKING OILS & GHEESUNFLOWER OIL", "BISCUITS & CAKESRUSKS", "BISCUITS & CAKESCAKES", "BISCUITS & CAKESFIBER BISCUITS", "ICE CREAM & DESSERTSICECREAM IMPULSEPACK", "PASTA & NOODLEINSTANT NOODLE", "CONFECTIONERYCHOCOLATE BAGS", "PRESERVATIVES & SPREADSCHOCO SPREAD", "BISCUITS & CAKESSHARING PACKS")
    OR t2.category_name = "WATER")
    AND t3.tayeb_flag = 0
    AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1, 2, 3, 4, 5
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH"
            END AS region_name,
        year_month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month BETWEEN 202309 AND 202408
),

combined AS (
    SELECT
        t1.*,
        COALESCE(t1.sales * t2.gp_wth_chargeback / 100, 0) AS gp_abs
    FROM sales_data AS t1
    LEFT JOIN gp_data AS t2
        ON t1.region_name = t2.region_name
        AND t1.year_month = t2.year_month
        AND t1.material_id = t2.material_id
),

non_water AS (
    SELECT
        category_name,
        material_group_name,
        ROUND(SUM(sales)) AS total_sales,
        ROUND(SUM(gp_abs)) AS total_gp_abs,
        ROUND(total_gp_abs / total_sales * 100, 2) AS gp_margin
    FROM combined
    WHERE category_name != "WATER"
    GROUP BY 1, 2
),

water AS (
    SELECT
        category_name,
        "OVERALL" AS material_group_name,
        ROUND(SUM(sales)) AS total_sales,
        ROUND(SUM(gp_abs)) AS total_gp_abs,
        ROUND(total_gp_abs / total_sales * 100, 2) AS gp_margin
    FROM combined
    WHERE category_name = "WATER"
    GROUP BY 1
),

all_categories AS (
    SELECT * FROM non_water
    UNION
    SELECT * FROM water
)

SELECT *
FROM all_categories
ORDER BY gp_margin DESC

-- COMMAND ----------

WITH cte AS (
    SELECT
        CASE WHEN t1.business_day <= "2024-04-28" THEN "Pre-delist" ELSE "Post Delist" END AS period_type,
        t1.business_day,
        SUM(t1.amount) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        (t1.business_day BETWEEN "2024-02-01" AND "2024-04-28"
        OR t1.business_day BETWEEN "2024-06-15" AND "2024-08-08")
        AND t2.category_name = "COOKING OILS & GHEE"
        AND t2.material_group_name = "COCONUT OIL"
        AND t3.tayeb_flag = 0
        AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2
    ORDER BY 2
)

SELECT
    ROUND(SUM(CASE WHEN period_type = "Pre-delist" THEN sales END) / COUNT(CASE WHEN period_type = "Pre-delist" THEN business_day END)) AS pre_avg_daily_sales,
    ROUND(SUM(CASE WHEN period_type = "Post Delist" THEN sales END) / COUNT(CASE WHEN period_type = "Post Delist" THEN business_day END)) AS post_avg_daily_sales,
    ROUND((post_avg_daily_sales - pre_avg_daily_sales)/pre_avg_daily_sales, 4) AS daily_rate_of_sales_growth
FROM cte

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Mobiles Margin

-- COMMAND ----------

WITH sales AS (
    SELECT
        region_name,
        INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
        material_id,
        SUM(amount) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.store.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_name = "MOBILE PHONES"
        AND tayeb_flag = 0
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

gp AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH"
            END AS region_name,
        year_month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE country = 'AE'
    AND year_month BETWEEN 202308 AND 202407
),

combined AS (
    SELECT
        *,
        COALESCE(sales*gp_wth_chargeback/100, 0) AS gp_abs
    FROM sales AS t1
    LEFT JOIN gp AS t2
        ON t1.region_name = t2.region_name
        AND t1.year_month = t2.year_month
        AND t1.material_id = t2.material_id
)

SELECT
    ROUND(SUM(gp_abs)/SUM(sales), 4) AS gp_margin
FROM combined

-- COMMAND ----------

WITH sales AS (
    SELECT
        region_name,
        INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
        material_id,
        brand,
        SUM(amount) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.store.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_name = "MOBILE PHONES"
        AND brand IN ("APPLE", "SAMSUNG")
        AND tayeb_flag = 0
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3, 4
),

gp AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH"
            END AS region_name,
        year_month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE country = 'AE'
    AND year_month BETWEEN 202308 AND 202407
),

combined AS (
    SELECT
        *,
        COALESCE(sales*gp_wth_chargeback/100, 0) AS gp_abs
    FROM sales AS t1
    LEFT JOIN gp AS t2
        ON t1.region_name = t2.region_name
        AND t1.year_month = t2.year_month
        AND t1.material_id = t2.material_id
)

SELECT
    brand,
    ROUND(SUM(gp_abs)/SUM(sales), 4) AS gp_margin
FROM combined
GROUP BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Water Sales of 2024 Jan

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     business_day,
-- MAGIC     transaction_id,
-- MAGIC     customer_id,
-- MAGIC     material_id,
-- MAGIC     ROUND(SUM(amount),0) AS sales,
-- MAGIC     ROUND(SUM(quantity),0) AS quantity_sold
-- MAGIC FROM gold.pos_transactions AS t1
-- MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
-- MAGIC WHERE business_day BETWEEN '2024-01-01' AND '2024-01-31'
-- MAGIC AND category_name = 'WATER'
-- MAGIC AND region_name = 'DUBAI'
-- MAGIC AND amount > 0
-- MAGIC AND quantity > 0
-- MAGIC GROUP BY business_day, transaction_id, customer_id, material_id
-- MAGIC ORDER BY business_day, transaction_id
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC # df.to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/water_data_temp/water_sales_raw.txt', index=False)

-- COMMAND ----------

SELECT DISTINCT
    material_id,
    material_name,
    material_group_name,
    brand
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id -- 262 materials
JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id -- 
WHERE business_day BETWEEN '2024-01-01' AND '2024-01-31'
AND category_name = 'WATER'
AND region_name = 'DUBAI'
AND amount > 0
AND quantity > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Assortment Categories Long Tail Check

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC WITH gp_perc AS (
-- MAGIC     SELECT
-- MAGIC         CASE WHEN region = "AUH" THEN "ABU DHABI"
-- MAGIC             WHEN region = "ALN" THEN "AL AIN"
-- MAGIC             WHEN region = "DXB" THEN "DUBAI"
-- MAGIC             WHEN region = "SHJ" THEN "SHARJAH"
-- MAGIC             END AS region_name,
-- MAGIC         year_month,
-- MAGIC         material_id,
-- MAGIC         gp_wth_chargeback
-- MAGIC     FROM gold.gross_profit
-- MAGIC     WHERE country = 'AE'
-- MAGIC ),
-- MAGIC
-- MAGIC sales_data AS (
-- MAGIC     SELECT
-- MAGIC         business_day,
-- MAGIC         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
-- MAGIC         region_name,
-- MAGIC         t2.material_id,
-- MAGIC         category_id,
-- MAGIC         category_name,
-- MAGIC         SUM(amount) AS sales
-- MAGIC     FROM gold.pos_transactions AS t1
-- MAGIC     LEFT JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC     LEFT JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
-- MAGIC     WHERE
-- MAGIC         business_day BETWEEN "2023-06-01" AND "2024-05-31"
-- MAGIC         AND transaction_type IN ("SALE", "SELL_MEDIA")
-- MAGIC         AND amount > 0
-- MAGIC         AND quantity > 0
-- MAGIC         AND category_name != "WATER"
-- MAGIC         AND material_group_name NOT IN ("PASTA", "INSTANT NOODLE", "CUP NOODLE", "COCONUT OIL", "OLIVE OIL", "SUNFLOWER OIL", "VEGETABLE OIL")
-- MAGIC         AND department_class_id IN (1, 2)
-- MAGIC         AND tayeb_flag = 0
-- MAGIC     GROUP BY 1, 2, 3, 4, 5, 6
-- MAGIC ),
-- MAGIC
-- MAGIC all_data AS (
-- MAGIC     SELECT
-- MAGIC         t1.*,
-- MAGIC         COALESCE((sales * gp_wth_chargeback / 100), 0) AS gross_profit
-- MAGIC     FROM sales_data AS t1
-- MAGIC     LEFT JOIN gp_perc AS t2
-- MAGIC         ON t1.region_name = t2.region_name
-- MAGIC         AND t1.year_month = t2.year_month
-- MAGIC         AND t1.material_id = t2.material_id
-- MAGIC ),
-- MAGIC
-- MAGIC main_metrics AS (
-- MAGIC     SELECT
-- MAGIC         category_id,
-- MAGIC         category_name,
-- MAGIC         material_id,
-- MAGIC         SUM(sales) AS mat_sales,
-- MAGIC         SUM(gross_profit) AS mat_gp
-- MAGIC     FROM all_data
-- MAGIC     GROUP BY 1, 2, 3
-- MAGIC )
-- MAGIC
-- MAGIC SELECT
-- MAGIC     category_id,
-- MAGIC     category_name,
-- MAGIC     material_id,
-- MAGIC     mat_sales,
-- MAGIC     mat_gp,
-- MAGIC     SUM(mat_sales) OVER(PARTITION BY category_id) AS cat_sales,
-- MAGIC     SUM(mat_gp) OVER(PARTITION BY category_id) AS cat_gp,
-- MAGIC     mat_sales/cat_sales AS mat_sales_perc,
-- MAGIC     mat_gp/cat_gp AS mat_gp_perc,
-- MAGIC     SUM(SUM(mat_sales)/SUM(SUM(mat_sales)) OVER(PARTITION BY category_id)) OVER(PARTITION BY category_id ORDER BY SUM(mat_sales)/SUM(SUM(mat_sales)) OVER(PARTITION BY category_id) DESC) AS cumulative_sales_contri,
-- MAGIC     SUM(SUM(mat_gp)/SUM(SUM(mat_gp)) OVER(PARTITION BY category_id)) OVER(PARTITION BY category_id ORDER BY SUM(mat_gp)/SUM(SUM(mat_gp)) OVER(PARTITION BY category_id) DESC) AS cumulative_gp_contri
-- MAGIC FROM main_metrics
-- MAGIC GROUP BY 1, 2, 3, 4, 5
-- MAGIC ORDER BY cat_sales DESC, mat_sales DESC
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df2 = df.copy()
-- MAGIC df2 = df2.drop(columns=['mat_sales', 'mat_gp', 'cat_gp', 'mat_sales_perc', 'mat_gp_perc'])

-- COMMAND ----------

-- MAGIC %py
-- MAGIC filtered_df = df2[df2['cumulative_sales_contri'] <= 0.8]
-- MAGIC count_df = filtered_df.groupby('category_id').size().reset_index(name='80_perc_sales_skus')
-- MAGIC result_df = df2.merge(count_df, on='category_id', how='left')
-- MAGIC
-- MAGIC filtered_df = df2[df2['cumulative_sales_contri'] > 0.8]
-- MAGIC count_df = filtered_df.groupby('category_id').size().reset_index(name='20_perc_sales_skus')
-- MAGIC result_df = result_df.merge(count_df, on='category_id', how='left')
-- MAGIC
-- MAGIC filtered_df = df2[df2['cumulative_gp_contri'] <= 0.8]
-- MAGIC count_df = filtered_df.groupby('category_id').size().reset_index(name='80_perc_gp_skus')
-- MAGIC result_df = result_df.merge(count_df, on='category_id', how='left')
-- MAGIC
-- MAGIC filtered_df = df2[df2['cumulative_gp_contri'] > 0.8]
-- MAGIC count_df = filtered_df.groupby('category_id').size().reset_index(name='20_perc_gp_skus')
-- MAGIC result_df = result_df.merge(count_df, on='category_id', how='left')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC result_df = result_df.drop(columns=['material_id', 'cumulative_sales_contri', 'cumulative_gp_contri'])
-- MAGIC result_df.rename(columns={'cat_sales': 'cy_sales'}, inplace=True)
-- MAGIC result_df = result_df.drop_duplicates().sort_values(by = 'cy_sales', ascending = False).reset_index(drop = True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC nan_mask = result_df['80_perc_sales_skus'].isna()
-- MAGIC result_df.loc[nan_mask, '80_perc_sales_skus'] = result_df.loc[nan_mask, '20_perc_sales_skus']
-- MAGIC result_df.loc[nan_mask, '20_perc_sales_skus'] = 0
-- MAGIC
-- MAGIC nan_mask = result_df['80_perc_gp_skus'].isna()
-- MAGIC result_df.loc[nan_mask, '80_perc_gp_skus'] = result_df.loc[nan_mask, '20_perc_gp_skus']
-- MAGIC result_df.loc[nan_mask, '20_perc_gp_skus'] = 0

-- COMMAND ----------

-- MAGIC %py
-- MAGIC result_df['80_perc_sales_skus'] = result_df['80_perc_sales_skus'].astype(int)
-- MAGIC result_df['80_perc_gp_skus'] = result_df['80_perc_gp_skus'].fillna(0)
-- MAGIC result_df['80_perc_gp_skus'] = result_df['80_perc_gp_skus'].astype(int)
-- MAGIC result_df['20_perc_sales_skus'] = result_df['20_perc_sales_skus'].astype(int)
-- MAGIC result_df['20_perc_gp_skus'] = result_df['20_perc_gp_skus'].astype(int)
-- MAGIC
-- MAGIC result_df = result_df[['category_id', 'category_name', '80_perc_sales_skus', '20_perc_sales_skus', '80_perc_gp_skus', '20_perc_gp_skus', 'cy_sales']]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC result_df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Assortment Categories GP Margin

-- COMMAND ----------

WITH gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH"
            END AS region_name,
        year_month,
        material_id,
        gp_wth_chargeback
    FROM gold.gross_profit
    WHERE country = 'AE'
),

sales_data AS (
    SELECT
        INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
        region_name,
        category_name,
        CASE WHEN category_name = "WATER" THEN "OVERALL" ELSE material_group_name END AS material_group,
        material_id,
        SUM(amount) AS sales
    FROM gold.pos_transactions AS t1
    LEFT JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    LEFT JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        business_day BETWEEN "2023-06-01" AND "2024-05-31"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
        AND tayeb_flag = 0
        AND (
            material_group_name IN ("PASTA", "INSTANT NOODLE", "CUP NOODLE", "COCONUT OIL", "OLIVE OIL", "SUNFLOWER OIL", "VEGETABLE OIL", "PULSES", "SPICES", "CHOCOLATE BAGS", "PICKLES", "JAMS", "HONEY", "CHOCO SPREAD", "PEANUT BUTTER", "VINEGAR")
            OR category_name IN ("ICE CREAM & DESSERTS", "PAPER GOODS", "CANNED MEATS", "BISCUITS & CAKES", "WATER")
        )
    GROUP BY 1, 2, 3, 4, 5
),

main_table AS (
    SELECT
        t1.*,
        COALESCE((sales * gp_wth_chargeback / 100), 0) AS gross_profit
    FROM sales_data AS t1
    LEFT JOIN gp_data AS t2
        ON t1.region_name = t2.region_name
        AND t1.year_month = t2.year_month
        AND t1.material_id = t2.material_id
)

SELECT
    category_name,
    material_group AS material_group_name,
    (SUM(gross_profit) / SUM(sales)) AS GP_margin,
    SUM(gross_profit) AS GP_ABS
FROM main_table
GROUP BY 1, 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Assortment Delist Reco GP Margin

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC WITH gp_data AS (
-- MAGIC     SELECT
-- MAGIC         year_month,
-- MAGIC         material_id,
-- MAGIC         gp_wth_chargeback
-- MAGIC     FROM gold.gross_profit
-- MAGIC     WHERE region = "AUH"
-- MAGIC ),
-- MAGIC
-- MAGIC sales_data AS (
-- MAGIC     SELECT
-- MAGIC         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
-- MAGIC         material_id,
-- MAGIC         material_name,
-- MAGIC         brand,
-- MAGIC         SUM(amount) AS sales
-- MAGIC     FROM gold.pos_transactions AS t1
-- MAGIC     JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC     JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
-- MAGIC     WHERE
-- MAGIC         business_day BETWEEN "2024-02-29" AND "2024-05-29"
-- MAGIC         AND transaction_type IN ("SALE", "SELL_MEDIA")
-- MAGIC         AND amount > 0
-- MAGIC         AND quantity > 0
-- MAGIC         AND category_name = "COOKING OILS & GHEE"
-- MAGIC         AND material_group_name = "VEGETABLE OIL"
-- MAGIC         AND region_name = "ABU DHABI"
-- MAGIC     GROUP BY 1, 2, 3, 4
-- MAGIC ),
-- MAGIC
-- MAGIC main_table AS (
-- MAGIC     SELECT
-- MAGIC         t1.*,
-- MAGIC         COALESCE((sales * gp_wth_chargeback / 100), 0) AS gross_profit
-- MAGIC     FROM sales_data AS t1
-- MAGIC     LEFT JOIN gp_data AS t2
-- MAGIC         ON t1.year_month = t2.year_month
-- MAGIC         AND t1.material_id = t2.material_id
-- MAGIC )
-- MAGIC
-- MAGIC SELECT
-- MAGIC     material_id,
-- MAGIC     material_name,
-- MAGIC     brand,
-- MAGIC     SUM(sales) AS sales_Q4,
-- MAGIC     (SUM(gross_profit) / SUM(sales)) AS GP_margin
-- MAGIC FROM main_table
-- MAGIC GROUP BY 1, 2, 3
-- MAGIC ORDER BY GP_margin DESC
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC reco_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/vegetable_oil/ao_gp_vegetable_oil_auh.csv')[['material_id', 'new_buckets']]
-- MAGIC
-- MAGIC gp_contri_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/vegetable_oil/ao_gp_vegetable_oil_auh_3m.csv')
-- MAGIC
-- MAGIC df = pd.merge(df, reco_df, on='material_id', how='left')
-- MAGIC df = pd.merge(df, gp_contri_df, on='material_id', how='left')
-- MAGIC
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Data Requirement for Govind

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Abnormal GP margins (Margins > 100%)

-- COMMAND ----------

WITH gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH"
            END AS region_name,
        year_month,
        material_id,
        sales_wo_tax,
        sales_chargeback_wo_tax,
        sales_wth_chargeback_wo_tax,
        gp_cost_of_goods_sold,
        gp_wth_chargeback
    FROM gold.gross_profit
    WHERE country = 'AE'
    AND gp_wth_chargeback > 100
),

sales_data AS (
    SELECT
        region_name,
        INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
        material_id,
        material_name,
        category_id,
        category_name,
        t1.store_id,
        store_name,
        SUM(amount) AS pos_total_sales,
        SUM(quantity) AS pos_total_quantity
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
    WHERE
        business_day BETWEEN "2023-06-01" AND "2024-05-31"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
        AND tayeb_flag = 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT
    t1.region_name,
    store_id,
    store_name,
    category_id,
    category_name,
    t1.material_id,
    material_name,
    t1.year_month,
    pos_total_sales,
    pos_total_quantity,
    sales_wo_tax,
    sales_chargeback_wo_tax,
    sales_wth_chargeback_wo_tax,
    gp_cost_of_goods_sold,
    gp_wth_chargeback
FROM sales_data AS t1
JOIN gp_data AS t2
    ON t1.region_name = t2.region_name
    AND t1.year_month = t2.year_month
    AND t1.material_id = t2.material_id
ORDER BY 1, 2, 3, 4, 5, 6, 7

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Mismatching records of GP & POS

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pandas as pd
-- MAGIC import numpy as np

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     CASE WHEN region = "AUH" THEN "ABU DHABI"
-- MAGIC         WHEN region = "ALN" THEN "AL AIN"
-- MAGIC         WHEN region = "DXB" THEN "DUBAI"
-- MAGIC         WHEN region = "SHJ" THEN "SHARJAH"
-- MAGIC         WHEN region = "QA" THEN "QATAR"
-- MAGIC         WHEN region = "BH" THEN "BAHRAIN"
-- MAGIC         WHEN region = "KW" THEN "KUWAIT"
-- MAGIC         END AS region_name,
-- MAGIC     year_month,
-- MAGIC     material_id
-- MAGIC FROM gold.gross_profit
-- MAGIC WHERE country IN ("AE", "QA", "BH", "KW")
-- MAGIC """
-- MAGIC
-- MAGIC df1 = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     region_name,
-- MAGIC     INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
-- MAGIC     material_id,
-- MAGIC     material_name,
-- MAGIC     category_id,
-- MAGIC     category_name,
-- MAGIC     t1.store_id,
-- MAGIC     store_name,
-- MAGIC     ROUND(SUM(amount), 2) AS pos_total_sales,
-- MAGIC     ROUND(SUM(quantity), 2) AS pos_total_quantity
-- MAGIC FROM gold.pos_transactions AS t1
-- MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
-- MAGIC WHERE
-- MAGIC     business_day BETWEEN "2023-06-01" AND "2024-05-31"
-- MAGIC     business_day >= '2023-01-01'
-- MAGIC     AND transaction_type IN ("SALE", "SELL_MEDIA")
-- MAGIC     AND amount > 0
-- MAGIC     AND quantity > 0
-- MAGIC     AND tayeb_flag = 0
-- MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
-- MAGIC ORDER BY RAND()
-- MAGIC LIMIT 100000
-- MAGIC """
-- MAGIC
-- MAGIC df2 = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     region_name,
-- MAGIC     INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
-- MAGIC     material_id,
-- MAGIC     material_name,
-- MAGIC     category_id,
-- MAGIC     category_name,
-- MAGIC     t1.store_id,
-- MAGIC     store_name,
-- MAGIC     ROUND(SUM(amount), 2) AS pos_total_sales,
-- MAGIC     ROUND(SUM(quantity), 2) AS pos_total_quantity
-- MAGIC FROM gold.qatar_pos_transactions AS t1
-- MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
-- MAGIC WHERE
-- MAGIC     business_day BETWEEN "2023-06-01" AND "2024-05-31"
-- MAGIC     AND transaction_type_id NOT IN ("RT", "RR")
-- MAGIC     AND amount > 0
-- MAGIC     AND quantity > 0
-- MAGIC     AND tayeb_flag = 0
-- MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
-- MAGIC ORDER BY RAND()
-- MAGIC LIMIT 100000
-- MAGIC """
-- MAGIC
-- MAGIC df3 = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     region_name,
-- MAGIC     INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
-- MAGIC     material_id,
-- MAGIC     material_name,
-- MAGIC     category_id,
-- MAGIC     category_name,
-- MAGIC     t1.store_id,
-- MAGIC     store_name,
-- MAGIC     ROUND(SUM(amount), 2) AS pos_total_sales,
-- MAGIC     ROUND(SUM(quantity), 2) AS pos_total_quantity
-- MAGIC FROM gold.bahrain_pos_transactions AS t1
-- MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
-- MAGIC WHERE
-- MAGIC     business_day BETWEEN "2023-06-01" AND "2024-05-31"
-- MAGIC     AND transaction_type_id NOT IN ("RT", "RR")
-- MAGIC     AND amount > 0
-- MAGIC     AND quantity > 0
-- MAGIC     AND tayeb_flag = 0
-- MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
-- MAGIC ORDER BY RAND()
-- MAGIC LIMIT 100000
-- MAGIC """
-- MAGIC
-- MAGIC df4 = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     region_name,
-- MAGIC     INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
-- MAGIC     material_id,
-- MAGIC     material_name,
-- MAGIC     category_id,
-- MAGIC     category_name,
-- MAGIC     t1.store_id,
-- MAGIC     store_name,
-- MAGIC     ROUND(SUM(amount), 2) AS pos_total_sales,
-- MAGIC     ROUND(SUM(quantity), 2) AS pos_total_quantity
-- MAGIC FROM gold.kuwait_pos_transactions AS t1
-- MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
-- MAGIC WHERE
-- MAGIC     business_day BETWEEN "2023-06-01" AND "2024-05-31"
-- MAGIC     AND transaction_type_id NOT IN ("RT", "RR")
-- MAGIC     AND amount > 0
-- MAGIC     AND quantity > 0
-- MAGIC     AND tayeb_flag = 0
-- MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
-- MAGIC ORDER BY RAND()
-- MAGIC LIMIT 100000
-- MAGIC """
-- MAGIC
-- MAGIC df5 = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC gp_df = df1.copy()
-- MAGIC pos_uae_df = df2.copy()
-- MAGIC pos_qatar_df = df3.copy()
-- MAGIC pos_bahrain_df = df4.copy()
-- MAGIC pos_kuwait_df = df5.copy()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC pos_uae_only_df = pos_uae_df.merge(gp_df, on=['region_name', 'year_month', 'material_id'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
-- MAGIC pos_uae_only_df = pos_uae_only_df[['region_name', 'store_id', 'store_name', 'material_id', 'material_name', 'category_id', 'category_name', 'year_month', 'pos_total_sales', 'pos_total_quantity']]
-- MAGIC
-- MAGIC pos_qatar_only_df = pos_qatar_df.merge(gp_df, on=['region_name', 'year_month', 'material_id'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
-- MAGIC pos_qatar_only_df = pos_qatar_only_df[['region_name', 'store_id', 'store_name', 'material_id', 'material_name', 'category_id', 'category_name', 'year_month', 'pos_total_sales', 'pos_total_quantity']]
-- MAGIC
-- MAGIC pos_bahrain_only_df = pos_bahrain_df.merge(gp_df, on=['region_name', 'year_month', 'material_id'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
-- MAGIC pos_bahrain_only_df = pos_bahrain_only_df[['region_name', 'store_id', 'store_name', 'material_id', 'material_name', 'category_id', 'category_name', 'year_month', 'pos_total_sales', 'pos_total_quantity']]
-- MAGIC
-- MAGIC pos_kuwait_only_df = pos_kuwait_df.merge(gp_df, on=['region_name', 'year_month', 'material_id'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
-- MAGIC pos_kuwait_only_df = pos_kuwait_only_df[['region_name', 'store_id', 'store_name', 'material_id', 'material_name', 'category_id', 'category_name', 'year_month', 'pos_total_sales', 'pos_total_quantity']]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC pos_only_df = pd.concat([pos_uae_only_df, pos_qatar_only_df, pos_bahrain_only_df, pos_kuwait_only_df], ignore_index = True)
-- MAGIC pos_only_df = pos_only_df.sort_values(by = ['region_name', 'store_id', 'material_id', 'year_month']).reset_index(drop = True)
-- MAGIC pos_only_df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Govind's Findings Confirmations

-- COMMAND ----------

SELECT *
FROM gold.business.gross_profit
WHERE material_id = 2171837001
AND region = 'AUH'
AND year_month = 202403
-- 000000002171837001
-- 2171804004

-- COMMAND ----------

SELECT *
FROM gold.gross_profit
WHERE material_id = 1644658
AND region = 'AUH'
AND year_month = 202403

-- COMMAND ----------

SELECT
    SUM(amount) AS all_trans_sales,
    SUM(CASE WHEN transaction_type IN ("SALE", "SELL_MEDIA") THEN amount ELSE 0 END) AS only_sales
FROM gold.pos_transactions AS t1
JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
WHERE business_day BETWEEN "2024-03-01" AND "2024-03-31"
AND product_id = 1644658
AND region_name = "ABU DHABI"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##% of Erroneous Records

-- COMMAND ----------

SELECT
    CASE WHEN region = "AUH" THEN "ABU DHABI"
        WHEN region = "ALN" THEN "AL AIN"
        WHEN region = "DXB" THEN "DUBAI"
        WHEN region = "SHJ" THEN "SHARJAH"
        END AS region_name,
    year_month,
    material_id,
    sales_wo_tax,
    sales_chargeback_wo_tax,
    sales_wth_chargeback_wo_tax,
    gp_cost_of_goods_sold,
    gp_wth_chargeback
FROM gold.gross_profit
WHERE country = 'AE'
AND gp_wth_chargeback > 100

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     CASE WHEN region = "AUH" THEN "ABU DHABI"
-- MAGIC         WHEN region = "ALN" THEN "AL AIN"
-- MAGIC         WHEN region = "DXB" THEN "DUBAI"
-- MAGIC         WHEN region = "SHJ" THEN "SHARJAH"
-- MAGIC         END AS region_name,
-- MAGIC     year_month,
-- MAGIC     material_id
-- MAGIC FROM gold.gross_profit
-- MAGIC WHERE country = 'AE'
-- MAGIC """
-- MAGIC
-- MAGIC df1 = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     region_name,
-- MAGIC     INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
-- MAGIC     material_id,
-- MAGIC     ROUND(SUM(amount), 2) AS pos_total_sales,
-- MAGIC     ROUND(SUM(quantity), 2) AS pos_total_quantity
-- MAGIC FROM gold.pos_transactions AS t1
-- MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
-- MAGIC JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
-- MAGIC WHERE
-- MAGIC     business_day >= '2023-01-01'
-- MAGIC     AND transaction_type IN ("SALE", "SELL_MEDIA")
-- MAGIC     AND amount > 0
-- MAGIC     AND quantity > 0
-- MAGIC     AND tayeb_flag = 0
-- MAGIC GROUP BY 1, 2, 3
-- MAGIC """
-- MAGIC
-- MAGIC df2 = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC gp_df = df1.copy()
-- MAGIC pos_uae_df = df2.copy()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC pos_uae_only_df = pos_uae_df.merge(gp_df, on=['region_name', 'year_month', 'material_id'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC pos_uae_only_df = pos_uae_only_df.sort_values(by = ['region_name', 'year_month', 'material_id']).reset_index(drop = True)
-- MAGIC pos_uae_only_df.display()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC len(pos_uae_only_df)

-- COMMAND ----------

SELECT COUNT(*)
FROM gold.gross_profit
WHERE country = 'AE'

-- COMMAND ----------

SELECT ROUND((342605 + 465) / 7314393, 4)

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


