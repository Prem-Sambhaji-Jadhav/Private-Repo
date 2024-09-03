-- Databricks notebook source
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
        business_day BETWEEN "2023-09-01" AND "2024-08-31"
        AND department_class_id = 1
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
    AND year_month BETWEEN 202309 AND 202408
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



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Test Check

-- COMMAND ----------

SELECT
    segment,
    COUNT(DISTINCT t1.customer_id) AS customers,
    ROUND(SUM(amount)) AS sales,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(orders/customers,2) AS avg_freq,
    ROUND(sales/orders,2) AS atv
FROM gold.pos_transactions AS t1
JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
WHERE
    business_day BETWEEN "2023-05-01" AND "2024-04-30"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND key = 'rfm'
    AND channel = 'pos'
    AND t2.country = 'uae'
    AND month_year = 202404
    AND department_id BETWEEN 1 AND 13
GROUP BY segment

-- COMMAND ----------

WITH cte AS (
  SELECT
      segment,
      QUARTER(business_day) AS quarter_info,
      ROUND(SUM(amount)) AS sales
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  WHERE YEAR(business_day) = 2023
  AND transaction_type IN ("SALE", "SELL_MEDIA")
  AND key = 'rfm'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = 202312
  GROUP BY segment, quarter_info
)

SELECT
    segment,
    MAX(CASE WHEN quarter_info = 1 THEN sales END) AS Q1,
    MAX(CASE WHEN quarter_info = 2 THEN sales END) AS Q2,
    MAX(CASE WHEN quarter_info = 3 THEN sales END) AS Q3,
    MAX(CASE WHEN quarter_info = 4 THEN sales END) AS Q4
FROM cte
GROUP BY segment

-- COMMAND ----------

WITH cte AS (
  SELECT
      segment,
      gender,
      COUNT(DISTINCT t1.customer_id) AS customers
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
  JOIN gold.customer_profile AS t3 ON t1.customer_id = t3.account_key
  WHERE YEAR(business_day) = 2023
  AND transaction_type IN ("SALE", "SELL_MEDIA")
  AND key = 'rfm'
  AND channel = 'pos'
  AND t2.country = 'uae'
  AND month_year = 202312
  GROUP BY segment, gender
)

SELECT
    segment,
    MAX(CASE WHEN gender = "MALE" THEN customers END) AS males,
    MAX(CASE WHEN gender = "FEMALE" THEN customers END) AS females
FROM cte
GROUP BY segment

-- COMMAND ----------

SELECT
    segment,
    ROUND(COUNT(DISTINCT transaction_id) / COUNT(DISTINCT t1.customer_id),2) AS avg_transactions
FROM gold.pos_transactions AS t1
JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
WHERE
    YEAR(business_day) = 2023
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND key = 'rfm'
    AND channel = 'pos'
    AND t2.country = 'uae'
    AND month_year = 202312
    AND department_id BETWEEN 1 AND 13
GROUP BY segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Material Attributes Table

-- COMMAND ----------

SELECT
    t1.material_id,
    material_name,
    material_description,
    ean,
    conversion_numerator,
    content,
    content_unit,
    volume
FROM gold.material_master AS t1
JOIN gold.material_attributes AS t2 ON t1.material_id = t2.material_id
WHERE material_group_name = "COCONUT OIL"

-- COMMAND ----------

SELECT t1.*
FROM gold.material_attributes AS t1
JOIN gold.material_master AS t2 ON t1.material_id = t2.material_id
WHERE t2.category_name = "CONFECTIONERY"

-- COMMAND ----------

SELECT DISTINCT manufacturer_brand
FROM gold.material_attributes

-- COMMAND ----------

SELECT DISTINCT product_heirarchy_desc
FROM gold.material_attributes

-- COMMAND ----------

SELECT COUNT(DISTINCT ean), COUNT(ean)
FROM gold.material_attributes

-- COMMAND ----------

SELECT DISTINCT material_id, material_description, ean, main_ean, ean_category, alt_unit, conversion_numerator, base_unit
FROM gold.material_attributes
WHERE material_id IN (334678, 1294454)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #YoY and Top Brands - AO

-- COMMAND ----------

SELECT
    CASE WHEN business_day < "2023-03-01" THEN "PY" ELSE "CY" END AS Year,
    ROUND(SUM(amount),0) AS Sales,
    ROUND(SUM(quantity),0) AS Volume
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
WHERE business_day BETWEEN '2022-03-01' AND '2024-02-29'
AND category_name = 'PASTA & NOODLE'
AND material_group_name = 'INSTANT NOODLE'
AND amount > 0
AND quantity > 0
GROUP BY Year

-- COMMAND ----------

SELECT
    brand,
    ROUND(SUM(amount),0) AS sales,
    SUM(ROUND(SUM(amount),0)) OVER () AS total_sales,
    (sales/total_sales) AS sales_contri
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
WHERE business_day BETWEEN '2023-03-01' AND '2024-02-29'
AND category_name = 'PASTA & NOODLE'
AND material_group_name = 'INSTANT NOODLE'
AND amount > 0
AND quantity > 0
GROUP BY brand
ORDER BY sales_contri DESC

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
    business_day BETWEEN "2023-09-01" AND "2024-08-29"
    AND (
        category_name IN ("ICE CREAM & DESSERTS", "PAPER GOODS", "CANNED MEATS", "BISCUITS & CAKES")
        OR material_group_name IN ("PULSES", "SPICES", "CHOCOLATE BAGS", "PICKLES", "JAMS", "HONEY", "CHOCO SPREAD", "PEANUT BUTTER", "VINEGAR")
        )
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND tayeb_flag = 0
    AND amount > 0
    AND quantity > 0
GROUP BY 1, 2
ORDER BY 3

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


