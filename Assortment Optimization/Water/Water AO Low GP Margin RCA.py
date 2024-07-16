# Databricks notebook source
# MAGIC %md
# MAGIC #Sandbox Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sandbox.pj_water_gp_margin_rca AS (
# MAGIC     WITH cte1 AS (
# MAGIC         SELECT
# MAGIC             business_day,
# MAGIC             region_name,
# MAGIC             category_period_type_lfl,
# MAGIC             material_id,
# MAGIC             material_name,
# MAGIC             recommendation,
# MAGIC             delisted_date,
# MAGIC             gp_wth_chargeback,
# MAGIC             SUM(sales) AS tot_sales,
# MAGIC             SUM(gross_profit) AS tot_gp
# MAGIC         FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC         WHERE business_day >= "2023-12-01"
# MAGIC         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
# MAGIC     ),
# MAGIC
# MAGIC     cte2 AS (
# MAGIC         SELECT
# MAGIC             product_id,
# MAGIC             CONCAT(SUBSTRING(business_date, 1, 4), '-',
# MAGIC                 SUBSTRING(business_date, 5, 2), '-',
# MAGIC                 SUBSTRING(business_date, 7, 2)) AS formatted_date
# MAGIC         FROM gold.pos_sales_campaign
# MAGIC         WHERE
# MAGIC             void_flag IS NULL
# MAGIC             AND campaign_id IS NOT NULL
# MAGIC             AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
# MAGIC             AND (pm_reason_code != "HAPPINESS VOUCHER")
# MAGIC             AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
# MAGIC             AND business_date BETWEEN "20240101" AND "20240527"
# MAGIC         GROUP BY product_id, business_date
# MAGIC     )
# MAGIC
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         CASE WHEN formatted_date IS NULL THEN "Not on promo" ELSE "On promo" END AS promo_flag
# MAGIC     FROM cte1 AS t1
# MAGIC     LEFT JOIN cte2 AS t2
# MAGIC         ON t1.material_id = t2.product_id
# MAGIC         AND t1.business_day = t2.formatted_date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.pj_water_gp_margin_rca

# COMMAND ----------

# MAGIC %md
# MAGIC #RCA Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         material_id,
# MAGIC         material_name,
# MAGIC         recommendation,
# MAGIC         CASE WHEN delisted_date = "NA" THEN "Not Delisted" ELSE "Delisted" END AS delist_status,
# MAGIC         category_period_type_lfl,
# MAGIC         ROUND(SUM(tot_sales), 2) AS sales,
# MAGIC         ROUND(SUM(tot_gp), 2) AS gp,
# MAGIC         ROUND(gp/sales, 4) AS gp_margin,
# MAGIC         ROUND(SUM(tot_sales)/COUNT(DISTINCT WEEKOFYEAR(business_day)), 2) AS rate_of_sales
# MAGIC     FROM sandbox.pj_water_gp_margin_rca
# MAGIC     WHERE promo_flag = "Not on promo"
# MAGIC     GROUP BY 1, 2, 3, 4, 5, 6
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     material_id,
# MAGIC     material_name,
# MAGIC     recommendation,
# MAGIC     delist_status,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN sales END), 0) AS pre_sales,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN sales END), 0) AS post_sales,
# MAGIC     COALESCE(ROUND((post_sales - pre_sales) / pre_sales, 4), 0) AS sales_growth,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN gp END), 0) AS pre_gp,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN gp END), 0) AS post_gp,
# MAGIC     COALESCE(ROUND((post_gp - pre_gp) / pre_gp, 4), 0) AS gp_growth,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN gp_margin END), 0) AS pre_gp_margin,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN gp_margin END), 0) AS post_gp_margin,
# MAGIC     COALESCE((post_gp_margin - pre_gp_margin), 0) AS gp_margin_delta,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN rate_of_sales END), 0) AS pre_rate_of_sales,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN rate_of_sales END), 0) AS post_rate_of_sales,
# MAGIC     COALESCE(ROUND((post_rate_of_sales - pre_rate_of_sales) / pre_rate_of_sales, 4), 0) AS rate_of_sales_growth
# MAGIC FROM cte
# MAGIC GROUP BY 1, 2, 3, 4, 5
# MAGIC ORDER BY pre_gp_margin DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         material_id,
# MAGIC         material_name,
# MAGIC         recommendation,
# MAGIC         CASE WHEN delisted_date = "NA" THEN "Not Delisted" ELSE "Delisted" END AS delist_status,
# MAGIC         category_period_type_lfl,
# MAGIC         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
# MAGIC         ROUND(SUM(tot_sales), 2) AS sales,
# MAGIC         ROUND(SUM(tot_gp), 2) AS gp,
# MAGIC         ROUND(gp/sales, 4) AS gp_margin,
# MAGIC         ROUND(SUM(tot_sales)/COUNT(DISTINCT WEEKOFYEAR(business_day)), 2) AS rate_of_sales
# MAGIC     FROM sandbox.pj_water_gp_margin_rca
# MAGIC     WHERE promo_flag = "Not on promo"
# MAGIC     GROUP BY 1, 2, 3, 4, 5, 6, 7
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     material_id,
# MAGIC     material_name,
# MAGIC     recommendation,
# MAGIC     delist_status,
# MAGIC     year_month,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN sales END), 0) AS pre_sales,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN sales END), 0) AS post_sales,
# MAGIC     COALESCE(ROUND((post_sales - pre_sales) / pre_sales, 4), 0) AS sales_growth,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN gp END), 0) AS pre_gp,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN gp END), 0) AS post_gp,
# MAGIC     COALESCE(ROUND((post_gp - pre_gp) / pre_gp, 4), 0) AS gp_growth,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN gp_margin END), 0) AS pre_gp_margin,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN gp_margin END), 0) AS post_gp_margin,
# MAGIC     COALESCE((post_gp_margin - pre_gp_margin), 0) AS gp_margin_delta,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN rate_of_sales END), 0) AS pre_rate_of_sales,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN rate_of_sales END), 0) AS post_rate_of_sales,
# MAGIC     COALESCE(ROUND((post_rate_of_sales - pre_rate_of_sales) / pre_rate_of_sales, 4), 0) AS rate_of_sales_growth
# MAGIC FROM cte
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6
# MAGIC ORDER BY region_name, material_id, year_month

# COMMAND ----------

# MAGIC %md
# MAGIC #Date Adjustments

# COMMAND ----------

# MAGIC %md
# MAGIC ##User Inputs

# COMMAND ----------

py_start_date = '2022-10-02'
py_end_date = '2023-07-02'
cy_start_date = '2023-10-01'
cy_end_date = '2024-06-30'
rfm_month_year = 202405

categories = ['WATER']
categories_recommendation_dates = ['2024-02-12']

analysis_period_start_dates = ["2023-01-01"]

# COMMAND ----------

categories_sql = ', '.join([f"'{item}'" for item in categories])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Statements

# COMMAND ----------

import pandas as pd
import numpy as np
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales & Volume

# COMMAND ----------

query = f"""
SELECT
    business_day,
    region_name,
    t1.store_id,
    store_name,
    department_name,
    category_name,
    material_group_name,
    material_id,
    material_name,
    brand,
    SUM(amount) AS sales,
    SUM(quantity) AS volume
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
WHERE
    (business_day BETWEEN '{py_start_date}' AND '{py_end_date}'
    OR business_day BETWEEN '{cy_start_date}' AND '{cy_end_date}')
    AND category_name IN ({categories_sql})
    AND transaction_type IN ('SALE', 'SELL_MEDIA')
    AND tayeb_flag = 0
    AND amount > 0
    AND quantity > 0
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
"""

region_view_df = spark.sql(query).toPandas()

# COMMAND ----------

df = region_view_df.copy()
df['year_month'] = df['business_day'].str[:7].str.replace('-', '')
df['year_month'] = df['year_month'].astype(int)
df.head()

# COMMAND ----------

# df[(df['region_name'] == 'ABU DHABI') & (df['business_day'] >= '2023-10-01') & (df['business_day'] <= '2023-12-07')]['sales'].sum()

# COMMAND ----------

# %sql
# SELECT SUM(amount) AS sales
# FROM gold.pos_transactions AS t1
# JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
# WHERE business_day BETWEEN "2023-10-01" AND "2023-12-07"
# AND transaction_type IN ("SALE", "SELL_MEDIA")
# AND amount > 0
# AND quantity > 0
# AND category_name = "WATER"
# AND region_name = "ABU DHABI"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(amount) AS sales
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC WHERE business_day BETWEEN "2024-04-01" AND "2024-05-31"
# MAGIC AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC AND amount > 0
# MAGIC AND quantity > 0
# MAGIC AND category_name = "WATER"
# MAGIC -- AND region_name = "ABU DHABI"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reco Dates & Delisted Dates

# COMMAND ----------

temp = pd.DataFrame({
    'category_name': categories,
    'recommendation_date': categories_recommendation_dates,
    'analysis_period_start_dates': analysis_period_start_dates
})

df = pd.merge(df, temp, on='category_name', how='left')

# COMMAND ----------

regions = ['auh', 'aln', 'dxb', 'shj']
delisted_dates = []

for category in categories:
    category = category.lower()
    category = category.replace(' ', '_')

    delisted_auh = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/{category}_rationalized_auh.csv")
    delisted_aln = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/{category}_rationalized_aln.csv")
    delisted_dxb = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/{category}_rationalized_dxb.csv")
    delisted_shj = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/{category}_rationalized_shj.csv")

    delisted_auh['region_name'] = 'ABU DHABI'
    delisted_aln['region_name'] = 'AL AIN'
    delisted_dxb['region_name'] = 'DUBAI'
    delisted_shj['region_name'] = 'SHARJAH'

    delisted_uae = pd.concat([delisted_auh, delisted_aln, delisted_dxb, delisted_shj], ignore_index=True)

    delisted_uae = delisted_uae[['region_name', 'material_id', 'delisted_date', 'confirmed_by']]

    delisted_uae['delisted_date'] = pd.to_datetime(delisted_uae['delisted_date'], format='%d/%m/%Y')
    delisted_uae['delisted_date'] = delisted_uae['delisted_date'].dt.strftime('%Y-%m-%d')

    df = pd.merge(df, delisted_uae, on=['region_name', 'material_id'], how='left')
    df['period_type'] = np.where(df.business_day >= df.delisted_date, "Post delist", "Pre-delist")

    delisted_uae = delisted_uae.dropna(subset=['delisted_date'])

    temp = delisted_uae[['region_name', 'delisted_date']].drop_duplicates().sort_values(by='region_name', ascending=True).reset_index(drop=True)
    for i in temp['delisted_date'].tolist():
        delisted_dates.append(i)

df['delisted_date'] = df['delisted_date'].fillna("NA")
df['confirmed_by'] = df['confirmed_by'].fillna("NA")

# COMMAND ----------

delisted_regions = ['ABU DHABI', 'AL AIN', 'DUBAI', 'SHARJAH'] * len(categories)
delisted_categories = [x for x in categories for _ in range(4)]

delisted_categories = pd.DataFrame({
    'region_name': delisted_regions,
    'category_name': delisted_categories,
    'category_delisted_date': delisted_dates
})

df = pd.merge(df, delisted_categories, on=['region_name', 'category_name'], how='left')
df['category_period_type'] = np.where(df.business_day >= "2024-04-01", "Post delist", np.where(df.business_day <= "2023-12-30", "Pre-delist", "Intermediate"))

# COMMAND ----------

# df['business_day'] = pd.to_datetime(df['business_day'])
# df['category_delisted_date'] = pd.to_datetime(df['category_delisted_date'])

# category_delisted_date_lfl = df.category_delisted_date - pd.DateOffset(years=1) + pd.DateOffset(days=2)
df['category_period_type_lfl'] = np.where(
    (df.business_day >= "2024-04-01") | ((df.business_day >= "2023-04-03") & (df.business_day <= "2023-07-02")),
    'Post delist',
    np.where(((df.business_day <= "2023-12-30") & (df.business_day >= "2023-10-01")) | ((df.business_day <= "2022-12-31") & (df.business_day >= "2022-10-02")), 'Pre-delist', 'Intermediate')
)

# df['business_day'] = df['business_day'].dt.strftime('%Y-%m-%d')
# df['category_delisted_date'] = df['category_delisted_date'].dt.strftime('%Y-%m-%d')

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ###GP

# COMMAND ----------

year_month_start = int(py_start_date[:7].replace('-', ''))
year_month_end = int(cy_end_date[:7].replace('-', ''))

query = f"""
SELECT
    region,
    year_month,
    material_id,
    gp_wth_chargeback
FROM gold.gross_profit
WHERE
    country = 'AE'
    AND year_month BETWEEN {year_month_start} AND {year_month_end}
    AND category_name IN ({categories_sql})
"""

gp = spark.sql(query).toPandas()
gp.rename(columns = {'region':'region_name'}, inplace=True)

# COMMAND ----------

gp['region_name'] = gp['region_name'].replace(['AUH', 'ALN', 'DXB', 'SHJ'], ['ABU DHABI', 'AL AIN', 'DUBAI', 'SHARJAH'])

# COMMAND ----------

df = pd.merge(df, gp, on=['region_name', 'year_month', 'material_id'], how='left')

df['gp_wth_chargeback'] = df['gp_wth_chargeback'].fillna(0)
df['gross_profit'] = df['gp_wth_chargeback'] * df['sales']/100

# COMMAND ----------

# MAGIC %md
# MAGIC ###New SKUs Tagging

# COMMAND ----------

new_skus = df[['region_name', 'category_period_type', 'material_id']].drop_duplicates().reset_index(drop=True)

pre_period_dict = {}
post_period_dict = {}

for region in new_skus['region_name'].unique():
    pre_period_dict[region] = set(df[(df['region_name'] == region) & (df['category_period_type'] == 'Pre-delist')]['material_id'].unique())
    post_period_dict[region] = set(df[(df['region_name'] == region) & (df['category_period_type'] == 'Post delist')]['material_id'].unique())

def find_new_sku_flag(row):
    region = row['region_name']
    material_id = row['material_id']
    category_period_type = row['category_period_type']
    
    if category_period_type == 'Post delist' and material_id in post_period_dict[region] - pre_period_dict[region]:
        return 1
    else:
        return 0

new_skus['new_sku_flag'] = new_skus.apply(lambda row: find_new_sku_flag(row), axis=1)
new_skus = new_skus[['region_name', 'material_id', 'new_sku_flag']].drop_duplicates().reset_index(drop =True)
df = pd.merge(df, new_skus, on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Recommendations Table

# COMMAND ----------

reco_buckets = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/ao_gp.csv")
reco_buckets = reco_buckets[['material_id', 'new_buckets']]

reco_buckets.rename(columns = {'new_buckets':'recommendation'}, inplace=True)
reco_buckets['recommendation'] = reco_buckets['recommendation'].replace('Maintain','Support_with_more_distribution')
reco_buckets['recommendation'] = reco_buckets['recommendation'].replace('Grow','Maintain_or_grow_by_promo')

temp = df[['region_name', 'material_id']].drop_duplicates().reset_index(drop=True)
reco_buckets = pd.merge(reco_buckets, temp, on='material_id', how='left')

# COMMAND ----------

reco_buckets_uae = pd.DataFrame()

for category in categories:
    category = category.lower()
    category = category.replace(' ', '_')

    reco_buckets_auh = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/ao_gp_{category}_auh.csv")
    reco_buckets_aln = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/ao_gp_{category}_aln.csv")
    reco_buckets_dxb = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/ao_gp_{category}_dxb.csv")
    reco_buckets_shj = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{category}/ao_gp_{category}_shj.csv")

    reco_buckets_auh['region_name'] = 'ABU DHABI'
    reco_buckets_aln['region_name'] = 'AL AIN'
    reco_buckets_dxb['region_name'] = 'DUBAI'
    reco_buckets_shj['region_name'] = 'SHARJAH'

    reco_buckets_uae = pd.concat([reco_buckets_uae, reco_buckets_auh, reco_buckets_aln, reco_buckets_dxb, reco_buckets_shj], ignore_index=True)

reco_buckets_uae = reco_buckets_uae[['region_name', 'material_id', 'new_buckets']]

reco_buckets_uae.rename(columns = {'new_buckets':'recommendation'}, inplace=True)
reco_buckets_uae['recommendation'] = reco_buckets_uae['recommendation'].replace('Maintain','Support_with_more_distribution')
reco_buckets_uae['recommendation'] = reco_buckets_uae['recommendation'].replace('Grow','Maintain_or_grow_by_promo')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.pj_assortment_dashboard_reco

# COMMAND ----------

# MAGIC %md
# MAGIC ###Main Table

# COMMAND ----------

df = pd.merge(df, reco_buckets_uae[['region_name', 'material_id', 'recommendation']], on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

spark.createDataFrame(df).createOrReplaceTempView('main')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sandbox.pj_water_gp_margin_rca_table2 AS (
# MAGIC   SELECT * FROM main
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.pj_water_gp_margin_rca_table2 LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sandbox.pj_water_gp_margin_rca2 AS (
# MAGIC     WITH cte1 AS (
# MAGIC         SELECT
# MAGIC             business_day,
# MAGIC             region_name,
# MAGIC             category_period_type_lfl,
# MAGIC             material_id,
# MAGIC             material_name,
# MAGIC             recommendation,
# MAGIC             delisted_date,
# MAGIC             gp_wth_chargeback,
# MAGIC             SUM(sales) AS tot_sales,
# MAGIC             SUM(gross_profit) AS tot_gp
# MAGIC         FROM sandbox.pj_water_gp_margin_rca_table2
# MAGIC         WHERE business_day >= "2023-10-01"
# MAGIC         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
# MAGIC     ),
# MAGIC
# MAGIC     cte2 AS (
# MAGIC         SELECT
# MAGIC             product_id,
# MAGIC             CONCAT(SUBSTRING(business_date, 1, 4), '-',
# MAGIC                 SUBSTRING(business_date, 5, 2), '-',
# MAGIC                 SUBSTRING(business_date, 7, 2)) AS formatted_date
# MAGIC         FROM gold.pos_sales_campaign
# MAGIC         WHERE
# MAGIC             void_flag IS NULL
# MAGIC             AND campaign_id IS NOT NULL
# MAGIC             AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
# MAGIC             AND (pm_reason_code != "HAPPINESS VOUCHER")
# MAGIC             AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
# MAGIC             AND business_date BETWEEN "20231001" AND "20240604"
# MAGIC         GROUP BY product_id, business_date
# MAGIC     )
# MAGIC
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         CASE WHEN formatted_date IS NULL THEN "Not on promo" ELSE "On promo" END AS promo_flag
# MAGIC     FROM cte1 AS t1
# MAGIC     LEFT JOIN cte2 AS t2
# MAGIC         ON t1.material_id = t2.product_id
# MAGIC         AND t1.business_day = t2.formatted_date
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##RCA Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pre vs Post

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         material_id,
# MAGIC         material_name,
# MAGIC         recommendation,
# MAGIC         CASE WHEN delisted_date = "NA" THEN "Not Delisted" ELSE "Delisted" END AS delist_status,
# MAGIC         category_period_type_lfl,
# MAGIC         ROUND(SUM(tot_sales), 2) AS sales,
# MAGIC         ROUND(SUM(tot_gp), 2) AS gp,
# MAGIC         ROUND(gp/sales, 4) AS gp_margin,
# MAGIC         ROUND(SUM(tot_sales)/COUNT(DISTINCT WEEKOFYEAR(business_day)), 2) AS rate_of_sales
# MAGIC     FROM sandbox.pj_water_gp_margin_rca2
# MAGIC     -- WHERE promo_flag = "Not on promo"
# MAGIC     WHERE category_period_type_lfl != 'Intermediate'
# MAGIC     GROUP BY 1, 2, 3, 4, 5, 6
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     material_id,
# MAGIC     material_name,
# MAGIC     recommendation,
# MAGIC     delist_status,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN sales END), 0) AS pre_sales,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN sales END), 0) AS post_sales,
# MAGIC     COALESCE(ROUND((post_sales - pre_sales) / pre_sales, 4), 0) AS sales_growth,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN gp END), 0) AS pre_gp,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN gp END), 0) AS post_gp,
# MAGIC     COALESCE(ROUND((post_gp - pre_gp) / pre_gp, 4), 0) AS gp_growth,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN gp_margin END), 0) AS pre_gp_margin,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN gp_margin END), 0) AS post_gp_margin,
# MAGIC     COALESCE((post_gp_margin - pre_gp_margin), 0) AS gp_margin_delta,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Pre-delist" THEN rate_of_sales END), 0) AS pre_rate_of_sales,
# MAGIC     COALESCE(MAX(CASE WHEN category_period_type_lfl = "Post delist" THEN rate_of_sales END), 0) AS post_rate_of_sales,
# MAGIC     COALESCE(ROUND((post_rate_of_sales - pre_rate_of_sales) / pre_rate_of_sales, 4), 0) AS rate_of_sales_growth
# MAGIC FROM cte
# MAGIC GROUP BY 1, 2, 3, 4, 5
# MAGIC ORDER BY pre_gp_margin DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Month-Over-Month

# COMMAND ----------

query = """
WITH cte1 AS (
    SELECT
        region_name,
        material_id,
        material_name,
        recommendation,
        CASE WHEN delisted_date = "NA" THEN "Not Delisted" ELSE "Delisted" END AS delist_status,
        category_period_type_lfl,
        INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
        ROUND(SUM(tot_sales), 2) AS sales,
        ROUND(SUM(tot_gp), 2) AS gp,
--         ROUND(gp/sales, 4) AS gp_margin,
        (gp_wth_chargeback/100) AS gp_wth_chargeback,
        ROUND(SUM(tot_sales)/(COUNT(DISTINCT business_day)/7), 2) AS rate_of_sales
    FROM sandbox.pj_water_gp_margin_rca2
--     WHERE promo_flag = "Not on promo"
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 10
    ORDER BY 1, 2, 7
),

cte2 AS (
    SELECT
        region_name,
        material_id,
        material_name,
        recommendation,
        delist_status,
        category_period_type_lfl,
        year_month,

        sales,
        COALESCE(LAG(sales) OVER (PARTITION BY region_name, material_id ORDER BY year_month), 0) AS prev_sales,
        COALESCE((sales - LAG(sales) OVER (PARTITION BY region_name, material_id ORDER BY year_month)) / LAG(sales) OVER (PARTITION BY region_name, material_id ORDER BY year_month), 0) AS sales_growth,

        gp,
        COALESCE(LAG(gp) OVER (PARTITION BY region_name, material_id ORDER BY year_month), 0) AS prev_gp,
        COALESCE((gp - LAG(gp) OVER (PARTITION BY region_name, material_id ORDER BY year_month)) / LAG(gp) OVER (PARTITION BY region_name, material_id ORDER BY year_month), 0) AS gp_growth,

        gp_wth_chargeback,
        COALESCE(LAG(gp_wth_chargeback) OVER (PARTITION BY region_name, material_id ORDER BY year_month), 0) AS prev_gp_wth_chargeback,
        COALESCE((gp_wth_chargeback - LAG(gp_wth_chargeback) OVER (PARTITION BY region_name, material_id ORDER BY year_month)), 0) AS gp_wth_chargeback_delta,

        rate_of_sales,
        COALESCE(LAG(rate_of_sales) OVER (PARTITION BY region_name, material_id ORDER BY year_month), 0) AS prev_rate_of_sales,
        COALESCE((rate_of_sales - LAG(rate_of_sales) OVER (PARTITION BY region_name, material_id ORDER BY year_month)) / LAG(rate_of_sales) OVER (PARTITION BY region_name, material_id ORDER BY year_month), 0) AS rate_of_sales_growth

    FROM cte1
)

SELECT
    region_name,
    material_id,
    material_name,
    recommendation,
    delist_status,
    category_period_type_lfl,
    year_month,
    sales,
    sales_growth,
    gp,
    gp_growth,
    gp_wth_chargeback,
    gp_wth_chargeback_delta,
    rate_of_sales,
    rate_of_sales_growth
FROM cte2
ORDER BY region_name, material_id, year_month
"""

df = spark.sql(query).toPandas()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Interactions 

# COMMAND ----------

df['year_month'] = df['year_month'].astype('str')

# COMMAND ----------

def get_top_N_list(dataframe, start, end):
    listname = pd.DataFrame(dataframe.groupby(
        'material_id')['sales'].sum()).reset_index().sort_values(
            by='sales', ascending=False)['material_id'][start:end].tolist()
    return listname

# COMMAND ----------

for region in df['region_name'].unique():
    temp = df[df['region_name'] == region]
    tbl = temp[temp['material_id'].isin(get_top_N_list(temp, 0, 16))]

    fig = px.line(tbl, x = "year_month", y = "sales", color = 'material_id', width = 700, height = 600, title = region)
    fig.update_layout(
        xaxis=dict(
            categoryorder='category ascending'
        )
    )
    fig.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


