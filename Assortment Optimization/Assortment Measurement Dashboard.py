# Databricks notebook source
# MAGIC %md
# MAGIC #Import Statements

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #User Inputs

# COMMAND ----------

cy_start_date = '2023-12-01'
cy_end_date = str(date.today() - timedelta(days = 1))

# rfm_month_year = 202405

categories = ['WATER']
categories_recommendation_dates = ['2024-02-12']
catg_analysis_period_start_dates = ['2023-01-01']

material_groups = ['PASTA', 'INSTANT NOODLE', 'CUP NOODLE', 'COCONUT OIL']
material_groups_recommendation_date = ['2024-04-26', '2024-04-26', '2024-04-26', '2024-05-24']
mg_analysis_period_start_dates = ['2023-03-01', '2023-03-01', '2023-03-01', '2023-05-01']

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales & Volume

# COMMAND ----------

py_start_date = (datetime.strptime(cy_start_date, "%Y-%m-%d") - timedelta(days=364)).strftime("%Y-%m-%d")
py_end_date = str(date.today() - timedelta(days=365))
# 365 days because of leap year in CY. Otherwise, 364 days

categories_sorted = sorted(categories)
material_groups_sorted = sorted(material_groups)

categories_sql = ', '.join([f"'{item}'" for item in categories])
material_groups_sql = ', '.join([f"'{item}'" for item in material_groups])

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
    ROUND(SUM(amount), 2) AS sales,
    ROUND(SUM(quantity), 2) AS volume
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
WHERE
    (business_day BETWEEN '{py_start_date}' AND '{py_end_date}'
    OR business_day BETWEEN '{cy_start_date}' AND '{cy_end_date}')

    AND (category_name IN ({categories_sql})
    OR material_group_name IN ({material_groups_sql}))
    
    AND tayeb_flag = 0
    AND transaction_type IN ('SALE', 'SELL_MEDIA')
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

# MAGIC %md
# MAGIC ##Mapping DataFrame

# COMMAND ----------

query = f"""
SELECT
    department_name,
    category_name,
    material_group_name,
    brand,
    material_id,
    material_name
FROM gold.material_master
WHERE
    category_name IN ({categories_sql})
    OR material_group_name IN ({material_groups_sql})
GROUP BY 1, 2, 3, 4, 5, 6
"""

mapping_df = spark.sql(query).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reco Dates & Delisted Dates

# COMMAND ----------

temp_catg = pd.DataFrame({
    'category_name': categories,
    'recommendation_date': categories_recommendation_dates,
    'analysis_period_start_dates': catg_analysis_period_start_dates
})

temp_matg = pd.DataFrame({
    'material_group_name': material_groups,
    'recommendation_date': material_groups_recommendation_date,
    'analysis_period_start_dates': mg_analysis_period_start_dates
})

df = pd.merge(df, temp_catg, on='category_name', how='left')
df = pd.merge(df, temp_matg, on='material_group_name', how='left', suffixes=('', '2'))

df['recommendation_date'] = df['recommendation_date'].fillna(df['recommendation_date2'])
df['analysis_period_start_dates'] = df['analysis_period_start_dates'].fillna(df['analysis_period_start_dates2'])
df = df.drop(columns = ['recommendation_date2', 'analysis_period_start_dates2'])

# COMMAND ----------

all_mg_catg = categories_sorted + material_groups_sorted
delisted_uae = pd.DataFrame()

for mg_catg in all_mg_catg:
    mg_catg = mg_catg.lower()
    mg_catg = mg_catg.replace(' ', '_')

    delisted_auh = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_auh.csv")
    delisted_aln = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_aln.csv")
    delisted_dxb = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_dxb.csv")
    delisted_shj = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_shj.csv")

    delisted_auh['region_name'] = 'ABU DHABI'
    delisted_aln['region_name'] = 'AL AIN'
    delisted_dxb['region_name'] = 'DUBAI'
    delisted_shj['region_name'] = 'SHARJAH'

    delisted_uae = pd.concat([delisted_uae, delisted_auh, delisted_aln, delisted_dxb, delisted_shj], ignore_index=True)

delisted_uae = delisted_uae[['region_name', 'material_id', 'delisted_date', 'confirmed_by']]
delisted_uae = delisted_uae.dropna(subset=['delisted_date']).reset_index(drop = True)

delisted_uae['delisted_date'] = pd.to_datetime(delisted_uae['delisted_date'], format='%d/%m/%Y')
delisted_uae['delisted_date'] = delisted_uae['delisted_date'].dt.strftime('%Y-%m-%d')

df = pd.merge(df, delisted_uae, on=['region_name', 'material_id'], how='left')
df['period_type'] = np.where(df.business_day >= df.delisted_date, "Post delist", "Pre-delist")

df['delisted_date'] = df['delisted_date'].fillna("NA")
df['confirmed_by'] = df['confirmed_by'].fillna("NA")

# COMMAND ----------

delisted_mg_catg_df = pd.merge(delisted_uae, mapping_df, on='material_id', how='left')

delisted_mg_catg_df['material_group_name'] = np.where(delisted_mg_catg_df.category_name == 'WATER', 'OVERALL', delisted_mg_catg_df.material_group_name)

delisted_mg_catg_df = delisted_mg_catg_df[['region_name', 'category_name', 'material_group_name', 'delisted_date']].drop_duplicates().reset_index(drop = True)

delisted_mg_catg_df.rename(columns = {'delisted_date':'category_delisted_date'}, inplace=True)

delisted_mg_catg_df = delisted_mg_catg_df.groupby(['region_name', 'category_name', 'material_group_name'])['category_delisted_date'].max().reset_index()

# COMMAND ----------

temp = delisted_mg_catg_df[delisted_mg_catg_df['category_name'].isin(categories)][['region_name', 'category_name', 'category_delisted_date']]
df = pd.merge(df, temp, on=['region_name', 'category_name'], how='left')

temp = delisted_mg_catg_df[delisted_mg_catg_df['material_group_name'].isin(material_groups)][['region_name', 'material_group_name', 'category_delisted_date']]
df = pd.merge(df, temp, on=['region_name', 'material_group_name'], how='left', suffixes=('','2'))

df['category_delisted_date'] = df['category_delisted_date'].fillna(df['category_delisted_date2'])
df = df.drop(columns = ['category_delisted_date2'])

df['category_period_type'] = np.where(df.business_day >= df.category_delisted_date, "Post delist", "Pre-delist")

# COMMAND ----------

df['business_day'] = pd.to_datetime(df['business_day'])
df['category_delisted_date'] = pd.to_datetime(df['category_delisted_date'])

category_delisted_date_lfl = df.category_delisted_date - pd.DateOffset(days=364)
df['category_period_type_lfl'] = np.where(
    (df.business_day >= df.category_delisted_date) | ((df.business_day >= category_delisted_date_lfl) & (df.business_day <= py_end_date)),
    'Post delist',
    'Pre-delist'
)

df['business_day'] = df['business_day'].dt.strftime('%Y-%m-%d')
df['category_delisted_date'] = df['category_delisted_date'].dt.strftime('%Y-%m-%d')

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##GP

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
"""

gp = spark.sql(query).toPandas()
gp.rename(columns = {'region':'region_name'}, inplace=True)
gp['region_name'] = gp['region_name'].replace(['AUH', 'ALN', 'DXB', 'SHJ'], ['ABU DHABI', 'AL AIN', 'DUBAI', 'SHARJAH'])

# COMMAND ----------

df = pd.merge(df, gp, on=['region_name', 'year_month', 'material_id'], how='left')

df['gp_wth_chargeback'] = df['gp_wth_chargeback'].fillna(0)
df['gross_profit'] = df['gp_wth_chargeback'] * df['sales']/100

# COMMAND ----------

# MAGIC %md
# MAGIC ##New SKUs Tagging

# COMMAND ----------

def find_new_sku_flag(row):
    region = row['region_name']
    material_id = row['material_id']
    category_period_type = row['category_period_type']
    
    if category_period_type == 'Post delist' and material_id in post_period_dict[region] - pre_period_dict[region]:
        return 1
    else:
        return 0

# COMMAND ----------

new_skus = df[['region_name', 'category_period_type', 'material_id']].drop_duplicates().reset_index(drop=True)

pre_period_dict = {}
post_period_dict = {}

for region in new_skus['region_name'].unique():
    pre_period_dict[region] = set(df[(df['region_name'] == region) & (df['category_period_type'] == 'Pre-delist')]['material_id'].unique())
    post_period_dict[region] = set(df[(df['region_name'] == region) & (df['category_period_type'] == 'Post delist')]['material_id'].unique())

new_skus['new_sku_flag'] = new_skus.apply(lambda row: find_new_sku_flag(row), axis=1)
new_skus = new_skus[['region_name', 'material_id', 'new_sku_flag']].drop_duplicates().reset_index(drop =True)
df = pd.merge(df, new_skus, on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Recommendations Table

# COMMAND ----------

reco_buckets_uae = pd.DataFrame()

for mg_catg in all_mg_catg:
    mg_catg = mg_catg.lower()
    mg_catg = mg_catg.replace(' ', '_')

    reco_buckets_auh = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_auh.csv")
    reco_buckets_aln = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_aln.csv")
    reco_buckets_dxb = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_dxb.csv")
    reco_buckets_shj = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_shj.csv")

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

reco_buckets_uae = pd.merge(reco_buckets_uae, delisted_uae, on = ['region_name', 'material_id'], how = 'outer')

reco_buckets_uae[['recommendation', 'delisted_date', 'confirmed_by']] = reco_buckets_uae[['recommendation', 'delisted_date', 'confirmed_by']].fillna("NA")

reco_buckets_uae = pd.merge(reco_buckets_uae, mapping_df, on='material_id', how='left')

# COMMAND ----------

spark_df = spark.createDataFrame(reco_buckets_uae)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("sandbox.pj_assortment_dashboard_reco")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.pj_assortment_dashboard_reco

# COMMAND ----------

# MAGIC %md
# MAGIC ##Main Table

# COMMAND ----------

df = pd.merge(df, reco_buckets_uae[['region_name', 'material_id', 'recommendation']], on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("sandbox.pj_assortment_dashboard_region_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.pj_assortment_dashboard_region_view LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calendar Table

# COMMAND ----------

query = f"""
SELECT business_day AS cy_date
FROM sandbox.pj_assortment_dashboard_region_view
WHERE business_day >= '{cy_start_date}'
GROUP BY 1
ORDER BY 1
"""

cal_df = spark.sql(query).toPandas()

cal_df['cy_date'] = pd.to_datetime(cal_df['cy_date']).dt.date
cal_df['py_date'] = cal_df.cy_date - pd.DateOffset(days=364)
cal_df['py_date'] = cal_df['py_date'].dt.date
cal_df = cal_df[cal_df['cy_date'] != pd.to_datetime('2024-02-29').date()].reset_index(drop = True)
cal_df['dayno'] = range(1, len(cal_df) + 1)

spark_df = spark.createDataFrame(cal_df)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dashboard.tbl_assortment_lfl_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Current & Previous Year Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_cy_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM sandbox.pj_assortment_dashboard_region_view AS t1
# MAGIC     JOIN dashboard.tbl_assortment_lfl_calendar AS t2 ON t1.business_day = t2.cy_date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_py_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM sandbox.pj_assortment_dashboard_region_view AS t1
# MAGIC     JOIN dashboard.tbl_assortment_lfl_calendar AS t2 ON t1.business_day = t2.py_date
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Customer Penetration Table

# COMMAND ----------

# query = f"""
# SELECT DISTINCT
#     business_day,
#     region_name,
#     t1.store_id,
#     store_name,
#     department_name,
#     category_name,
#     material_group_name,
#     t1.customer_id,
#     segment
# FROM gold.pos_transactions AS t1
# JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
# JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# WHERE
#     (business_day BETWEEN '{py_start_date}' AND '{py_end_date}'
#     OR business_day BETWEEN '{cy_start_date}' AND '{cy_end_date}')

#     AND (category_name IN ({categories_sql})
#     OR material_group_name IN ({material_groups_sql}))

#     AND transaction_type IN ('SALE', 'SELL_MEDIA')
#     AND key = 'rfm'
#     AND channel = 'pos'
#     AND t4.country = 'uae'
#     AND month_year = {rfm_month_year}
# """

# cust_df = spark.sql(query).toPandas()

# COMMAND ----------

# spark_df = spark.createDataFrame(cust_df)
# spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("sandbox.pj_assortment_dashboard_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transactions Table

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_cy AS (
#     WITH pj_assortment_dashboard_cy_trans AS (
#         SELECT
#             a.business_day AS trans_date,
#             cast(a.transaction_id AS STRING) AS transaction_id,
#             a.ean,
#             a.product_id AS material_id,
#             material_name,
#             material_group_name,
#             category_name,
#             department_name,
#             brand,
#             store_name,
#             region_name,
#             a.store_id AS store_id,
#             SUM(a.amount) AS amount,
#             SUM(a.quantity) AS quantity 
#         FROM gold.pos_transactions a
#         LEFT JOIN gold.material_master e ON a.product_id = e.material_id
#         LEFT JOIN gold.store_master f ON a.store_id = f.store_id
#         WHERE
#             a.business_day BETWEEN '2023-12-01' AND DATE_SUB(CURRENT_DATE(), 1)
#             AND f.tayeb_flag = 0
#             AND amount > 0
#             AND quantity > 0
#             AND a.transaction_type IN ('SALE','SELL_MEDIA')
#             AND a.product_id IS NOT NULL
#             AND category_name = "WATER"
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
#     ),

#     pj_assortment_dashboard_cy_ovr AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             'overall' AS material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_cy_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_assortment_dashboard_cy_dept AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_cy_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_assortment_dashboard_cy AS (
#         SELECT *
#         FROM pj_assortment_dashboard_cy_ovr

#         UNION

#         SELECT *
#         FROM pj_assortment_dashboard_cy_dept
#     )

#     SELECT
#         a.*,
#         b.cy_date,
#         b.dayno AS r_day
#     FROM pj_assortment_dashboard_cy a
#     JOIN dashboard.tbl_water_assortment_lfl_calendar b ON a.trans_date = b.cy_date
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_py AS (
#     WITH pj_assortment_dashboard_py_trans AS (
#         SELECT
#             a.business_day AS trans_date,
#             cast(a.transaction_id AS STRING) AS transaction_id,
#             a.ean,
#             a.product_id AS material_id,
#             material_name,
#             material_group_name,
#             category_name,
#             department_name,
#             brand,
#             store_name,
#             region_name,
#             a.store_id AS store_id,
#             SUM(a.amount) AS amount,
#             SUM(a.quantity) AS quantity 
#         FROM gold.pos_transactions a
#         LEFT JOIN gold.material_master e ON a.product_id = e.material_id
#         LEFT JOIN gold.store_master f ON a.store_id = f.store_id
#         WHERE
#             a.business_day BETWEEN '2022-12-01' AND DATE_SUB(CURRENT_DATE(), 365)
#             AND f.tayeb_flag = 0
#             AND amount > 0
#             AND quantity > 0
#             AND a.transaction_type IN ('SALE','SELL_MEDIA')
#             AND a.product_id IS NOT NULL
#             AND category_name = "WATER"
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
#     ),

#     pj_assortment_dashboard_py_ovr AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             'overall' AS material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_py_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),


#     pj_assortment_dashboard_py_dept AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_py_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_assortment_dashboard_py AS (
#         SELECT *
#         FROM pj_assortment_dashboard_py_ovr

#         UNION

#         SELECT *
#         FROM pj_assortment_dashboard_py_dept
#     )

#     SELECT
#         a.*,
#         b.py_date,
#         b.dayno AS r_day
#     FROM pj_assortment_dashboard_py a
#     JOIN dashboard.tbl_water_assortment_lfl_calendar b ON a.trans_date = b.py_date
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_lfl_data AS (
#     SELECT
#         NVL(a.r_day, b.r_day) AS r_day,
#         a.trans_date AS cy_date,
#         b.trans_date AS py_date,
#         NVL(a.department_name, b.department_name) AS department_name,
#         NVL(a.category_name, b.category_name) AS category_name,
#         NVL(a.material_group_name, b.material_group_name) AS material_group_name,
#         NVL(a.store_id, b.store_id) AS store_id,
#         NVL(a.store_name, b.store_name) AS store_name,
#         NVL(a.region_name, b.region_name) AS region_name,
#         NVL(a.amount, 0) AS cy_amount,
#         NVL(a.quantity, 0) AS cy_quantity,
#         NVL(a.trans, 0) AS cy_trans,
#         NVL(b.amount, 0) AS py_amount,
#         NVL(b.quantity, 0) AS py_quantity,
#         NVL(b.trans, 0) AS py_trans
#     FROM sandbox.pj_assortment_dashboard_cy AS a
#     FULL JOIN sandbox.pj_assortment_dashboard_py AS b
#         ON a.r_day = b.r_day
#         AND a.store_id = b.store_id
#         AND a.department_name = b.department_name
#         AND a.category_name = b.category_name
#         AND a.material_group_name = b.material_group_name
#         AND a.region_name = b.region_name
# )

# COMMAND ----------

# MAGIC %md
# MAGIC #EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ##KPI Cards - Top

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delist Recommended SKUs
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM sandbox.pj_assortment_dashboard_reco
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rationalized SKUs
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE delisted_date != "NA"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rationalized SKUs From Recommendation
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE delisted_date != "NA"
# MAGIC AND category_name = "WATER"
# MAGIC AND recommendation = "Delist"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sales of Delisted SKUs
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC     SUM(sales) AS total_sales,
# MAGIC     ROUND(delist_sales/total_sales,4) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE category_period_type = "Pre-delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 Sales of Delisted SKUs in Pre-delist Period
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-07"
# MAGIC     AND region_name = "ABU DHABI"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-08"
# MAGIC     AND region_name = "AL AIN"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-13"
# MAGIC     AND region_name IN ("DUBAI", "SHARJAH")
# MAGIC     AND category_name = "WATER"
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(delist_sales) AS tot_delist_sales,
# MAGIC     SUM(total_sales) AS tot_sales,
# MAGIC     ROUND(tot_delist_sales/tot_sales, 4) AS perc
# MAGIC FROM cte

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GP of Delisted SKUs
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC     SUM(gross_profit) AS total_gp,
# MAGIC     ROUND(delist_gp/total_gp,4) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE category_period_type = "Pre-delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 GP of Delisted SKUs in Pre-delist Period
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC         SUM(gross_profit) AS total_gp
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-07"
# MAGIC     AND region_name = "ABU DHABI"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC         SUM(gross_profit) AS total_gp
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-08"
# MAGIC     AND region_name = "AL AIN"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC         SUM(gross_profit) AS total_gp
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-13"
# MAGIC     AND region_name IN ("DUBAI", "SHARJAH")
# MAGIC     AND category_name = "WATER"
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(delist_gp) AS tot_delist_gp,
# MAGIC     SUM(total_gp) AS tot_gp,
# MAGIC     ROUND(tot_delist_gp/tot_gp, 4) AS perc
# MAGIC FROM cte

# COMMAND ----------

# MAGIC %md
# MAGIC ##Region-wise Delist Recommended vs Rationalized

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise SKU Count of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     COUNT(DISTINCT material_id) AS recommended,
# MAGIC     COUNT(DISTINCT CASE WHEN delisted_date != "NA" THEN material_id END) AS rationalized,
# MAGIC     ROUND(rationalized/recommended,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND material_group_name = "COCONUT OIL"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Sales of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(sales) AS sales_reco,
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS sales_reco_rationalized,
# MAGIC     ROUND(sales_reco_rationalized/sales_reco,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Volume of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(volume) AS volume_reco,
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN volume END) AS volume_reco_rationalized,
# MAGIC     ROUND(volume_reco_rationalized/volume_reco,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC -- FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise GP of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(gross_profit) AS gp_reco,
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS gp_reco_rationalized,
# MAGIC     ROUND(gp_reco_rationalized/gp_reco,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC -- FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##Region-wise Pre-delist vs LFL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Pre-delist vs LFL Sales
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN sales END) AS pre_delist_sales,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN sales END) AS lfl_sales,
# MAGIC     ROUND((pre_delist_sales - lfl_sales)/lfl_sales,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Post delist vs LFL Sales
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Post delist" THEN sales END) AS post_delist_sales,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Post delist" THEN sales END) AS lfl_sales,
# MAGIC     ROUND((post_delist_sales - lfl_sales)/lfl_sales,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Pre-delist vs LFL Volume
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN volume END) AS pre_delist_volume,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN volume END) AS lfl_volume,
# MAGIC     ROUND((pre_delist_volume - lfl_volume)/lfl_volume,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Post delist vs LFL Volume
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Post delist" THEN volume END) AS post_delist_volume,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Post delist" THEN volume END) AS lfl_volume,
# MAGIC     ROUND((post_delist_volume - lfl_volume)/lfl_volume,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Pre-delist vs LFL GP
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN gross_profit END) AS pre_delist_gp,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN gross_profit END) AS lfl_gp,
# MAGIC     ROUND((pre_delist_gp - lfl_gp)/lfl_gp,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Post delist vs LFL GP
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Post delist" THEN gross_profit END) AS post_delist_gp,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Post delist" THEN gross_profit END) AS lfl_gp,
# MAGIC     ROUND((post_delist_gp - lfl_gp)/lfl_gp,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##KPI Cards - Middle

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(gross_profit)
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE new_sku_flag = 1
# MAGIC AND region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         category_period_type,
# MAGIC         MONTH(business_day) AS month,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE region_name = "ABU DHABI"
# MAGIC     AND category_name = "WATER"
# MAGIC     GROUP BY category_period_type, month
# MAGIC     ORDER BY 1, 2
# MAGIC )
# MAGIC
# MAGIC SELECT category_period_type, AVG(total_sales)
# MAGIC FROM cte
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trend Chart

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(volume)) AS periodly_volume
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(volume)) AS periodly_volume
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     WEEKOFYEAR(business_day) AS week,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales,
# MAGIC     ROUND((periodly_gp/periodly_sales)*100,2) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales,
# MAGIC     ROUND((periodly_gp/periodly_sales)*100,2) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales,
# MAGIC     ROUND((periodly_gp/periodly_sales)*100,2) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top 10 Brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 Brands by Sales - Pre-delist vs Post Delist
# MAGIC SELECT
# MAGIC     brand,
# MAGIC     SUM(CASE WHEN category_period_type = "Post delist" THEN sales END) AS post_delist_sales,
# MAGIC     SUM(CASE WHEN category_period_type = "Pre-delist" THEN sales END) AS pre_delist_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY brand
# MAGIC ORDER BY pre_delist_sales DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 Brands by Volume - Pre-delist vs Post Delist
# MAGIC SELECT
# MAGIC     brand,
# MAGIC     SUM(CASE WHEN category_period_type = "Post delist" THEN volume END) AS post_delist_volume,
# MAGIC     SUM(CASE WHEN category_period_type = "Pre-delist" THEN volume END) AS pre_delist_volume
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY brand
# MAGIC ORDER BY pre_delist_volume DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 Brands by GP - Pre-delist vs Post Delist
# MAGIC SELECT
# MAGIC     brand,
# MAGIC     SUM(CASE WHEN category_period_type = "Post delist" THEN gross_profit END) AS post_delist_gp,
# MAGIC     SUM(CASE WHEN category_period_type = "Pre-delist" THEN gross_profit END) AS pre_delist_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY brand
# MAGIC ORDER BY pre_delist_gp DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top 10 SKUs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_id,
# MAGIC     material_name,
# MAGIC     SUM(sales) AS total_sales,
# MAGIC     SUM(gross_profit) AS total_gp,
# MAGIC     (total_gp/total_sales) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY material_id, material_name
# MAGIC ORDER BY gp_margin DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delist View

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     material_id,
# MAGIC     SUM(sales) AS tot_sales,
# MAGIC     SUM(volume) AS tot_volume,
# MAGIC     SUM(gross_profit) AS tot_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE material_id IN (5539, 91929)
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reco View

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(sales)
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE business_day BETWEEN "2023-12-01" AND "2024-01-30"
# MAGIC AND material_id = 123693

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delist Reco Adoption

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     material_id,
# MAGIC     material_name,
# MAGIC     brand,
# MAGIC     CASE WHEN delisted_date = "NA" THEN "NA" ELSE "Delisted" END AS delist_status
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY ALL
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     COUNT(DISTINCT material_id) AS delisted
# MAGIC FROM sandbox.pj_assortment_dashboard_reco
# MAGIC WHERE material_group_name = "COCONUT OIL"
# MAGIC AND recommendation = "Delist"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##Daily Rate of Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     category_period_type,
# MAGIC     ROUND(SUM(sales) / COUNT(DISTINCT business_day), 2) AS daily_ros
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND((297441.74 - 221098.56) / 221098.56, 4)

# COMMAND ----------

# MAGIC %md
# MAGIC #Misc

# COMMAND ----------

# %sql
# with qa_Data 
# as 
# (select 
#         DISTINCT transaction_id, 
#         business_day, 
#         transaction_begin_datetime, 
#         store_id, 
#         billed_amount,
#         gift_sale, 
#         mobile, 
#         loyalty_account_id,
#         t1.redemption_loyalty_id,
#         t1.returned_loyalty_id,
#         loyalty_points, 
#         loyalty_points_returned,
#         redeemed_points,
#         redeemed_amount, 
#         --voucher_sale, 
#         transaction_type_id
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration))
# select
# sum(billed_amount) 
# from
# qa_Data

# COMMAND ----------

# %sql
# select 
#         sum(t1.amount)
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)

# COMMAND ----------

# %sql
# with qa_bill_amt_data
# as (
# select 
#         DISTINCT transaction_id, 
#         business_day,
#         billed_amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
# ),
# qa_billamt_final as (
#   select
#   business_day,
#   sum(billed_amount) as bill_amount
#   from   
#   qa_bill_amt_data
#   group by
#   1
# ),
# qa_amt_final as (
#   select 
#         t1.business_day,
#         sum(t1.amount) as amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
#          group by 1
# )
# select
# a.business_day,
# a.bill_amount,
# b.amount
# from   
# qa_billamt_final a 
# join
# qa_amt_final b 
# on a.business_day = b.business_day



# COMMAND ----------

# %sql
# with qa_bill_amt_data
# as (
# select 
#         DISTINCT transaction_id, 
#         t1.store_id,
#         business_day,
#         billed_amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day = "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
# ),
# qa_billamt_final as (
#   select
#   business_day,
#   store_id,
#   sum(billed_amount) as bill_amount
#   from   
#   qa_bill_amt_data
#   group by
#   1,2
# ),
# qa_amt_final as (
#   select 
#         t1.business_day,
#         t1.store_id.
#         sum(t1.amount) as amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day = "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
#          group by 1,2
# )
# select
# a.store_id,
# a.bill_amount,
# b.amount
# from   
# qa_billamt_final a 
# join
# qa_amt_final b 
# on a.business_day = b.business_day


