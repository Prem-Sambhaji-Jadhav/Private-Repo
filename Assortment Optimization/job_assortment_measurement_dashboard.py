# Databricks notebook source
# MAGIC %md
# MAGIC #Import Statements

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC #User Inputs

# COMMAND ----------

cy_start_date = '2023-12-01'
cy_end_date = str(date.today() - timedelta(days = 1))[:10]

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
py_end_date = str(date.today() - timedelta(days=365))[:10]

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
    ROUND(SUM(amount),2) AS sales,
    ROUND(SUM(quantity),2) AS volume
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
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dashboard.tbl_assortment_reco")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Main Table

# COMMAND ----------

df = pd.merge(df, reco_buckets_uae[['region_name', 'material_id', 'recommendation']], on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

spark.createDataFrame(df).createOrReplaceTempView('main')
spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dashboard.tbl_assortment_overall")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calendar Table

# COMMAND ----------

query = f"""
SELECT business_day AS cy_date
FROM dashboard.tbl_assortment_overall
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
# MAGIC CREATE OR REPLACE TABLE dashboard.tbl_assortment_dashboard_cy_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM dashboard.tbl_assortment_overall AS t1
# MAGIC     JOIN dashboard.tbl_assortment_lfl_calendar AS t2 ON t1.business_day = t2.cy_date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dashboard.tbl_assortment_dashboard_py_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM dashboard.tbl_assortment_overall AS t1
# MAGIC     JOIN dashboard.tbl_assortment_lfl_calendar AS t2 ON t1.business_day = t2.py_date
# MAGIC )
