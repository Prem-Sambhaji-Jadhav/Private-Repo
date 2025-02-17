# Databricks notebook source
# MAGIC %md
# MAGIC #Import Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #User Inputs

# COMMAND ----------

categories = ['WATER']
material_groups = ['PASTA', 'INSTANT NOODLE', 'CUP NOODLE', 'COCONUT OIL', 'OLIVE OIL', 'SUNFLOWER OIL', 'VEGETABLE OIL', 'FRUIT JUICES']

skip_records = 0

cy_start_date = '2023-12-01'
cy_end_date = str(date.today() - timedelta(days = 1))

# rfm_month_year = 202405

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales & Volume

# COMMAND ----------

py_start_date = (datetime.strptime(cy_start_date, "%Y-%m-%d") - timedelta(days = 364)).strftime("%Y-%m-%d")
py_end_date = str(date.today() - timedelta(days = 365))
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
FROM gold.transaction.uae_pos_transactions AS t1
JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
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
FROM gold.material.material_master
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

delisted_materials_df = pd.read_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_delisted_materials.csv')

date_format_cols = ['recommendation_date', 'delisted_date', 'analysis_period_start_dates', 'sub_analysis_period_start_dates']
for col in date_format_cols:
    delisted_materials_df[col] = pd.to_datetime(delisted_materials_df[col], format = '%m/%d/%Y')
    delisted_materials_df[col] = delisted_materials_df[col].dt.strftime('%Y-%m-%d')

delisted_materials_df[['recommendation_date', 'delisted_date', 'confirmed_by']] = delisted_materials_df[['recommendation_date', 'delisted_date', 'confirmed_by']].fillna('NA')

reco_analysis_dates = delisted_materials_df[['category_name', 'material_group_name', 'recommendation_date', 'analysis_period_start_dates', 'sub_analysis_period_start_dates']].drop_duplicates().reset_index(drop = True)

temp_catg = reco_analysis_dates[reco_analysis_dates['category_name'].isin(categories_sorted)]
temp_catg = temp_catg.drop(columns = 'material_group_name').drop_duplicates().reset_index(drop = True)
temp_catg = temp_catg[temp_catg['recommendation_date'] != 'NA'].reset_index(drop = True)

temp_matg = reco_analysis_dates[reco_analysis_dates['material_group_name'].isin(material_groups_sorted)]
temp_matg = temp_matg.drop(columns = 'category_name').drop_duplicates().reset_index(drop = True)
temp_matg = temp_matg[temp_matg['recommendation_date'] != 'NA'].reset_index(drop = True)

df = pd.merge(df, temp_catg, on = 'category_name', how = 'left')
df = pd.merge(df, temp_matg, on = 'material_group_name', how = 'left', suffixes = ('', '2'))

df['recommendation_date'] = df['recommendation_date'].fillna(df['recommendation_date2'])
df['analysis_period_start_dates'] = df['analysis_period_start_dates'].fillna(df['analysis_period_start_dates2'])
df['sub_analysis_period_start_dates'] = df['sub_analysis_period_start_dates'].fillna(df['sub_analysis_period_start_dates2'])

df = df.drop(columns = ['recommendation_date2', 'analysis_period_start_dates2', 'sub_analysis_period_start_dates2'])

# COMMAND ----------

delisted_materials_df = delisted_materials_df[delisted_materials_df['delisted_date'] != 'NA'].reset_index(drop = True)

all_mg_catg = categories_sorted + material_groups_sorted
delisted_uae = pd.DataFrame()

for mg_catg in all_mg_catg:
    if mg_catg in material_groups_sorted:
        temp = delisted_materials_df[delisted_materials_df['material_group_name'] == mg_catg][['region_name', 'material_id', 'delisted_date', 'confirmed_by']]
    else:
        temp = delisted_materials_df[delisted_materials_df['category_name'] == mg_catg][['region_name', 'material_id', 'delisted_date', 'confirmed_by']]

    delisted_uae = pd.concat([delisted_uae, temp], ignore_index = True)

# delisted_uae = delisted_uae.dropna(subset = ['delisted_date']).reset_index(drop = True)

df = pd.merge(df, delisted_uae, on = ['region_name', 'material_id'], how = 'left')
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
    CASE WHEN region = 'AUH' THEN 'ABU DHABI'
        WHEN region = 'ALN' THEN 'AL AIN'
        WHEN region = 'DXB' THEN 'DUBAI'
        ELSE 'SHARJAH' END AS region_name,
    year_month,
    material_id,
    gp_wth_chargeback
FROM gold.business.gross_profit
WHERE
    country = 'AE'
    AND year_month BETWEEN {year_month_start} AND {year_month_end}
"""

gp = spark.sql(query).toPandas()

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

reco_buckets_main = spark.sql("SELECT * FROM dev.sandbox.pj_ao_dashboard_reco").toPandas()
reco_buckets_main = reco_buckets_main[['region_name', 'material_id', 'recommendation', 'material_group_name', 'category_name']]

reco_buckets_uae = pd.DataFrame()

for mg_catg in all_mg_catg:

    if mg_catg in material_groups_sorted:
        temp = reco_buckets_main[reco_buckets_main['material_group_name'] == mg_catg][['region_name', 'material_id', 'recommendation']]
    else:
        temp = reco_buckets_main[reco_buckets_main['category_name'] == mg_catg][['region_name', 'material_id', 'recommendation']]

    reco_buckets_uae = pd.concat([reco_buckets_uae, temp], ignore_index=True)

reco_buckets_uae['recommendation'] = reco_buckets_uae['recommendation'].replace('Maintain','Support_with_more_distribution')
reco_buckets_uae['recommendation'] = reco_buckets_uae['recommendation'].replace('Grow','Maintain_or_grow_by_promo')

# COMMAND ----------

reco_buckets_uae = pd.merge(reco_buckets_uae, delisted_uae, on = ['region_name', 'material_id'], how = 'outer')

reco_buckets_uae[['recommendation', 'delisted_date', 'confirmed_by']] = reco_buckets_uae[['recommendation', 'delisted_date', 'confirmed_by']].fillna("NA")

reco_buckets_uae = pd.merge(reco_buckets_uae, mapping_df, on='material_id', how='left')

# COMMAND ----------

spark_df = spark.createDataFrame(reco_buckets_uae)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.pj_ao_dashboard_reco")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.sandbox.pj_ao_dashboard_reco

# COMMAND ----------

# MAGIC %md
# MAGIC ##Main Table

# COMMAND ----------

df = pd.merge(df, reco_buckets_uae[['region_name', 'material_id', 'recommendation']], on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC ###EDA Temp (Start)

# COMMAND ----------

# var_mg = ['PASTA', 'INSTANT NOODLE', 'CUP NOODLE', 'COCONUT OIL']
# for i in var_mg:
#     temp = reco_buckets_uae[reco_buckets_uae['material_group_name'] == i]

#     temp_auh = temp[temp['region_name'] == 'ABU DHABI'].reset_index(drop = True)
#     temp_aln = temp[temp['region_name'] == 'AL AIN'].reset_index(drop = True)
#     temp_dxb = temp[temp['region_name'] == 'DUBAI'].reset_index(drop = True)
#     temp_shj = temp[temp['region_name'] == 'SHARJAH'].reset_index(drop = True)

#     delist_from_reco_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['recommendation'] == 'Delist']['material_id'].nunique() * 100)
#     delist_from_reco_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['recommendation'] == 'Delist']['material_id'].nunique() * 100)
#     delist_from_reco_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['recommendation'] == 'Delist']['material_id'].nunique() * 100)
#     delist_from_reco_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['recommendation'] == 'Delist']['material_id'].nunique() * 100)

#     reco_from_delist_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['delisted_date'] != 'NA']['material_id'].nunique() * 100)
#     reco_from_delist_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['delisted_date'] != 'NA']['material_id'].nunique() * 100)
#     reco_from_delist_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['delisted_date'] != 'NA']['material_id'].nunique() * 100)
#     reco_from_delist_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['delisted_date'] != 'NA']['material_id'].nunique() * 100)

#     print(f"----------{i}----------")
#     print(f"% Delisted From Reco\nAbu Dhabi: {delist_from_reco_perc_auh}%\nAl Ain: {delist_from_reco_perc_aln}%\nDubai: {delist_from_reco_perc_dxb}%\nSharjah: {delist_from_reco_perc_shj}%")
#     print(f"\nReco From Delisted\nAbu Dhabi: {reco_from_delist_perc_auh}%\nAl Ain: {reco_from_delist_perc_aln}%\nDubai: {reco_from_delist_perc_dxb}%\nSharjah: {reco_from_delist_perc_shj}%\n")

# COMMAND ----------

# temp = reco_buckets_uae[reco_buckets_uae['material_group_name'].isin(var_mg)]

# temp_auh = temp[temp['region_name'] == 'ABU DHABI'].reset_index(drop = True)
# temp_aln = temp[temp['region_name'] == 'AL AIN'].reset_index(drop = True)
# temp_dxb = temp[temp['region_name'] == 'DUBAI'].reset_index(drop = True)
# temp_shj = temp[temp['region_name'] == 'SHARJAH'].reset_index(drop = True)

# delist_from_reco_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['recommendation'] == 'Delist']['material_id'].nunique() * 100)
# delist_from_reco_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['recommendation'] == 'Delist']['material_id'].nunique() * 100)
# delist_from_reco_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['recommendation'] == 'Delist']['material_id'].nunique() * 100)
# delist_from_reco_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['recommendation'] == 'Delist']['material_id'].nunique() * 100)

# reco_from_delist_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['delisted_date'] != 'NA']['material_id'].nunique() * 100)
# reco_from_delist_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['delisted_date'] != 'NA']['material_id'].nunique() * 100)
# reco_from_delist_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['delisted_date'] != 'NA']['material_id'].nunique() * 100)
# reco_from_delist_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['delisted_date'] != 'NA']['material_id'].nunique() * 100)

# print("----------OVERALL----------")
# print(f"% Delisted From Reco\nAbu Dhabi: {delist_from_reco_perc_auh}%\nAl Ain: {delist_from_reco_perc_aln}%\nDubai: {delist_from_reco_perc_dxb}%\nSharjah: {delist_from_reco_perc_shj}%")
# print(f"\nReco From Delisted\nAbu Dhabi: {reco_from_delist_perc_auh}%\nAl Ain: {reco_from_delist_perc_aln}%\nDubai: {reco_from_delist_perc_dxb}%\nSharjah: {reco_from_delist_perc_shj}%\n")

# COMMAND ----------

# dates_dct = {'WATER': {'pre_start': '2023-10-01', 'pre_end': '2023-12-30', 'post_start': '2024-04-01',
#                        'post_end': '2024-06-06', 'pre_months': 3, 'post_months': 2.2},
#              'PASTA': {'pre_start': '2024-02-19', 'pre_end': '2024-05-17', 'post_start': '2024-06-08',
#                        'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 2},
#              'INSTANT NOODLE': {'pre_start': '2024-02-01', 'pre_end': '2024-04-28', 'post_start': '2024-06-01',
#                        'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 2.266},
#              'CUP NOODLE': {'pre_start': '2023-12-01', 'pre_end': '2024-02-27', 'post_start': '2024-06-01',
#                        'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 2.266},
#              'COCONUT OIL': {'pre_start': '2024-02-01', 'pre_end': '2024-04-28', 'post_start': '2024-06-15',
#                        'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 1.766}}

# for i in var_mg:
#     pre_start = dates_dct[i]['pre_start']
#     pre_end = dates_dct[i]['pre_end']
#     post_start = dates_dct[i]['post_start']
#     post_end = dates_dct[i]['post_end']
#     pre_months = dates_dct[i]['pre_months']
#     post_months = dates_dct[i]['post_months']

#     temp = df[df['material_group_name'] == i]

#     temp_pre_uae = temp[(temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
#     temp_pre_auh = temp[(temp['region_name'] == 'ABU DHABI') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
#     temp_pre_aln = temp[(temp['region_name'] == 'AL AIN') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
#     temp_pre_dxb = temp[(temp['region_name'] == 'DUBAI') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
#     temp_pre_shj = temp[(temp['region_name'] == 'SHARJAH') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)

#     temp_post_uae = temp[(temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
#     temp_post_auh = temp[(temp['region_name'] == 'ABU DHABI') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
#     temp_post_aln = temp[(temp['region_name'] == 'AL AIN') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
#     temp_post_dxb = temp[(temp['region_name'] == 'DUBAI') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
#     temp_post_shj = temp[(temp['region_name'] == 'SHARJAH') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)

#     temp_pre_uae_reco = temp_pre_uae[temp_pre_uae['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_pre_auh_reco = temp_pre_auh[temp_pre_auh['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_pre_aln_reco = temp_pre_aln[temp_pre_aln['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_pre_dxb_reco = temp_pre_dxb[temp_pre_dxb['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_pre_shj_reco = temp_pre_shj[temp_pre_shj['recommendation'] != 'Delist'].reset_index(drop = True)

#     temp_post_uae_reco = temp_post_uae[temp_post_uae['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_post_auh_reco = temp_post_auh[temp_post_auh['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_post_aln_reco = temp_post_aln[temp_post_aln['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_post_dxb_reco = temp_post_dxb[temp_post_dxb['recommendation'] != 'Delist'].reset_index(drop = True)
#     temp_post_shj_reco = temp_post_shj[temp_post_shj['recommendation'] != 'Delist'].reset_index(drop = True)

#     ########## Average Monthly Sales Growth

#     avg_monthly_sales_pre_uae = temp_pre_uae['sales'].sum() / pre_months
#     avg_monthly_sales_pre_auh = temp_pre_auh['sales'].sum() / pre_months
#     avg_monthly_sales_pre_aln = temp_pre_aln['sales'].sum() / pre_months
#     avg_monthly_sales_pre_dxb = temp_pre_dxb['sales'].sum() / pre_months
#     avg_monthly_sales_pre_shj = temp_pre_shj['sales'].sum() / pre_months

#     avg_monthly_sales_post_uae = temp_post_uae['sales'].sum() / post_months
#     avg_monthly_sales_post_auh = temp_post_auh['sales'].sum() / post_months
#     avg_monthly_sales_post_aln = temp_post_aln['sales'].sum() / post_months
#     avg_monthly_sales_post_dxb = temp_post_dxb['sales'].sum() / post_months
#     avg_monthly_sales_post_shj = temp_post_shj['sales'].sum() / post_months

#     avg_month_sales_growth_uae = round((avg_monthly_sales_post_uae - avg_monthly_sales_pre_uae) / avg_monthly_sales_pre_uae*100, 2)
#     avg_month_sales_growth_auh = round((avg_monthly_sales_post_auh - avg_monthly_sales_pre_auh) / avg_monthly_sales_pre_auh*100, 2)
#     avg_month_sales_growth_aln = round((avg_monthly_sales_post_aln - avg_monthly_sales_pre_aln) / avg_monthly_sales_pre_aln*100, 2)
#     avg_month_sales_growth_dxb = round((avg_monthly_sales_post_dxb - avg_monthly_sales_pre_dxb) / avg_monthly_sales_pre_dxb*100, 2)
#     avg_month_sales_growth_shj = round((avg_monthly_sales_post_shj - avg_monthly_sales_pre_shj) / avg_monthly_sales_pre_shj*100, 2)

#     ########## Average Monthly GP Growth

#     avg_monthly_gp_pre_uae = temp_pre_uae['gross_profit'].sum() / pre_months
#     avg_monthly_gp_pre_auh = temp_pre_auh['gross_profit'].sum() / pre_months
#     avg_monthly_gp_pre_aln = temp_pre_aln['gross_profit'].sum() / pre_months
#     avg_monthly_gp_pre_dxb = temp_pre_dxb['gross_profit'].sum() / pre_months
#     avg_monthly_gp_pre_shj = temp_pre_shj['gross_profit'].sum() / pre_months

#     avg_monthly_gp_post_uae = temp_post_uae['gross_profit'].sum() / post_months
#     avg_monthly_gp_post_auh = temp_post_auh['gross_profit'].sum() / post_months
#     avg_monthly_gp_post_aln = temp_post_aln['gross_profit'].sum() / post_months
#     avg_monthly_gp_post_dxb = temp_post_dxb['gross_profit'].sum() / post_months
#     avg_monthly_gp_post_shj = temp_post_shj['gross_profit'].sum() / post_months

#     avg_month_gp_growth_uae = round((avg_monthly_gp_post_uae - avg_monthly_gp_pre_uae) / avg_monthly_gp_pre_uae*100, 2)
#     avg_month_gp_growth_auh = round((avg_monthly_gp_post_auh - avg_monthly_gp_pre_auh) / avg_monthly_gp_pre_auh*100, 2)
#     avg_month_gp_growth_aln = round((avg_monthly_gp_post_aln - avg_monthly_gp_pre_aln) / avg_monthly_gp_pre_aln*100, 2)
#     avg_month_gp_growth_dxb = round((avg_monthly_gp_post_dxb - avg_monthly_gp_pre_dxb) / avg_monthly_gp_pre_dxb*100, 2)
#     avg_month_gp_growth_shj = round((avg_monthly_gp_post_shj - avg_monthly_gp_pre_shj) / avg_monthly_gp_pre_shj*100, 2)

#     ########## GP Margin Delta (Actual Delisted)

#     gp_margin_pre_actual_uae = temp_pre_uae['gross_profit'].sum() / temp_pre_uae['sales'].sum()
#     gp_margin_pre_actual_auh = temp_pre_auh['gross_profit'].sum() / temp_pre_auh['sales'].sum()
#     gp_margin_pre_actual_aln = temp_pre_aln['gross_profit'].sum() / temp_pre_aln['sales'].sum()
#     gp_margin_pre_actual_dxb = temp_pre_dxb['gross_profit'].sum() / temp_pre_dxb['sales'].sum()
#     gp_margin_pre_actual_shj = temp_pre_shj['gross_profit'].sum() / temp_pre_shj['sales'].sum()

#     gp_margin_post_actual_uae = temp_post_uae['gross_profit'].sum() / temp_post_uae['sales'].sum()
#     gp_margin_post_actual_auh = temp_post_auh['gross_profit'].sum() / temp_post_auh['sales'].sum()
#     gp_margin_post_actual_aln = temp_post_aln['gross_profit'].sum() / temp_post_aln['sales'].sum()
#     gp_margin_post_actual_dxb = temp_post_dxb['gross_profit'].sum() / temp_post_dxb['sales'].sum()
#     gp_margin_post_actual_shj = temp_post_shj['gross_profit'].sum() / temp_post_shj['sales'].sum()

#     gp_margin_delta_actual_uae = round((gp_margin_post_actual_uae - gp_margin_pre_actual_uae)*100, 2)
#     gp_margin_delta_actual_auh = round((gp_margin_post_actual_auh - gp_margin_pre_actual_auh)*100, 2)
#     gp_margin_delta_actual_aln = round((gp_margin_post_actual_aln - gp_margin_pre_actual_aln)*100, 2)
#     gp_margin_delta_actual_dxb = round((gp_margin_post_actual_dxb - gp_margin_pre_actual_dxb)*100, 2)
#     gp_margin_delta_actual_shj = round((gp_margin_post_actual_shj - gp_margin_pre_actual_shj)*100, 2)

#     ########## GP Margin Delta (Delist Reco)

#     gp_margin_pre_reco_uae = temp_pre_uae_reco['gross_profit'].sum() / temp_pre_uae_reco['sales'].sum()
#     gp_margin_pre_reco_auh = temp_pre_auh_reco['gross_profit'].sum() / temp_pre_auh_reco['sales'].sum()
#     gp_margin_pre_reco_aln = temp_pre_aln_reco['gross_profit'].sum() / temp_pre_aln_reco['sales'].sum()
#     gp_margin_pre_reco_dxb = temp_pre_dxb_reco['gross_profit'].sum() / temp_pre_dxb_reco['sales'].sum()
#     gp_margin_pre_reco_shj = temp_pre_shj_reco['gross_profit'].sum() / temp_pre_shj_reco['sales'].sum()

#     gp_margin_post_reco_uae = temp_post_uae_reco['gross_profit'].sum() / temp_post_uae_reco['sales'].sum()
#     gp_margin_post_reco_auh = temp_post_auh_reco['gross_profit'].sum() / temp_post_auh_reco['sales'].sum()
#     gp_margin_post_reco_aln = temp_post_aln_reco['gross_profit'].sum() / temp_post_aln_reco['sales'].sum()
#     gp_margin_post_reco_dxb = temp_post_dxb_reco['gross_profit'].sum() / temp_post_dxb_reco['sales'].sum()
#     gp_margin_post_reco_shj = temp_post_shj_reco['gross_profit'].sum() / temp_post_shj_reco['sales'].sum()

#     gp_margin_delta_reco_uae = round((gp_margin_post_reco_uae - gp_margin_pre_reco_uae)*100, 2)
#     gp_margin_delta_reco_auh = round((gp_margin_post_reco_auh - gp_margin_pre_reco_auh)*100, 2)
#     gp_margin_delta_reco_aln = round((gp_margin_post_reco_aln - gp_margin_pre_reco_aln)*100, 2)
#     gp_margin_delta_reco_dxb = round((gp_margin_post_reco_dxb - gp_margin_pre_reco_dxb)*100, 2)
#     gp_margin_delta_reco_shj = round((gp_margin_post_reco_shj - gp_margin_pre_reco_shj)*100, 2)

#     print(f"----------{i}----------")
#     print(f"Pre-delist Sales\nUAE: {temp_pre_uae['sales'].sum().round()}\nAbu Dhabi: {temp_pre_auh['sales'].sum().round()}\nAl Ain: {temp_pre_aln['sales'].sum().round()}\nDubai: {temp_pre_dxb['sales'].sum().round()}\nSharjah: {temp_pre_shj['sales'].sum().round()}")
#     print(f"\nPost Delist Sales\nUAE: {temp_post_uae['sales'].sum().round()}\nAbu Dhabi: {temp_post_auh['sales'].sum().round()}\nAl Ain: {temp_post_aln['sales'].sum().round()}\nDubai: {temp_post_dxb['sales'].sum().round()}\nSharjah: {temp_post_shj['sales'].sum().round()}")
#     print(f"\nAverage Monthly Sales Growth\nUAE: {avg_month_sales_growth_uae}%\nAbu Dhabi: {avg_month_sales_growth_auh}%\nAl Ain: {avg_month_sales_growth_aln}%\nDubai: {avg_month_sales_growth_dxb}%\nSharjah: {avg_month_sales_growth_shj}%")
#     print(f"\nAverage Monthly GP Growth\nUAE: {avg_month_gp_growth_uae}%\nAbu Dhabi: {avg_month_gp_growth_auh}%\nAl Ain: {avg_month_gp_growth_aln}%\nDubai: {avg_month_gp_growth_dxb}%\nSharjah: {avg_month_gp_growth_shj}%")
#     print(f"\nGP Margin Delta (Actual Delistings)\nUAE: {gp_margin_delta_actual_uae}%\nAbu Dhabi: {gp_margin_delta_actual_auh}%\nAl Ain: {gp_margin_delta_actual_aln}%\nDubai: {gp_margin_delta_actual_dxb}%\nSharjah: {gp_margin_delta_actual_shj}%\n")
#     print(f"\nGP Margin Delta (Delist Reco)\nUAE: {gp_margin_delta_reco_uae}%\nAbu Dhabi: {gp_margin_delta_reco_auh}%\nAl Ain: {gp_margin_delta_reco_aln}%\nDubai: {gp_margin_delta_reco_dxb}%\nSharjah: {gp_margin_delta_reco_shj}%\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ###RCA

# COMMAND ----------

# %sql
# WITH sales AS (
#     SELECT
#         region_name,
#         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
#         category_name,
#         material_group_name,
#         material_id,
#         SUM(amount) AS sales
#     FROM gold.transaction.uae_pos_transactions AS t1
#     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#     WHERE
#         business_day BETWEEN "2024-01-01" AND "2024-08-08"
#         AND category_name = "PASTA & NOODLE"
#         AND tayeb_flag = 0
#         AND transaction_type IN ("SALE", "SELL_MEDIA")
#         AND amount > 0
#         AND quantity > 0
#     GROUP BY 1, 2, 3, 4, 5
# ),

# gp AS (
#     SELECT
#         CASE WHEN region = 'AUH' THEN 'ABU DHABI'
#             WHEN region = 'ALN' THEN 'AL AIN'
#             WHEN region = 'DXB' THEN 'DUBAI'
#             ELSE 'SHARJAH' END AS region_name,
#         year_month,
#         material_id,
#         gp_wth_chargeback
#     FROM gold.business.gross_profit
#     WHERE year_month BETWEEN 202401 AND 202408
#     AND country = 'AE'
# ),

# combined AS (
#     SELECT
#         t1.region_name,
#         t1.year_month,
#         category_name,
#         material_group_name,
#         t1.material_id,
#         sales,
#         COALESCE(sales*gp_wth_chargeback/100, 0) AS gp
#     FROM sales AS t1
#     LEFT JOIN gp AS t2
#         ON t1.region_name = t2.region_name
#         AND t1.year_month = t2.year_month
#         AND t1.material_id = t2.material_id
# )

# SELECT
#     category_name,
#     material_group_name,
#     ROUND(SUM(gp) / SUM(sales) * 100, 2) AS gp_margin,
#     ROUND(SUM(sales) / SUM(SUM(sales)) OVER () * 100, 2) AS sales_contri,
#     ROUND(SUM(gp) / SUM(SUM(gp)) OVER () * 100, 2) AS gp_contri
# FROM combined
# GROUP BY 1, 2
# ORDER BY 4 DESC

# COMMAND ----------

# %sql
# SELECT
#     material_group_name,
#     ROUND(SUM(amount)) AS post_sales
# FROM gold.transaction.uae_pos_transactions AS t1
# JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# WHERE
#     business_day BETWEEN "2024-06-08" AND "2024-08-08"
#     AND material_group_name = "PASTA"
#     AND tayeb_flag = 0
#     AND transaction_type IN ("SALE", "SELL_MEDIA")
#     AND amount > 0
#     AND quantity > 0
# GROUP BY 1

# COMMAND ----------

# %sql
# SELECT
#     material_group_name,
#     ROUND(SUM(amount)) AS post_sales
# FROM gold.transaction.uae_pos_transactions AS t1
# JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# WHERE
#     business_day BETWEEN "2024-06-01" AND "2024-08-08"
#     AND material_group_name IN ("INSTANT NOODLE", "CUP NOODLE")
#     AND tayeb_flag = 0
#     AND transaction_type IN ("SALE", "SELL_MEDIA")
#     AND amount > 0
#     AND quantity > 0
# GROUP BY 1

# COMMAND ----------

# %sql
# SELECT
#     material_group_name,
#     ROUND(SUM(amount)) AS post_sales
# FROM gold.transaction.uae_pos_transactions AS t1
# JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# WHERE
#     business_day BETWEEN "2024-06-15" AND "2024-08-08"
#     AND material_group_name = "COCONUT OIL"
#     AND tayeb_flag = 0
#     AND transaction_type IN ("SALE", "SELL_MEDIA")
#     AND amount > 0
#     AND quantity > 0
# GROUP BY 1

# COMMAND ----------

# %sql
# SELECT
#     material_group_name,
#     region_name,
#     CASE WHEN business_day <= "2023-01-01" THEN "Year 1" ELSE "Year 2" END AS year,
#     ROUND(SUM(CASE WHEN MONTH(business_day) <= 5 THEN amount END)) AS pre_sales,
#     ROUND(SUM(CASE WHEN MONTH(business_day) >= 6 THEN amount END)) AS post_sales,
#     ROUND((post_sales - pre_sales) / pre_sales * 100, 2) AS growth
# FROM gold.transaction.uae_pos_transactions AS t1
# JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# WHERE
#     (business_day BETWEEN "2022-02-19" AND "2022-05-17"
#         OR business_day BETWEEN "2022-06-08" AND "2022-08-08"
#         OR business_day BETWEEN "2023-02-19" AND "2023-05-17"
#         OR business_day BETWEEN "2023-06-08" AND "2023-08-08")
#     AND material_group_name = "PASTA"
#     AND tayeb_flag = 0
#     AND transaction_type IN ("SALE", "SELL_MEDIA")
#     AND amount > 0
#     AND quantity > 0
# GROUP BY 1, 2, 3
# ORDER BY 2, 3

# COMMAND ----------

# %sql
# SELECT
#     material_group_name,
#     region_name,
#     CASE WHEN business_day <= "2023-01-01" THEN "Year 1" ELSE "Year 2" END AS year,
#     ROUND(SUM(CASE WHEN MONTH(business_day) <= 5 THEN amount END)) AS pre_sales,
#     ROUND(SUM(CASE WHEN MONTH(business_day) >= 6 THEN amount END)) AS post_sales,
#     ROUND((post_sales - pre_sales) / pre_sales * 100, 2) AS growth
# FROM gold.transaction.uae_pos_transactions AS t1
# JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# WHERE
#     (business_day BETWEEN "2022-01-01" AND "2022-04-28"
#         OR business_day BETWEEN "2022-06-15" AND "2022-08-08"
#         OR business_day BETWEEN "2023-02-01" AND "2023-04-28"
#         OR business_day BETWEEN "2023-06-15" AND "2023-08-08")
#     AND material_group_name = "COCONUT OIL"
#     AND tayeb_flag = 0
#     AND transaction_type IN ("SALE", "SELL_MEDIA")
#     AND amount > 0
#     AND quantity > 0
# GROUP BY 1, 2, 3
# ORDER BY 2, 3

# COMMAND ----------

# %sql
# WITH sales as (
#     SELECT
#         region_name,
#         transaction_id,
#         product_id,
#         material_group_name,
#         SUM(CASE WHEN business_day <= "2024-05-17" THEN amount END) AS pre_sales,
#         SUM(CASE WHEN business_day >= "2024-06-08" THEN amount END) AS post_sales
#     FROM gold.transaction.uae_pos_transactions AS t1
#     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#     WHERE
#         business_day BETWEEN "2024-02-19" AND "2024-08-08"
#         AND material_group_name = "PASTA"
#         AND tayeb_flag = 0
#         AND transaction_type IN ("SALE", "SELL_MEDIA")
#         AND amount > 0
#         AND quantity > 0
#     GROUP BY 1, 2, 3, 4
# ),

# promo_table AS (
#     SELECT
#         transaction_id,
#         product_id,
#         CONCAT(SUBSTRING(business_date, 1, 4), '-',
#             SUBSTRING(business_date, 5, 2), '-',
#             SUBSTRING(business_date, 7, 2)) AS formatted_date
#     FROM gold.marketing.uae_pos_sales_campaign
#     WHERE
#         void_flag IS NULL
#         AND campaign_id IS NOT NULL
#         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
#         AND (pm_reason_code != "HAPPINESS VOUCHER")
#         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
#         AND business_date BETWEEN "20240201" AND "20240808"
#     GROUP BY transaction_id, product_id, business_date
# ),

# final AS (
#     SELECT
#         region_name,
#         material_group_name,
#         ROUND(SUM(pre_sales)) AS total_pre_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
#         ROUND(pre_promo_sales/total_pre_sales*100, 2) AS pre_promo_sales_perc,
#         ROUND(SUM(post_sales)) AS total_post_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales,
#         ROUND(post_promo_sales/total_post_sales*100, 2) AS post_promo_sales_perc
#     FROM sales AS t1
#     LEFT JOIN promo_table AS t2
#         ON t1.transaction_id = t2.transaction_id
#         AND t1.product_id = t2.product_id
#     GROUP BY 1, 2
# )

# SELECT
#     material_group_name,
#     region_name,
#     pre_promo_sales_perc,
#     post_promo_sales_perc,
#     ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta
# FROM final

# COMMAND ----------

# %sql
# WITH sales as (
#     SELECT
#         region_name,
#         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
#         transaction_id,
#         product_id,
#         material_group_name,
#         SUM(CASE WHEN business_day <= "2024-05-17" THEN amount END) AS pre_sales,
#         SUM(CASE WHEN business_day >= "2024-06-08" THEN amount END) AS post_sales
#     FROM gold.transaction.uae_pos_transactions AS t1
#     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#     WHERE
#         (business_day BETWEEN "2024-02-19" AND "2024-05-17"
#         OR business_day BETWEEN "2024-06-08" AND "2024-08-08")
#         AND material_group_name = "PASTA"
#         AND tayeb_flag = 0
#         AND transaction_type IN ("SALE", "SELL_MEDIA")
#         AND amount > 0
#         AND quantity > 0
#     GROUP BY 1, 2, 3, 4, 5
# ),

# promo_table AS (
#     SELECT
#         transaction_id,
#         product_id,
#         CONCAT(SUBSTRING(business_date, 1, 4), '-',
#             SUBSTRING(business_date, 5, 2), '-',
#             SUBSTRING(business_date, 7, 2)) AS formatted_date
#     FROM gold.marketing.uae_pos_sales_campaign
#     WHERE
#         void_flag IS NULL
#         AND campaign_id IS NOT NULL
#         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
#         AND (pm_reason_code != "HAPPINESS VOUCHER")
#         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
#         AND business_date BETWEEN "20240201" AND "20240808"
#     GROUP BY transaction_id, product_id, business_date
# ),

# combined AS (
#     SELECT
#         region_name,
#         year_month,
#         material_group_name,
#         t1.product_id,
#         ROUND(SUM(pre_sales)) AS total_pre_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
#         ROUND(SUM(post_sales)) AS total_post_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales
#     FROM sales AS t1
#     LEFT JOIN promo_table AS t2
#         ON t1.transaction_id = t2.transaction_id
#         AND t1.product_id = t2.product_id
#     GROUP BY 1, 2, 3, 4
#     ORDER BY 5 DESC
# ),

# final AS (
#     SELECT
#         material_group_name,
#         region_name,
#         year_month,
#         product_id,
#         total_pre_sales,
#         pre_promo_sales,
#         total_post_sales,
#         post_promo_sales
#     FROM combined
# ),

# gp_data AS (
#     SELECT
#         CASE WHEN region = "AUH" THEN "ABU DHABI"
#             WHEN region = "ALN" THEN "AL AIN"
#             WHEN region = "DXB" THEN "DUBAI"
#             ELSE "SHARJAH" END AS region_name,
#         year_month,
#         material_id,
#         gp_wth_chargeback
#     FROM gold.business.gross_profit
#     WHERE
#         country = 'AE'
#         AND year_month BETWEEN 202402 AND 202405
# ),

# final_2 AS (
#     SELECT
#         material_group_name,
#         t1.year_month,
#         t1.region_name,
#         product_id,
#         total_pre_sales,
#         pre_promo_sales,
#         total_post_sales,
#         post_promo_sales,
#         ROUND(total_pre_sales * gp_wth_chargeback / 100, 2) AS gp
#     FROM final AS t1
#     LEFT JOIN gp_data AS t2
#         ON t1.region_name = t2.region_name
#         AND t1.year_month = t2.year_month
#         AND t1.product_id = t2.material_id
# ),

# ranked AS (
#     SELECT
#         material_group_name,
#         region_name,
#         product_id,
#         SUM(total_pre_sales) AS total_pre_sales_,
#         ROUND(SUM(pre_promo_sales)/total_pre_sales_*100, 2) AS pre_promo_sales_perc,
#         ROUND(SUM(post_promo_sales)/SUM(total_post_sales)*100, 2) AS post_promo_sales_perc,
#         ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta,
#         ROUND(total_pre_sales_ / SUM(gp) * 100, 2) AS gp_margin,
#         ROW_NUMBER() OVER(PARTITION BY region_name ORDER BY SUM(total_pre_sales) DESC) AS rk
#     FROM final_2
#     GROUP BY 1, 2, 3
#     ORDER BY region_name, total_pre_sales_ DESC
# )

# SELECT
#     material_group_name,
#     region_name,
#     product_id,
#     total_pre_sales_,
#     pre_promo_sales_perc,
#     post_promo_sales_perc,
#     delta,
#     gp_margin,
#     rk
# FROM ranked
# WHERE rk <= 20

# COMMAND ----------

# %sql
# WITH sales as (
#     SELECT
#         region_name,
#         transaction_id,
#         product_id,
#         material_group_name,
#         SUM(CASE WHEN business_day <= "2024-04-28" THEN amount END) AS pre_sales,
#         SUM(CASE WHEN business_day >= "2024-06-01" THEN amount END) AS post_sales
#     FROM gold.transaction.uae_pos_transactions AS t1
#     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#     WHERE
#         business_day BETWEEN "2024-02-01" AND "2024-08-08"
#         AND material_group_name = "INSTANT NOODLE"
#         AND tayeb_flag = 0
#         AND transaction_type IN ("SALE", "SELL_MEDIA")
#         AND amount > 0
#         AND quantity > 0
#     GROUP BY 1, 2, 3, 4
# ),

# promo_table AS (
#     SELECT
#         transaction_id,
#         product_id,
#         CONCAT(SUBSTRING(business_date, 1, 4), '-',
#             SUBSTRING(business_date, 5, 2), '-',
#             SUBSTRING(business_date, 7, 2)) AS formatted_date
#     FROM gold.marketing.uae_pos_sales_campaign
#     WHERE
#         void_flag IS NULL
#         AND campaign_id IS NOT NULL
#         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
#         AND (pm_reason_code != "HAPPINESS VOUCHER")
#         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
#         AND business_date BETWEEN "20240201" AND "20240808"
#     GROUP BY transaction_id, product_id, business_date
# ),

# final AS (
#     SELECT
#         region_name,
#         material_group_name,
#         ROUND(SUM(pre_sales)) AS total_pre_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
#         ROUND(pre_promo_sales/total_pre_sales*100, 2) AS pre_promo_sales_perc,
#         ROUND(SUM(post_sales)) AS total_post_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales,
#         ROUND(post_promo_sales/total_post_sales*100, 2) AS post_promo_sales_perc
#     FROM sales AS t1
#     LEFT JOIN promo_table AS t2
#         ON t1.transaction_id = t2.transaction_id
#         AND t1.product_id = t2.product_id
#     GROUP BY 1, 2
# )

# SELECT
#     material_group_name,
#     region_name,
#     pre_promo_sales_perc,
#     post_promo_sales_perc,
#     ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta
# FROM final

# COMMAND ----------

# %sql
# WITH sales as (
#     SELECT
#         region_name,
#         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
#         transaction_id,
#         product_id,
#         material_group_name,
#         SUM(CASE WHEN business_day <= "2024-04-28" THEN amount END) AS pre_sales,
#         SUM(CASE WHEN business_day >= "2024-06-01" THEN amount END) AS post_sales
#     FROM gold.transaction.uae_pos_transactions AS t1
#     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#     WHERE
#         (business_day BETWEEN "2024-02-01" AND "2024-04-28"
#         OR business_day BETWEEN "2024-06-01" AND "2024-08-08")
#         AND material_group_name = "INSTANT NOODLE"
#         AND tayeb_flag = 0
#         AND transaction_type IN ("SALE", "SELL_MEDIA")
#         AND amount > 0
#         AND quantity > 0
#     GROUP BY 1, 2, 3, 4, 5
# ),

# promo_table AS (
#     SELECT
#         transaction_id,
#         product_id,
#         CONCAT(SUBSTRING(business_date, 1, 4), '-',
#             SUBSTRING(business_date, 5, 2), '-',
#             SUBSTRING(business_date, 7, 2)) AS formatted_date
#     FROM gold.marketing.uae_pos_sales_campaign
#     WHERE
#         void_flag IS NULL
#         AND campaign_id IS NOT NULL
#         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
#         AND (pm_reason_code != "HAPPINESS VOUCHER")
#         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
#         AND business_date BETWEEN "20240201" AND "20240808"
#     GROUP BY transaction_id, product_id, business_date
# ),

# combined AS (
#     SELECT
#         region_name,
#         year_month,
#         material_group_name,
#         t1.product_id,
#         ROUND(SUM(pre_sales)) AS total_pre_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
#         ROUND(SUM(post_sales)) AS total_post_sales,
#         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales
#     FROM sales AS t1
#     LEFT JOIN promo_table AS t2
#         ON t1.transaction_id = t2.transaction_id
#         AND t1.product_id = t2.product_id
#     GROUP BY 1, 2, 3, 4
#     ORDER BY 5 DESC
# ),

# final AS (
#     SELECT
#         material_group_name,
#         region_name,
#         year_month,
#         product_id,
#         total_pre_sales,
#         pre_promo_sales,
#         total_post_sales,
#         post_promo_sales
#     FROM combined
# ),

# gp_data AS (
#     SELECT
#         CASE WHEN region = "AUH" THEN "ABU DHABI"
#             WHEN region = "ALN" THEN "AL AIN"
#             WHEN region = "DXB" THEN "DUBAI"
#             ELSE "SHARJAH" END AS region_name,
#         year_month,
#         material_id,
#         gp_wth_chargeback
#     FROM gold.business.gross_profit
#     WHERE
#         country = 'AE'
#         AND year_month BETWEEN 202402 AND 202404
# ),

# final_2 AS (
#     SELECT
#         material_group_name,
#         t1.year_month,
#         t1.region_name,
#         product_id,
#         total_pre_sales,
#         pre_promo_sales,
#         total_post_sales,
#         post_promo_sales,
#         ROUND(total_pre_sales * gp_wth_chargeback / 100, 2) AS gp
#     FROM final AS t1
#     LEFT JOIN gp_data AS t2
#         ON t1.region_name = t2.region_name
#         AND t1.year_month = t2.year_month
#         AND t1.product_id = t2.material_id
# ),

# ranked AS (
#     SELECT
#         material_group_name,
#         region_name,
#         product_id,
#         SUM(total_pre_sales) AS total_pre_sales_,
#         ROUND(SUM(pre_promo_sales)/total_pre_sales_*100, 2) AS pre_promo_sales_perc,
#         ROUND(SUM(post_promo_sales)/SUM(total_post_sales)*100, 2) AS post_promo_sales_perc,
#         ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta,
#         ROUND(total_pre_sales_ / SUM(gp) * 100, 2) AS gp_margin,
#         ROW_NUMBER() OVER(PARTITION BY region_name ORDER BY SUM(total_pre_sales) DESC) AS rk
#     FROM final_2
#     GROUP BY 1, 2, 3
#     ORDER BY region_name, total_pre_sales_ DESC
# )

# SELECT
#     material_group_name,
#     region_name,
#     product_id,
#     total_pre_sales_,
#     pre_promo_sales_perc,
#     post_promo_sales_perc,
#     delta,
#     gp_margin,
#     rk
# FROM ranked
# WHERE rk <= 20

# COMMAND ----------

# MAGIC %md
# MAGIC ###EDA Temp (End)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.sandbox.pj_ao_dashboard_region_view_copy AS (
# MAGIC     SELECT * FROM dev.sandbox.pj_ao_dashboard_region_view
# MAGIC );
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.sandbox.pj_ao_dashboard_region_view;

# COMMAND ----------

df.shape

# COMMAND ----------

df_copy = df.copy()
df_copy = df_copy[skip_records:]
records_split = 1000000 # 1 million records

total_loops = round(np.ceil(df.shape[0] / 1000000))
print(f"Total loops to be completed: {total_loops}")

i = 1
while df_copy.empty == False:
    temp = df_copy.iloc[:records_split].copy()
    df_copy = df_copy.iloc[records_split:].reset_index(drop = True)

    spark_df = spark.createDataFrame(temp)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_dashboard_region_view")
    print(f"Loop {i} completed")
    i += 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.sandbox.pj_ao_dashboard_region_view LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calendar Table

# COMMAND ----------

query = f"""
SELECT business_day AS cy_date
FROM dev.sandbox.pj_ao_dashboard_region_view
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
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.pj_ao_lfl_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Current & Previous Year Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.sandbox.pj_ao_dashboard_cy_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_region_view AS t1
# MAGIC     JOIN dev.sandbox.pj_ao_lfl_calendar AS t2 ON t1.business_day = t2.cy_date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.sandbox.pj_ao_dashboard_py_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_region_view AS t1
# MAGIC     JOIN dev.sandbox.pj_ao_lfl_calendar AS t2 ON t1.business_day = t2.py_date
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
# spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("sandbox.pj_ao_dashboard_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transactions Table

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_ao_dashboard_cy AS (
#     WITH pj_ao_dashboard_cy_trans AS (
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

#     pj_ao_dashboard_cy_ovr AS (
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
#         FROM pj_ao_dashboard_cy_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_ao_dashboard_cy_dept AS (
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
#         FROM pj_ao_dashboard_cy_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_ao_dashboard_cy AS (
#         SELECT *
#         FROM pj_ao_dashboard_cy_ovr

#         UNION

#         SELECT *
#         FROM pj_ao_dashboard_cy_dept
#     )

#     SELECT
#         a.*,
#         b.cy_date,
#         b.dayno AS r_day
#     FROM pj_ao_dashboard_cy a
#     JOIN dashboard.pj_ao_lfl_calendar b ON a.trans_date = b.cy_date
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_ao_dashboard_py AS (
#     WITH pj_ao_dashboard_py_trans AS (
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

#     pj_ao_dashboard_py_ovr AS (
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
#         FROM pj_ao_dashboard_py_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),


#     pj_ao_dashboard_py_dept AS (
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
#         FROM pj_ao_dashboard_py_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_ao_dashboard_py AS (
#         SELECT *
#         FROM pj_ao_dashboard_py_ovr

#         UNION

#         SELECT *
#         FROM pj_ao_dashboard_py_dept
#     )

#     SELECT
#         a.*,
#         b.py_date,
#         b.dayno AS r_day
#     FROM pj_ao_dashboard_py a
#     JOIN dashboard.pj_ao_lfl_calendar b ON a.trans_date = b.py_date
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_ao_dashboard_lfl_data AS (
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
#     FROM dev.sandbox.pj_ao_dashboard_cy AS a
#     FULL JOIN dev.sandbox.pj_ao_dashboard_py AS b
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_reco
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rationalized SKUs
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
# MAGIC WHERE delisted_date != "NA"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rationalized SKUs From Recommendation
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
# MAGIC WHERE category_period_type = "Pre-delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 Sales of Delisted SKUs in Pre-delist Period
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
# MAGIC WHERE category_period_type = "Pre-delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 GP of Delisted SKUs in Pre-delist Period
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC         SUM(gross_profit) AS total_gp
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
# MAGIC -- FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
# MAGIC -- FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC     FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
# MAGIC WHERE material_id IN (5539, 91929)
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reco View

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(sales)
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY ALL
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     COUNT(DISTINCT material_id) AS delisted
# MAGIC FROM dev.sandbox.pj_ao_dashboard_reco
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
# MAGIC FROM dev.sandbox.pj_ao_dashboard_cy_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND((297441.74 - 221098.56) / 221098.56, 4)
