# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

query = """
SELECT
    region_name,
    MONTH(business_day) AS month,
    material_id,
    material_name,
    brand,
    ROUND(SUM(amount),1) AS sales
FROM gold.pos_transactions AS t1
JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
WHERE business_day BETWEEN "2023-03-01" AND "2024-02-29"
AND transaction_type IN ("SALE", "SELL_MEDIA")
AND material_group_name = "PASTA"
AND category_name = "PASTA & NOODLE"
GROUP BY region_name, month, material_id, material_name, brand
"""

trend_data = spark.sql(query).toPandas()

# COMMAND ----------

df = trend_data.copy()

# COMMAND ----------

gp_report_auh = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/ao_gp_pasta_auh.csv')
gp_report_aln = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/ao_gp_pasta_aln.csv')
gp_report_dxb = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/ao_gp_pasta_dxb.csv')
gp_report_shj = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/ao_gp_pasta_shj.csv')

gp_report_auh['region_name'] = 'ABU DHABI'
gp_report_aln['region_name'] = 'AL AIN'
gp_report_dxb['region_name'] = 'DUBAI'
gp_report_shj['region_name'] = 'SHARJAH'

gp_report = pd.concat([gp_report_auh, gp_report_aln, gp_report_dxb, gp_report_shj], ignore_index=True)

gp_report = gp_report[['region_name', 'material_id', 'new_buckets']]
gp_report.rename(columns={'new_buckets': 'recommendation'}, inplace=True)

# COMMAND ----------

rationalized_auh_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/pasta_rationalized_auh.csv', encoding='latin1')
rationalized_aln_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/pasta_rationalized_aln.csv')
rationalized_dxb_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/pasta_rationalized_dxb.csv')
rationalized_shj_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/pasta_rationalized_shj.csv')

rationalized_auh_df['region_name'] = 'ABU DHABI'
rationalized_aln_df['region_name'] = 'AL AIN'
rationalized_dxb_df['region_name'] = 'DUBAI'
rationalized_shj_df['region_name'] = 'SHARJAH'

rationalized_df = pd.concat([rationalized_auh_df, rationalized_aln_df, rationalized_dxb_df, rationalized_shj_df], ignore_index=True)

rationalized_df = rationalized_df[['material_id', 'region_name']]
rationalized_df['delisted'] = 1

# COMMAND ----------

gp_report = pd.merge(gp_report, rationalized_df, on=['material_id', 'region_name'], how='left')
gp_report['delisted'] = gp_report['delisted'].fillna(0)
df = pd.merge(df, gp_report, on=['material_id', 'region_name'], how='right')

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


