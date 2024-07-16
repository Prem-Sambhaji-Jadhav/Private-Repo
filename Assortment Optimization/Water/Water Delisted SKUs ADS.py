# Databricks notebook source
# MAGIC %md
# MAGIC #Daily Sales of Delisted SKUs

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

query = """
SELECT
    region_name,
    material_id,
    business_day,
    SUM(amount) AS sales
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
WHERE business_day BETWEEN "2023-10-01" AND "2024-04-29"
AND transaction_type IN ("SALE", "SELL_MEDIA")
AND category_name = "WATER"
GROUP BY region_name, material_id, business_day
"""

daily_sales_df = spark.sql(query).toPandas()

# COMMAND ----------

df = daily_sales_df.copy()

# COMMAND ----------

rationalized_auh_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/water_rationalized_auh.csv')
rationalized_aln_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/water_rationalized_aln.csv')
rationalized_dxb_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/water_rationalized_dxb.csv')
rationalized_shj_df = pd.read_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/water_rationalized_shj.csv')

rationalized_auh_df['region_name'] = 'ABU DHABI'
rationalized_aln_df['region_name'] = 'AL AIN'
rationalized_dxb_df['region_name'] = 'DUBAI'
rationalized_shj_df['region_name'] = 'SHARJAH'

rationalized_df = pd.concat([rationalized_auh_df, rationalized_aln_df, rationalized_dxb_df, rationalized_shj_df], ignore_index=True)

rationalized_df['delisted_date'] = pd.to_datetime(rationalized_df['delisted_date'], format='%d-%m-%Y')
rationalized_df['delisted_date'] = rationalized_df['delisted_date'].dt.strftime('%Y-%m-%d')

rationalized_df = rationalized_df[['material_id', 'region_name', 'delisted_date']]

# COMMAND ----------

df = pd.merge(df, rationalized_df, on=['material_id', 'region_name'], how='inner')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Validation

# COMMAND ----------

df[(df['business_day'].isnull() == False) & (df['region_name'] == 'SHARJAH')]['material_id'].nunique()

# COMMAND ----------

rationalized_shj_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Average Daily Sales

# COMMAND ----------

Q4_last_date = "2023-12-30"

pre_delist_df = df[df['business_day'] <= Q4_last_date]
pre_delist_df2 = pre_delist_df.groupby(['region_name', 'material_id'])['sales'].mean().reset_index()

# COMMAND ----------

pre_delist_df2.display()

# COMMAND ----------

post_delist_df = df[df['business_day'] >= df['delisted_date']]
post_delist_df2 = post_delist_df.groupby(['region_name', 'material_id'])['sales'].mean().reset_index()

# COMMAND ----------

post_delist_df2.display()

# COMMAND ----------

temp1 = post_delist_df.merge(pre_delist_df, on=['region_name', 'material_id'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
temp2 = pre_delist_df.merge(post_delist_df, on=['region_name', 'material_id'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

# COMMAND ----------

temp1[['region_name', 'material_id']].drop_duplicates().sort_values(by = ['region_name', 'material_id'])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     product_id,
# MAGIC     MIN(business_day)
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
# MAGIC WHERE business_day BETWEEN "2023-10-01" AND "2024-04-29"
# MAGIC AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC AND (
# MAGIC   (region_name = "ABU DHABI" AND product_id IN (554815, 1168340, 2083789)) OR
# MAGIC   (region_name = "AL AIN" AND product_id IN (649263, 1943686)) OR
# MAGIC   (region_name = "DUBAI" AND product_id IN (347534, 581854, 649263)) OR
# MAGIC   (region_name = "SHARJAH" AND product_id IN (581854, 649263))
# MAGIC   )
# MAGIC GROUP BY region_name, product_id
# MAGIC ORDER BY region_name, product_id

# COMMAND ----------

temp2[['region_name', 'material_id']].drop_duplicates().sort_values(by = ['region_name', 'material_id'])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     product_id,
# MAGIC     MIN(business_day),
# MAGIC     MAX(business_day)
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
# MAGIC WHERE business_day BETWEEN "2023-10-01" AND "2024-04-29"
# MAGIC AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC AND (
# MAGIC   (region_name = "ABU DHABI" AND product_id IN (1193068, 1306396, 1333721, 1403108, 1743443)) OR
# MAGIC   (region_name = "AL AIN" AND product_id IN (1193068, 1306396, 1403108, 2079852)) OR
# MAGIC   (region_name = "DUBAI" AND product_id IN (45025, 356313, 855264, 1021745, 1306396, 1462834, 1536585, 1824713, 2012059, 2088109, 2104671)) OR
# MAGIC   (region_name = "SHARJAH" AND product_id IN (5539, 1306396, 1536585, 1743443, 1877061, 2012059, 2088109))
# MAGIC   )
# MAGIC GROUP BY region_name, product_id
# MAGIC ORDER BY region_name, product_id

# COMMAND ----------



# COMMAND ----------


