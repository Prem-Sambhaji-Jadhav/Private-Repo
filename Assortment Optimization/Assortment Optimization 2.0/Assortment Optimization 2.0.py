# Databricks notebook source
# MAGIC %md
# MAGIC #Sandbox Table

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_ao_v2 AS (
#     WITH sales_data AS (
#         SELECT
#             business_day,
#             INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
#             region_name,
#             transaction_id,
#             t2.material_id,
#             t2.material_name,
#             t2.brand,
#             t1.ean,
#             INT(conversion_numerator) AS conversion_numerator,
#             unit_price,
#             regular_unit_price,
#             quantity AS quantity,
#             regular_unit_price * quantity AS amount,
#             ROUND(unit_price - regular_unit_price, 2) AS discount,
#             ROUND(discount/regular_unit_price) AS discount_perc,
#             CASE WHEN discount > 0 THEN 1 ELSE 0 END AS discount_flag,
#             1 AS purchase_flag
#         FROM gold.pos_transactions AS t1
#         LEFT JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
#         LEFT JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
#         LEFT JOIN gold.material_attributes AS t4 ON t1.ean = t4.ean
#         WHERE
#             business_day BETWEEN "2023-07-01" AND "2024-06-30"
#             AND t2.category_name = "PASTA & NOODLE"
#             AND t2.material_group_name = "PASTA"
#             AND tayeb_flag = 0
#             AND transaction_type IN ("SALE", "SELL_MEDIA")
#             AND amount > 0
#             AND quantity > 0
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
#     ),

#     gp_data AS (
#         SELECT
#             CASE WHEN region = "AUH" THEN "ABU DHABI"
#                 WHEN region = "ALN" THEN "AL AIN"
#                 WHEN region = "DXB" THEN "DUBAI"
#                 ELSE "SHARJAH" END AS region_name,
#             year_month,
#             material_id,
#             gp_wth_chargeback
#         FROM gold.gross_profit
#         WHERE country = 'AE'
#         AND year_month BETWEEN 202307 AND 202406
#     )

#     SELECT
#         t1.*,
#         ROUND(COALESCE(amount*gp_wth_chargeback/100, 0), 2) AS abs_gp
#     FROM sales_data AS t1
#     LEFT JOIN gp_data AS t2
#         ON t1.region_name = t2.region_name
#         AND t1.year_month = t2.year_month
#         AND t1.material_id = t2.material_id
# )

# COMMAND ----------

# MAGIC %md
# MAGIC #Attributes Data

# COMMAND ----------

import pandas as pd
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import matplotlib.pyplot as plt

# COMMAND ----------

attr = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/pasta/pasta_attributes_updated.csv")
attr = attr.drop(columns = ['sales', 'material_name', 'brand', 'material_group_name', 'type_bin', 'volume_bin', 'packaging', 'volume', 'units'])

# COMMAND ----------

query = """
SELECT *
FROM sandbox.pj_ao_v2
WHERE year_month BETWEEN 202403 AND 202406
"""

df = spark.sql(query).toPandas()
df = df.drop(columns='year_month')
df = pd.merge(df, attr, on = 'material_id', how = 'left')

# COMMAND ----------

df.loc[df['item_count'] == '2+1', 'item_count'] = '3'
df.loc[df['item_count'] == '1+1', 'item_count'] = '2'
df['Packs'] = np.where(df.item_count == '1', df.item_count + ' Pack', df.item_count + ' Packs')
df['item_count'] = df['item_count'].astype('int32')
df['conversion_numerator'] = df['conversion_numerator'].fillna(1)
df['unit_wgt_price'] = df['amount'] / (df['conversion_numerator'] * df['volume_in_grams'] * df['quantity'] * df['item_count'])
df['volume_in_grams'] = df['volume_in_grams'].astype(str)
df['volume_in_grams'] = df['volume_in_grams'] + 'G'
df['attribute_combination'] = df['type'] + ", " + df['volume_in_grams'] + ", " + df['Packs']

# COMMAND ----------

df['material_id'].nunique(), df.shape

# COMMAND ----------

df.head().display()

# COMMAND ----------

# df['customer_id'].nunique(), df['material_id'].nunique(), df['business_day'].nunique()
# display(pd.DataFrame(df.groupby('customer_id')['material_id'].nunique()).reset_index())
# temp = df.groupby('customer_id')[['material_id', 'business_day']].nunique().reset_index()
# temp[temp['business_day'] >= 3]['customer_id'].nunique()
# display(temp)
# display(pd.DataFrame(df.groupby('customer_id')['material_id'].nunique()).reset_index())

# COMMAND ----------

df2 = df.groupby(['business_day', 'region_name', 'transaction_id', 'attribute_combination']).agg(
    {'unit_wgt_price': 'mean',
     'amount': 'sum',
     'quantity': 'sum',
     'discount': 'sum',
     'abs_gp': 'sum',
     'purchase_flag': 'mean',
     'discount_flag': 'mean'}).reset_index()

df2['discount_flag'] = round(df2['discount_flag']).astype('int32')
df2['purchase_flag'] = df2['purchase_flag'].astype('int32')
df2['unit_wgt_price'] = round(df2['unit_wgt_price'], 4)
df2['amount'] = round(df2['amount'], 2)
# df2['quantity'] = round(df2['quantity'], 2)
df2['discount'] = round(df2['discount'], 2)

df2['discount_perc'] = round(df2['discount']/df2['amount'], 4)

attr_share_df = df.groupby('attribute_combination')['amount'].sum().reset_index()
total_attr_share = attr_share_df['amount'].sum()
attr_share_df['attribute_share'] = round(attr_share_df['amount']/total_attr_share, 4)
attr_share_df = attr_share_df.drop(columns='amount')
df2 = pd.merge(df2, attr_share_df, on='attribute_combination', how='left')

df2 = df2.rename(columns={'unit_wgt_price': 'avg_unit_wgt_price', 'amount': 'sales', 'discount_flag': 'dominant_discount_flag'})

df2['discount_flag'] = np.where(df2.discount > 0, 1, 0)

# COMMAND ----------

df2.head().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Compression Check

# COMMAND ----------

len(df) - len(df2)

# COMMAND ----------

temp = df.groupby('transaction_id')['material_id'].nunique().reset_index()
temp[temp['material_id'] > 1]

# COMMAND ----------

# MAGIC %md
# MAGIC #Attribute Shares Check

# COMMAND ----------

temp = df.groupby('type')['amount'].sum().reset_index()
temp = temp.rename(columns={'amount': 'type_sales'})
temp = temp.sort_values(by = 'type_sales', ascending = False).reset_index(drop = True)
temp['type_sales'] = round(temp['type_sales'], 0)
temp['type_sales_perc'] = round(temp.type_sales.cumsum() / temp.type_sales.sum() * 100, 2)

df = pd.merge(df, temp, on='type', how='inner')

temp = df.groupby('volume_in_grams')['amount'].sum().reset_index()
temp = temp.rename(columns={'amount': 'volume_sales'})
temp = temp.sort_values(by = 'volume_sales', ascending = False).reset_index(drop = True)
temp['volume_sales'] = round(temp['volume_sales'], 0)
temp['volume_sales_perc'] = round(temp.volume_sales.cumsum() / temp.volume_sales.sum() * 100, 2)
df = pd.merge(df, temp, on='volume_in_grams', how='inner')

temp = df.groupby('Packs')['amount'].sum().reset_index()
temp = temp.rename(columns={'amount': 'Pack_sales'})
temp = temp.sort_values(by = 'Pack_sales', ascending = False).reset_index(drop = True)
temp['Pack_sales'] = round(temp['Pack_sales'], 0)
temp['Pack_sales_perc'] = round(temp.Pack_sales.cumsum() / temp.Pack_sales.sum() * 100, 2)
df = pd.merge(df, temp, on='Packs', how='inner')

temp = df.groupby('attribute_combination')['amount'].sum().reset_index()
temp = temp.rename(columns={'amount': 'attr_combo_sales'})
temp = temp.sort_values(by = 'attr_combo_sales', ascending = False).reset_index(drop = True)
temp['attr_combo_sales'] = round(temp['attr_combo_sales'], 0)
temp['attr_combo_sales_perc'] = round(temp.attr_combo_sales.cumsum() / temp.attr_combo_sales.sum() * 100, 2)
df = pd.merge(df, temp, on='attribute_combination', how='inner')

temp = df[['type', 'type_sales', 'type_sales_perc', 'volume_in_grams', 'volume_sales', 'volume_sales_perc', 'Packs', 'Pack_sales', 'Pack_sales_perc', 'attribute_combination', 'attr_combo_sales', 'attr_combo_sales_perc']].drop_duplicates().reset_index(drop=True)

# COMMAND ----------

temp.iloc[:, :3].drop_duplicates().sort_values(by = 'type_sales', ascending = False).display()

# temp.iloc[:, 3:6].drop_duplicates().sort_values(by = 'volume_sales', ascending = False).display()

# temp.iloc[:, 6:9].drop_duplicates().sort_values(by = 'Pack_sales', ascending = False).display()

# temp.iloc[:, 9:].drop_duplicates().sort_values(by = 'attr_combo_sales', ascending = False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Attribute Combination Tail Check

# COMMAND ----------

temp = df2.groupby('attribute_combination')['transaction_id'].nunique().reset_index().sort_values(by = 'transaction_id', ascending = False).reset_index(drop=True)
temp['transaction_cum_sum'] = np.round(100 * (temp.transaction_id.cumsum() / temp['transaction_id'].sum()), 2)
cum_sum_90_attrs = temp[temp['transaction_cum_sum'] < 90]['attribute_combination'].count()
cum_sum_90_attrs_perc = round(cum_sum_90_attrs/len(temp)*100, 2)
print(f'90% of transactions is covered by {cum_sum_90_attrs} ({cum_sum_90_attrs_perc}%) of the attribute combinations')

fig = go.Figure()
fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.Bar(y=temp['transaction_id'], x=temp['attribute_combination'], name="attribute_combination_transactions"), secondary_y=False)
fig.add_trace(go.Scatter(x=temp['attribute_combination'], y=temp['transaction_cum_sum'],
                         mode='lines', name="Transactions Cumulative Contri"), secondary_y=True)
fig.update_layout(
    title_text="Transactions Distribution"
)
fig.update_xaxes(title_text="attribute_combination")
fig.update_yaxes(title_text="transaction count", secondary_y=False)
fig.update_yaxes(title_text="% Cumulative Contribution", secondary_y=True)
fig.update_layout(showlegend=False)
fig.update_layout(autosize=False, width=900, height=400)
fig.update_layout(xaxis=dict(ticktext=[], tickvals=[]))
fig.show()

temp = attr_share_df.sort_values(by = 'attribute_share', ascending = False).reset_index(drop=True)
temp['attribute_share_cum_sum'] = np.round(100 * (temp.attribute_share.cumsum() / temp['attribute_share'].sum()), 2)
cum_sum_90_attrs = temp[temp['attribute_share_cum_sum'] < 90]['attribute_combination'].count()
cum_sum_90_attrs_perc = round(cum_sum_90_attrs/len(temp)*100, 2)
print(f'90% of sales is covered by {cum_sum_90_attrs} ({cum_sum_90_attrs_perc}%) of the attribute combinations')

fig = go.Figure()
fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.Bar(y=temp['attribute_share'], x=temp['attribute_combination'], name="attribute_combination_shares"), secondary_y=False)
fig.add_trace(go.Scatter(x=temp['attribute_combination'], y=temp['attribute_share_cum_sum'],
                         mode='lines', name="Attribute Share Cumulative Contri"), secondary_y=True)
fig.update_layout(
    title_text="Attribute Share Distribution"
)
fig.update_xaxes(title_text="attribute_combination")
fig.update_yaxes(title_text="sales share", secondary_y=False)
fig.update_yaxes(title_text="% Cumulative Contribution", secondary_y=True)
fig.update_layout(showlegend=False)
fig.update_layout(autosize=False, width=900, height=400)
fig.update_layout(xaxis=dict(ticktext=[], tickvals=[]))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Dummy Data Creation

# COMMAND ----------

temp1 = df2[['business_day']].drop_duplicates().reset_index(drop = True)
temp2 = df2[['region_name']].drop_duplicates().reset_index(drop = True)
temp3 = df2[['attribute_combination']].drop_duplicates().reset_index(drop = True)
temp4 = pd.merge(temp1, temp2, how='cross')
temp4 = pd.merge(temp4, temp3, how='cross')

df3 = pd.merge(df2, temp4, on=['business_day', 'region_name', 'attribute_combination'], how='outer')

# COMMAND ----------

df3['transaction_id'] = df3['transaction_id'].fillna('0')
df3['purchase_flag'] = df3['purchase_flag'].fillna(0)

# COMMAND ----------

# df4 = df3.copy()

# def impute_values(row, df, columns):
#     region = row['region_name']
#     attribute = row['attribute_combination']
#     business_day = row['business_day']

#     filtered_df = df[(df['region_name'] == region) & (df['attribute_combination'] == attribute)]
#     filtered_df = filtered_df[filtered_df.index != row.name]

#     non_null_sales = filtered_df.dropna(subset=['sales'])

#     imputed_values = {}
#     for column in columns:
#         if pd.isnull(row[column]):
#             non_null_column = filtered_df.dropna(subset=[column])
#             if not non_null_column.empty:
#                 closest_index = (non_null_column['business_day'] - business_day).abs().idxmin()
#                 imputed_values[column] = non_null_column.loc[closest_index, column]
#             else:
#                 imputed_values[column] = np.nan
#         else:
#             imputed_values[column] = row[column]
    
#     return pd.Series(imputed_values)

# df4['business_day'] = pd.to_datetime(df4['business_day'])

# impute_columns = ['avg_unit_wgt_price', 'sales', 'discount', 'abs_gp', 'dominant_discount_flag']
# imputed_df = df4.apply(lambda row: impute_values(row, df4, impute_columns), axis=1)
# df4[impute_columns] = imputed_df[impute_columns]

# df4['business_day'] = df4['business_day'].dt.strftime('%Y-%m-%d')

# df4['discount_perc'] = round(df4['discount']/df2['amount'], 4)
# df4['discount_flag'] = np.where(df4.discount > 0, 1, 0)
# df4 = df4.drop(columns = 'attribute_share')
# df4 = pd.merge(df4, attr_share_df, on='attribute_combination', how='left')

# COMMAND ----------

# spark_df = spark.createDataFrame(df4)
# spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("sandbox.pj_ao_v2_dummy_data")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


