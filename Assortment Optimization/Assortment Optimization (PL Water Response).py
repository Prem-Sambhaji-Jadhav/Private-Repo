# Databricks notebook source
# MAGIC %md
# MAGIC #Import Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC #Getting Data

# COMMAND ----------

attr = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/ao_attributes.csv")

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_water_pl_response_mom AS (
#   SELECT
#       material_id,
#       CONCAT(YEAR(business_day), MONTH(business_day)) AS month_year,
#       ROUND(SUM(amount),2) AS sales,
#       ROUND(SUM(quantity),2) AS quantity_sold
#   FROM gold.pos_transactions AS t1
#   JOIN gold.material_master AS t2
#   ON t1.product_id = t2.material_id
#   WHERE business_day BETWEEN '2023-03-06' AND '2024-03-05'
#   AND category_name = 'WATER'
#   AND amount > 0
#   AND quantity > 0
#   GROUP BY material_id, month_year
#   ORDER BY material_id, month_year
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_water_pl_response_qoq AS (
#   SELECT
#       material_id,
#       CASE WHEN business_day BETWEEN "2023-03-06" AND "2023-06-05" THEN "Q1"
#       WHEN business_day BETWEEN "2023-06-06" AND "2023-09-05" THEN "Q2"
#       WHEN business_day BETWEEN "2023-09-06" AND "2023-12-05" THEN "Q3"
#       ELSE "Q4" END AS quarter,
#       ROUND(SUM(amount),2) AS sales,
#       ROUND(SUM(quantity),2) AS quantity_sold
#   FROM gold.pos_transactions AS t1
#   JOIN gold.material_master AS t2
#   ON t1.product_id = t2.material_id
#   WHERE business_day BETWEEN '2023-03-06' AND '2024-03-05'
#   AND category_name = 'WATER'
#   AND amount > 0
#   AND quantity > 0
#   GROUP BY material_id, quarter
#   ORDER BY material_id, quarter
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_water_pl_response_yoy AS (
#   SELECT
#       material_id,
#       CASE WHEN business_day BETWEEN "2022-03-06" AND "2023-03-05" THEN "Y1"
#       ELSE "Y2" END AS year,
#       ROUND(SUM(amount),2) AS sales,
#       ROUND(SUM(quantity),2) AS quantity_sold
#   FROM gold.pos_transactions AS t1
#   JOIN gold.material_master AS t2
#   ON t1.product_id = t2.material_id
#   WHERE business_day BETWEEN '2022-03-06' AND '2024-03-05'
#   AND category_name = 'WATER'
#   AND amount > 0
#   AND quantity > 0
#   GROUP BY material_id, year
#   ORDER BY material_id, year
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_water_pl_response_cwd AS (
#   SELECT
#       material_id,
#       store_id,
#       ROUND(SUM(amount),2) AS sales
#   FROM gold.pos_transactions AS t1
#   JOIN gold.material_master AS t2
#   ON t1.product_id = t2.material_id
#   WHERE business_day BETWEEN '2023-03-06' AND '2024-03-05'
#   AND category_name = 'WATER'
#   AND amount > 0
#   AND quantity > 0
#   GROUP BY material_id, store_id
#   ORDER BY material_id, store_id
# )

# COMMAND ----------

material_store_df = spark.sql("SELECT * FROM sandbox.pj_water_pl_response_cwd").toPandas()

# COMMAND ----------

cwd_df = material_store_df.copy()
cwd = []

all_store_sales = cwd_df['sales'].sum()
for material in cwd_df['material_id'].unique():
    stores = cwd_df[cwd_df['material_id'] == material]['store_id'].unique()
    material_store_sales = cwd_df[cwd_df['store_id'].isin(stores)]['sales'].sum()
    cwd.append(material_store_sales/all_store_sales)

cwd_df['material_store_count'] = cwd_df.groupby('material_id')['store_id'].transform('nunique')
cwd_df = cwd_df.groupby(['material_id']).agg({'sales': 'sum', 'material_store_count': 'mean'}).reset_index()
cwd_df['cwd'] = cwd

# COMMAND ----------

mom_df = spark.sql("SELECT * FROM sandbox.pj_water_pl_response_mom").toPandas()
mom_df = pd.merge(mom_df, attr[['material_id', 'volume', 'item_count', 'material_name']], on='material_id', how='left')

qoq_df = spark.sql("SELECT * FROM sandbox.pj_water_pl_response_qoq").toPandas()
qoq_df = pd.merge(qoq_df, attr[['material_id', 'volume', 'item_count', 'material_name']], on='material_id', how='left')

yoy_df = spark.sql("SELECT * FROM sandbox.pj_water_pl_response_yoy").toPandas()
yoy_df = pd.merge(yoy_df, attr[['material_id', 'volume', 'item_count', 'material_name']], on='material_id', how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Prep

# COMMAND ----------

mom_df['month_year'] = mom_df['month_year'].replace(
    ['20233', '20234', '20235', '20236', '20237', '20238', '20239', '20241', '20242', '20243'],
    ['202303', '202304', '202305', '202306', '202307', '202308', '202309', '202401', '202402', '202403'])

mom_df = mom_df.sort_values(by = ['material_id', 'month_year'])
mom_df = mom_df[mom_df['month_year'] != '202403'].reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Growth Calculation

# COMMAND ----------

def growth(df):
    period_type = df.columns[1]
    sales_growth = []
    quantity_growth = []
    for material in df['material_id'].unique():
        for period in df[df['material_id'] == material][period_type].unique():
            if period == df[df['material_id'] == material][period_type].min():
                sales_growth.append(0)
                quantity_growth.append(0)
                previous_period = period
            else:
                previous_sales = df[(df['material_id'] == material) & (df[period_type] == previous_period)]['sales'].iloc[0]
                current_sales = df[(df['material_id'] == material) & (df[period_type] == period)]['sales'].iloc[0]
                sales_change = (current_sales - previous_sales)/previous_sales
                sales_growth.append(sales_change)

                previous_quantity = df[(df['material_id'] == material) & (df[period_type] == previous_period)]['quantity_sold'].iloc[0]
                current_quantity = df[(df['material_id'] == material) & (df[period_type] == period)]['quantity_sold'].iloc[0]
                quantity_change = (current_quantity - previous_quantity)/previous_quantity
                quantity_growth.append(quantity_change)
                
                previous_period = period

    df['sales_growth'] = sales_growth
    df['quantity_growth'] = quantity_growth
    
    return df

# COMMAND ----------

mom_df = growth(mom_df)
qoq_df = growth(qoq_df)
yoy_df = growth(yoy_df)

# COMMAND ----------

mom_200ml_df = mom_df[(mom_df['volume'] == 200.0) & (mom_df['item_count'] == '1')].reset_index(drop=True)
mom_330ml_df = mom_df[(mom_df['volume'] == 330.0) & (mom_df['item_count'] == '1')].reset_index(drop=True)

qoq_200ml_df = qoq_df[(qoq_df['volume'] == 200.0) & (qoq_df['item_count'] == '1')].reset_index(drop=True)
qoq_330ml_df = qoq_df[(qoq_df['volume'] == 330.0) & (qoq_df['item_count'] == '1')].reset_index(drop=True)

yoy_200ml_df = yoy_df[(yoy_df['volume'] == 200.0) & (yoy_df['item_count'] == '1')].reset_index(drop=True)
yoy_330ml_df = yoy_df[(yoy_df['volume'] == 330.0) & (yoy_df['item_count'] == '1')].reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Average Growth Calculation

# COMMAND ----------

def avg_growth(df):
    avg_sales_growth = []
    avg_quantity_growth = []
    for material in df['material_id'].unique():
        sales_mean = df[df['material_id'] == material]['sales_growth'].iloc[1:].mean()
        quantity_mean = df[df['material_id'] == material]['quantity_growth'].iloc[1:].mean()
        
        if np.isnan(sales_mean):
            sales_mean = 0.0
        if np.isnan(quantity_mean):
            quantity_mean = 0.0
        
        avg_sales_growth.append(sales_mean)
        avg_quantity_growth.append(quantity_mean)

    avg_growth_df = pd.DataFrame({'material_id': df['material_id'].unique(),
                                'avg_sales_growth': avg_sales_growth,
                                'avg_quantity_growth': avg_quantity_growth})
    
    avg_growth_df['sales_growth_rank'] = avg_growth_df['avg_sales_growth'].rank(ascending=False)
    avg_growth_df['quanity_growth_rank'] = avg_growth_df['avg_quantity_growth'].rank(ascending=False)

    temp = df.groupby('material_id')[['sales', 'quantity_sold']].sum().reset_index()
    total_sales = temp['sales'].sum()
    total_quantity = temp['quantity_sold'].sum()
    temp['sales_contri'] = temp['sales'] / total_sales
    temp['quantity_contri'] = temp['quantity_sold'] / total_quantity

    avg_growth_df = pd.merge(avg_growth_df, temp[['material_id', 'sales_contri', 'quantity_contri']], on='material_id', how = 'left')

    avg_growth_df['wt_avg_sales_growth'] = avg_growth_df['avg_sales_growth'] * avg_growth_df['sales_contri']
    avg_growth_df['wt_avg_quantity_growth'] = avg_growth_df['avg_quantity_growth'] * avg_growth_df['quantity_contri']

    avg_growth_df['wt_avg_sales_growth_rank'] = avg_growth_df['wt_avg_sales_growth'].rank(ascending=False)
    avg_growth_df['wt_avg_quanity_growth_rank'] = avg_growth_df['wt_avg_quantity_growth'].rank(ascending=False)

    return avg_growth_df

# COMMAND ----------

mom_200ml_avg_growth_df = mom_200ml_df.copy()
mom_330ml_avg_growth_df = mom_330ml_df.copy()
qoq_200ml_avg_growth_df = qoq_200ml_df.copy()
qoq_330ml_avg_growth_df = qoq_330ml_df.copy()
yoy_200ml_avg_growth_df = yoy_200ml_df.copy()
yoy_330ml_avg_growth_df = yoy_330ml_df.copy()

mom_200ml_avg_growth_df = avg_growth(mom_200ml_avg_growth_df)
mom_330ml_avg_growth_df = avg_growth(mom_330ml_avg_growth_df)
qoq_200ml_avg_growth_df = avg_growth(qoq_200ml_avg_growth_df)
qoq_330ml_avg_growth_df = avg_growth(qoq_330ml_avg_growth_df)
yoy_200ml_avg_growth_df = avg_growth(yoy_200ml_avg_growth_df)
yoy_330ml_avg_growth_df = avg_growth(yoy_330ml_avg_growth_df)

# COMMAND ----------

mom_200ml_avg_growth_df = pd.merge(mom_200ml_avg_growth_df, cwd_df[['material_id', 'cwd']], on='material_id', how='inner')
qoq_200ml_avg_growth_df = pd.merge(qoq_200ml_avg_growth_df, cwd_df[['material_id', 'cwd']], on='material_id', how='inner')
yoy_200ml_avg_growth_df = pd.merge(yoy_200ml_avg_growth_df, cwd_df[['material_id', 'cwd']], on='material_id', how='inner')
mom_330ml_avg_growth_df = pd.merge(mom_330ml_avg_growth_df, cwd_df[['material_id', 'cwd']], on='material_id', how='inner')
qoq_330ml_avg_growth_df = pd.merge(qoq_330ml_avg_growth_df, cwd_df[['material_id', 'cwd']], on='material_id', how='inner')
yoy_330ml_avg_growth_df = pd.merge(yoy_330ml_avg_growth_df, cwd_df[['material_id', 'cwd']], on='material_id', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Periodly Sales Contribution Calculation

# COMMAND ----------

def periodly_contri(df):
    period_type = df.columns[1]
    periodly_sales_contri = []
    periodly_quantity_contri = []
    for material in df['material_id'].unique():
        for period in df[df['material_id'] == material][period_type].unique():
            material_sale = df[(df['material_id'] == material) & (df[period_type] == period)]['sales'].sum()
            period_total_sales = df[df[period_type] == period]['sales'].sum()
            periodly_sales_contri.append(material_sale / period_total_sales)

            material_quantity = df[(df['material_id'] == material) & (df[period_type] == period)]['quantity_sold'].sum()
            period_total_quantity = df[df[period_type] == period]['quantity_sold'].sum()
            periodly_quantity_contri.append(material_quantity / period_total_quantity)

    df['periodly_sales_contri'] = periodly_sales_contri
    df['periodly_quantity_contri'] = periodly_quantity_contri
    
    sales_contri_growth = []
    quantity_contri_growth = []
    for material in df['material_id'].unique():
        for period in df[df['material_id'] == material][period_type].unique():
            if period == df[df['material_id'] == material][period_type].min():
                sales_contri_growth.append(0)
                quantity_contri_growth.append(0)
                previous_period = period
            else:
                previous_sales_contri = df[(df['material_id'] == material) & (df[period_type] == previous_period)]['periodly_sales_contri'].iloc[0]
                current_sales_contri = df[(df['material_id'] == material) & (df[period_type] == period)]['periodly_sales_contri'].iloc[0]
                sales_contri_change = (current_sales_contri - previous_sales_contri)/previous_sales_contri
                sales_contri_growth.append(sales_contri_change)

                previous_quantity_contri = df[(df['material_id'] == material) & (df[period_type] == previous_period)]['periodly_quantity_contri'].iloc[0]
                current_quantity_contri = df[(df['material_id'] == material) & (df[period_type] == period)]['periodly_quantity_contri'].iloc[0]
                quantity_contri_change = (current_quantity_contri - previous_quantity_contri)/previous_quantity_contri
                quantity_contri_growth.append(quantity_contri_change)
                
                previous_period = period

    df['sales_contri_growth'] = sales_contri_growth
    df['quantity_contri_growth'] = quantity_contri_growth

    return df

# COMMAND ----------

mom_200ml_df = periodly_contri(mom_200ml_df)
qoq_200ml_df = periodly_contri(qoq_200ml_df)
yoy_200ml_df = periodly_contri(yoy_200ml_df)
mom_330ml_df = periodly_contri(mom_330ml_df)
qoq_330ml_df = periodly_contri(qoq_330ml_df)
yoy_330ml_df = periodly_contri(yoy_330ml_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #LULU DRINK.WATER BTL 200ML

# COMMAND ----------

# MAGIC %md
# MAGIC ##Month over Month

# COMMAND ----------

# MAGIC %md
# MAGIC ###Comparison With Average

# COMMAND ----------

avg_mom_200ml_df = mom_200ml_df.groupby('month_year')[['sales', 'quantity_sold']].mean().reset_index()
avg_mom_200ml_df['material_name'] = 'Overall Average'

temp = mom_200ml_df[mom_200ml_df['material_id'] == 1631281][['material_name', 'month_year', 'sales', 'quantity_sold']]
avg_mom_200ml_df = pd.concat([avg_mom_200ml_df, temp], ignore_index=True)

fig = px.line(avg_mom_200ml_df, x = "month_year", y = "sales", color = 'material_name', width = 900, height = 600)
fig.show()

fig2 = px.line(avg_mom_200ml_df, x = "month_year", y = "quantity_sold", color = 'material_name', width = 900, height = 600)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Growth Rank

# COMMAND ----------

mom_200ml_avg_growth_df[['wt_avg_sales_growth', 'wt_avg_quantity_growth']].mean()

# COMMAND ----------

mom_200ml_avg_growth_df[mom_200ml_avg_growth_df['material_id'] == 1631281]
# rank out of 18

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quarter over Quarter

# COMMAND ----------

# MAGIC %md
# MAGIC ###Comparison With Average

# COMMAND ----------

avg_qoq_200ml_df = qoq_200ml_df.groupby('quarter')[['sales', 'quantity_sold']].mean().reset_index()
avg_qoq_200ml_df['material_name'] = 'Overall Average'

temp = qoq_200ml_df[qoq_200ml_df['material_id'] == 1631281][['material_name', 'quarter', 'sales', 'quantity_sold']]
avg_qoq_200ml_df = pd.concat([avg_qoq_200ml_df, temp], ignore_index=True)

fig = px.bar(avg_qoq_200ml_df, x = "quarter", y = "sales", color = 'material_name', width = 900, height = 600, barmode='group')
fig.show()

fig2 = px.bar(avg_qoq_200ml_df, x = "quarter", y = "quantity_sold", color = 'material_name', width = 900, height = 600, barmode='group')
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Q3 vs Q4 Contribution

# COMMAND ----------

q3vsq4 = qoq_200ml_df.groupby('quarter')[['periodly_sales_contri', 'periodly_quantity_contri']].mean().reset_index()
q3vsq4['material_name'] = 'Overall Average'

temp = qoq_200ml_df[qoq_200ml_df['material_id'] == 1631281][['material_name', 'quarter', 'periodly_sales_contri', 'periodly_quantity_contri']]
q3vsq4 = pd.concat([q3vsq4, temp], ignore_index=True)

fig = px.bar(q3vsq4, x = "quarter", y = "periodly_sales_contri", color = 'material_name', width = 900, height = 600, barmode='group')
fig.show()

fig2 = px.bar(q3vsq4, x = "quarter", y = "periodly_quantity_contri", color = 'material_name', width = 900, height = 600, barmode='group')
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Growth Rank

# COMMAND ----------

qoq_200ml_avg_growth_df[['wt_avg_sales_growth', 'wt_avg_quantity_growth']].mean()

# COMMAND ----------

qoq_200ml_avg_growth_df[qoq_200ml_avg_growth_df['material_id'] == 1631281]
# rank out of 18

# COMMAND ----------

# MAGIC %md
# MAGIC ##Year over Year

# COMMAND ----------

# MAGIC %md
# MAGIC ###Comparison With Average

# COMMAND ----------

avg_yoy_200ml_df = yoy_200ml_df.groupby('year')[['sales', 'quantity_sold']].mean().reset_index()
avg_yoy_200ml_df['material_name'] = 'Overall Average'

temp = yoy_200ml_df[yoy_200ml_df['material_id'] == 1631281][['material_name', 'year', 'sales', 'quantity_sold']]
avg_yoy_200ml_df = pd.concat([avg_yoy_200ml_df, temp], ignore_index=True)

fig = px.bar(avg_yoy_200ml_df, x = "year", y = "sales", color = 'material_name', width = 900, height = 600, barmode='group')
fig.show()

fig2 = px.bar(avg_yoy_200ml_df, x = "year", y = "quantity_sold", color = 'material_name', width = 900, height = 600, barmode='group')
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Growth Rank

# COMMAND ----------

yoy_200ml_avg_growth_df[['wt_avg_sales_growth', 'wt_avg_quantity_growth']].mean()

# COMMAND ----------

yoy_200ml_avg_growth_df[yoy_200ml_avg_growth_df['material_id'] == 1631281]
# rank out of 18

# COMMAND ----------

# yoy_200ml_avg_growth_df.sort_values(by='sales_contri', ascending = False).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #LULU BOTTLED DRINK.WATER 330ML

# COMMAND ----------

# MAGIC %md
# MAGIC ##Month over Month

# COMMAND ----------

# MAGIC %md
# MAGIC ###Comparison With Average

# COMMAND ----------

avg_mom_330ml_df = mom_330ml_df.groupby('month_year')[['sales', 'quantity_sold']].mean().reset_index()
avg_mom_330ml_df['material_name'] = 'Overall Average'

temp = mom_330ml_df[mom_330ml_df['material_id'] == 595793][['material_name', 'month_year', 'sales', 'quantity_sold']]
avg_mom_330ml_df = pd.concat([avg_mom_330ml_df, temp], ignore_index=True)

fig = px.line(avg_mom_330ml_df, x = "month_year", y = "sales", color = 'material_name', width = 900, height = 600)
fig.show()

fig2 = px.line(avg_mom_330ml_df, x = "month_year", y = "quantity_sold", color = 'material_name', width = 900, height = 600)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Growth Rank

# COMMAND ----------

mom_330ml_avg_growth_df[['wt_avg_sales_growth', 'wt_avg_quantity_growth']].mean()

# COMMAND ----------

mom_330ml_avg_growth_df[mom_330ml_avg_growth_df['material_id'] == 595793]
# rank out of 43

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quarter over Quarter

# COMMAND ----------

# MAGIC %md
# MAGIC ###Comparison With Average

# COMMAND ----------

avg_qoq_330ml_df = qoq_330ml_df.groupby('quarter')[['sales', 'quantity_sold']].mean().reset_index()
avg_qoq_330ml_df['material_name'] = 'Overall Average'

temp = qoq_330ml_df[qoq_330ml_df['material_id'] == 595793][['material_name', 'quarter', 'sales', 'quantity_sold']]
avg_qoq_330ml_df = pd.concat([avg_qoq_330ml_df, temp], ignore_index=True)

fig = px.bar(avg_qoq_330ml_df, x = "quarter", y = "sales", color = 'material_name', width = 900, height = 600, barmode='group')
fig.show()

fig2 = px.bar(avg_qoq_330ml_df, x = "quarter", y = "quantity_sold", color = 'material_name', width = 900, height = 600, barmode='group')
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Q3 vs Q4 Contribution

# COMMAND ----------

q3vsq4 = qoq_330ml_df.groupby('quarter')[['periodly_sales_contri', 'periodly_quantity_contri']].mean().reset_index()
q3vsq4['material_name'] = 'Overall Average'

temp = qoq_330ml_df[qoq_330ml_df['material_id'] == 595793][['material_name', 'quarter', 'periodly_sales_contri', 'periodly_quantity_contri']]
q3vsq4 = pd.concat([q3vsq4, temp], ignore_index=True)

fig = px.bar(q3vsq4, x = "quarter", y = "periodly_sales_contri", color = 'material_name', width = 900, height = 600, barmode='group')
fig.show()

fig2 = px.bar(q3vsq4, x = "quarter", y = "periodly_quantity_contri", color = 'material_name', width = 900, height = 600, barmode='group')
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Growth Rank

# COMMAND ----------

qoq_330ml_avg_growth_df[['wt_avg_sales_growth', 'wt_avg_quantity_growth']].mean()

# COMMAND ----------

qoq_330ml_avg_growth_df[qoq_330ml_avg_growth_df['material_id'] == 595793]
# rank out of 43

# COMMAND ----------

# MAGIC %md
# MAGIC ##Year over Year

# COMMAND ----------

# MAGIC %md
# MAGIC ###Comparison With Average

# COMMAND ----------

avg_yoy_330ml_df = yoy_330ml_df.groupby('year')[['sales', 'quantity_sold']].mean().reset_index()
avg_yoy_330ml_df['material_name'] = 'Overall Average'

temp = yoy_330ml_df[yoy_330ml_df['material_id'] == 595793][['material_name', 'year', 'sales', 'quantity_sold']]
avg_yoy_330ml_df = pd.concat([avg_yoy_330ml_df, temp], ignore_index=True)

fig = px.bar(avg_yoy_330ml_df, x = "year", y = "sales", color = 'material_name', width = 900, height = 600, barmode='group')
fig.show()

fig2 = px.bar(avg_yoy_330ml_df, x = "year", y = "quantity_sold", color = 'material_name', width = 900, height = 600, barmode='group')
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Growth Rank

# COMMAND ----------

yoy_330ml_avg_growth_df[['wt_avg_sales_growth', 'wt_avg_quantity_growth']].mean()

# COMMAND ----------

yoy_330ml_avg_growth_df[yoy_330ml_avg_growth_df['material_id'] == 595793]
# rank out of 43

# COMMAND ----------

# MAGIC %md
# MAGIC #Delisted vs Rejected

# COMMAND ----------

# MAGIC %md
# MAGIC ##Year over Year

# COMMAND ----------

yoy_vs_df = yoy_df[yoy_df['material_id'].isin([1631281, 595793, 858604])].reset_index(drop=True)

new_record = {'material_id': 858604, 'year': 'Y2', 'sales': 0.0, 'quantity_sold': 0.0, 'volume': 250.0, 'item_count': '1', 'material_name': 'LULU DRINKING WATER CUP 250ML'}

yoy_vs_df = pd.concat([yoy_vs_df, pd.DataFrame([new_record])], ignore_index=True)
yoy_vs_df = yoy_vs_df.sort_values(by = ['material_id', 'year']).reset_index(drop = True)

# COMMAND ----------

fig = px.line(yoy_vs_df, x = "year", y = "sales", color = 'material_name', width = 900, height = 600)
fig.show()

fig2 = px.line(yoy_vs_df, x = "year", y = "quantity_sold", color = 'material_name', width = 900, height = 600)
fig2.show()

# COMMAND ----------

# qoq_200ml_df.display()
# yoy_200ml_df.display()
# qoq_330ml_df.display()
# yoy_330ml_df.display()
# qoq_200ml_avg_growth_df.display()
# yoy_200ml_avg_growth_df.display()
# qoq_330ml_avg_growth_df.display()
# yoy_330ml_avg_growth_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


