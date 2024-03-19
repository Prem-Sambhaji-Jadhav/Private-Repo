# Databricks notebook source
# MAGIC %md
# MAGIC ###Requirements

# COMMAND ----------

# MAGIC %md
# MAGIC GP Report should have only the following two columns (column name should match as well):
# MAGIC 1. material_id - Integer data type
# MAGIC 2. GP with ChargeBack & Bin Promo (%) - Float data type
# MAGIC
# MAGIC Also, please make sure that the GP report is of the last 3 months period of the respective date range that you have taken for the analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #User Inputs

# COMMAND ----------

gp_path = "/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/gp_report_water.csv"

# Please make sure that the date range is EXACTLY 52 weeks (364 days) long, and does not include the present day's date
start_date = "2023-01-01"
end_date = "2023-12-30"

LOOKALIKES_START_DATE = "2022-01-30"
# This must be minimum 30 days after the earliest date available in the source table
# The end date will be the same as the end date of rolling 52 weeks period
# The day of the week should be the same as the day of the above start_date

category = "WATER"

currently_selling_range = 51 # <specified value> to 52nd week will be categorized as 'Currently selling'
low_no_supply_range = 44 # <specified value> to <currently_selling_range - 1> week will be categorized as 'Low/no supply'
# 1 to <low_no_supply_range - 1> week will be categorized as 'No recent sales'

sales_weightage = 50 # (% format) Weightage for sales contribution
quantity_weightage = 100 - sales_weightage

new_sku_date_range = 6 # Products launched in the last 6 months will be taken as new SKUs

delist_contri_threshold = 0.03 # (% format) Contribution threshold for 'Low' contribution category to be categorized as Delist
delist_product_count = 15 # (% format) Product count threshold for 'Low' contribution category that can be categorized as Delist

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Statements

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1, 2 - Category's weekly sales

# COMMAND ----------

# # Gathering weekly data of all materials that have had a sale in the last 52 weeks

# query = f"""
# WITH 52_weeks AS (SELECT material_id,
#                          business_day,
#                          ROUND(SUM(amount),0) AS sales,
#                          ROUND(SUM(quantity),0) AS quantity_sold
#                   FROM gold.pos_transactions AS t1
#                   JOIN gold.material_master AS t2
#                   ON t1.product_id = t2.material_id
#                   WHERE business_day BETWEEN '{start_date}' AND '{end_date}'
#                   AND category_name = '{category}'
#                   AND amount > 0
#                   AND quantity > 0
#                   GROUP BY material_id, business_day)

# SELECT material_id,
#         FLOOR(DATEDIFF(business_day, '{start_date}') / 7) + 1 AS week_number,
#         SUM(sales) AS total_sales,
#         SUM(quantity_sold) AS total_quantity_sold
# FROM 52_weeks
# GROUP BY material_id, week_number
# ORDER BY material_id, week_number
# """

# weeks52 = spark.sql(query).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3, 5 - SD_last Categorization

# COMMAND ----------

df = spark.sql("SELECT * FROM sandbox.pj_AO_52_weeks").toPandas().sort_values(by=['material_id', 'week_number']).reset_index(drop = True)

# COMMAND ----------

# Categorize products based on their last selling week

df['SD_last'] = df.groupby('material_id')['week_number'].transform('max')

currently_selling_threshold = 52 - currently_selling_range
low_no_supply_threshold = 52 - low_no_supply_range

df['category_SD_last'] = pd.cut(52 - df['SD_last'],
                                bins=[-float('inf'), currently_selling_threshold, low_no_supply_threshold, float('inf')],
                                labels=["Currently selling", "Low/no supply", "No recent sales"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 6 - Recent sales & quantity

# COMMAND ----------

# Calculate total sales and quantity of all SKUs in the last 12 weeks (41-52 = 12 weeks)

total_sales_12weeks = df[df['week_number'] >= 41].groupby('material_id')['total_sales'].sum().reset_index()
total_quantity_sold_12weeks = df[df['week_number'] >= 41].groupby('material_id')['total_quantity_sold'].sum().reset_index()

# Merge the summed values back into the original dataframe
df = pd.merge(df, total_sales_12weeks, on='material_id', how='left', suffixes=('', '2'))
df = pd.merge(df, total_quantity_sold_12weeks, on='material_id', how='left', suffixes=('', '2'))

# NaN values will be present for those materials whose last selling date was not in the last 12 weeks. Fill them with 0 in the new columns
df['total_sales_12weeks'] = df['total_sales2'].fillna(0)
df['total_quantity_sold_12weeks'] = df['total_quantity_sold2'].fillna(0)

df = df.drop(columns=['total_sales2', 'total_quantity_sold2'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 7 - Contribution Calculation

# COMMAND ----------

# Calculate the weighted combined contribution of sales and quantity

total_sales_12weeks_sum = df.groupby('material_id')['total_sales_12weeks'].head(1).sum()
total_quantity_sold_12weeks_sum = df.groupby('material_id')['total_quantity_sold_12weeks'].head(1).sum()

df['sales_contri'] = (df['total_sales_12weeks'] / total_sales_12weeks_sum)
df['quantity_contri'] = (df['total_quantity_sold_12weeks'] / total_quantity_sold_12weeks_sum)
df['CONTRI_sales_and_quantity'] = (df['sales_contri']*sales_weightage/100 + df['quantity_contri']*quantity_weightage/100)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 8 - Contribution Categories

# COMMAND ----------

# Calculate the cummulative sum of combined contribution of sales and quantity

material_wise_contri = df[['material_id', 'CONTRI_sales_and_quantity']].drop_duplicates()
material_wise_contri = material_wise_contri.sort_values(by = 'CONTRI_sales_and_quantity', ascending = False).reset_index(drop = True)
material_wise_contri['cumulative_contri'] = material_wise_contri['CONTRI_sales_and_quantity'].cumsum()

df = pd.merge(df, material_wise_contri, on = 'material_id', how = 'left', suffixes=('', '2'))
df = df.drop(columns = ['CONTRI_sales_and_quantity2'])

# COMMAND ----------

# # Categorise the SKUs into 3 categories based on their cummulative contribution

# top_contri_threshold = 0.7 # Threshold for products with top cumulative contribution to be categorized as 'Top'
# middle_contri_threshold = 0.29 # Threshold for products with middle cumulative contribution to be categorized as 'Middle'
# # Leftover percentage will be categorized as 'Low'

# bins = [0, top_contri_threshold, top_contri_threshold + middle_contri_threshold, float('inf')]
# labels = ['Top', 'Middle', 'Low']
# df['final_contri'] = pd.cut(df['cumulative_contri'], bins=bins, labels=labels, include_lowest=True)

# COMMAND ----------

# Categorise the SKUs into 3 categories based on deciles

no_contri_df = df[df['CONTRI_sales_and_quantity'] == 0].reset_index(drop=True)
df = df[~df['material_id'].isin(no_contri_df['material_id'].unique())]

bin_edges = [float('-inf'), df['CONTRI_sales_and_quantity'].quantile(0.5), df['CONTRI_sales_and_quantity'].quantile(0.8), float('inf')]
bin_labels = ['Low', 'Middle', 'Top']

df['final_contri'] = pd.cut(df['CONTRI_sales_and_quantity'], bins=bin_edges, labels=bin_labels, include_lowest=True)

no_contri_df['final_contri'] = 'Low'
df = pd.concat([df, no_contri_df], ignore_index=True)
df = df.sort_values(by=['material_id','week_number']).reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 4, 9, 11 - SD_first Categorization, New SKUs, Lookalikes

# COMMAND ----------

# # Gathering weekly data of all materials that have had a sale 1 month after the earliest sales data available in gold.pos_transactions

# query = f"""
# WITH all_weeks AS (SELECT material_id,
#                     business_day,
#                     ROUND(SUM(amount),0) AS sales, ROUND(SUM(quantity),0) AS quantity_sold
#             FROM gold.pos_transactions AS t1
#             JOIN gold.material_master AS t2
#             ON t1.product_id = t2.material_id
#             WHERE business_day BETWEEN '{LOOKALIKES_START_DATE}' AND '{end_date}'
#             AND category_name = '{category}'
#             AND amount > 0
#             AND quantity > 0
#             GROUP BY material_id, business_day)

# SELECT material_id,
#         FLOOR(DATEDIFF(business_day, '{LOOKALIKES_START_DATE}') / 7) + 1 AS week_number,
#         SUM(sales) AS total_sales, SUM(quantity_sold) AS total_quantity_sold
# FROM all_weeks
# GROUP BY material_id, week_number
# ORDER BY material_id, week_number
# """

# all_weeks = spark.sql(query).toPandas()

# COMMAND ----------

df2 = spark.sql("SELECT * FROM sandbox.pj_AO_all_weeks").toPandas().sort_values(by=['material_id', 'week_number']).reset_index(drop = True)

# COMMAND ----------

# Categorize products based on their first selling week
# SD_first - Rolling_52Wfirst >= 26 weeks
# 26 weeks > SD_first - Rolling_52Wfirst >= 13 weeks 
# 13 weeks > SD_first - Rolling_52Wfirst > 0 weeks
# SD_first - Rolling_52Wfirst <= 0 weeks (launched in the rolling 52 weeks period)

df2['SD_first'] = df2.groupby('material_id')['week_number'].transform('min')

Rolling_52Wfirst = df2['week_number'].max() - 51
df2['category_SD_first'] = pd.cut(Rolling_52Wfirst - df2['SD_first'], bins=[-float('inf'), 0, 12, 25, float('inf')],
                                labels=["New Launch", "Settling", "Stable", "Established"])

SD_first_df = df2[['material_id', 'category_SD_first', 'SD_first']].drop_duplicates()
df = pd.merge(df, SD_first_df, on='material_id', how='left')

# COMMAND ----------

# Create a separate list of new SKUs whose first selling date was in the last 182 days (26 weeks)

new_sku_date_range_weeks = int(new_sku_date_range/3 * 13)
new_sku_date_threshold = df2['week_number'].max() - new_sku_date_range_weeks + 1

new_sku = df[df['SD_first'] >= new_sku_date_threshold][['material_id', 'week_number', 'total_sales', 'total_quantity_sold', 'category_SD_last']].reset_index(drop = True)

# COMMAND ----------

# Lookalikes must have 26 weeks of sale in its initial 32 weeks period after its launch

min_week = df2.groupby('material_id')['week_number'].min()
max_week = df2.groupby('material_id')['week_number'].max()
consecutive_week_range = [np.arange(min_week, min_week + 32) for min_week in min_week]

materials_to_keep = []
for material_id, weeks_range in zip(min_week.index, consecutive_week_range):
    if df2[(df2['material_id'] == material_id) & (df2['week_number'].isin(weeks_range))].shape[0] >= 26:
        materials_to_keep.append(material_id)

df2 = df2[df2['material_id'].isin(materials_to_keep)]

# Keep only the first 26 weeks where sales took place for each material
df2 = df2.groupby('material_id').head(26).reset_index(drop = True)

# COMMAND ----------

# Lookalikes must be currently selling

df2 = df2.merge(df[['material_id', 'category_SD_last']], on='material_id', how='left').drop_duplicates().reset_index(drop = True)
df2 = df2[df2['category_SD_last'] == 'Currently selling'].drop('category_SD_last', axis=1)

# COMMAND ----------

# Lookalikes must not be new SKUs

new_sku_to_remove = df[df['SD_first'] >= 27][['material_id']].drop_duplicates()
df2 = df2.merge(new_sku_to_remove, on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
df2 = df2.reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 10 - New SKUs Contribution & Growth

# COMMAND ----------

# Calculate weighted combined contribution of sales and quantity for new SKUs

sales_contri = []
quantity_contri = []
contri_sales_quantity = []
for material in new_sku['material_id'].unique():
    min_week_number = new_sku[new_sku['material_id'] == material]['week_number'].min()
    
    total_sales = new_sku[new_sku['material_id'] == material]['total_sales'].sum()
    total_sales_all_sku = new_sku[new_sku['week_number'] >= min_week_number]['total_sales'].sum()
    sales_contri.append(total_sales / total_sales_all_sku)

    total_quantity = new_sku[new_sku['material_id'] == material]['total_quantity_sold'].sum()
    total_quantity_all_sku = new_sku[new_sku['week_number'] >= min_week_number]['total_quantity_sold'].sum()
    quantity_contri.append(total_quantity / total_quantity_all_sku)

    contri_sales_quantity.append(sales_contri[-1]*sales_weightage/100 + quantity_contri[-1]*quantity_weightage/100)


new_df = new_sku[['material_id']].drop_duplicates().reset_index(drop = True)
new_df['sales_contri'] = sales_contri
new_df['quantity_contri'] = quantity_contri
new_df['CONTRI_sales_and_quantity'] = contri_sales_quantity

new_sku = pd.merge(new_sku, new_df, on='material_id', how='left', suffixes=('', '2'))

# COMMAND ----------

# Calculate weekly weighted combined growth of sales and quantity for new SKUs

sales_growth = []
quantity_growth = []
for material in new_sku['material_id'].unique():
    for week in new_sku[new_sku['material_id'] == material]['week_number'].unique():
        if week == new_sku[new_sku['material_id'] == material]['week_number'].min():
            sales_growth.append(0)
            quantity_growth.append(0)
            previous_week = week
        
        else:
            previous_sales = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == previous_week)]['total_sales'].iloc[0]
            current_sales = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == week)]['total_sales'].iloc[0]
            sales_change = (current_sales - previous_sales)/previous_sales
            sales_growth.append(sales_change)

            previous_quantity = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == previous_week)]['total_quantity_sold'].iloc[0]
            current_quantity = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == week)]['total_quantity_sold'].iloc[0]
            quantity_change = (current_quantity - previous_quantity)/previous_quantity
            quantity_growth.append(quantity_change)
            
            previous_week = week

new_sku['sales_growth'] = sales_growth
new_sku['quantity_growth'] = quantity_growth
new_sku['GROWTH_sales_and_quantity'] = new_sku['sales_growth']*sales_weightage/100 + new_sku['quantity_growth']*quantity_weightage/100

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 12 - Lookalikes Growth

# COMMAND ----------

# Calculate weekly weighted combined growth of sales and quantity for lookalikes

sales_growth = []
quantity_growth = []
for material in df2['material_id'].unique():
    for week in df2[df2['material_id'] == material]['week_number'].unique():
        if week == df2[df2['material_id'] == material]['week_number'].min():
            sales_growth.append(0)
            quantity_growth.append(0)
            previous_week = week
        else:
            previous_sales = df2[(df2['material_id'] == material) & (df2['week_number'] == previous_week)]['total_sales'].iloc[0]
            current_sales = df2[(df2['material_id'] == material) & (df2['week_number'] == week)]['total_sales'].iloc[0]
            sales_change = (current_sales - previous_sales)/previous_sales
            sales_growth.append(sales_change)

            previous_quantity = df2[(df2['material_id'] == material) & (df2['week_number'] == previous_week)]['total_quantity_sold'].iloc[0]
            current_quantity = df2[(df2['material_id'] == material) & (df2['week_number'] == week)]['total_quantity_sold'].iloc[0]
            quantity_change = (current_quantity - previous_quantity)/previous_quantity
            quantity_growth.append(quantity_change)
            
            previous_week = week

df2['sales_growth'] = sales_growth
df2['quantity_growth'] = quantity_growth
df2['GROWTH_sales_and_quantity'] = df2['sales_growth']*sales_weightage/100 + df2['quantity_growth']*quantity_weightage/100

# COMMAND ----------

# Calculate the overall average growth of lookalikes over their respective 26 weeks period

avg_growth_lookalikes = []
for material in df2['material_id'].unique():
    avg_growth_lookalikes.append(df2[df2['material_id'] == material]['GROWTH_sales_and_quantity'].iloc[1:].mean())

avg_growth_lookalikes = sum(avg_growth_lookalikes)/len(avg_growth_lookalikes)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 13 - New SKU Buckets

# COMMAND ----------

# Calculate the average growth of each new SKU over their respective 26 weeks period

avg_growth_new_sku = []
for material in new_sku['material_id'].unique():
    mean = new_sku[new_sku['material_id'] == material]['GROWTH_sales_and_quantity'].iloc[1:].mean()
    
    if np.isnan(mean):
        mean = 0.0
    
    avg_growth_new_sku.append(mean)

new_sku_growth = pd.DataFrame({'material_id': new_sku['material_id'].unique(),
                               'avg_weekly_growth': avg_growth_new_sku})

SD_last_catg_and_contri = new_sku[['material_id', 'CONTRI_sales_and_quantity', 'category_SD_last']].drop_duplicates()
new_sku_growth = pd.merge(new_sku_growth, SD_last_catg_and_contri, on='material_id', how='left')

# COMMAND ----------

# Calculate contribution of the lowest contributing product of Middle SKUs

Contri_lowestM30 = df[df['final_contri'] == 'Middle']['CONTRI_sales_and_quantity'].min()

# Extract numpy arrays of new SKUs to be used in creating buckets

materials = new_sku_growth['material_id'].values
avg_weekly_growth = new_sku_growth['avg_weekly_growth'].values
category_SD_last = new_sku_growth['category_SD_last'].values
contri = new_sku_growth['CONTRI_sales_and_quantity'].values

# COMMAND ----------

# Categorize the new SKUs into buckets

buckets = []
for i in range(len(materials)):
    if (avg_weekly_growth[i] >= avg_growth_lookalikes) & (contri[i] >= Contri_lowestM30):
        buckets.append('Grow')
    
    elif (avg_weekly_growth[i] >= avg_growth_lookalikes) & (contri[i] < Contri_lowestM30):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] < avg_growth_lookalikes) & (contri[i] >= Contri_lowestM30):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] < avg_growth_lookalikes) & (contri[i] < Contri_lowestM30) & (category_SD_last[i] == 'Currently selling'):
        buckets.append('Observe')
    
    elif (avg_weekly_growth[i] < avg_growth_lookalikes) & (contri[i] < Contri_lowestM30) & (category_SD_last[i] != 'Currently selling'):
        buckets.append('Delist')

    else:
        buckets.append('None')

new_sku_growth['buckets'] = buckets

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 3

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 14 - Top SKUs Growth

# COMMAND ----------

# Calculate weekly weighted combined growth of sales and quantity of the last 12 weeks for all SKUs of the 52 weeks period

df_12weeks = df[df['week_number'] >= 41].reset_index(drop = True)

sales_growth = []
quantity_growth = []
for material in df_12weeks['material_id'].unique():
    for week in df_12weeks[df_12weeks['material_id'] == material]['week_number'].unique():
        if week == df_12weeks[df_12weeks['material_id'] == material]['week_number'].min():
            sales_growth.append(0)
            quantity_growth.append(0)
            previous_week = week
        else:
            previous_sales = df_12weeks[(df_12weeks['material_id'] == material) & (df_12weeks['week_number'] == previous_week)]['total_sales'].iloc[0]
            current_sales = df_12weeks[(df_12weeks['material_id'] == material) & (df_12weeks['week_number'] == week)]['total_sales'].iloc[0]
            sales_change = (current_sales - previous_sales)/previous_sales
            sales_growth.append(sales_change)

            previous_quantity = df_12weeks[(df_12weeks['material_id'] == material) & (df_12weeks['week_number'] == previous_week)]['total_quantity_sold'].iloc[0]
            current_quantity = df_12weeks[(df_12weeks['material_id'] == material) & (df_12weeks['week_number'] == week)]['total_quantity_sold'].iloc[0]
            quantity_change = (current_quantity - previous_quantity)/previous_quantity
            quantity_growth.append(quantity_change)
            
            previous_week = week

df_12weeks['sales_growth'] = sales_growth
df_12weeks['quantity_growth'] = quantity_growth
df_12weeks['GROWTH_sales_and_quantity'] = df_12weeks['sales_growth']*sales_weightage/100 + df_12weeks['quantity_growth']*quantity_weightage/100

# COMMAND ----------

# Calculate the average growth of the last 12 weeks of each Top contributing SKU over the 52 weeks period

top_contri_sku = df_12weeks[df_12weeks['final_contri'] == 'Top'][['material_id', 'week_number', 'GROWTH_sales_and_quantity']].reset_index(drop = True)

avg_growth_top_sku = []
for material in top_contri_sku['material_id'].unique():
    mean = top_contri_sku[top_contri_sku['material_id'] == material]['GROWTH_sales_and_quantity'].iloc[1:].mean()
    
    if np.isnan(mean):
        mean = 0.0
    
    avg_growth_top_sku.append(mean)

top_sku_growth = pd.DataFrame({'material_id': top_contri_sku['material_id'].unique(),
                               'avg_weekly_growth': avg_growth_top_sku})

avg_growth_top_sku = sorted(avg_growth_top_sku)
median_growth_top_sku = avg_growth_top_sku[len(avg_growth_top_sku)//2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 15 - Top SKUs Buckets

# COMMAND ----------

# Extract numpy arrays of Top SKUs to be used in creating buckets

materials = top_sku_growth['material_id'].values
avg_weekly_growth = top_sku_growth['avg_weekly_growth'].values

# COMMAND ----------

# Categorize the Top contributing SKUs into buckets

buckets = []
for i in range(len(materials)):
    if avg_weekly_growth[i] < median_growth_top_sku:
        buckets.append('Grow')
    
    else:
        buckets.append('Maintain')

top_sku_growth['buckets'] = buckets

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 4

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 16 - Middle SKUs Growth

# COMMAND ----------

# Calculate the average growth of the last 12 weeks of each Middle contributing SKU over the 52 weeks period

middle_contri_sku = df_12weeks[df_12weeks['final_contri'] == 'Middle'][['material_id', 'week_number', 'GROWTH_sales_and_quantity']].reset_index(drop = True)

avg_growth_middle_sku = []
for material in middle_contri_sku['material_id'].unique():
    mean = middle_contri_sku[middle_contri_sku['material_id'] == material]['GROWTH_sales_and_quantity'].iloc[1:].mean()
    
    if np.isnan(mean):
        mean = 0.0
    
    avg_growth_middle_sku.append(mean)

middle_sku_growth = pd.DataFrame({'material_id': middle_contri_sku['material_id'].unique(),
                               'avg_weekly_growth': avg_growth_middle_sku})

SD_last_category = df_12weeks[df_12weeks['final_contri'] == 'Middle'][['material_id', 'category_SD_last']].drop_duplicates()
middle_sku_growth = pd.merge(middle_sku_growth, SD_last_category, on='material_id', how='left')

avg_growth_middle_sku = sorted(avg_growth_middle_sku)
median_growth_middle_sku = avg_growth_middle_sku[len(avg_growth_middle_sku)//2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 17 - Store Penetration

# COMMAND ----------

# # Gathering data for number of stores where each product was sold in the last 12 weeks period

# query = f"""
# SELECT material_id, COUNT(DISTINCT store_id) AS num_stores
# FROM gold.pos_transactions AS t1
# JOIN gold.material_master AS t2
# ON t1.product_id = t2.material_id
# WHERE business_day BETWEEN DATE_ADD('{start_date}', 280) AND '{end_date}'
# AND category_name = '{category}'
# AND amount > 0
# AND quantity > 0
# GROUP BY material_id
# ORDER BY material_id
# """

# store_pnt = spark.sql(query).toPandas()

# COMMAND ----------

# # Calculating the total number of stores where a sale was made in the last 12 weeks

# query = f"""
# SELECT COUNT(DISTINCT store_id) AS total_stores
# FROM gold.pos_transactions AS t1
# JOIN gold.material_master AS t2
# ON t1.product_id = t2.material_id
# WHERE business_day BETWEEN DATE_ADD('{start_date}', 280) AND '{end_date}'
# AND category_name = '{category}'
# AND ROUND(amount,0) > 0
# AND quantity > 0
# """

# total_store_count = spark.sql(query).toPandas()

# COMMAND ----------

store_counts = spark.sql("SELECT * FROM sandbox.pj_AO_products_store_count").toPandas().sort_values(by = 'material_id').reset_index(drop=True)
total_stores = spark.sql("SELECT * FROM sandbox.pj_AO_total_store_count").toPandas().values[0,0]

# COMMAND ----------

# Calculate the store penetration of each SKU

materials_to_add = pd.merge(df['material_id'].drop_duplicates(), store_counts['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)
materials_to_add['num_stores'] = 0
store_counts = pd.concat([store_counts, materials_to_add], ignore_index=True)

store_counts['store_pnt'] = (store_counts['num_stores']/total_stores).round(2)

bins = [0, 0.3, 0.7, 1.0]
labels = ['Low', 'Middle', 'High']
store_counts['category_store_pnt'] = pd.cut(store_counts['store_pnt'], bins=bins, labels=labels, include_lowest=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 18 - Middle SKUs Buckets

# COMMAND ----------

# Extract numpy arrays of Middle SKUs to be used in creating buckets

middle_sku_growth = pd.merge(middle_sku_growth, store_counts, on='material_id', how='left')

materials = middle_sku_growth['material_id'].values
avg_weekly_growth = middle_sku_growth['avg_weekly_growth'].values
category_SD_last = middle_sku_growth['category_SD_last'].values
category_store_pnt = middle_sku_growth['category_store_pnt'].values

# COMMAND ----------

# Categorize the Middle contributing SKUs into buckets

buckets = []
for i in range(len(materials)):
    if (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] == 'Currently selling') & (category_store_pnt[i] != 'High'):
        buckets.append('Grow')
    
    elif (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] == 'Currently selling') & (category_store_pnt[i] == 'High'):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] == 'Low/no supply') & (category_store_pnt[i] == 'High'):
        buckets.append('Observe')
    
    elif (avg_weekly_growth[i] < median_growth_middle_sku):
        buckets.append('Observe')
    
    elif (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] == 'No recent sales'):
        buckets.append('Observe')
    
    else:
        buckets.append('None')

middle_sku_growth['buckets'] = buckets

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 19 - Low SKUs Growth

# COMMAND ----------

# Calculate the average growth of the last 12 weeks of each Low contributing SKU over the 52 weeks period

low_contri_sku = df_12weeks[df_12weeks['final_contri'] == 'Low'][['material_id', 'week_number', 'GROWTH_sales_and_quantity', 'CONTRI_sales_and_quantity', 'sales_contri', 'cumulative_contri']].reset_index(drop = True)

avg_growth_low_sku = []
for material in low_contri_sku['material_id'].unique():
    mean = low_contri_sku[low_contri_sku['material_id'] == material]['GROWTH_sales_and_quantity'].iloc[1:].mean()
    
    if np.isnan(mean):
        mean = 0.0
    
    avg_growth_low_sku.append(mean)

low_sku_growth = pd.DataFrame({'material_id': low_contri_sku['material_id'].unique(),
                               'avg_weekly_growth': avg_growth_low_sku})

materials_to_add = pd.merge(df[df['final_contri'] == 'Low']['material_id'].drop_duplicates(), low_sku_growth['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)
materials_to_add['avg_weekly_growth'] = 0
low_sku_growth = pd.concat([low_sku_growth, materials_to_add], ignore_index=True)

SD_last_catg_cumulative_contri = df[df['final_contri'] == 'Low'][['material_id', 'category_SD_last', 'CONTRI_sales_and_quantity', 'sales_contri', 'cumulative_contri']].drop_duplicates()
low_sku_growth = pd.merge(low_sku_growth, SD_last_catg_cumulative_contri, on='material_id', how='left')
low_sku_growth = pd.merge(low_sku_growth, store_counts, on='material_id', how='left')

avg_growth_low_sku = sorted(avg_growth_low_sku)
median_growth_low_sku = avg_growth_low_sku[len(avg_growth_low_sku)//2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 20 - Low SKUs Buckets

# COMMAND ----------

# Extract numpy arrays of Low SKUs to be used in creating buckets

materials = low_sku_growth['material_id'].values
avg_weekly_growth = low_sku_growth['avg_weekly_growth'].values
category_SD_last = low_sku_growth['category_SD_last'].values
category_store_pnt = low_sku_growth['category_store_pnt'].values

# COMMAND ----------

# Categorize the Low contributing SKUs into buckets

buckets = []
for i in range(len(materials)):
    if (avg_weekly_growth[i] >= median_growth_low_sku) & (category_SD_last[i] == 'Currently selling') & (category_store_pnt[i] != 'Low'):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] >= median_growth_low_sku) & (category_SD_last[i] == 'Currently selling') & (category_store_pnt[i] == 'Low'):
        buckets.append('Observe')
    
    elif (avg_weekly_growth[i] >= median_growth_low_sku) & (category_SD_last[i] != 'Currently selling'):
        buckets.append('Observe')

    elif (avg_weekly_growth[i] < median_growth_low_sku) & (category_SD_last[i] == 'Currently selling') & (category_store_pnt[i] == 'Low'):
        buckets.append('Observe')
    
    elif (avg_weekly_growth[i] < median_growth_low_sku) & (category_SD_last[i] != 'Currently selling'):
        buckets.append('Delist')
    
    elif (avg_weekly_growth[i] < median_growth_low_sku) & (category_SD_last[i] == 'Currently selling') & (category_store_pnt[i] != 'Low'):
        buckets.append('Delist')
    
    else:
        buckets.append('None')

low_sku_growth['buckets'] = buckets

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 21 - Delist Categorization

# COMMAND ----------

allowed_product_count = total_sales_12weeks['material_id'].nunique()*delist_product_count//100

materials_12weeks = total_sales_12weeks['material_id'].unique()
materials_to_delist = low_sku_growth[(low_sku_growth['material_id'].isin(materials_12weeks)) &
                                     (low_sku_growth['cumulative_contri'] > (1 - delist_contri_threshold/100))]['material_id'].values
delist_materials_count = len(materials_to_delist)

if delist_materials_count > allowed_product_count:
    print(f"The number of products that can be delisted are {delist_materials_count}, which is greater than {delist_product_count}% of the total number of products ({allowed_product_count}). Please decrease the contribution threshold ({delist_contri_threshold}%) or increase the product count percentage ({delist_product_count})")

else:
    for i in range(len(materials)):
        if materials[i] in materials_to_delist:
            low_sku_growth.at[i, 'buckets'] = 'Delist'

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 6

# COMMAND ----------

# MAGIC %md
# MAGIC ##GP & Sales of 6 Months

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS sandbox.pj_ao_6_months_sales;

# CREATE TABLE sandbox.pj_ao_6_months_sales AS (
#   SELECT region_name, material_id, MONTH(business_day) AS month_year, ROUND(SUM(amount),0) AS total_sales
#   FROM gold.store_master AS t1
#   JOIN gold.pos_transactions AS t2
#   ON t1.store_id = t2.store_id
#   JOIN gold.material_master AS t3
#   ON t2.product_id = t3.material_id
#   WHERE category_name = 'WATER'
#   AND business_day BETWEEN "2023-07-01" AND "2023-09-30"
#   AND ROUND(amount) > 0
#   AND quantity > 0
#   GROUP BY region_name, material_id, month_year
#   ORDER BY region_name, material_id, month_year
# )

# COMMAND ----------

# df_6_months = spark.sql("SELECT * FROM sandbox.pj_ao_6_months_sales").toPandas()

# COMMAND ----------

# gp_report_6_months = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/gp_report_water_6_months.csv")
# gp_report_6_months.rename(columns={'GP with ChargeBack & Bin Promo (%)': 'gp_perc'}, inplace=True)
# gp_report_6_months['month_year'] = gp_report_6_months['month_year'].replace(['Jul-23', 'Aug-23', 'Sep-23'], [7, 8, 9])

# COMMAND ----------

# df_6_months['region_name'] = df_6_months['region_name'].replace(['ABU DHABI', 'AL AIN', 'DUBAI', 'SHARJAH'], ['AUH', 'ALN', 'DXB', 'SHJ'])

# df_6_months = pd.merge(df_6_months, gp_report_6_months, how = 'left', on = ['region_name', 'material_id', 'month_year'], suffixes=('', '2'))

# COMMAND ----------

# df_6_months['gp_value'] = df_6_months['gp_perc'] * df_6_months['total_sales'] / 100
# df_6_months = df_6_months.groupby('material_id')['gp_value'].sum().reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 22 - GP Contribution

# COMMAND ----------

# MAGIC %md
# MAGIC GP Report should have only the following two columns (column name should match as well):
# MAGIC 1. material_id - Integer data type
# MAGIC 2. GP with ChargeBack & Bin Promo (%) - Float data type
# MAGIC
# MAGIC Also, please make sure that the GP report is of the last 3 months period of the respective date range that you have taken for the analysis

# COMMAND ----------

gp_report = pd.read_csv(gp_path)
gp_report = gp_report.sort_values(by = 'material_id').reset_index(drop = True)
gp_report.rename(columns={'GP with ChargeBack & Bin Promo (%)': 'gp_perc'}, inplace=True)

# COMMAND ----------

# Add the extra materials from the rolling 52 weeks period into the gp_report and set their GP as 0
# This is because they had no sales in the last 12 weeks period

materials_to_add = pd.merge(df['material_id'].drop_duplicates(), gp_report['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)

materials_to_add['gp_perc'] = 0

gp_report = pd.concat([gp_report, materials_to_add], ignore_index=True)

# COMMAND ----------

# Calculate the GP value

total_sales_12weeks = df[df['week_number'] >= 41].groupby('material_id')['total_sales'].sum().reset_index()

gp_report['gp_value'] = gp_report['gp_perc'] * total_sales_12weeks['total_sales']/100
gp_report['gp_value'] = gp_report['gp_value'].fillna(0)
# gp_report = pd.merge(df_6_months, gp_report, on='material_id', how='outer', suffixes=('', '2'))
# gp_report['gp_value'] = gp_report['gp_value'].fillna(0)
# gp_report['gp_value'] = gp_report['gp_value'] + gp_report['gp_value2']
gp_report['gp_value_positives'] = gp_report['gp_value'].apply(lambda x: max(0, x)) # Replace negative values with 0
gp_report = gp_report.drop(columns = ['gp_perc'])
gp_report = gp_report.sort_values(by='material_id').reset_index(drop = True)

# COMMAND ----------

# Calculate the GP contribution

total_gp_value = gp_report['gp_value_positives'].sum()
gp_report['gp_contri'] = gp_report['gp_value_positives'] / total_gp_value

# COMMAND ----------

# Calculate the cummulative sum of GP contribution

gp_contri_df = gp_report[['material_id', 'gp_contri']]
gp_contri_df = gp_contri_df.sort_values(by = 'gp_contri', ascending = False).reset_index(drop = True)
gp_contri_df['cumulative_contri'] = gp_contri_df['gp_contri'].cumsum()

gp_report = pd.merge(gp_report, gp_contri_df, on = 'material_id', how = 'left', suffixes=('', '2'))
gp_report = gp_report.drop(columns = ['gp_contri2'])

# COMMAND ----------

# Categorise the SKUs into 3 categories based on deciles

no_contri_df = df[df['CONTRI_sales_and_quantity'] == 0]['material_id'].unique()
no_contri_df = gp_report[gp_report['material_id'].isin(no_contri_df)]
gp_report = gp_report[~gp_report['material_id'].isin(no_contri_df['material_id'].unique())]

bin_edges = [float('-inf'), gp_report['gp_contri'].quantile(0.5), gp_report['gp_contri'].quantile(0.8), float('inf')]
bin_labels = ['Low', 'Middle', 'Top']

gp_report['category_contri'] = pd.cut(gp_report['gp_contri'], bins=bin_edges, labels=bin_labels, include_lowest=True)

no_contri_df['category_contri'] = 'Low'
gp_report = pd.concat([gp_report, no_contri_df], ignore_index=True)
gp_report = gp_report.sort_values(by=['material_id']).reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 23 - New Buckets for all SKUs

# COMMAND ----------

# Extract numpy arrays of Top SKUs to be used in creating the new buckets

top_materials = top_sku_growth['material_id'].values
top_buckets = top_sku_growth['buckets'].values
gp_top_df = gp_report[gp_report['material_id'].isin(top_materials)]
gp_top_materials = gp_top_df['material_id'].values
gp_top_category_contris = gp_top_df['category_contri'].values

# COMMAND ----------

# Categorize the Top contributing SKUs into the new buckets based on their GP contribution

new_buckets = []
for i in range(len(top_materials)):
    if (top_buckets[i] == 'Grow') & (gp_top_category_contris[i] == 'Low'):
        new_buckets.append('Maintain')
    
    elif (top_buckets[i] == 'Maintain') & (gp_top_category_contris[i] == 'Top'):
        new_buckets.append('Grow')
    
    else:
        new_buckets.append(top_buckets[i])

top_sku_growth['new_buckets'] = new_buckets

# COMMAND ----------

# Extract numpy arrays of Middle SKUs to be used in creating the new buckets

middle_materials = middle_sku_growth['material_id'].values
middle_buckets = middle_sku_growth['buckets'].values
gp_middle_df = gp_report[gp_report['material_id'].isin(middle_materials)]
gp_middle_materials = gp_middle_df['material_id'].values
gp_middle_category_contris = gp_middle_df['category_contri'].values

# COMMAND ----------

# Categorize the Middle contributing SKUs into the new buckets based on their GP contribution

new_buckets = []
for i in range(len(middle_materials)):
    if (middle_buckets[i] == 'Grow') & (gp_middle_category_contris[i] == 'Low'):
        new_buckets.append('Maintain')
    
    elif (middle_buckets[i] == 'Maintain') & (gp_middle_category_contris[i] == 'Top'):
        new_buckets.append('Grow')
    
    elif (middle_buckets[i] == 'Maintain') & (gp_middle_category_contris[i] == 'Low'):
        new_buckets.append('Observe')
    
    elif (middle_buckets[i] == 'Observe') & (gp_middle_category_contris[i] == 'Top'):
        new_buckets.append('Maintain')
    
    else:
        new_buckets.append(middle_buckets[i])

middle_sku_growth['new_buckets'] = new_buckets

# COMMAND ----------

# Extract numpy arrays of Low SKUs to be used in creating the new buckets

low_materials = low_sku_growth['material_id'].values
low_buckets = low_sku_growth['buckets'].values
gp_low_df = gp_report[gp_report['material_id'].isin(low_materials)]
gp_low_materials = gp_low_df['material_id'].values
gp_low_category_contris = gp_low_df['category_contri'].values

# COMMAND ----------

# Categorize the Low contributing SKUs into the new buckets based on their GP contribution

category_store_pnt = low_sku_growth['category_store_pnt'].values

new_buckets = []
for i in range(len(low_materials)):
    if (low_buckets[i] == 'Maintain') & (gp_low_category_contris[i] == 'Low') & (category_store_pnt[i] != 'Low'):
        new_buckets.append('Observe')
    
    elif (low_buckets[i] == 'Observe') & (gp_low_category_contris[i] == 'Top') & (category_store_pnt[i] != 'Low'):
        new_buckets.append('Maintain')
    
    elif (low_buckets[i] == 'Delist') & (gp_low_category_contris[i] != 'Low') & (category_store_pnt[i] != 'Low'):
        new_buckets.append('Observe')

    else:
        new_buckets.append(low_buckets[i])

low_sku_growth['new_buckets'] = new_buckets

# COMMAND ----------

# MAGIC %md
# MAGIC #Misc

# COMMAND ----------

all_products_catg = pd.concat([top_sku_growth[['material_id', 'new_buckets']], middle_sku_growth[['material_id', 'new_buckets']], low_sku_growth[['material_id', 'new_buckets']]], ignore_index=True)
all_products_catg_sdf = spark.createDataFrame(all_products_catg)
all_products_catg_sdf.createOrReplaceTempView('all_products_catg')

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/prem@loyalytics.in/pl_product_list.csv")
df1.createOrReplaceTempView('private_labels')

# COMMAND ----------

# %sql
# SELECT *
# FROM all_products_catg

# COMMAND ----------

# MAGIC %md
# MAGIC ##EDA Data Prep

# COMMAND ----------

# # Gathering weekly data of all materials with respect to the stores they had sales during the rolling 52 weeks

# query = """
# WITH cwd AS (SELECT material_id, business_day, store_id,
#                           ROUND(SUM(amount),0) AS sales
#             FROM gold.pos_transactions AS t1
#             JOIN gold.material_master AS t2
#             ON t1.product_id = t2.material_id
#             WHERE business_day BETWEEN '{}' AND '{}'
#             AND category_name = '{}'
#             AND amount > 0
#             AND quantity > 0
#             GROUP BY material_id, business_day, store_id),

# min_date_cte AS (SELECT MIN(business_day) AS min_business_day
#                 FROM cwd)

# SELECT material_id,
#         FLOOR(DATEDIFF(business_day, min_business_day) / 7) + 1 AS week_number,
#         store_id,
#         SUM(sales) AS total_sales
# FROM cwd, min_date_cte
# GROUP BY week_number, material_id, store_id
# ORDER BY week_number, material_id, store_id
# """.format(start_date, end_date, category)

# material_store_df = spark.sql(query).toPandas()
# material_store_df.to_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/ao_material_store_data.csv', index = False)

# COMMAND ----------

# # Calculate Category Weighted Distribution for each material in every week

# cwd_df = material_store_df.copy()
# cwd = []
# for week in cwd_df['week_number'].unique():
#   sales = cwd_df[cwd_df['week_number'] == week][['material_id', 'store_id', 'total_sales']]
#   all_store_sales = sales['total_sales'].sum()
#   for material in sales['material_id'].unique():
#     stores = sales[sales['material_id'] == material]['store_id'].unique()
#     material_store_sales = sales[sales['store_id'].isin(stores)]['total_sales'].sum()
#     cwd.append(material_store_sales/all_store_sales)

# cwd_df['material_store_count'] = cwd_df.groupby('material_id')['store_id'].transform('nunique')
# cwd_df['material_weekly_store_count'] = cwd_df.groupby(['material_id', 'week_number'])['store_id'].transform('nunique')
# cwd_df = cwd_df.groupby(['week_number', 'material_id']).agg({'total_sales': 'sum', 'material_store_count': 'mean', 'material_weekly_store_count': 'mean'}).reset_index()
# cwd_df['cwd'] = cwd

# COMMAND ----------

# # Gathering customer counts of all materials in the rolling 52 weeks period

# query = """
# WITH total_cust AS (SELECT COUNT(DISTINCT t3.customer_id) AS tot_cust,
#                             COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN t3.customer_id END) AS tot_vip,
#                             COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN t3.customer_id END) AS tot_freq
#                     FROM gold.pos_transactions AS t1
#                     JOIN gold.material_master AS t2
#                     ON t1.product_id = t2.material_id
#                     JOIN analytics.customer_segments AS t3
#                     ON t1.customer_id = t3.customer_id
#                     WHERE business_day BETWEEN '{}' AND '{}'
#                     AND category_name = '{}'
#                     AND ROUND(amount,0) > 0
#                     AND quantity > 0
#                     AND month_year = '202312'
#                     AND t3.country = 'uae'
#                     AND key = 'rfm'
#                     AND channel = 'pos'
# )

# SELECT material_id, material_name,
#         COUNT(DISTINCT t3.customer_id) AS cust,
#         COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN t3.customer_id END) AS vip_cust,
#         COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN t3.customer_id END) AS freq_cust,
#         tot_cust, tot_vip, tot_freq,
#         (cust/tot_cust) AS tot_cust_perc,
#         (vip_cust/tot_vip) AS vip_cust_perc,
#         (freq_cust/tot_freq) AS freq_cust_perc
# FROM total_cust, gold.pos_transactions AS t1
# JOIN gold.material_master AS t2
# ON t1.product_id = t2.material_id
# JOIN analytics.customer_segments AS t3
# ON t1.customer_id = t3.customer_id
# WHERE business_day BETWEEN '{}' AND '{}'
# AND category_name = '{}'
# AND ROUND(amount,0) > 0
# AND quantity > 0
# AND month_year = '202312'
# AND t3.country = 'uae'
# AND key = 'rfm'
# AND channel = 'pos'
# GROUP BY material_id, material_name, tot_cust, tot_vip, tot_freq
# ORDER BY material_id
# """.format(start_date, end_date, category, start_date, end_date, category)

# cust = spark.sql(query).toPandas()
# cust.to_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/ao_cust.csv', index = False)

# COMMAND ----------

# # Save all the weekly data of the materials into csv formats to be used in the EDA notebook

# weeks52.rename(columns={'total_sales': 'sale'}, inplace=True)
# weeks52.rename(columns={'total_quantity_sold': 'vol'}, inplace=True)

# weekly_data = pd.merge(weeks52, cwd_df, on=['material_id', 'week_number'], how = 'inner')
# weekly_data = weekly_data.drop(columns = 'total_sales')

# weekly_data.to_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/ao_products_weekly_data.csv', index = False)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS sandbox.pj_ao_12_months_sales;

# CREATE TABLE sandbox.pj_ao_12_months_sales AS (
#   SELECT region_name, material_id, MONTH(business_day) AS month_year, ROUND(SUM(amount),0) AS total_sales
#   FROM gold.store_master AS t1
#   JOIN gold.pos_transactions AS t2
#   ON t1.store_id = t2.store_id
#   JOIN gold.material_master AS t3
#   ON t2.product_id = t3.material_id
#   WHERE category_name = 'WATER'
#   AND business_day BETWEEN "2023-01-01" AND "2023-09-30"
#   AND ROUND(amount) > 0
#   AND quantity > 0
#   GROUP BY region_name, material_id, month_year
#   ORDER BY region_name, material_id, month_year
# )

# COMMAND ----------

df_12_months = spark.sql("SELECT * FROM sandbox.pj_ao_12_months_sales").toPandas()

# COMMAND ----------

gp_report_12_months = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/gp_report_water_12_months.csv")
gp_report_12_months.rename(columns={'GP with ChargeBack & Bin Promo (%)': 'gp_perc'}, inplace=True)
gp_report_12_months['month_year'] = gp_report_12_months['month_year'].replace(['Jan-23', 'Feb-23', 'Mar-23', 'Apr-23', 'May-23', 'Jun-23', 'Jul-23', 'Aug-23', 'Sep-23'], [1, 2, 3, 4, 5, 6, 7, 8, 9])

# COMMAND ----------

df_12_months['region_name'] = df_12_months['region_name'].replace(['ABU DHABI', 'AL AIN', 'DUBAI', 'SHARJAH'], ['AUH', 'ALN', 'DXB', 'SHJ'])

df_12_months = pd.merge(df_12_months, gp_report_12_months, how = 'left', on = ['region_name', 'material_id', 'month_year'], suffixes=('', '2'))

# COMMAND ----------

df_12_months['gp_value'] = df_12_months['gp_perc'] * df_12_months['total_sales'] / 100
df_12_months = df_12_months.groupby('material_id')['gp_value'].sum().reset_index()

# COMMAND ----------

temp = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/gp_report_water.csv")
temp = temp.sort_values(by = 'material_id').reset_index(drop = True)
temp.rename(columns={'GP with ChargeBack & Bin Promo (%)': 'gp_perc'}, inplace=True)

# COMMAND ----------

# Add the extra materials from the rolling 52 weeks period into the gp_report and set their GP as 0
# This is because they had no sales in the last 12 weeks period

materials_to_add = pd.merge(df['material_id'].drop_duplicates(), temp['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)

materials_to_add['gp_perc'] = 0

temp = pd.concat([temp, materials_to_add], ignore_index=True)

# COMMAND ----------

# Calculate the GP value

total_sales_12weeks = df[df['week_number'] >= 41].groupby('material_id')['total_sales'].sum().reset_index()

temp['gp_value'] = temp['gp_perc'] * total_sales_12weeks['total_sales']/100
temp['gp_value'] = temp['gp_value'].fillna(0)
df_12_months = pd.merge(df_12_months, temp, on='material_id', how='outer', suffixes=('', '2'))
df_12_months['gp_value'] = df_12_months['gp_value'].fillna(0)
df_12_months['gp_value'] = df_12_months['gp_value'] + df_12_months['gp_value2']
df_12_months['gp_value_positives'] = df_12_months['gp_value'].apply(lambda x: max(0, x)) # Replace negative values with 0
df_12_months = df_12_months.drop(columns = ['gp_perc', 'gp_value2'])
df_12_months = df_12_months.sort_values(by='material_id').reset_index(drop = True)

# COMMAND ----------

# Calculate the GP contribution

total_gp_value = df_12_months['gp_value_positives'].sum()
df_12_months['gp_contri'] = df_12_months['gp_value_positives'] / total_gp_value

# COMMAND ----------

# df_12_months = pd.merge(df_12_months, all_products_catg, on='material_id', how = 'inner')

# df_12_months.rename(columns={'gp_value': 'GP'}, inplace=True)
# df_12_months[['material_id', 'GP', 'gp_value_positives', 'gp_contri', 'new_buckets']].to_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/ao_gp.csv', index = False)

# COMMAND ----------

# a = timeline.copy()

# b = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/water/ao_gp_2022.csv")
# c = b.copy()
# b = b[b['new_buckets'] != 'Delist']
# b.rename(columns={'new_buckets': 'buckets'}, inplace=True)
# b = b[['material_id', 'buckets']].reset_index(drop = True)

# b = pd.merge(b, all_products_catg, on='material_id', how='outer')

# ma = a[a['SD_first'] >= 49]['material_id'].unique()

# b[(b['buckets'].isnull() == False) | (b['material_id'].isin(ma))]['new_buckets'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ##New Rule (Delist -> Observe)

# COMMAND ----------

# Extract all the delist SKUs along with their calculated metrics
# 980147, 1012937, 1546450, 1573921

new_rule_df = df[['material_id', 'week_number', 'total_sales', 'total_quantity_sold', 'total_sales_12weeks', 'CONTRI_sales_and_quantity']]
new_rule_df = pd.merge(new_rule_df, low_sku_growth[['material_id', 'new_buckets']], on='material_id', how='left')

new_rule_df = new_rule_df[new_rule_df['new_buckets'] == 'Delist'].reset_index(drop=True)

new_rule_df = pd.merge(new_rule_df, gp_report[['material_id', 'gp_value']], on='material_id', how='left')

new_rule_df = new_rule_df.drop(columns='new_buckets')
new_rule_df.rename(columns={'gp_value': 'gp_Q4'}, inplace=True)
new_rule_df.rename(columns={'total_sales_12weeks': 'total_sales_Q4'}, inplace=True)
new_rule_df.rename(columns={'CONTRI_sales_and_quantity': 'CONTRI_Q4'}, inplace=True)

# COMMAND ----------

# Calculate the Q3 sales and quantity

total_sales_Q3 = new_rule_df[(new_rule_df['week_number'] >= 27) & (new_rule_df['week_number'] <= 40)].groupby('material_id')['total_sales'].sum().reset_index()
total_quantity_sold_Q3 = new_rule_df[(new_rule_df['week_number'] >= 27) & (new_rule_df['week_number'] <= 40)].groupby('material_id')['total_quantity_sold'].sum().reset_index()

new_rule_df = pd.merge(new_rule_df, total_sales_Q3, on='material_id', how='left', suffixes=('', '2'))
new_rule_df = pd.merge(new_rule_df, total_quantity_sold_Q3, on='material_id', how='left', suffixes=('', '2'))

new_rule_df['total_sales_Q3'] = new_rule_df['total_sales2'].fillna(0)
new_rule_df['total_quantity_sold_Q3'] = new_rule_df['total_quantity_sold2'].fillna(0)

new_rule_df = new_rule_df.drop(columns=['total_sales2', 'total_quantity_sold2'])

# COMMAND ----------

# Assign ranks to the GP and sales of Q4

f = new_rule_df[['material_id', 'gp_Q4', 'total_sales_Q4']].drop_duplicates().reset_index(drop=True)
f['gp_rank_Q4'] = f['gp_Q4'].rank(ascending=False)
f['sales_rank_Q4'] = f['total_sales_Q4'].rank(ascending=False)

new_rule_df = pd.merge(new_rule_df, f[['material_id', 'gp_rank_Q4', 'sales_rank_Q4']], on='material_id', how='left')

# COMMAND ----------

# Calculate an growth from Q4 sales vs Q3 sales

new_rule_df['sales_growth_Q4_vs_Q3'] = (new_rule_df['total_sales_Q4'] - new_rule_df['total_sales_Q3'])/new_rule_df['total_sales_Q3']

new_rule_df['sales_growth_Q4_vs_Q3'] = new_rule_df['sales_growth_Q4_vs_Q3'].replace(float('inf'), 0)
new_rule_df['sales_growth_Q4_vs_Q3'] = new_rule_df['sales_growth_Q4_vs_Q3'].fillna(0)

# COMMAND ----------

# Calculate the weighted combined contribution of sales and quantity for Q3

total_sales_Q3_sum = new_rule_df.groupby('material_id')['total_sales_Q3'].head(1).sum()
total_quantity_sold_Q3_sum = new_rule_df.groupby('material_id')['total_quantity_sold_Q3'].head(1).sum()

new_rule_df['sales_contri_Q3'] = (new_rule_df['total_sales_Q3'] / total_sales_Q3_sum)
new_rule_df['quantity_contri_Q3'] = (new_rule_df['total_quantity_sold_Q3'] / total_quantity_sold_Q3_sum)
new_rule_df['CONTRI_Q3'] = (new_rule_df['sales_contri_Q3']*sales_weightage/100 + new_rule_df['quantity_contri_Q3']*quantity_weightage/100)

# COMMAND ----------

# Calculate average growth, average contribution, and growth in contribution
# 980147, 1012937, 1546450, 1573921

growth_avg = new_rule_df.drop(columns=['week_number', 'total_sales']).drop_duplicates()
growth_avg = growth_avg[growth_avg['gp_rank_Q4'] <= 15]['sales_growth_Q4_vs_Q3'].mean()

new_rule_df2 = new_rule_df.drop(columns=['week_number', 'total_sales', 'total_quantity_sold']).drop_duplicates().reset_index(drop=True)
contri_avg = new_rule_df2[new_rule_df2['gp_rank_Q4'] <= 15]['CONTRI_Q4'].mean()
new_rule_df2['Contri_index_Q4'] = new_rule_df2['CONTRI_Q4'] / contri_avg
new_rule_df2['Contri_growth_Q4_vs_Q3'] = (new_rule_df2['CONTRI_Q4'] - new_rule_df2['CONTRI_Q3']) / new_rule_df2['CONTRI_Q3']

new_rule_df2 = new_rule_df2[['material_id', 'sales_growth_Q4_vs_Q3', 'Contri_index_Q4', 'Contri_growth_Q4_vs_Q3', 'gp_rank_Q4', 'sales_rank_Q4']]

new_rule_df2[(new_rule_df2['gp_rank_Q4'] <= 15) & (new_rule_df2['sales_growth_Q4_vs_Q3'] > growth_avg) & (new_rule_df2['sales_growth_Q4_vs_Q3'] > 0)]

# COMMAND ----------

# # New SKUs that are delisted

# p = new_sku_growth[['material_id']]
# p = pd.merge(p, df[['material_id', 'CONTRI_sales_and_quantity', 'final_contri']].drop_duplicates(), on = 'material_id', how='left')
# p = p[p['final_contri'] == 'Low'].reset_index(drop=True)
# p = pd.merge(p, low_sku_growth[['material_id', 'new_buckets', 'category_SD_last', 'category_store_pnt', 'avg_weekly_growth']], on='material_id', how='left')
# p = pd.merge(p, gp_report[['material_id', 'category_contri']], on='material_id', how='left')
# print(median_growth_low_sku)
# p[p['new_buckets'] == 'Delist']

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


