# Databricks notebook source
# MAGIC %md
# MAGIC #Change Here

# COMMAND ----------

start_date = "2022-11-06" # Start date of the date range for the rolling 52 weeks period in your analysis
end_date = "2023-10-31" # End date of the date range for the rolling 52 weeks period in your analysis
# Please make sure that the date range is EXACTLY 52 weeks (364 days) long, and does not include the present day's date

lookalikes_start_date = "2022-01-30" # Start date of the date range for the lookalikes
# This must be minimum 30 days after the earliest date available in the source table and it should be a Sunday
# The end date will be the same as the end date of rolling 52 weeks period

category = "WATER" # Change this category as per your requirement

currently_selling_range = 51 # <specified value> to 52 weeks will be categorized as 'Currently selling'
low_no_supply_range = 44 # <specified value> to <currently_selling_range - 1> weeks will be categorized as 'Low/no supply'
# 1 to <low_no_supply_range - 1> weeks will be categorized as 'No recent sales'
# Note: 1st week is the start date of the date range taken and 52nd week is the end date

sales_weightage = 50 # Change the sales weightage (% format) for sales contribution as per your requirement
quantity_weightage = 100 - sales_weightage

top_contri_threshold = 0.7 # Threshold for products with top cumulative contribution to be categorized as 'Top'
middle_contri_threshold = 0.29 # Threshold for products with middle cumulative contribution to be categorized as 'Middle'
# Leftover percentage will be categorized as 'Low'

new_sku_date_range = 26 # Products launched in the last <specified value> weeks will be taken as new SKUs

delist_contri_threshold = 0.0 # Threshold for 'Low' contribution category of products to be categorized as Delist
delist_product_count = 15 # (% format) Threshold for 'Low' contribution of products that can be categorized as Delist

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

# Gathering weekly data of all materials that have had a sale in the last 52 weeks

query = """
WITH 52_weeks AS (SELECT material_id, business_day,
                    ROUND(SUM(amount),0) AS sales, ROUND(SUM(quantity),0) AS quantity_sold
            FROM gold.pos_transactions AS t1
            JOIN gold.material_master AS t2
            ON t1.product_id = t2.material_id
            WHERE business_day BETWEEN '{}' AND '{}'
            AND category_name = '{}'
            GROUP BY material_id, business_day),

min_date_cte AS (SELECT MIN(business_day) AS min_business_day
                FROM 52_weeks)

SELECT material_id,
        FLOOR(DATEDIFF(business_day, min_business_day) / 7) + 1 AS week_number,
        SUM(sales) AS total_sales, SUM(quantity_sold) AS total_quantity_sold
FROM 52_weeks, min_date_cte
GROUP BY material_id, week_number
""".format(start_date, end_date, category)

weeks52 = spark.sql(query).toPandas().sort_values(by=['material_id', 'week_number']).reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3, 5 - SD_last Categorization

# COMMAND ----------

df = weeks52.copy()

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
# total_quantity_sold_12weeks = df[df['week_number'] >= 41].groupby('material_id')['total_quantity_sold'].sum().reset_index()

# Merge the summed values back into the original dataframe
df = pd.merge(df, total_sales_12weeks, on='material_id', how='left', suffixes=('', '2'))
# df = pd.merge(df, total_quantity_sold_12weeks, on='material_id', how='left', suffixes=('', '2'))

# NaN values will be present for those materials whose last selling date was not in the last 12 weeks. Fill them with 0 in the new column
df['total_sales_12weeks'] = df['total_sales2'].fillna(0)
# df['total_quantity_sold_12weeks'] = df['total_quantity_sold2'].fillna(0)

df = df.drop(columns=['total_sales2'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 7 - Contribution Calculation

# COMMAND ----------

# Calculate the weighted combined contribution of sales and quantity

total_sales_12weeks_sum = df.groupby('material_id')['total_sales_12weeks'].head(1).sum()
# total_quantity_sold_12weeks_sum = df.groupby('material_id')['total_quantity_sold_12weeks'].head(1).sum()

df['sales_contri'] = (df['total_sales_12weeks'] / total_sales_12weeks_sum)
# df['quantity_contri'] = (df['total_quantity_sold_12weeks'] / total_quantity_sold_12weeks_sum)
# df['CONTRI_sales_and_quantity'] = (df['sales_contri']*sales_weightage/100 + df['quantity_contri']*quantity_weightage/100)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 8 - Contribution Categories

# COMMAND ----------

# Calculate the cummulative sum of combined contribution of sales and quantity

material_wise_contri = df[['material_id', 'sales_contri']].drop_duplicates()
material_wise_contri = material_wise_contri.sort_values(by = 'sales_contri', ascending = False).reset_index(drop = True)
material_wise_contri['cumulative_contri'] = material_wise_contri['sales_contri'].cumsum()

df = pd.merge(df, material_wise_contri, on = 'material_id', how = 'left', suffixes=('', '2'))
df = df.drop(columns = ['sales_contri2'])

# COMMAND ----------

# Categorise the SKUs into 3 categories based on their cummulative contribution

bins = [0, top_contri_threshold, top_contri_threshold + middle_contri_threshold, float('inf')]
labels = ['Top', 'Middle', 'Low']
df['final_contri'] = pd.cut(df['cumulative_contri'], bins=bins, labels=labels, include_lowest=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 4, 9, 11 - SD_first Categorization, New SKUs, Lookalikes

# COMMAND ----------

# Gathering weekly data of all materials that have had a sale 1 month after the earliest sales data available in gold.pos_transactions

query = """
WITH all_weeks AS (SELECT material_id, business_day,
                    ROUND(SUM(amount),0) AS sales, ROUND(SUM(quantity),0) AS quantity_sold
            FROM gold.pos_transactions AS t1
            JOIN gold.material_master AS t2
            ON t1.product_id = t2.material_id
            WHERE business_day BETWEEN '{}' AND '{}'
            AND category_name = '{}'
            GROUP BY material_id, business_day),

min_date_cte AS (SELECT MIN(business_day) AS min_business_day
                FROM all_weeks)

SELECT material_id,
        FLOOR(DATEDIFF(business_day, min_business_day) / 7) + 1 AS week_number,
        SUM(sales) AS total_sales, SUM(quantity_sold) AS total_quantity_sold
FROM all_weeks, min_date_cte
GROUP BY material_id, week_number
""".format(lookalikes_start_date, end_date, category)

all_weeks = spark.sql(query).toPandas().sort_values(by=['material_id', 'week_number']).reset_index(drop = True)

# COMMAND ----------

df2 = all_weeks.copy()

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

new_sku_date_threshold = df2['week_number'].max() - new_sku_date_range + 1

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
# quantity_contri = []
# contri_sales_quantity = []
for material in new_sku['material_id'].unique():
    min_week_number = new_sku[new_sku['material_id'] == material]['week_number'].min()
    
    total_sales = new_sku[new_sku['material_id'] == material]['total_sales'].sum()
    total_sales_all_sku = new_sku[new_sku['week_number'] >= min_week_number]['total_sales'].sum()
    sales_contri.append(total_sales / total_sales_all_sku)

    # total_quantity = new_sku[new_sku['material_id'] == material]['total_quantity_sold'].sum()
    # total_quantity_all_sku = new_sku[new_sku['week_number'] >= min_week_number]['total_quantity_sold'].sum()
    # quantity_contri.append(total_quantity / total_quantity_all_sku)

    # contri_sales_quantity.append(sales_contri[-1]*sales_weightage/100 + quantity_contri[-1]*quantity_weightage/100)


new_df = new_sku[['material_id']].drop_duplicates().reset_index(drop = True)
new_df['sales_contri'] = sales_contri
# new_df['quantity_contri'] = quantity_contri
# new_df['CONTRI_sales_and_quantity'] = contri_sales_quantity

new_sku = pd.merge(new_sku, new_df, on='material_id', how='left', suffixes=('', '2'))

# COMMAND ----------

# Calculate weekly weighted combined growth of sales and quantity for new SKUs

sales_growth = []
# quantity_growth = []
for material in new_sku['material_id'].unique():
    for week in new_sku[new_sku['material_id'] == material]['week_number'].unique():
        if week == new_sku[new_sku['material_id'] == material]['week_number'].min():
            sales_growth.append(0)
            # quantity_growth.append(0)
            previous_week = week
        else:
            previous_sales = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == previous_week)]['total_sales'].iloc[0]
            current_sales = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == week)]['total_sales'].iloc[0]
            sales_change = (current_sales - previous_sales)/previous_sales
            sales_growth.append(sales_change)

            # previous_quantity = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == previous_week)]['total_quantity_sold'].iloc[0]
            # current_quantity = new_sku[(new_sku['material_id'] == material) & (new_sku['week_number'] == week)]['total_quantity_sold'].iloc[0]
            # quantity_change = (current_quantity - previous_quantity)/previous_quantity
            # quantity_growth.append(quantity_change)
            
            previous_week = week

new_sku['sales_growth'] = sales_growth
# new_sku['quantity_growth'] = quantity_growth
# new_sku['GROWTH_sales_and_quantity'] = new_sku['sales_growth']*sales_weightage/100 + new_sku['quantity_growth']*quantity_weightage/100

# COMMAND ----------

new_sku['sales_growth'] = new_sku['sales_growth'].fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 12 - Lookalikes Growth

# COMMAND ----------

# Calculate weekly weighted combined growth of sales and quantity for lookalikes

sales_growth = []
# quantity_growth = []
for material in df2['material_id'].unique():
    for week in df2[df2['material_id'] == material]['week_number'].unique():
        if week == df2[df2['material_id'] == material]['week_number'].min():
            sales_growth.append(0)
            # quantity_growth.append(0)
            previous_week = week
        else:
            previous_sales = df2[(df2['material_id'] == material) & (df2['week_number'] == previous_week)]['total_sales'].iloc[0]
            current_sales = df2[(df2['material_id'] == material) & (df2['week_number'] == week)]['total_sales'].iloc[0]
            sales_change = (current_sales - previous_sales)/previous_sales
            sales_growth.append(sales_change)

            # previous_quantity = df2[(df2['material_id'] == material) & (df2['week_number'] == previous_week)]['total_quantity_sold'].iloc[0]
            # current_quantity = df2[(df2['material_id'] == material) & (df2['week_number'] == week)]['total_quantity_sold'].iloc[0]
            # quantity_change = (current_quantity - previous_quantity)/previous_quantity
            # quantity_growth.append(quantity_change)
            
            previous_week = week

df2['sales_growth'] = sales_growth
# df2['quantity_growth'] = quantity_growth
# df2['GROWTH_sales_and_quantity'] = df2['sales_growth']*sales_weightage/100 + df2['quantity_growth']*quantity_weightage/100

# COMMAND ----------

# Calculate the overall average growth of lookalikes over their respective 26 weeks period

avg_growth_lookalikes = []
for material in df2['material_id'].unique():
    avg_growth_lookalikes.append(df2[df2['material_id'] == material]['sales_growth'].iloc[1:].mean())

avg_growth_lookalikes = sum(avg_growth_lookalikes)/len(avg_growth_lookalikes)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 13 - New SKU Buckets

# COMMAND ----------

# Calculate the average growth of each new SKU over their respective 26 weeks period

avg_growth_new_sku = []
for material in new_sku['material_id'].unique():
    mean = new_sku[new_sku['material_id'] == material]['sales_growth'].iloc[1:].mean()
    
    if np.isnan(mean):
        mean = 0.0
    
    avg_growth_new_sku.append(mean)

new_sku_growth = pd.DataFrame({'material_id': new_sku['material_id'].unique(),
                               'avg_weekly_growth': avg_growth_new_sku})

SD_last_catg_and_contri = new_sku[['material_id', 'sales_contri', 'category_SD_last']].drop_duplicates()
new_sku_growth = pd.merge(new_sku_growth, SD_last_catg_and_contri, on='material_id', how='left')

# COMMAND ----------

# Calculate contribution of the lowest contributing product of Middle SKUs

Contri_lowestM40 = df[df['final_contri'] == 'Middle']['sales_contri'].min()

# Extract numpy arrays of new SKUs to be used in creating buckets

materials = new_sku_growth['material_id'].values
avg_weekly_growth = new_sku_growth['avg_weekly_growth'].values
category_SD_last = new_sku_growth['category_SD_last'].values
contri = new_sku_growth['sales_contri'].values

# COMMAND ----------

# Categorize the new SKUs into buckets

buckets = []
for i in range(len(materials)):
    if (avg_weekly_growth[i] >= avg_growth_lookalikes) & (contri[i] >= Contri_lowestM40):
        buckets.append('Grow')
    
    elif (avg_weekly_growth[i] >= avg_growth_lookalikes) & (contri[i] < Contri_lowestM40):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] < avg_growth_lookalikes) & (contri[i] >= Contri_lowestM40):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] < avg_growth_lookalikes) & (contri[i] < Contri_lowestM40) & (category_SD_last[i] == 'Currently selling'):
        buckets.append('Observe')
    
    elif (avg_weekly_growth[i] < avg_growth_lookalikes) & (contri[i] < Contri_lowestM40) & (category_SD_last[i] != 'Currently selling'):
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

# Calculate weekly weighted combined growth of sales and quantity for all SKUs of the 52 weeks period

sales_growth = []
# quantity_growth = []
for material in df['material_id'].unique():
    for week in df[df['material_id'] == material]['week_number'].unique():
        if week == df[df['material_id'] == material]['week_number'].min():
            sales_growth.append(0)
            # quantity_growth.append(0)
            previous_week = week
        else:
            previous_sales = df[(df['material_id'] == material) & (df['week_number'] == previous_week)]['total_sales'].iloc[0]
            current_sales = df[(df['material_id'] == material) & (df['week_number'] == week)]['total_sales'].iloc[0]
            sales_change = (current_sales - previous_sales)/previous_sales
            sales_growth.append(sales_change)

            # previous_quantity = df[(df['material_id'] == material) & (df['week_number'] == previous_week)]['total_quantity_sold'].iloc[0]
            # current_quantity = df[(df['material_id'] == material) & (df['week_number'] == week)]['total_quantity_sold'].iloc[0]
            # quantity_change = (current_quantity - previous_quantity)/previous_quantity
            # quantity_growth.append(quantity_change)
            
            previous_week = week

df['sales_growth'] = sales_growth
# df['quantity_growth'] = quantity_growth
# df['GROWTH_sales_and_quantity'] = df['sales_growth']*sales_weightage/100 + df['quantity_growth']*quantity_weightage/100

# COMMAND ----------

# Calculate the average growth of each Top contributing SKU over the 52 weeks period

top_contri_sku = df[df['final_contri'] == 'Top'][['material_id', 'week_number', 'sales_growth']].reset_index(drop = True)

avg_growth_top_sku = []
for material in top_contri_sku['material_id'].unique():
    mean = top_contri_sku[top_contri_sku['material_id'] == material]['sales_growth'].iloc[1:].mean()
    
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

# Calculate the average growth of each Middle contributing SKU over the 52 weeks period

middle_contri_sku = df[df['final_contri'] == 'Middle'][['material_id', 'week_number', 'sales_growth']].reset_index(drop = True)

avg_growth_middle_sku = []
for material in middle_contri_sku['material_id'].unique():
    mean = middle_contri_sku[middle_contri_sku['material_id'] == material]['sales_growth'].iloc[1:].mean()
    
    if np.isnan(mean):
        mean = 0.0
    
    avg_growth_middle_sku.append(mean)

middle_sku_growth = pd.DataFrame({'material_id': middle_contri_sku['material_id'].unique(),
                               'avg_weekly_growth': avg_growth_middle_sku})

SD_last_category = df[df['final_contri'] == 'Middle'][['material_id', 'category_SD_last']].drop_duplicates()
middle_sku_growth = pd.merge(middle_sku_growth, SD_last_category, on='material_id', how='left')

avg_growth_middle_sku = sorted(avg_growth_middle_sku)
median_growth_middle_sku = avg_growth_middle_sku[len(avg_growth_middle_sku)//2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 17 - Store Penetration

# COMMAND ----------

# Gathering data for number of stores where each product was sold

query = """
SELECT material_id, COUNT(DISTINCT store_id) AS num_stores
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2
ON t1.product_id = t2.material_id
WHERE business_day BETWEEN '{}' AND '{}'
AND category_name = '{}'
GROUP BY material_id
""".format(start_date, end_date, category)

store_pnt = spark.sql(query).toPandas().sort_values(by='material_id').reset_index(drop = True)

# COMMAND ----------

# Calculating the total number of stores

query = """
SELECT COUNT(DISTINCT store_id) AS total_stores
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2
ON t1.product_id = t2.material_id
WHERE business_day BETWEEN '{}' AND '{}'
AND category_name = '{}'
""".format(start_date, end_date, category)

total_store_count = spark.sql(query).toPandas()

# COMMAND ----------

store_counts = store_pnt.copy()
total_stores = total_store_count.values[0,0]

# COMMAND ----------

# Calculate the store penetration of each SKU

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
    if (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] == 'Currently selling'):
        buckets.append('Grow')
    
    elif (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] == 'Low/no supply'):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] < median_growth_middle_sku) & (category_SD_last[i] == 'Currently selling'):
        buckets.append('Maintain')
    
    elif (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] == 'No recent sales'):
        buckets.append('Observe')
    
    elif (avg_weekly_growth[i] < median_growth_middle_sku) & (category_SD_last[i] != 'Currently selling'):
        buckets.append('Observe')
    
    else:
        buckets.append('None')

middle_sku_growth['buckets'] = buckets

# COMMAND ----------

middle_sku_growth['buckets'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 19 - Low SKUs Growth

# COMMAND ----------

# Calculate the average growth of each Low contributing SKU over the 52 weeks period

low_contri_sku = df[df['final_contri'] == 'Low'][['material_id', 'week_number', 'sales_growth', 'sales_contri', 'cumulative_contri']].reset_index(drop = True)

avg_growth_low_sku = []
for material in low_contri_sku['material_id'].unique():
    mean = low_contri_sku[low_contri_sku['material_id'] == material]['sales_growth'].iloc[1:].mean()
    
    if np.isnan(mean):
        mean = 0.0
    
    avg_growth_low_sku.append(mean)

low_sku_growth = pd.DataFrame({'material_id': low_contri_sku['material_id'].unique(),
                               'avg_weekly_growth': avg_growth_low_sku})

SD_last_catg_cumulative_contri = df[df['final_contri'] == 'Low'][['material_id', 'category_SD_last', 'sales_contri', 'cumulative_contri']].drop_duplicates()
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
    if (avg_weekly_growth[i] >= median_growth_low_sku) & (category_SD_last[i] == 'Currently selling'):
        buckets.append('Maintain')
    
    else:
        buckets.append('Observe')

low_sku_growth['buckets'] = buckets

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 21 - Delist Categorization

# COMMAND ----------

allowed_product_count = df['material_id'].nunique()*delist_product_count//100

materials_to_delist = low_sku_growth[low_sku_growth['cumulative_contri'] > (1 - delist_contri_threshold)]['material_id'].values
delist_materials_count = len(materials_to_delist)

# if delist_materials_count > allowed_product_count:
#     print("The number of products that can be delisted are {}. Which is greater than 15% of the total number of products ({}). Please decrease the contribution threshold ({}) or increase the product count percentage ({})".format(delist_materials_count, allowed_product_count, delist_contri_threshold, delist_product_count))

# else:
for i in range(len(materials)):
    if materials[i] in materials_to_delist:
        low_sku_growth.at[i, 'buckets'] = 'Delist'

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 6

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

gp_report = pd.read_csv("/dbfs/FileStore/shared_uploads/prem@loyalytics.in/gp_report_water_validation.csv")
gp_report = gp_report.sort_values(by = 'material_id').reset_index(drop = True)
gp_report.rename(columns={'GP with ChargeBack & Bin Promo (%)': 'gp_perc'}, inplace=True)

# COMMAND ----------

# Add the extra materials from the rolling 52 weeks period into the gp_report and set their GP as 0
# This is because they had no sales in the last 12 weeks period

materials_to_add = pd.merge(df['material_id'].drop_duplicates(), gp_report['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)

materials_to_add['gp_perc'] = 0

gp_report = pd.concat([gp_report, materials_to_add], ignore_index=True)
gp_report = gp_report.sort_values(by='material_id').reset_index(drop = True)

# COMMAND ----------

# Calculate the GP contribution

gp_report['gp_value'] = gp_report['gp_perc'] * total_sales_12weeks['total_sales']/100
gp_report['gp_value_positives'] = gp_report['gp_value'].apply(lambda x: max(0, x)) # Replace negative values with 0
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

# Categorise the SKUs into 3 categories based on their cummulative contribution

bins = [0, top_contri_threshold, top_contri_threshold + middle_contri_threshold, float('inf')]
labels = ['Top', 'Middle', 'Low']
gp_report['category_contri'] = pd.cut(gp_report['cumulative_contri'], bins=bins, labels=labels, include_lowest=True)

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

new_buckets = []
for i in range(len(low_materials)):
    if (low_buckets[i] == 'Maintain') & (gp_low_category_contris[i] == 'Low'):
        new_buckets.append('Observe')
    
    elif (low_buckets[i] == 'Observe') & (gp_low_category_contris[i] == 'Top'):
        new_buckets.append('Maintain')
    
    elif (low_buckets[i] == 'Delist') & (gp_low_category_contris[i] != 'Low'):
        new_buckets.append('Observe')

    else:
        new_buckets.append(low_buckets[i])

low_sku_growth['new_buckets'] = new_buckets

# COMMAND ----------

low_sku_growth['buckets'].value_counts()

# COMMAND ----------



# COMMAND ----------

low_sku_growth[low_sku_growth['buckets'] == 'Delist']
