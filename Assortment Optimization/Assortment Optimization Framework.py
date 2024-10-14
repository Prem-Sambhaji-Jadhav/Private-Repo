# Databricks notebook source
# MAGIC %md
# MAGIC #Function Initializaitons

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# COMMAND ----------

dbutils.widgets.text(name='Category', defaultValue='null')
dbutils.widgets.text(name='Material Group', defaultValue='null')
dbutils.widgets.text(name='Region', defaultValue='null')
dbutils.widgets.text(name='Start Date', defaultValue='null')
dbutils.widgets.text(name='End Date', defaultValue='null')

# COMMAND ----------

# MAGIC %md
# MAGIC #User Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC Mandatory inputs

# COMMAND ----------

category = dbutils.widgets.get('Category') # Mandatory field
material_group_name = dbutils.widgets.get('Material Group') # Leave it blank if you want the recommendations on category level
region = dbutils.widgets.get('Region') # ABU DHABI, AL AIN, DUBAI, SHARJAH

# catg_lower = material_group_name.lower() # 'material_group_name' OR 'category'

# Please make sure that the date range is EXACTLY 52 weeks (364 days) long, and does not include the present day's date
start_date = dbutils.widgets.get('Start Date')
end_date = dbutils.widgets.get('End Date')

# COMMAND ----------

# MAGIC %md
# MAGIC Optional inputs

# COMMAND ----------

analysis_period = 3 # Last [X] months will be taken as the analysis period

currently_selling_range = 51 # <specified value> to 52nd week will be categorized as 'Currently selling'
low_no_supply_range = 44 # <specified value> to <currently_selling_range - 1> week will be categorized as 'Low/no supply'
# 1 to <low_no_supply_range - 1> week will be categorized as 'No recent sales'

sales_weightage = 50 # (% format) Weightage for sales contribution
quantity_weightage = 100 - sales_weightage

new_sku_date_range = 6 # Products launched in the last [X] months will be taken as new SKUs

delist_contri_threshold = 0.03 # (% format) Contribution threshold for 'Low' contribution category to be categorized as Delist
delist_product_count = 15 # (% format) Product count threshold for 'Low' contribution category that can be categorized as Delist

save_files_flag = 1 # 1 is for saving files, 0 is for not saving files (for experimental purposes)

# COMMAND ----------

# MAGIC %md
# MAGIC No inputs required below

# COMMAND ----------

# Do not change anything here

lookalikes_start_date = "2022-01-31"

# If the days do not match, adjust lookalikes_start_date to the next closest date whose day is matching
lookalikes_dt = datetime.strptime(lookalikes_start_date, "%Y-%m-%d")
start_day = datetime.strptime(start_date, "%Y-%m-%d").weekday()
lookalikes_day = lookalikes_dt.weekday()

if start_day != lookalikes_day:
    days_diff = (start_day - lookalikes_day) % 7
    lookalikes_dt += timedelta(days = days_diff)

lookalikes_start_date = lookalikes_dt.strftime("%Y-%m-%d")

# Store abbreviated version of region in a variable
if region == "ABU DHABI":
    region_abr = "auh"
elif region == "AL AIN":
    region_abr = "aln"
elif region == "DUBAI":
    region_abr = "dxb"
else:
    region_abr = "shj"

# Get the latest RFM month_year
query = """
SELECT MAX(month_year)
FROM analytics.segment.customer_segments
WHERE
    key = 'rfm'
    AND country = 'uae'
    AND channel = 'pos'
"""
rfm_month_year = spark.sql(query).toPandas().iloc[0,0]

# Creating a condition for material_group_name depending on whether the exercise is on material group or category
if material_group_name == "":
    material_group_condition = ""
else:
    material_group_condition = "AND material_group_name = '" + material_group_name + "'"

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 1 - Base Table Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1 - Sandbox Table Creation

# COMMAND ----------

# Creating a sandbox table for all the required details of the category/material group

query = f"""
SELECT *
FROM dev.sandbox.pj_ao_framework_data
WHERE
    category_name = '{category}'
    {material_group_condition}
"""

data_empty = spark.sql(query).toPandas().empty

if data_empty:
    query = f"""
    WITH customer_segments AS (
        SELECT
            customer_id,
            segment
        FROM analytics.segment.customer_segments
        WHERE key = 'rfm'
        AND channel = 'pos'
        AND country = 'uae'
        AND month_year = {rfm_month_year}
    )

    SELECT
        t1.business_day,
        t3.region_name,
        t1.store_id,
        t2.material_id,
        t2.brand,
        t2.material_group_name,
        t2.category_name,
        t1.customer_id,
        t4.segment,
        SUM(amount) AS sales,
        SUM(quantity) AS quantity_sold
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    LEFT JOIN customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE
        t1.business_day BETWEEN '{lookalikes_start_date}' AND '{end_date}'
        AND t2.category_name = '{category}'
        {material_group_condition}
        AND t3.tayeb_flag = 0
        AND t1.transaction_type IN ('SALE', 'SELL_MEDIA')
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    """

    spark_df = spark.sql(query)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_framework_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2 - Weekly Sales & Quantity Table

# COMMAND ----------

# Gathering weekly data of all materials that have had a sale in the last 52 weeks

query = f"""
SELECT
    material_id,
    FLOOR(DATEDIFF(business_day, '{start_date}') / 7) + 1 AS week_number,
    SUM(sales) AS total_sales,
    SUM(quantity_sold) AS total_quantity_sold
FROM dev.sandbox.pj_ao_framework_data
WHERE
    business_day >= '{start_date}'
    AND region_name = '{region}'
    AND category_name = '{category}'
    {material_group_condition}
GROUP BY material_id, week_number
ORDER BY material_id, week_number
"""

weeks52 = spark.sql(query).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3 - Last Week Sold Tagging

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
# MAGIC ##Step 4 - Recent Sales & Quantity

# COMMAND ----------

# Calculate total sales and quantity of all SKUs in the analysis period

analysis_period_weeks = 52 - int(analysis_period/3 * 13) + 1

total_sales_12weeks = df[df['week_number'] >= analysis_period_weeks].groupby('material_id')['total_sales'].sum().reset_index()
total_quantity_sold_12weeks = df[df['week_number'] >= analysis_period_weeks].groupby('material_id')['total_quantity_sold'].sum().reset_index()

# Merge the summed values back into the original dataframe
df = pd.merge(df, total_sales_12weeks, on='material_id', how='left', suffixes=('', '2'))
df = pd.merge(df, total_quantity_sold_12weeks, on='material_id', how='left', suffixes=('', '2'))

# NaN values will be present for those materials whose last selling date was not in the analysis period. Fill them with 0 in the new columns
df['total_sales_12weeks'] = df['total_sales2'].fillna(0)
df['total_quantity_sold_12weeks'] = df['total_quantity_sold2'].fillna(0)

df = df.drop(columns=['total_sales2', 'total_quantity_sold2'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 5 - Contribution Calculation

# COMMAND ----------

# Calculate the weighted combined contribution of sales and quantity

total_sales_12weeks_sum = df.groupby('material_id')['total_sales_12weeks'].head(1).sum()
total_quantity_sold_12weeks_sum = df.groupby('material_id')['total_quantity_sold_12weeks'].head(1).sum()

df['sales_contri'] = (df['total_sales_12weeks'] / total_sales_12weeks_sum)
df['quantity_contri'] = (df['total_quantity_sold_12weeks'] / total_quantity_sold_12weeks_sum)
df['CONTRI_sales_and_quantity'] = (df['sales_contri']*sales_weightage/100 + df['quantity_contri']*quantity_weightage/100)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 6 - Contribution Categorization

# COMMAND ----------

# Calculate the cummulative sum of combined contribution of sales and quantity

material_wise_contri = df[['material_id', 'CONTRI_sales_and_quantity']].drop_duplicates()
material_wise_contri = material_wise_contri.sort_values(by = 'CONTRI_sales_and_quantity', ascending = False).reset_index(drop = True)
material_wise_contri['cumulative_contri'] = material_wise_contri['CONTRI_sales_and_quantity'].cumsum()

df = pd.merge(df, material_wise_contri, on = 'material_id', how = 'left', suffixes=('', '2'))
df = df.drop(columns = ['CONTRI_sales_and_quantity2'])

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
# MAGIC ##Step 7 - Lookalikes Table

# COMMAND ----------

# Gathering weekly data of all materials that have had a sale 1 month after the earliest sales data available in gold.pos_transactions

query = f"""
SELECT
    material_id,
    FLOOR(DATEDIFF(business_day, '{lookalikes_start_date}') / 7) + 1 AS week_number,
    SUM(sales) AS total_sales,
    SUM(quantity_sold) AS total_quantity_sold
FROM dev.sandbox.pj_ao_framework_data
WHERE
    region_name = '{region}'
    AND category_name = '{category}'
    {material_group_condition}
GROUP BY material_id, week_number
ORDER BY material_id, week_number
"""

all_weeks = spark.sql(query).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 8 - First Week Sold Tagging

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
df2['category_SD_first'] = pd.cut(Rolling_52Wfirst - df2['SD_first'], bins=[-float('inf'), 0, 13, 26, float('inf')],
                                labels=["New Launch", "Settling", "Stable", "Established"])

SD_first_df = df2[['material_id', 'category_SD_first', 'SD_first']].drop_duplicates()
df = pd.merge(df, SD_first_df, on='material_id', how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 9 - New SKUs Segregation

# COMMAND ----------

# Create a separate list of new SKUs whose first selling date was in the last 182 days (26 weeks)

new_sku_date_range_weeks = int(new_sku_date_range/3 * 13)
new_sku_date_threshold = df2['week_number'].max() - new_sku_date_range_weeks + 1

new_sku = df[df['SD_first'] >= new_sku_date_threshold][['material_id', 'week_number', 'total_sales', 'total_quantity_sold', 'category_SD_last']].reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 10 - Lookalikes Preparation

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

new_sku_to_remove = new_sku[['material_id']].drop_duplicates().reset_index(drop=True)
df2 = df2.merge(new_sku_to_remove, on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
df2 = df2.reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 2 - New SKUs Recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 11 - New SKUs Contribution & Growth

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
# MAGIC ##Step 13 - New SKUs Buckets

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

Contri_lowestM30 = df[(~df['material_id'].isin(new_sku_to_remove['material_id'].tolist())) & (df['final_contri'] == 'Middle')]['CONTRI_sales_and_quantity'].min()

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
# MAGIC #Part 3 - Top SKUs Recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 14 - Top SKUs Growth

# COMMAND ----------

# Calculate weekly weighted combined growth of sales and quantity of the analysis period for all SKUs

df_12weeks = df[df['week_number'] >= analysis_period_weeks].reset_index(drop = True)

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

# Calculate the average growth of the analysis period of each Top contributing SKU

top_contri_sku = df_12weeks[df_12weeks['final_contri'] == 'Top'][['material_id', 'week_number', 'GROWTH_sales_and_quantity']].reset_index(drop = True)

top_contri_sku = top_contri_sku.merge(new_sku_to_remove, on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
top_contri_sku = top_contri_sku.reset_index(drop = True)

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
# MAGIC #Part 4 - Middle SKUs Recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 16 - Middle SKUs Growth

# COMMAND ----------

# Calculate the average growth of the analysis period of each Middle contributing SKU over the 52 weeks period

middle_contri_sku = df_12weeks[df_12weeks['final_contri'] == 'Middle'][['material_id', 'week_number', 'GROWTH_sales_and_quantity']].reset_index(drop = True)

middle_contri_sku = middle_contri_sku.merge(new_sku_to_remove, on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
middle_contri_sku = middle_contri_sku.reset_index(drop = True)

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

# Gathering data for number of stores where each product was sold in the analysis period

analysis_period_days = 364 - (52 - analysis_period_weeks + 1) * 7

query = f"""
SELECT
    material_id,
    COUNT(DISTINCT store_id) AS num_stores
FROM dev.sandbox.pj_ao_framework_data
WHERE
    business_day >= DATE_ADD('{start_date}', {analysis_period_days})
    AND region_name = '{region}'
    AND category_name = '{category}'
    {material_group_condition}
GROUP BY material_id
ORDER BY material_id
"""

store_pnt = spark.sql(query).toPandas()

# COMMAND ----------

# Calculating the total number of stores where a sale was made in the analysis period

query = f"""
SELECT COUNT(DISTINCT t1.store_id) AS total_stores
FROM gold.transaction.uae_pos_transactions AS t1
JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
WHERE
    t1.business_day BETWEEN DATE_ADD('{start_date}', {analysis_period_days}) AND '{end_date}'
    AND t2.category_name = '{category}'
    {material_group_condition}
    AND t3.region_name = '{region}'
    AND t3.tayeb_flag = 0
    AND t1.transaction_type IN ('SALE', 'SELL_MEDIA')
    AND t1.amount > 0
    AND t1.quantity > 0
"""

total_store_count = spark.sql(query).toPandas()

# COMMAND ----------

store_counts = store_pnt.copy()
total_stores = total_store_count.iloc[0,0]

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
    
    elif (avg_weekly_growth[i] >= median_growth_middle_sku) & (category_SD_last[i] != 'Currently selling'):
        buckets.append('Observe')

    elif (avg_weekly_growth[i] < median_growth_middle_sku):
        buckets.append('Observe')
    
    else:
        buckets.append('None')

middle_sku_growth['buckets'] = buckets

# COMMAND ----------

# MAGIC %md
# MAGIC #Part 5 - Low SKUs Recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 19 - Low SKUs Growth

# COMMAND ----------

# Calculate the average growth of the analysis period of each Low contributing SKU over the 52 weeks period

low_contri_sku = df_12weeks[df_12weeks['final_contri'] == 'Low'][['material_id', 'week_number', 'GROWTH_sales_and_quantity', 'CONTRI_sales_and_quantity', 'sales_contri', 'cumulative_contri']].reset_index(drop = True)

low_contri_sku = low_contri_sku.merge(new_sku_to_remove, on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
low_contri_sku = low_contri_sku.reset_index(drop = True)

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
materials_to_add = materials_to_add[(~materials_to_add['material_id'].isin(new_sku_to_remove['material_id'].tolist()))]
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
# MAGIC #Part 6 - New Recommendations With GP

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 22 - GP Contribution

# COMMAND ----------

# Convert the start date of the analysis period to have the start date as the first date of the month, so that it matches with the GP month-wise report

date_obj = datetime.strptime(end_date, "%Y-%m-%d")

gp_start_date = datetime(date_obj.year, date_obj.month, 1)
gp_start_month = gp_start_date.month - analysis_period + 1
if gp_start_month == 0:
    gp_start_month = 12
    gp_start_date = datetime(gp_start_date.year - 1, gp_start_month, 1)
elif gp_start_month == -1:
    gp_start_month = 11
    gp_start_date = datetime(gp_start_date.year - 1, gp_start_month, 1)
else:
    gp_start_date = datetime(gp_start_date.year, gp_start_month, 1)

analysis_period_start_month = int(str(gp_start_date.year) + str(gp_start_date.month).zfill(2))

# COMMAND ----------

# Gather monthly sales data for the rolling 52 weeks period

query = f"""
SELECT
    material_id,
    INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
    SUM(sales) AS sales
FROM dev.sandbox.pj_ao_framework_data
WHERE
    business_day >= '{start_date}'
    AND region_name = '{region}'
    AND category_name = '{category}'
    {material_group_condition}
GROUP BY material_id, year_month
ORDER BY material_id, year_month
"""

monthly_sales_df = spark.sql(query).toPandas()

# COMMAND ----------

# Extract monthly sales of only the analysis period

gp_sales = monthly_sales_df[monthly_sales_df['year_month'] >= analysis_period_start_month].reset_index(drop = True)

# COMMAND ----------

# Gather GP data of the rolling 52 weeks period

year_month_start = int(start_date[:7].replace('-', ''))
year_month_end = int(end_date[:7].replace('-', ''))

query = f"""
SELECT
    material_id,
    year_month,
    gp_wth_chargeback
FROM gold.business.gross_profit
WHERE
    country = 'AE'
    AND region = '{region_abr.upper()}'
    AND year_month BETWEEN {year_month_start} AND {year_month_end}
    AND material_id IS NOT NULL
ORDER BY material_id, year_month
"""

gp_df = spark.sql(query).toPandas()

# COMMAND ----------

# Keep only those SKUs that are present in the given category/material group

materials = monthly_sales_df['material_id'].unique()
gp_df = gp_df[gp_df['material_id'].isin(materials)].reset_index(drop = True)

# COMMAND ----------

# Extract the GP data of only the analysis period

gp_report = gp_df[gp_df['year_month'] >= analysis_period_start_month].reset_index(drop = True)

# COMMAND ----------

# Calculate GP value

gp_report = pd.merge(gp_report, gp_sales, on=['material_id', 'year_month'], how='inner')

gp_report['gp_value'] = gp_report['gp_wth_chargeback'] * gp_report['sales']/100
gp_report = gp_report.groupby('material_id')[['gp_value']].sum().reset_index()

# COMMAND ----------

# Add the materials from the analysis period into the GP report as some materials could be missing in the report

materials_to_add = pd.merge(total_sales_12weeks['material_id'].drop_duplicates(), gp_report['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)

materials_to_add['gp_value'] = 0

gp_report = pd.concat([gp_report, materials_to_add], ignore_index=True)
gp_report = gp_report.sort_values(by = 'material_id').reset_index(drop = True)

print(f"Added {materials_to_add['material_id'].nunique()} new SKUs")

# COMMAND ----------

# Add the extra materials from the rolling 52 weeks period into the gp_report and set their GP as 0. This is because they had no sales in the analysis period

materials_to_add = pd.merge(df['material_id'].drop_duplicates(), gp_report['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)

materials_to_add['gp_value'] = 0

gp_report = pd.concat([gp_report, materials_to_add], ignore_index=True)

# COMMAND ----------

# Calculate the GP contribution

total_gp_value = gp_report['gp_value'].sum()
gp_report['gp_contri'] = gp_report['gp_value'] / total_gp_value

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
# MAGIC ##Step 24 - PL Delist to Observe

# COMMAND ----------

# Identify PL SKUs recommended for delist and change their bucket to observe

attr = spark.sql('SELECT * FROM dev.sandbox.pj_ao_attributes').toPandas()

temp = low_sku_growth[['material_id', 'new_buckets']]
temp = pd.merge(temp, attr[['material_id', 'brand']], on='material_id', how='inner')

def check_pl_flag(text):
    words = text.split()
    if 'IMP' in words or 'IMPORT' in words or text == 'LULU PRIVATE LABEL' or text == 'REPACKED PACKETS':
        return 1
    return 0

temp['pl_flag'] = temp['brand'].apply(check_pl_flag)
temp['new_buckets'] = np.where((temp.pl_flag == 1) & (temp.new_buckets == 'Delist'), "Observe", temp.new_buckets)
temp = temp[['material_id', 'new_buckets']]

low_sku_growth.drop(columns=['new_buckets'], inplace=True)
low_sku_growth = pd.merge(low_sku_growth, temp[['material_id', 'new_buckets']], on='material_id', how='inner')

# COMMAND ----------

# Identify newly launched PL SKUs recommended for delist and change their bucket to observe

temp = new_sku_growth[['material_id', 'buckets']]
temp = pd.merge(temp, attr[['material_id', 'brand']], on='material_id', how='inner')

def check_pl_flag(text):
    words = text.split()
    if 'IMP' in words or 'IMPORT' in words or text == 'LULU PRIVATE LABEL':
        return 1
    return 0

temp['pl_flag'] = temp['brand'].apply(check_pl_flag)
temp['buckets'] = np.where((temp.pl_flag == 1) & (temp.buckets == 'Delist'), "Observe", temp.buckets)
temp = temp[['material_id', 'buckets']]

new_sku_growth.drop(columns=['buckets'], inplace=True)
new_sku_growth = pd.merge(new_sku_growth, temp[['material_id', 'buckets']], on='material_id', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 25 - Delist to Observe

# COMMAND ----------

# Extract all the delist SKUs along with their calculated metrics. Calculate Q4 sales

new_rule_df = df[['material_id', 'week_number', 'total_sales', 'total_quantity_sold', 'CONTRI_sales_and_quantity']]

total_sales_12weeks = new_rule_df[new_rule_df['week_number'] >= 40].groupby('material_id')['total_sales'].sum().reset_index()
new_rule_df = pd.merge(new_rule_df, total_sales_12weeks, on='material_id', how='left', suffixes=('', '2'))
new_rule_df['total_sales_12weeks'] = new_rule_df['total_sales2'].fillna(0)
new_rule_df = new_rule_df.drop(columns=['total_sales2'])

new_rule_df = pd.merge(new_rule_df, low_sku_growth[['material_id', 'new_buckets']], on='material_id', how='left')

new_rule_df = new_rule_df[new_rule_df['new_buckets'] == 'Delist'].reset_index(drop=True)

new_rule_df = pd.merge(new_rule_df, gp_report[['material_id', 'gp_value']], on='material_id', how='left')

new_rule_df = new_rule_df.drop(columns='new_buckets')
new_rule_df.rename(columns={'gp_value': 'gp_Q4'}, inplace=True)
new_rule_df.rename(columns={'total_sales_12weeks': 'total_sales_Q4'}, inplace=True)
new_rule_df.rename(columns={'CONTRI_sales_and_quantity': 'CONTRI_Q4'}, inplace=True)

# COMMAND ----------

# Calculate the Q3 sales and quantity

total_sales_Q3 = new_rule_df[(new_rule_df['week_number'] >= 27) & (new_rule_df['week_number'] <= 39)].groupby('material_id')['total_sales'].sum().reset_index()
total_quantity_sold_Q3 = new_rule_df[(new_rule_df['week_number'] >= 27) & (new_rule_df['week_number'] <= 39)].groupby('material_id')['total_quantity_sold'].sum().reset_index()

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

# Calculate growth from Q4 sales vs Q3 sales

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

growth_avg = new_rule_df.drop(columns=['week_number', 'total_sales']).drop_duplicates()
growth_avg = growth_avg[growth_avg['gp_rank_Q4'] <= 15]['sales_growth_Q4_vs_Q3'].mean()

new_rule_df2 = new_rule_df.drop(columns=['week_number', 'total_sales', 'total_quantity_sold']).drop_duplicates().reset_index(drop=True)
contri_avg = new_rule_df2[new_rule_df2['gp_rank_Q4'] <= 15]['CONTRI_Q4'].mean()
new_rule_df2['Contri_index_Q4'] = new_rule_df2['CONTRI_Q4'] / contri_avg
new_rule_df2['Contri_growth_Q4_vs_Q3'] = (new_rule_df2['CONTRI_Q4'] - new_rule_df2['CONTRI_Q3']) / new_rule_df2['CONTRI_Q3']

new_rule_df2 = new_rule_df2[['material_id', 'sales_growth_Q4_vs_Q3', 'Contri_index_Q4', 'Contri_growth_Q4_vs_Q3', 'gp_rank_Q4', 'sales_rank_Q4']]

delist_to_observe_materials = new_rule_df2[(new_rule_df2['gp_rank_Q4'] <= 15) & (new_rule_df2['sales_growth_Q4_vs_Q3'] > growth_avg) & (new_rule_df2['sales_growth_Q4_vs_Q3'] > 0) & (new_rule_df2['sales_rank_Q4'] <= 15)]['material_id'].values

# COMMAND ----------

# Update the recommendation of the materials satisfying the above rule

materials = low_sku_growth['material_id'].values

for i in range(len(materials)):
        if materials[i] in delist_to_observe_materials:
            low_sku_growth.at[i, 'new_buckets'] = 'Observe'

print(f"The recommendation for {len(delist_to_observe_materials)} of the delist materials have been changed to Observe.\nThe Material IDs are:", ', '.join(delist_to_observe_materials.astype(str)))

# COMMAND ----------

# MAGIC %md
# MAGIC #EDA Data Prep

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final Recommendations View

# COMMAND ----------

# Check the count of SKUs in each of the recommendation buckets

top_reco = top_sku_growth['new_buckets'].value_counts()
middle_reco = middle_sku_growth['new_buckets'].value_counts()
low_reco = low_sku_growth['new_buckets'].value_counts()
new_reco = new_sku_growth['buckets'].value_counts()

print(f"Top SKUs final reco:\n{top_reco}\nMiddle SKUs final reco:\n{middle_reco}\nLow SKUs final reco:\n{low_reco}\nNew SKUs reco:\n{new_reco}")

# COMMAND ----------

# Collate all the SKUs with their recommendation buckets into one single dataframe

new_sku_growth['new_buckets'] = new_sku_growth['buckets']
all_products_catg = pd.concat([top_sku_growth[['material_id', 'new_buckets']], middle_sku_growth[['material_id', 'new_buckets']], low_sku_growth[['material_id', 'new_buckets']], new_sku_growth[['material_id', 'new_buckets']]], ignore_index=True)
all_products_catg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Weekly CWD & Customer %

# COMMAND ----------

# Gathering weekly data of all materials with respect to the stores they had sales during the rolling 52 weeks

query = f"""
SELECT
    material_id,
    FLOOR(DATEDIFF(business_day, '{start_date}') / 7) + 1 AS week_number,
    store_id,
    SUM(sales) AS total_sales
FROM dev.sandbox.pj_ao_framework_data
WHERE
    business_day >= '{start_date}'
    AND region_name = '{region}'
    AND category_name = '{category}'
    {material_group_condition}
GROUP BY week_number, material_id, store_id
ORDER BY week_number, material_id, store_id
"""

material_store_df = spark.sql(query).toPandas()
if save_files_flag == 1:
    temp = material_store_df.copy()
    temp['region_name'] = region
    temp['category_name'] = category
    temp['material_group_name'] = material_group_name
    spark_df = spark.createDataFrame(temp)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_material_store_data")

# COMMAND ----------

# Calculate Category Weighted Distribution for each material in every week

cwd_df = material_store_df.copy()
cwd = []
weeks = cwd_df['week_number'].unique()

for week in weeks:

    sales = cwd_df[cwd_df['week_number'].values == week][['material_id', 'store_id', 'total_sales']]

    all_store_sales = sales['total_sales'].sum()
    materials = sales['material_id'].unique()
    store_sales_df = sales.groupby('store_id')['total_sales'].sum().reset_index()

    for material in materials:

        stores = sales[sales['material_id'].values == material]['store_id'].unique()

        material_store_sales = store_sales_df[store_sales_df['store_id'].isin(stores)]['total_sales'].sum()

        cwd.append(material_store_sales/all_store_sales)

cwd_df['material_store_count'] = cwd_df.groupby('material_id')['store_id'].transform('nunique')

cwd_df['material_weekly_store_count'] = cwd_df.groupby(['material_id', 'week_number'])['store_id'].transform('nunique')

cwd_df = cwd_df.groupby(['week_number', 'material_id']).agg({'total_sales': 'sum', 'material_store_count': 'mean', 'material_weekly_store_count': 'mean'}).reset_index()

cwd_df['cwd'] = cwd

# COMMAND ----------

# Save all the weekly data of the materials into csv formats to be used in the EDA notebook

weeks52.rename(columns={'total_sales': 'sale'}, inplace=True)
weeks52.rename(columns={'total_quantity_sold': 'vol'}, inplace=True)

weekly_data = pd.merge(weeks52, cwd_df, on=['material_id', 'week_number'], how = 'inner')
weekly_data = weekly_data.drop(columns = 'total_sales')

if save_files_flag == 1:
    temp = weekly_data.copy()
    temp['region_name'] = region
    temp['category_name'] = category
    temp['material_group_name'] = material_group_name
    spark_df = spark.createDataFrame(temp)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_weekly_data")

# COMMAND ----------

# Gathering customer counts of all materials in the rolling 52 weeks period

query = f"""
WITH total_cust AS (
    SELECT
        COUNT(DISTINCT customer_id) AS tot_cust,
        COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN customer_id END) AS tot_vip,
        COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN customer_id END) AS tot_freq
    FROM dev.sandbox.pj_ao_framework_data
    WHERE
        business_day >= '{start_date}'
        AND region_name = '{region}'
        AND category_name = '{category}'
        {material_group_condition}
)

SELECT
    material_id,
    COUNT(DISTINCT customer_id) AS cust,
    COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN customer_id END) AS vip_cust,
    COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN customer_id END) AS freq_cust,
    tot_cust,
    tot_vip,
    tot_freq,
    (cust/tot_cust) AS tot_cust_perc,
    (vip_cust/tot_vip) AS vip_cust_perc,
    (freq_cust/tot_freq) AS freq_cust_perc
FROM dev.sandbox.pj_ao_framework_data, total_cust
WHERE
    business_day >= '{start_date}'
    AND region_name = '{region}'
    AND category_name = '{category}'
    {material_group_condition}
GROUP BY material_id, tot_cust, tot_vip, tot_freq
ORDER BY material_id
"""

cust = spark.sql(query).toPandas()
cust = pd.merge(cust, attr[['material_id', 'material_name']], on = 'material_id', how = 'inner')

if save_files_flag == 1:
    temp = cust.copy()
    temp['region_name'] = region
    temp['category_name'] = category
    temp['material_group_name'] = material_group_name
    spark_df = spark.createDataFrame(temp)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_customer_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##GP - 3 Months & 12 Months

# COMMAND ----------

# Save the 3 months GP Data to DBFS

gp_report.rename(columns={'gp_value': 'gp_abs_Q4'}, inplace=True)
gp_report.rename(columns={'gp_contri': 'gp_contri_Q4'}, inplace=True)
gp_report = gp_report[['material_id', 'gp_abs_Q4', 'gp_contri_Q4']]

if save_files_flag == 1:
    temp = gp_report.copy()
    temp['region_name'] = region
    temp['category_name'] = category
    temp['material_group_name'] = material_group_name
    spark_df = spark.createDataFrame(temp)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_gp_3months")

# COMMAND ----------

# Gather monthly sales data and GP data for the rolling 52 weeks

gp_sales_12m = monthly_sales_df.copy()
gp_report_12m = gp_df.copy()

# COMMAND ----------

# Calculate GP value and absolute GP value

gp_report_12m = pd.merge(gp_report_12m, gp_sales_12m, on=['material_id', 'year_month'], how='inner')

gp_report_12m['gp_value'] = gp_report_12m['gp_wth_chargeback'] * gp_report_12m['sales']/100
gp_report_12m = gp_report_12m.groupby('material_id')[['gp_value']].sum().reset_index()

# COMMAND ----------

# Add the extra materials from the rolling 52 weeks period into the gp_report_12m and set their GP as 0. This is because they had no sales in the analysis period

materials_to_add = pd.merge(df['material_id'].drop_duplicates(), gp_report_12m['material_id'].drop_duplicates(), on='material_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1).reset_index(drop=True)

materials_to_add['gp_value'] = 0

gp_report_12m = pd.concat([gp_report_12m, materials_to_add], ignore_index=True)

print(f"Added {materials_to_add['material_id'].nunique()} new SKUs")

# COMMAND ----------

# Calculate the GP contribution

total_gp_value = gp_report_12m['gp_value'].sum()
gp_report_12m['gp_contri'] = gp_report_12m['gp_value'] / total_gp_value

# COMMAND ----------

# Bring the recommendation buckets into the gp dataframe and save it to DBFS

gp_report_12m = pd.merge(gp_report_12m, all_products_catg, on='material_id', how = 'inner')
gp_report_12m.rename(columns={'gp_value': 'GP'}, inplace=True)

if save_files_flag == 1:
    temp = gp_report_12m.copy()
    temp['region_name'] = region
    temp['category_name'] = category
    temp['material_group_name'] = material_group_name
    spark_df = spark.createDataFrame(temp)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_gp_12months")
