# Databricks notebook source
# MAGIC %md
# MAGIC #Function Intializations

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.preprocessing import MinMaxScaler
from sklearn.utils import resample
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix, classification_report
from sklearn.linear_model import LogisticRegression

# COMMAND ----------

dbutils.widgets.text(name = 'Category', defaultValue = 'null')
dbutils.widgets.text(name = 'Material Group', defaultValue = 'null')
dbutils.widgets.text(name = 'Region', defaultValue = 'null')
dbutils.widgets.text(name = 'Current Date', defaultValue = 'null')

# COMMAND ----------

# MAGIC %md
# MAGIC #User Inputs

# COMMAND ----------

category_name = dbutils.widgets.get('Category')
material_group_name = dbutils.widgets.get('Material Group')
region_name = dbutils.widgets.get('Region')
current_date = dbutils.widgets.get('Current Date')

analysis_period = 12 # Last [X] months will be taken as the analysis period

store_format = 'Overall'
store_id = 'Overall'

delist_materials_threshold = 25 # % format

brands_to_exclude = [] # Keep it empty if no brands are to be excluded
impact_variety_flag = 0 # 0 is for not impacting variety, 1 is for impacting variety

new_listings_threshold = 3 # If the material was newly launched in the last 3 or 6 months, then consider it newly launched

sampling_fraction = 1 # % format

gp_delist_threshold = 10 # % format
sales_delist_threshold = 10 # % format

# COMMAND ----------

# MAGIC %md
# MAGIC No inputs required below

# COMMAND ----------

# Add LULU PRIVATE LABEL in brands to exclude
brands_to_exclude.append('LULU PRIVATE LABEL')
brands_to_exclude = list(set(brands_to_exclude))

# Create a condition for store_id depending on whether the exercise is on store level or region level
if store_id == 'Overall':
    store_condition = ""
else:
    store_condition = f"AND store_id = {store_id}"

# Set the start date to 24 months prior to current date, and end date as the last date of the previous month
# current_date = str(date.today())[:8] + '01'
start_date_formatted = datetime.strptime(current_date, '%Y-%m-%d').date()
start_date = str(int(current_date[:4]) - 1) + current_date[4:]
end_date = str(start_date_formatted - timedelta(days = 1))

# Calculate the starting month of the analysis period
end_year = int(end_date[:4])
end_month = int(end_date[5:7])
start_month = end_month - analysis_period + 1
start_year = end_year

if start_month <= 0:
    start_year -= 1
    start_month += 12

analysis_start_month = int(f"{start_year}{str(start_month).zfill(2)}")
analysis_end_month = int(f"{end_date[:4]}{end_date[5:7]}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Sandbox Table

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_ao_v2 AS (
#     WITH sales_data AS (
#         SELECT
#             t1.business_day,
#             INT(CONCAT(YEAR(t1.business_day), LPAD(MONTH(t1.business_day), 2, '0'))) AS year_month,
#             t3.region_name,
#             t3.store_id,
#             t1.transaction_id,
#             t1.customer_id,
#             t2.material_id,
#             t2.material_name,
#             t2.brand,
#             t2.material_group_name,
#             t2.category_name,
#             t1.ean,
#             INT(t4.conversion_numerator) AS conversion_numerator,
#             t1.unit_price,
#             t1.regular_unit_price,
#             t1.quantity AS quantity,
#             (t1.quantity * t1.regular_unit_price) AS amount,
#             t1.unit_price - t1.regular_unit_price AS discount,
#             discount/t1.unit_price AS discount_perc,
#             CASE WHEN discount > 0 THEN 1 ELSE 0 END AS discount_flag,
#             1 AS purchase_flag
#         FROM gold.transaction.uae_pos_transactions AS t1
#         LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#         LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#         LEFT JOIN gold.material.material_attributes AS t4 ON t1.ean = t4.ean
#         WHERE
#             t1.business_day BETWEEN '2023-01-01' AND '2024-12-31'
#             AND t2.department_class_id IN (1, 2)
#             AND t3.tayeb_flag = 0
#             AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
#             AND t1.amount > 0
#             AND t1.quantity > 0
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
#         ORDER BY 1, 4, 5
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
#         FROM gold.business.gross_profit
#         WHERE country = 'AE'
#         AND year_month BETWEEN 202301 AND 202412
#     )

#     SELECT
#         t1.*,
#         COALESCE(t1.amount * t2.gp_wth_chargeback / 100, 0) AS abs_gp
#     FROM sales_data AS t1
#     LEFT JOIN gp_data AS t2
#         ON t1.region_name = t2.region_name
#         AND t1.year_month = t2.year_month
#         AND t1.material_id = t2.material_id
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_ao_v2_new_listings AS (
#     SELECT
#         t2.material_id,
#         MIN(t1.business_day) AS first_selling_date,
#         MAX(t1.business_day) AS last_selling_date,
#         t2.material_name,
#         t2.material_group_name,
#         t2.category_name,
#         t2.department_name
#     FROM gold.transaction.uae_pos_transactions AS t1
#     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#     WHERE
#         t2.department_class_id IN (1, 2)
#         AND t3.tayeb_flag = 0
#         AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
#         AND t1.amount > 0
#         AND t1.quantity > 0
#     GROUP BY 1, 4, 5, 6, 7
# )

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Prep

# COMMAND ----------

query = f"""
SELECT *
FROM dev.sandbox.pj_ao_v2
WHERE
    year_month BETWEEN {analysis_start_month} AND {analysis_end_month}
    AND category_name = '{category_name}'
    AND material_group_name = '{material_group_name}'
    AND region_name = '{region_name}'
    {store_condition}
"""
df = spark.sql(query).toPandas()

query = f"""
SELECT *
FROM dev.sandbox.pj_ao_attributes
WHERE
    category_name = '{category_name}'
    AND material_group_name = '{material_group_name}'
"""
attr = spark.sql(query).toPandas()

df = pd.merge(df, attr[['material_id', 'item_count', 'type', 'volume']], on = 'material_id', how = 'left')

# COMMAND ----------

# Make a column for number of packs in a material
df['item_count'] = df['item_count'].astype(str)
df['packs'] = np.where(df.item_count == '1', df.item_count + ' Pack', df.item_count + ' Packs')
df['item_count'] = df['item_count'].astype('int32')

# Calculate unit weight price for each product
df['conversion_numerator'] = df['conversion_numerator'].fillna(1)
df['unit_wgt_price'] = df['amount'] / (df['conversion_numerator'] * df['volume'] * df['quantity'] * df['item_count'])

# Convert volume to string type and concatenate the units to it
df['volume'] = df['volume'].astype(str)
df['volume'] = df['volume'] + 'G'

# COMMAND ----------

df['material_id'].nunique(), df.shape[0]

# COMMAND ----------

df.head().display()

# COMMAND ----------

# Prepare a list of new listings

new_listings_threshold_days = int(new_listings_threshold/3 * 13 * 7)

query = f"""
SELECT
    material_id,
    DATE_DIFF('{end_date}', first_selling_date) AS launch,
    DATE_DIFF('{end_date}', last_selling_date) AS recency
FROM dev.sandbox.pj_ao_v2_new_listings
WHERE
    category_name = '{category_name}'
    AND material_group_name = '{material_group_name}'
"""
listings_df = spark.sql(query).toPandas()

new_listings = listings_df[listings_df['launch'] <= new_listings_threshold_days]['material_id'].tolist()
new_listings = df[df['material_id'].isin(new_listings)]['material_id'].unique()

delistings = listings_df[listings_df['recency'] > new_listings_threshold_days]['material_id'].tolist()
delistings = df[df['material_id'].isin(delistings)]['material_id'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC #Attribute Shares Check

# COMMAND ----------

# Calculating cumulative sum of sales contribution for the unique type of products available in the category
attr_share_df = df.groupby('type')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'type_sales'})
attr_share_df = attr_share_df.sort_values(by = 'type_sales', ascending = False).reset_index(drop = True)
attr_share_df['type_sales'] = round(attr_share_df['type_sales'], 0)
attr_share_df['type_sales_perc'] = round(attr_share_df.type_sales.cumsum() / attr_share_df.type_sales.sum() * 100, 2)

df = pd.merge(df, attr_share_df, on='type', how='inner')

# Calculating cumulative sum of sales contribution for the unique product volumes available in the category
attr_share_df = df.groupby('volume')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'volume_sales'})
attr_share_df = attr_share_df.sort_values(by = 'volume_sales', ascending = False).reset_index(drop = True)
attr_share_df['volume_sales'] = round(attr_share_df['volume_sales'], 0)
attr_share_df['volume_sales_perc'] = round(attr_share_df.volume_sales.cumsum() / attr_share_df.volume_sales.sum() * 100, 2)
df = pd.merge(df, attr_share_df, on='volume', how='inner')

# Calculating cumulative sum of sales contribution for the unique pack types available in the category
attr_share_df = df.groupby('packs')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'pack_sales'})
attr_share_df = attr_share_df.sort_values(by = 'pack_sales', ascending = False).reset_index(drop = True)
attr_share_df['pack_sales'] = round(attr_share_df['pack_sales'], 0)
attr_share_df['pack_sales_perc'] = round(attr_share_df.pack_sales.cumsum() / attr_share_df.pack_sales.sum() * 100, 2)
df = pd.merge(df, attr_share_df, on='packs', how='inner')

# Calculating cumulative sum of sales contribution for the unique pack types available in the category
attr_share_df = df.groupby('brand')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'brand_sales'})
attr_share_df = attr_share_df.sort_values(by = 'brand_sales', ascending = False).reset_index(drop = True)
attr_share_df['brand_sales'] = round(attr_share_df['brand_sales'], 0)
attr_share_df['brand_sales_perc'] = round(attr_share_df.brand_sales.cumsum() / attr_share_df.brand_sales.sum() * 100, 2)
df = pd.merge(df, attr_share_df, on='brand', how='inner')

# Storing only the relevant columns from above into a dataframe
attr_share_df = df[['type', 'type_sales_perc', 'volume', 'volume_sales_perc', 'packs', 'pack_sales_perc', 'brand', 'brand_sales_perc']].drop_duplicates().sort_values(by = 'type_sales_perc').reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Compression

# COMMAND ----------

# Types that fall in the bottom 20% of sales are combined together and labeled as "Others"
top_attrs_count = len(attr_share_df[['type', 'type_sales_perc']].drop_duplicates()[attr_share_df['type_sales_perc'] < 80]) + 1
top_attrs = attr_share_df[['type', 'type_sales_perc']].drop_duplicates()[:top_attrs_count]['type'].tolist()
df['type'] = np.where(df.type.isin(top_attrs), df.type, 'Others')

# Volumes that fall in the bottom 20% of sales are combined together and labeled as "Others"
attr_share_df.sort_values(by = 'volume_sales_perc', inplace = True)
top_attrs_count = len(attr_share_df[['volume', 'volume_sales_perc']].drop_duplicates()[attr_share_df['volume_sales_perc'] < 80]) + 1
top_attrs = attr_share_df[['volume', 'volume_sales_perc']].drop_duplicates()[:top_attrs_count]['volume'].tolist()
df['volume'] = np.where(df.volume.isin(top_attrs), df.volume, 'Others')

# Packs that fall in the bottom 20% of sales are combined together and labeled as "Others"
attr_share_df.sort_values(by = 'pack_sales_perc', inplace = True)
top_attrs_count = len(attr_share_df[['packs', 'pack_sales_perc']].drop_duplicates()[attr_share_df['pack_sales_perc'] < 80]) + 1
top_attrs = attr_share_df[['packs', 'pack_sales_perc']].drop_duplicates()[:top_attrs_count]['packs'].tolist()
df['packs'] = np.where(df.packs.isin(top_attrs), df.packs, 'Others')

# Brands that fall in the bottom 20% of sales are combined together and labeled as "Others"
attr_share_df.sort_values(by = 'brand_sales_perc', inplace = True)
top_attrs_count = len(attr_share_df[['brand', 'brand_sales_perc']].drop_duplicates()[attr_share_df['brand_sales_perc'] < 80]) + 1
top_attrs = attr_share_df[['brand', 'brand_sales_perc']].drop_duplicates()[:top_attrs_count]['brand'].tolist()
df['brand'] = np.where(df.brand.isin(top_attrs), df.brand, 'Others')

# COMMAND ----------

# Define a function to apply sampling
def stratified_sampling(group):
    # Retain all records if the sampling fraction will round off the records to 0
    if round(len(group) * sampling_fraction / 100) == 0:
        return group.sample(n = 1, random_state = 42)
    # Otherwise, sample 10% of the group
    else:
        return group.sample(frac = sampling_fraction / 100, random_state = 42)

df_sampled = df.groupby(['material_id', 'store_id', 'year_month'], group_keys = False).apply(stratified_sampling)

# COMMAND ----------

df_sampled['material_id'].nunique(), df_sampled.shape[0]

# COMMAND ----------

# Aggregating the data on a transaction and attribute level
df2 = df_sampled.groupby(['business_day', 'transaction_id', 'type', 'volume', 'packs', 'brand']).agg(
    {'unit_wgt_price': 'mean',
     'amount': 'sum',
     'quantity': 'sum',
     'discount': 'sum',
     'abs_gp': 'sum',
     'purchase_flag': 'mean',
     'discount_flag': 'mean'}).reset_index()

# Correcting the datatypes of boolean columns
df2['discount_flag'] = round(df2['discount_flag']).astype('int32')
df2['purchase_flag'] = df2['purchase_flag'].astype('int32')

# Rounding off values for float type columns
df2['unit_wgt_price'] = round(df2['unit_wgt_price'], 4)
df2['amount'] = round(df2['amount'], 2)
df2['discount'] = round(df2['discount'], 2)

# Re-calculating discount percentage column
df2['discount_perc'] = round(df2['discount']/(df2['amount'] + df2['discount']), 4)

# Renaming columns to aggregated level names
df2 = df2.rename(columns = {'unit_wgt_price': 'avg_unit_wgt_price', 'amount': 'sales', 'discount_flag': 'dominant_discount_flag'})

# COMMAND ----------

df2.head().display()

# COMMAND ----------

# Records reduced from the data compression
len(df_sampled) - len(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #Non-Purchase Incidences Data Creation

# COMMAND ----------

# Creating all instances of purchases and non-purchases

temp1 = df2[['transaction_id']].drop_duplicates().reset_index(drop = True)
temp2 = df2[['type', 'volume', 'packs', 'brand']].drop_duplicates().reset_index(drop = True)
temp3 = pd.merge(temp1, temp2, how = 'cross')

df3 = pd.merge(df2, temp3, on = ['transaction_id', 'type', 'volume', 'packs', 'brand'], how = 'outer')

# COMMAND ----------

df3.info()

# COMMAND ----------

# Impute null values for non-purchases incidences
df3['business_day'] = df3.groupby('transaction_id')['business_day'].transform(lambda x: x.ffill().bfill())
df3['purchase_flag'] = df3['purchase_flag'].fillna(0)
df3['sales'] = df3['sales'].fillna(0)
df3['quantity'] = df3['quantity'].fillna(0)
df3['abs_gp'] = df3['abs_gp'].fillna(0)
df3['discount'] = df3['discount'].fillna(0)

# COMMAND ----------

# Price and Discount are values independent of purchase incidences. So, we impute them differently

columns_to_impute = ['avg_unit_wgt_price', 'discount_perc', 'dominant_discount_flag']
df3['business_day'] = pd.to_datetime(df3['business_day'])

# Do a backward fill and forward fill to impute null values grouped on attributes of the product
for col in columns_to_impute:
    df_temp = df3.groupby(['type', 'volume', 'packs', 'brand'])[col].transform(lambda x: x.fillna(method = 'bfill').fillna(method = 'ffill'))
    df3[col] = df3[col].fillna(df_temp)

# COMMAND ----------

# Convert the data type of business_day to string type
df3['business_day'] = df3['business_day'].dt.strftime('%Y-%m-%d')

# COMMAND ----------

df3.info()

# COMMAND ----------

# Model Dataset Preparation

# Taking only relevant columns for model dataset
df3 = df3[['type', 'volume', 'packs', 'brand', 'avg_unit_wgt_price', 'discount_perc', 'dominant_discount_flag', 'purchase_flag']]

# COMMAND ----------

# MAGIC %md
# MAGIC #MDS Prep

# COMMAND ----------

# Read model dataset
data_df = df3.copy()

# Normalize numeric columns
scaler = MinMaxScaler()
columns_to_scale = ['avg_unit_wgt_price', 'discount_perc']
data_df[columns_to_scale] = scaler.fit_transform(data_df[columns_to_scale])

data_df.drop(columns = 'discount_perc', inplace = True) # dropping this since its beta value is negative which doesn't make sense. The data likely isn't reliable

# Perform one-hot encoding for categorical columns
df_encoded = pd.get_dummies(data_df, columns = ['type', 'volume', 'packs', 'brand'], drop_first = True)

# Remove any blank spaces in column names
df_encoded.columns = [col.replace('_ ', '_') for col in df_encoded.columns]

# COMMAND ----------

df_encoded['purchase_flag'].value_counts()

# COMMAND ----------

# Undersample the majority class

# Separate majority (class 0) and minority (class 1) classes
df_majority = df_encoded[df_encoded.purchase_flag == 0]
df_minority = df_encoded[df_encoded.purchase_flag == 1]

# Undersample the majority class (class 0) to have the same number of instances as the minority class
df_majority_undersampled = resample(df_majority, 
                                    replace = False,    # Do not replace samples
                                    n_samples = len(df_minority),  # Make the size equal to the minority class
                                    random_state = 42)  # Ensure reproducibility

# Combine the undersampled majority class with the minority class
df_balanced = pd.concat([df_majority_undersampled, df_minority])

# Shuffle the resulting dataframe
df_balanced = df_balanced.sample(frac = 1, random_state = 42).reset_index(drop=True)

# Check the new class distribution
df_balanced['purchase_flag'].value_counts()

# COMMAND ----------

# X = np.array(df_balanced, dtype=float)
# vif_data = pd.DataFrame()
# vif_data["feature"] = df_balanced.columns
# vif_data["VIF"] = [variance_inflation_factor(X, i) for i in range(X.shape[1])]
# vif_data.sort_values(by = 'VIF', ascending = False).reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Model Building

# COMMAND ----------

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(df_balanced.drop(columns = 'purchase_flag'), df_balanced['purchase_flag'], test_size = 0.3, random_state = 42)

# Model initialization
model = LogisticRegression(max_iter = 10000)

# Model fitting and testing
model.fit(X_train, y_train)
y_pred = model.predict(X_test)
y_pred_train = model.predict(X_train)

# Calculate accuracy, precision, and recall
train_accuracy = accuracy_score(y_train, y_pred_train)
test_accuracy = accuracy_score(y_test, y_pred)
train_precision = precision_score(y_train, y_pred_train)
test_precision = precision_score(y_test, y_pred)
train_recall = recall_score(y_train, y_pred_train)
test_recall = recall_score(y_test, y_pred)

print(f"Train Accuracy: {train_accuracy:.3f}\nTest Accuracy: {test_accuracy:.3f}")
print(f"\nTrain Precision: {train_precision:.3f}\nTest Precision: {test_precision:.3f}")
print(f"\nTrain Recall: {train_recall:.3f}\nTest Recall: {test_recall:.3f}")

# Display confusion matrix
conf_matrix = confusion_matrix(y_test, y_pred)
print(f"\nConfusion Matrix:\n{conf_matrix}")

# Display classification report
# class_report = classification_report(y_test, y_pred)
# print(f"\nClassification Report:\n{class_report}")

# Extract beta coefficients
coefficients = model.coef_.flatten()
coef_df = pd.DataFrame({'Feature': X_train.columns, 'Coefficient': coefficients})

# Sort coefficients by absolute value for easier interpretation
coef_df['Abs_Coefficient'] = abs(coef_df['Coefficient'])
coef_df = coef_df.sort_values('Abs_Coefficient', ascending=False).drop('Abs_Coefficient', axis=1)
coef_df.display()

# COMMAND ----------

# Save feature importance
# coef_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_v2_poc_feature_importance.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Beta Values

# COMMAND ----------

# Calculate the adjusted beta intercept
true_p1 = len(df_encoded[df_encoded['purchase_flag'] == 1]) / len(df_encoded)
resampled_p1 = len(df_balanced[df_balanced['purchase_flag'] == 1]) / len(df_balanced)

adjusted_intercept = model.intercept_[0] - np.log(true_p1 / (1 - true_p1)) + np.log(resampled_p1 / (1 - resampled_p1))

# COMMAND ----------

# Create a dataframe with the dummification for each Material ID

materials_df = df_sampled[['material_id', 'material_name', 'type', 'packs', 'volume', 'brand']].drop_duplicates().reset_index(drop = True)

materials_df_encoded = pd.get_dummies(materials_df, columns = ['type', 'volume', 'packs', 'brand'], drop_first = True)

# Remove any blank spaces in column names
materials_df_encoded.columns = [col.replace('_ ', '_') for col in materials_df_encoded.columns]

# COMMAND ----------

# Multiply beta values with each respective attribute

columns = materials_df_encoded.columns.tolist()
columns_to_remove = ['material_id', 'material_name']

columns = [col for col in columns if col not in columns_to_remove]

materials_beta_df = materials_df_encoded.copy()
for col in columns:
    materials_beta_df[col] = materials_beta_df[col] * coef_df[coef_df['Feature'] == col]['Coefficient'].iloc[0]

materials_beta_df['beta_x_field'] = materials_beta_df[columns].sum(axis=1)
materials_beta_df['beta_x_field'] = materials_beta_df['beta_x_field'] + adjusted_intercept

# COMMAND ----------

# Calculate average price, total quantity sold, and the final probability value
query = f"""
SELECT
    material_id,
    brand,
    SUM(amount) / SUM(quantity) AS avg_price,
    SUM(quantity) AS total_units,
    SUM(amount) / SUM(SUM(amount)) OVER () AS sales_perc,
    SUM(abs_gp) / SUM(SUM(abs_gp)) OVER () AS abs_gp_perc,
    SUM(abs_gp) / SUM(amount) AS gp_margin
FROM dev.sandbox.pj_ao_v2
WHERE
    year_month BETWEEN {analysis_start_month} AND {analysis_end_month}
    AND category_name = '{category_name}'
    AND material_group_name = '{material_group_name}'
    AND region_name = '{region_name}'
    {store_condition}
GROUP BY material_id, brand
"""
price_units_df = spark.sql(query).toPandas()

# Calculate the predicted total value of each material
materials_value_df = materials_beta_df[['material_id', 'material_name', 'beta_x_field']].copy()
materials_value_df = materials_value_df.merge(price_units_df, on = 'material_id', how = 'left')
materials_value_df['total_units'] = materials_value_df['total_units'].astype(int)
materials_value_df['beta_exp'] = np.exp(materials_value_df['beta_x_field'])
materials_value_df = materials_value_df.sort_values(by = 'total_units', ascending = False).reset_index(drop = True)

materials_value_df['probability'] = materials_value_df['beta_exp'] / materials_value_df['beta_exp'].sum()
materials_value_df['prob_x_units'] = materials_value_df['probability'] * materials_value_df['total_units']
materials_value_df['value'] = materials_value_df['avg_price'] * materials_value_df['prob_x_units'] * materials_value_df['gp_margin']

# COMMAND ----------

# Save beta values
# materials_value_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_v2_poc_beta_values.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Optimization

# COMMAND ----------

# Function to calculate the total value after iteratively removing each material
def calculate_value(original_df, trial_df):
    trial_df['probability_new'] = trial_df['beta_exp'] / trial_df['beta_exp'].sum()

    trial_df_materials = trial_df['material_id'].tolist()
    original_df_materials = original_df['material_id'].tolist()
    materials_mismatching = [i for i in original_df_materials if i not in trial_df_materials]
    units_to_reallocate = original_df[original_df['material_id'].isin(materials_mismatching)]['prob_x_units'].sum()

    trial_df['reallocated_qty'] = trial_df['probability_new'] * units_to_reallocate
    trial_df['total_predicted_qty'] = trial_df['reallocated_qty'] + trial_df['prob_x_units']

    trial_df['sales'] = trial_df['avg_price'] * trial_df['total_predicted_qty']
    trial_df['value'] = trial_df['sales'] * trial_df['gp_margin']
    
    total_sales = trial_df['sales'].sum()
    total_value = trial_df['value'].sum()
    margin = total_value / total_sales

    return total_value, margin

# COMMAND ----------

# Calculate the count of materials that can be removed as per the constraint
max_remove = round(len(materials_value_df) * delist_materials_threshold / 100)

# Calculate the original total value and the predicted current margin to compare it with the optimized results
original_value, predicted_current_margin = calculate_value(materials_value_df, materials_value_df)

# Iteratively remove materials and until either the above count is reached or until the total value starts decreasing

prev_value = original_value
materials_optimized_df = materials_value_df.copy()
materials_temp_store = []
continue_flag = 0
for i in range(max_remove): # Each iteration will remove exactly 1 material

    records = len(materials_optimized_df)
    values_dct = {}
    materials = materials_optimized_df['material_id'].tolist()

    # Iterate as many as materials present in the data to find the one which would give the highest increase in the total value after its delisting
    for r in range(records):

        trial_df = materials_optimized_df.copy()
        trial_df = trial_df[trial_df['material_id'] != materials[r]]
        total_value, margin = calculate_value(materials_value_df, trial_df)
        values_dct[materials[r]] = total_value
    
    # If the material giving the highest increase in total value belongs to a brand that should not be delisted, if it is a new listing, or if it crosses the GP/Sales % threshold, then look for the next best material
    while True:
        max_key = max(values_dct, key = values_dct.get)
        max_brand = materials_optimized_df[materials_optimized_df['material_id'] == max_key]['brand'].iloc[0]
        max_gp = materials_optimized_df[materials_optimized_df['material_id'] == max_key]['abs_gp_perc'].iloc[0]
        max_sales = materials_optimized_df[materials_optimized_df['material_id'] == max_key]['sales_perc'].iloc[0]
        if (max_brand in brands_to_exclude) or (max_key in new_listings) or (max_gp > (gp_delist_threshold / 100)) or (max_sales > (sales_delist_threshold / 100)):
            values_dct.pop(max_key)
        else:
            break

    # If the new total value is less than the total value from the previous iteration, then still run the iterations some more times to see if the total value increases
    max_value = values_dct[max_key]
    if max_value < prev_value:
        temp_optimized_df = materials_optimized_df.copy()
        temp_optimized_df = temp_optimized_df[temp_optimized_df['material_id'] != max_key]

        materials_temp_store.append(max_key)
        continue_flag = 1
    else:
        prev_value = max_value

        # Remove the material which would give the highest increase in total value after its removal
        materials_temp_store.append(max_key)
        materials_optimized_df = materials_optimized_df[~materials_optimized_df['material_id'].isin(materials_temp_store)]

        materials_optimized_df.reset_index(drop = True, inplace = True)
        materials_temp_store = []

# COMMAND ----------

print(f"Original value: {round(original_value)}")
print(f"Optimized value: {round(max_value)}")
print(f"Increment: {round(max_value - original_value)}")

# COMMAND ----------

materials_optimized_df = materials_optimized_df[['material_id', 'material_name', 'brand']]

# COMMAND ----------

# If any variety is being impacted, then take the best material from the varities out of the delist recommendation
if not impact_variety_flag:

    materials_reco_df = materials_optimized_df[['material_id']].copy()
    materials_reco_df['delist_flag'] = 0
    materials_reco_df = materials_value_df[['material_id', 'material_name', 'brand', 'total_units']].merge(materials_reco_df, on = 'material_id', how = 'left')
    materials_reco_df['delist_flag'] = materials_reco_df['delist_flag'].fillna(1)
    materials_reco_df = materials_reco_df.merge(attr[['material_id', 'type']], on = 'material_id', how = 'left')
    
    type_delists_df = materials_reco_df.groupby('type').agg(
        total_count=('material_id', 'count'),
        delist_count=('delist_flag', lambda x: (x == 1).sum())
        ).reset_index()

    types_to_modify = type_delists_df[type_delists_df['total_count'] == type_delists_df['delist_count']]['type'].tolist()

    materials_reco_df.loc[
        materials_reco_df[materials_reco_df['type'].isin(types_to_modify)]
        .sort_values(by='total_units', ascending=False)
        .drop_duplicates('type').index,
        'delist_flag'] = 0

    materials_optimized_df = materials_reco_df[materials_reco_df['delist_flag'] == 0][['material_id', 'material_name', 'brand']].reset_index(drop = True)

    print(f"{len(types_to_modify)} materials removed from the recommendation, one from each of the following types:\n{types_to_modify}")

# COMMAND ----------

# If the GP of delist SKUs is greater than 3% then remove some products from delist
gp_contri_df = df.groupby('material_id')['abs_gp'].sum().reset_index()
gp_contri_df['abs_gp_perc'] = gp_contri_df['abs_gp'] / gp_contri_df['abs_gp'].sum()

gp_contri_df = gp_contri_df.merge(materials_reco_df, on = 'material_id', how = 'left')
gp_contri_df['delist_flag'] = gp_contri_df['delist_flag'].fillna(1)

gp_delist = gp_contri_df[gp_contri_df['delist_flag'] == 1]['abs_gp_perc'].sum()

if gp_delist >= (gp_delist_threshold / 100):
    gp_to_retain = gp_delist - (gp_delist_threshold / 100)
    gp_contri_df = gp_contri_df.sort_values(by = 'abs_gp_perc', ascending = False).reset_index(drop = True)

    temp_gp = -1
    i = 0
    while temp_gp < gp_to_retain:
        temp_materials = gp_contri_df[gp_contri_df['delist_flag'] == 1]['material_id'].iloc[:i+1]
        temp_gp = gp_contri_df[gp_contri_df['material_id'].isin(temp_materials)]['abs_gp_perc'].sum()
        i += 1
    
    materials_reco_df.loc[materials_reco_df['material_id'].isin(temp_materials), 'delist_flag'] = 0
    materials_optimized_df = materials_reco_df[materials_reco_df['delist_flag'] == 0][['material_id', 'material_name', 'brand']].reset_index(drop = True)

    print(f"{len(temp_materials)} materials removed from the recommendation")

# COMMAND ----------

# If the sales of delist SKUs is greater than 3% then remove some products from delist
sales_contri_df = df.groupby('material_id')['amount'].sum().reset_index()
sales_contri_df['sales_perc'] = sales_contri_df['amount'] / sales_contri_df['amount'].sum()

sales_contri_df = sales_contri_df.merge(materials_reco_df, on = 'material_id', how = 'left')
sales_contri_df['delist_flag'] = sales_contri_df['delist_flag'].fillna(1)

sales_delist = sales_contri_df[sales_contri_df['delist_flag'] == 1]['sales_perc'].sum()

if sales_delist >= (sales_delist_threshold / 100):
    sales_to_retain = sales_delist - (sales_delist_threshold / 100)
    sales_contri_df = sales_contri_df.sort_values(by = 'sales_perc', ascending = False).reset_index(drop = True)

    temp_sales = -1
    i = 0
    while temp_sales < sales_to_retain:
        temp_materials = sales_contri_df[sales_contri_df['delist_flag'] == 1]['material_id'].iloc[:i+1]
        temp_sales = sales_contri_df[sales_contri_df['material_id'].isin(temp_materials)]['sales_perc'].sum()
        i += 1
    
    materials_reco_df.loc[materials_reco_df['material_id'].isin(temp_materials), 'delist_flag'] = 0
    materials_optimized_df = materials_reco_df[materials_reco_df['delist_flag'] == 0][['material_id', 'material_name', 'brand']].reset_index(drop = True)

    print(f"{len(temp_materials)} materials removed from the recommendation")

# COMMAND ----------

# Calculate new maximized value and predicted expected margin

materials_optimized_df = materials_optimized_df.merge(materials_value_df[['material_id', 'prob_x_units', 'beta_exp', 'avg_price', 'gp_margin']], on = 'material_id', how = 'left')

max_value_new, predicted_expected_margin = calculate_value(materials_value_df, materials_optimized_df)

# If the predicted expected margin is less than the predicted current margin, then recommend no SKUs for delisting
if predicted_expected_margin < predicted_current_margin:
    materials_optimized_df = materials_value_df[['material_id', 'material_name', 'brand']].copy()

    # Set the optimized values to the original values
    predicted_expected_margin = predicted_current_margin
    max_value_new = original_value

# COMMAND ----------

materials_optimized_df = materials_optimized_df[['material_id', 'material_name', 'brand']]

# COMMAND ----------

# Save the optimized set of materials
# materials_optimized_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_v2_poc_optimized_results.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ##Overview

# COMMAND ----------

# Count of total SKUs and delist SKUs

materials_reco_df = materials_optimized_df[['material_id']].copy()
materials_reco_df['delist_flag'] = 0
materials_value_df = materials_value_df.merge(materials_reco_df, on = 'material_id', how = 'left')
materials_value_df['delist_flag'] = materials_value_df['delist_flag'].fillna(1)

total_skus = len(materials_value_df)
delist_skus = len(materials_value_df[materials_value_df['delist_flag'] == 1])

# COMMAND ----------

delist_skus_df = materials_value_df[materials_value_df['delist_flag'] == 1][['material_id', 'material_name', 'brand']].reset_index(drop = True)
# delist_skus_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_v2_poc_delist_skus.csv', index = False)

# COMMAND ----------

# Calculate the sales % of the delist recommended SKUs

materials_value_df['sales'] = materials_value_df['avg_price'] * materials_value_df['total_units']
materials_value_df['sales_perc'] = materials_value_df['sales'] / materials_value_df['sales'].sum()

sales_delist = materials_value_df[materials_value_df['delist_flag'] == 1]['sales_perc'].sum()

# COMMAND ----------

# Calculate the profit % of the delist recommended SKUs

materials_value_df['abs_gp'] = materials_value_df['sales'] * materials_value_df['gp_margin']

gp_delist = materials_value_df[materials_value_df['delist_flag'] == 1]['abs_gp_perc'].sum()

# COMMAND ----------

# Calculate the current margin and the expected margin after delisting the recommended materials
delist_materials = delist_skus_df['material_id'].tolist()

current_margin = df['abs_gp'].sum() / df['amount'].sum()
expected_margin = df[~df['material_id'].isin(delist_materials)]['abs_gp'].sum() / df[~df['material_id'].isin(delist_materials)]['amount'].sum()

# COMMAND ----------

if len(delist_materials) == 0:
    delist_materials = None

final_results = pd.DataFrame({'category_name': [category_name],
                              'material_group_name': [material_group_name],
                              'region_name': [region_name],
                              'store_format': [store_format],
                              'store_id': [str(store_id)],
                              'total_skus': [total_skus],
                              'delist_skus': [delist_skus],
                              'sales_perc_delist': [sales_delist],
                              'profit_perc_delist': [gp_delist],
                              'current_value': [original_value],
                              'expected_value': [max_value_new],
                              'incremental_value': [max_value_new - original_value],
                              'current_margin': [current_margin],
                              'expected_margin': [expected_margin],
                              'margin_delta': [expected_margin - current_margin],
                              'predicted_current_margin': [predicted_current_margin],
                              'predicted_expected_margin': [predicted_expected_margin],
                              'predicted_margin_delta': [predicted_expected_margin - predicted_current_margin],
                              'train_accuracy': [train_accuracy],
                              'test_accuracy': [test_accuracy],
                              'train_precision': [train_precision],
                              'test_precision': [test_precision],
                              'train_recall': [train_recall],
                              'test_recall': [test_recall],
                              'feature_importance': [coef_df.to_dict(orient="list")],
                              'delist_skus_lst': [delist_materials],
                              'start_date': [start_date],
                              'end_date': [end_date]})

sdf = spark.createDataFrame(final_results)
sdf.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_ao_v2_results_master")

# COMMAND ----------

# Display all high level results

print(f"Total SKUs present: {total_skus}")
print(f"Delist recommended SKUs: {delist_skus} ({round(delist_skus / total_skus * 100)}%)")
print(f"Sales contribution of recommended SKUs: {round(sales_delist * 100, 2)}%")
print(f"Profit contribution of recommended SKUs: {round(gp_delist * 100, 2)}%")
print(f"Original value: {round(original_value)}")
print(f"Optimized value: {round(max_value_new)} (+{round(max_value_new - original_value)})")
print(f"Current margin: {round(current_margin * 100, 2)}%")
print(f"Expected margin after rationalization: {round(expected_margin * 100, 2)}% (+{round((expected_margin - current_margin) * 100, 2)}%)")
print(f"Predicted current margin: {round(predicted_current_margin * 100, 2)}%")
print(f"Predicted expected margin: {round(predicted_expected_margin * 100, 2)}% (+{round((predicted_expected_margin - predicted_current_margin)* 100, 2)}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Variety Impact

# COMMAND ----------

# Calculate material level sales contribution
variety_brand_impact_df = materials_value_df[['material_id', 'delist_flag', 'avg_price', 'total_units']].copy()
variety_brand_impact_df['sales'] = variety_brand_impact_df['avg_price'] * variety_brand_impact_df['total_units']
variety_brand_impact_df['sales_perc'] = variety_brand_impact_df['sales'] / variety_brand_impact_df['sales'].sum()
variety_brand_impact_df.drop(columns = ['avg_price', 'total_units', 'sales'], inplace = True)

variety_brand_impact_df = variety_brand_impact_df.merge(attr[['material_id', 'type', 'brand']], on = 'material_id', how = 'left')

# variety_brand_impact_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/Dummy Tool/material_sales.csv', index = False)

# COMMAND ----------

# Calculate the impact of delisting on the variety
variety_impact_df = variety_brand_impact_df.drop(columns = 'brand')

type_delists_df = variety_impact_df.groupby('type').agg(
    total_count = ('material_id', 'count'),
    delist_count = ('delist_flag', lambda x: (x == 1).sum()),
    sales_perc = ('sales_perc', 'sum'),
    delist_sales_perc=('sales_perc', lambda x: x[variety_impact_df.loc[x.index, 'delist_flag'] == 1].sum())
    ).reset_index()

type_delists_df['delist_perc'] = round(type_delists_df['delist_count'] / type_delists_df['total_count'] * 100, 2)
type_delists_df['sales_perc'] = round(type_delists_df['sales_perc'] * 100, 2)
type_delists_df['delist_sales_perc'] = round(type_delists_df['delist_sales_perc'] * 100, 2)

type_delists_df = type_delists_df[['type', 'total_count', 'delist_perc', 'sales_perc', 'delist_sales_perc']]
type_delists_df.sort_values(by = ['delist_perc', 'delist_sales_perc', 'total_count', 'sales_perc', 'type'], ascending = [False, False, False, False, True], inplace = True)
type_delists_df.reset_index(drop = True, inplace = True)
print(f"Impact on variety post delisting:\n{type_delists_df.to_string(index=False)}")

# COMMAND ----------

# type_delists_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/Dummy Tool/variety_impact.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Brand Impact

# COMMAND ----------

# Calculate the impact of delisting on the brands
brand_impact_df = variety_brand_impact_df.drop(columns = 'type')

brand_delists_df = brand_impact_df.groupby('brand').agg(
    total_count=('material_id', 'count'),
    delist_count=('delist_flag', lambda x: (x == 1).sum()),
    sales_perc=('sales_perc', 'sum'),
    delist_sales_perc=('sales_perc', lambda x: x[brand_impact_df.loc[x.index, 'delist_flag'] == 1].sum())
    ).reset_index()

brand_delists_df['delist_perc'] = round(brand_delists_df['delist_count'] / brand_delists_df['total_count'] * 100, 2)
brand_delists_df['sales_perc'] = round(brand_delists_df['sales_perc'] * 100, 2)
brand_delists_df['delist_sales_perc'] = round(brand_delists_df['delist_sales_perc'] * 100, 2)

brand_delists_df = brand_delists_df[['brand', 'total_count', 'delist_perc', 'sales_perc', 'delist_sales_perc']]
brand_delists_df.sort_values(by = ['delist_perc', 'delist_sales_perc', 'total_count', 'sales_perc', 'brand'], ascending = [False, False, False, False, True], inplace = True)
brand_delists_df.reset_index(drop = True, inplace = True)
print(f"Impact on brands post delisting:\n{brand_delists_df.to_string(index=False)}")

# COMMAND ----------

# brand_delists_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/Dummy Tool/brand_impact.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##LFL Growth

# COMMAND ----------

# Material Group's LFL Growth

lfl_end_date = str(int(end_date[:4]) - 1) + end_date[4:]
lfl_start_date = str(int(current_date[:4]) - 2) + current_date[4:]
lfl_start_month = int(f"{lfl_start_date[:4]}{lfl_start_date[5:7]}")

query = f"""
WITH cte AS (
    SELECT
        (CASE WHEN business_day <= '{lfl_end_date}' THEN "LFL" ELSE "Current" END) AS year_info,
        material_id,
        material_name,
        ROUND(SUM(amount)) AS yearly_sales,
        ROUND(SUM(abs_gp)) AS yearly_abs_gp
    FROM dev.sandbox.pj_ao_v2
    WHERE
        year_month BETWEEN {lfl_start_month} AND {analysis_end_month}
        AND region_name = '{region_name}'
        {store_condition}
        AND category_name = '{category_name}'
        AND material_group_name = '{material_group_name}'
    GROUP BY 1, 2, 3
)

SELECT
    material_id,
    material_name,
    MAX(CASE WHEN year_info = "LFL" THEN yearly_sales ELSE 0 END) AS lfl_sales,
    MAX(CASE WHEN year_info = "Current" THEN yearly_sales ELSE 0 END) AS sales,
    (sales - lfl_sales)/lfl_sales AS sales_growth,
    MAX(CASE WHEN year_info = "Current" THEN yearly_abs_gp ELSE 0 END) AS abs_gp
FROM cte
GROUP BY 1, 2
ORDER BY 1
"""

lfl_growth_df = spark.sql(query).toPandas()
lfl_growth_df['sales_growth'] = lfl_growth_df['sales_growth'].fillna(0)
lfl_growth_df['sales_growth'] = lfl_growth_df['sales_growth'].replace(float('inf'), 0)

cy_sales = lfl_growth_df['sales'].sum()
py_sales = lfl_growth_df['lfl_sales'].sum()
yoy_df = pd.DataFrame({'year': ['PY', 'CY'], 'sales': [py_sales, cy_sales]})

growth = str(round((cy_sales - py_sales)/py_sales * 100, 1))

fig = px.bar(yoy_df, x = 'year', y = 'sales', title = f'Year Over Year Growth ({growth}%)')
fig.show()

# COMMAND ----------

# Material Group Performance LFL

x_val = 'sales'
y_val = 'sales_growth'
color_val = 'delist_flag'
hover_name_val = 'material_name'
size_max_val = 40
size_val = 'abs_gp'

lfl_growth_df = lfl_growth_df.merge(materials_value_df[['material_id', 'delist_flag']], on = 'material_id', how = 'left')

lfl_growth_df['delist_flag'] = lfl_growth_df['delist_flag'].fillna(0).astype(str)
# lfl_growth_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/Dummy Tool/lfl_growth.csv', index = False)
lfl_growth_df['sales_growth'] = np.log(lfl_growth_df['sales_growth'] + 0.001)

fig = px.scatter(lfl_growth_df, x = x_val, y = y_val, size = size_val, color = color_val,
                 hover_name = hover_name_val, log_x = False, log_y = False, size_max = size_max_val,
                 title = "ALL SKUS BY RECO", color_discrete_map = {0: "#00CC96", 1: "#EF553B"})
fig.add_hline(y = lfl_growth_df[y_val].quantile(0.95))
fig.add_vline(x = lfl_growth_df[x_val].quantile(0.95))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##YTD Growth

# COMMAND ----------

# Material Group Performance YTD

ytd_start_month = end_date[:4] + '01'

query = f"""
SELECT
    material_id,
    material_name,
    year_month,
    ROUND(SUM(amount)) AS sales,
    ROUND(SUM(abs_gp)) AS abs_gp
FROM dev.sandbox.pj_ao_v2
WHERE
    year_month BETWEEN {ytd_start_month} AND {analysis_end_month}
    AND category_name = '{category_name}'
    AND material_group_name = '{material_group_name}'
    AND region_name = '{region_name}'
    {store_condition}
GROUP BY 1, 2, 3
ORDER BY 1, 3
"""
ytd_df = spark.sql(query).toPandas()

sales_growth_df = ytd_df.copy()
sales_growth_df['sales_growth'] = sales_growth_df.groupby('material_id')['sales'].pct_change() * 100
sales_growth_df['avg_monthly_sales_growth'] = sales_growth_df.groupby('material_id')['sales_growth'].transform('mean')
sales_growth_df['sales'] = sales_growth_df.groupby('material_id')['sales'].transform('sum')
sales_growth_df = sales_growth_df[['material_id', 'material_name', 'sales', 'avg_monthly_sales_growth', 'abs_gp']].drop_duplicates().reset_index(drop = True)

sales_growth_df['avg_monthly_sales_growth'] = sales_growth_df['avg_monthly_sales_growth'].fillna(0)

x_val = 'sales'
y_val = 'avg_monthly_sales_growth'
color_val = 'delist_flag'
hover_name_val = 'material_name'
size_max_val = 40
size_val = 'abs_gp'

sales_growth_df = sales_growth_df.merge(materials_value_df[['material_id', 'delist_flag']], on = 'material_id', how = 'left')

sales_growth_df['delist_flag'] = sales_growth_df['delist_flag'].fillna(0).astype('int32').astype(str)
# sales_growth_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/Dummy Tool/ytd_growth.csv', index = False)
sales_growth_df['avg_monthly_sales_growth'] = np.log(sales_growth_df['avg_monthly_sales_growth'] + 0.001)

fig = px.scatter(sales_growth_df, x = x_val, y = y_val, size = size_val, color = color_val,
                 hover_name = hover_name_val, log_x = False, log_y = False, size_max = size_max_val,
                 title = "ALL SKUS BY RECO", color_discrete_map = {0: "#00CC96", 1: "#EF553B"})
fig.add_hline(y = sales_growth_df[y_val].quantile(0.95))
fig.add_vline(x = sales_growth_df[x_val].quantile(0.95))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Listings & Delistings

# COMMAND ----------

# New listings

if len(new_listings) > 0:
    print(f"Recent new listings: {new_listings}")
else:
    print("No new listings have been added recently")

# COMMAND ----------

# Delistings

if len(delistings) > 0:
    print(f"Recent delistings: {delistings}")
else:
    print("No delistings have occured recently")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales & Profit Distribution

# COMMAND ----------

tt = df.groupby('material_id').sum()[['abs_gp', 'amount']].reset_index()
tt.rename(columns = {'amount': 'sales'}, inplace = True)
tt = pd.merge(tt, attr[['material_id', 'brand']], on = 'material_id', how = 'inner')
# tt.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/Dummy Tool/distribution.csv', index = False)
tt['material_id'] = tt['material_id'].astype('str')

tt = tt.sort_values(by = 'abs_gp', ascending = False)
tt['gp_cum_sum'] = np.round(100 * (tt.abs_gp.cumsum() / tt['abs_gp'].sum()), 2)
cum_sum_80_gp_materials = tt[tt['gp_cum_sum'] < 80]['material_id'].count() + 1
cum_sum_80_gp_brands = tt['brand'][:cum_sum_80_gp_materials].nunique()

fig = go.Figure()
fig = make_subplots(specs = [[{"secondary_y": True}]])
fig.add_trace(go.Bar(y = tt['abs_gp'], x = tt['material_id'], name = "material GP"), secondary_y = False)
fig.add_trace(go.Scatter(x = tt['material_id'], y = tt['gp_cum_sum'],
                         mode = 'lines', name = "GP Cumulative Contri"), secondary_y = True)
fig.update_layout(title_text = "abs_gp and abs_gp dist", showlegend = False, autosize = False, width = 900, height = 400)
fig.update_xaxes(title_text = "material_id")
fig.update_yaxes(title_text = "material_id abs_gp", secondary_y = False)
fig.update_yaxes(title_text = "% Cumulative Contribution", secondary_y = True)
# fig.add_hline(secondary_y = 80)
fig.show()

tt = tt.sort_values(by = 'sales', ascending = False)
tt['sale_cum_sum'] = np.round(100 * (tt.sales.cumsum() / tt['sales'].sum()), 2)
cum_sum_80_sale_materials = tt[tt['sale_cum_sum'] < 80]['material_id'].count() + 1
cum_sum_80_sale_brands = tt['brand'][:cum_sum_80_sale_materials].nunique()

fig2 = go.Figure()
fig2 = make_subplots(specs = [[{"secondary_y": True}]])
fig2.add_trace(go.Bar(y = tt['sales'], x = tt['material_id'], name = "material sales"), secondary_y = False)
fig2.add_trace(go.Scatter(x = tt['material_id'], y = tt['sale_cum_sum'],
                         mode = 'lines', name = "Sales Cumulative Contri"), secondary_y = True)
fig2.update_layout(title_text = "Sales dist", showlegend = False, autosize = False, width = 900, height = 400)
fig2.update_xaxes(title_text = "material_id")
fig2.update_yaxes(title_text = "material_id sales", secondary_y = False)
fig2.update_yaxes(title_text = "% Cumulative Contribution", secondary_y = True)
# fig2.add_hline(secondary_y = 80)
fig2.show()

# COMMAND ----------

# Display the number of materials and brands contributing to 80% sales & profit

top_80_sale_materials_perc = round(cum_sum_80_sale_materials/tt['material_id'].nunique()*100,2)
top_80_gp_materials_perc = round(cum_sum_80_gp_materials/tt['material_id'].nunique()*100,2)
print(f"Only top {top_80_sale_materials_perc}% and {top_80_gp_materials_perc}% SKUs provide 80% of sales and profit respectively")
print(f"\n{cum_sum_80_sale_materials} SKUs belonging to {cum_sum_80_sale_brands} brands contribute to 80% Sales")
print(f"{cum_sum_80_gp_materials} SKUs belonging to {cum_sum_80_gp_brands} brands contribute to 80% profit")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Customer Penetration

# COMMAND ----------

# Calculate ending year_month for customer segments filter
end_month_year = end_date[:4] + end_date[5:7]

query = f"""
WITH sales AS (
    SELECT
        material_id,
        material_name,
        customer_id
    FROM dev.sandbox.pj_ao_v2
    WHERE
        year_month BETWEEN {analysis_start_month} AND {analysis_end_month}
        AND category_name = '{category_name}'
        AND material_group_name = '{material_group_name}'
        AND region_name = '{region_name}'
        {store_condition}
    GROUP BY 1, 2, 3
),

segments AS (
    SELECT
        customer_id,
        segment
    FROM analytics.segment.customer_segments
    WHERE
        key = 'rfm'
        AND channel = 'pos'
        AND country = 'uae'
        AND month_year = {end_month_year}
        AND segment IN ('VIP', 'Frequentist', 'Moderate', 'Splurger')
)

SELECT
    STRING(t1.material_id) AS material_id,
    t1.material_name,
    t1.customer_id,
    t2.segment
FROM sales AS t1
JOIN segments AS t2 ON t1.customer_id = t2.customer_id
"""

customer_df = spark.sql(query).toPandas()

# COMMAND ----------

segment_input = "All Customers" # All Customers, VIP, Frequentist, Splurger, Moderate

if segment_input != 'All Customers':
    segment_df = customer_df[customer_df['segment'] == segment_input].reset_index(drop = True)
else:
    segment_df = customer_df.copy()

total_customers = segment_df['customer_id'].nunique()
segment_df = segment_df.groupby(['material_id', 'material_name'])[['customer_id']].nunique().reset_index()
segment_df['customer_perc'] = round(segment_df['customer_id'] / total_customers * 100, 2)
segment_df.drop(columns = 'customer_id', inplace = True)

segment_df = segment_df.nlargest(20, "customer_perc")

# segment_df.to_csv('/Workspace/Users/prem@loyalytics.in/Assortment Optimization/Dummy Tool/customer_penetration.csv', index = False)

max_value = segment_df['customer_perc'].max()

fig = px.bar(segment_df, x = 'material_id', y = 'customer_perc', title = 'Top 20 Products by Customer %', hover_name = 'material_name')
fig.update_traces(width=0.4)
fig.update_layout(xaxis_title_font_size = 18, yaxis_title_font_size = 18, xaxis_tickfont_size = 16, yaxis_tickfont_size = 16,
                    title_font_size = 20, margin = dict(t = 50), title_x = 0.35,
                    yaxis=dict(range = [0, max_value + 1.5], tickmode='array'),
                    title=dict(x=0.5, xanchor='center'))
fig.show()
