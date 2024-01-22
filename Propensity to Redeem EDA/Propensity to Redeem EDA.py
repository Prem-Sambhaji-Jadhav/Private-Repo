# Databricks notebook source
# MAGIC %md
# MAGIC ##Saving Sandbox Table

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS sandbox.pj_propensity_to_redeem_data;

# CREATE TABLE sandbox.pj_propensity_to_redeem_data AS (
#   WITH customer_segment_profile AS (SELECT DISTINCT t2.customer_id, t1.segment, t1.recency,
#                                          ROUND(t1.inter_purchase_time,1) AS IPT,
#                                          ROUND(t1.average_order_value,1) AS ATV,
#                                          t1.total_orders AS frequency,
#                                          ROUND(t1.total_spend,1) AS total_amount_spent,
#                                          DATE_DIFF("2023-11-30", DATE(t1.first_purchase_date)) AS time_since_first_purchase,
#                                          t3.gender, t3.nationality_group, t3.age, t3.age_group, t3.accept_sms_flg,
#                                          t3.accept_email_flg, t3.accept_mms_flg, t3.accept_call_flg, t3.accept_mail_flg,
#                                          t3.account_status, t3.EBILL, t3.language, t3.card_status_desc, t3.card_main_desc,
#                                          CASE WHEN t3.mobile IS NOT NULL THEN 1 ELSE 0 END AS mobile_flag,
#                                          CASE WHEN t3.email IS NOT NULL THEN 1 ELSE 0 END AS email_flag
#                                   FROM analytics.customer_segments AS t1
#                                   JOIN gold.pos_transactions AS t2
#                                   ON t1.customer_id = t2.customer_id
#                                   JOIN gold.customer_profile AS t3
#                                   ON t2.customer_id = t3.account_key
#                                   WHERE t2.business_day BETWEEN '2022-12-01' AND '2023-11-30'
#                                   AND t1.key IN ('tms', 'rfm')
#                                   AND t1.channel = 'pos'
#                                   AND t1.country = 'uae'
#                                   AND t1.month_year = '202311'
#                                   AND t3.LHRDATE IS NOT NULL),

# loyalty_points AS (SELECT customer_id,
#                           SUM(CASE WHEN loyalty_points IS NOT NULL THEN loyalty_points ELSE 0 END) AS total_points_issued,
#                           SUM(CASE WHEN redeemed_points IS NOT NULL THEN redeemed_points ELSE 0 END) AS total_redeemed_points,
#                           SUM(CASE WHEN forced_redemption_points IS NOT NULL THEN forced_redemption_points ELSE 0 END) AS total_forced_redemption_points,
#                           (total_points_issued - total_redeemed_points - total_forced_redemption_points) AS total_points_available
#                   FROM (
#                         SELECT DISTINCT transaction_id, customer_id, loyalty_points, redeemed_points, forced_redemption_points
#                         FROM gold.pos_transactions
#                         WHERE customer_id IS NOT NULL
#                         AND business_day BETWEEN "2022-12-01" AND "2023-11-30"
#                         )
#                   GROUP BY customer_id
#                   HAVING total_points_issued >= 400)

# SELECT t1.*, t2.total_points_issued, t2.total_redeemed_points, t2.total_forced_redemption_points, t2.total_points_available
# FROM customer_segment_profile AS t1
# JOIN loyalty_points AS t2
# ON t1.customer_id = t2.customer_id
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Statements

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

query = "SELECT * FROM sandbox.pj_propensity_to_redeem_data"
df = spark.sql(query).toPandas()
# df.to_csv('dbfs:/FileStore/shared_uploads/prem@loyalytics.in/propensity_to_redeem_data.csv', index = False)

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Cleaning

# COMMAND ----------

# Fixing the issue in 'total_redeemed_points' column where forced redemption points are incorrectly present in 'total_redeemed_points'
condition = (df['total_redeemed_points'] >= 10000)
df.loc[condition, 'total_forced_redemption_points'] += df.loc[condition, 'total_redeemed_points'] // 10000 * 10000
df.loc[condition, 'total_redeemed_points'] -= df.loc[condition, 'total_redeemed_points'] // 10000 * 10000

# COMMAND ----------

df['redeem_flag'] = np.where(df['total_redeemed_points'] == 0, 0, 1) # Create target column

# COMMAND ----------

df.shape

# COMMAND ----------

# Imitation of the df.info() output. Since the number of records is too high, df.info() doesn't give the full output

# print("{:<30} {:<20} {:<20}".format("Column", "Non-Null Count", "DType"))
# print("-" * 70)

# for column in df.columns:
#     non_null_count = df[column].count()
#     data_type = df[column].dtype
#     print("{:<30} {:<20} {:<20}".format(column, non_null_count, str(data_type)))

# COMMAND ----------

df.info()

# COMMAND ----------

# MAGIC %md
# MAGIC We can drop 'age', 'EBILL' and 'language' columns because over 90% of their records are null values.

# COMMAND ----------

df.nunique()

# COMMAND ----------

# MAGIC %md
# MAGIC We can drop all the fields that have only one distinct value - 'accept_mms_flg', 'accept_call_flg', 'accept_mail_flg', and 'account_status'

# COMMAND ----------

print(df['mobile_flag'].value_counts()) # Count of distinct values

# COMMAND ----------

print(df[df['mobile_flag'] == 0]['redeem_flag'].value_counts()) # Count for 'redeem_flag' where 'mobile_flag' is 0

# COMMAND ----------

# MAGIC %md
# MAGIC 'mobile_flag' column would not be much helpful in our EDA since almost all of its records are only one distinct value. The records with a different value than the majority doesn't seem to have any correlation with the 'redeem_flag' column. So, we can drop this column.<br><br>
# MAGIC <b>Note:</b> 'mobile_flag' and 'email_flag' are two columns manually created for our EDA. Their value is 1 for customers whose mobile number or email ID respectively is present in the data, and otherwise the value is 0.

# COMMAND ----------

df['gender'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC We can drop the records where 'gender' column has the value 'UNKNOWN'.

# COMMAND ----------

df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC We can drop all the records where the columns 'ATV', 'total_amount_spent', and 'total_points_available' have negative values.

# COMMAND ----------

df2 = df.copy()

df2 = df2.drop(columns=['age', 'EBILL', 'language', 'customer_id', 'accept_mms_flg', 'accept_call_flg', 'accept_mail_flg', 'account_status', 'mobile_flag'], axis=1)

df2 = df2.dropna(subset=['IPT', 'card_status_desc', 'card_main_desc']) # Drop the null value records that are present in these columns
df2 = df2.loc[df2['ATV'] >= 0]
df2 = df2.loc[df2['total_amount_spent'] >= 0]
df2 = df2.loc[df2['total_points_available'] >= 0]

df2 = df2[~df2['nationality_group'].isin(['NA', 'OTHERS'])] # Drop irrelevant values of nationality_group
df2 = df2[~df2['gender'].isin(['UNKNOWN'])]
df2 = df2[~df2['segment'].isin(['Newbie'])] # Newbies would not be having any balance redemption points. Hence, their 'redeem_flag' value will always be 0

df2.drop_duplicates(inplace=True)
df2.reset_index(drop = True, inplace = True)

# COMMAND ----------

df2.describe()

# COMMAND ----------

df2.info()

# COMMAND ----------

df2.nunique()

# COMMAND ----------

# MAGIC %md
# MAGIC ##EDA

# COMMAND ----------

print(df2['redeem_flag'].value_counts()) # 7.5% of the customers have redeemed their loyalty points at least once

# COMMAND ----------

# MAGIC %md
# MAGIC ###Correlation Matrix of Numerical columns

# COMMAND ----------

correlation_matrix = df2.corr(method='spearman')
plt.figure(figsize=(12, 10))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", vmin=-1, vmax=1)
plt.title('Correlation Heatmap')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Strong positive correlations (greater than 0.70) between:<br>
# MAGIC 1. 'total_amount_spent' and 'frequency' - The more frequently a customer visits, the more total amount they spend.<br>
# MAGIC 2. 'total_amount_spent' and 'total_points_issued' - The more amount a customer spends, the more points they get.<br>
# MAGIC 3. 'frequency' and 'total_points_issued' - Indirect correlation from 'total_amount_spent' since it is correlated with both the columns.<br>
# MAGIC 4. 'total_amount_spent' and 'total_points_available' - Spending money will earn a customer loyalty points.<br>
# MAGIC 5. 'accept_sms_flg' and 'accept_email_flg' - Since almost all customers who have their email preferences on will also have their sms preferences on (and vice versa).<br>
# MAGIC 6. 'total_points_issued' and 'total_points_available' - The more points are issued to a customer, the more points accumulates in the customer's balance since most customers do not redeem their points.<br>
# MAGIC 7. 'total_redeemed_points' and 'redeem_flag' - Since any points redeemed would mean that the 'redeem_flag' will have a value of 1 and otherwise 0.<br><br>
# MAGIC
# MAGIC Strong negative correlations (less than -0.70) between:<br>
# MAGIC 1. 'IPT' and 'frequency' - Lower IPT (inter-purchase time) means that the customer visits frequently (and vice versa).

# COMMAND ----------

# MAGIC %md
# MAGIC ###Correlation Matrix of Categorical columns

# COMMAND ----------

!pip install association_metrics

# COMMAND ----------

import association_metrics as am

crv = df2.apply(lambda x: x.astype("category") if x.dtype == "O" else x)
cramers_v = am.CramersV(crv)
cfit = cramers_v.fit().round(2)

plt.figure(figsize=(12, 10))
sns.heatmap(cfit, annot=True, cmap='coolwarm', fmt=".2f", vmin=-1, vmax=1)
plt.title('Correlation Heatmap')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Count Plots for Categorical columns

# COMMAND ----------

# Display count plots for all object type fields and also 3 additional fields mentioned below

features = list(df2.select_dtypes(include=['object']).columns) + ['accept_sms_flg', 'accept_email_flg', 'email_flag']
for feature in features:
    df2['redeem_flag_percentage'] = df2.groupby(feature)['redeem_flag'].transform(lambda x: (x == 1).sum() / len(x))
    df2_sorted = df2.sort_values(by='redeem_flag_percentage', ascending=False)
    
    plt.figure(figsize=(18, 8))
    ax = sns.barplot(x=feature, y='redeem_flag_percentage', data=df2_sorted, color='#3374a0', order=df2_sorted[feature].unique())

    # Add data labels
    for p in ax.patches:
        ax.annotate(f'{p.get_height():.2%}', (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center', xytext=(0, 10), textcoords='offset points')

    plt.title(f'Percentage of Redeem_Flag=1 for {feature}')
    plt.ylabel('Percentage')
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Box plots to see outliers in numerical columns

# COMMAND ----------

numerical_columns = ['recency', 'IPT', 'ATV', 'frequency', 'time_since_first_purchase', 'total_amount_spent']
for col in numerical_columns:
    ax = df2[col].plot(kind='box', figsize=(8, 6))

    q1 = df2[col].quantile(0.25)
    q3 = df2[col].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    median = df2[col].median()

    ax.text(0.7, upper_bound, f'Upper bound: {upper_bound:.2f}', va='center', ha='left')
    ax.text(1.1, median, f'Median: {median:.1f}', va='center', ha='left')

    plt.title(f'Box Plot of {col}')
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove all records outliers

# COMMAND ----------

# def remove_outliers(df2):
#     numerical_columns = ['recency', 'IPT', 'ATV', 'frequency', 'time_since_first_purchase', 'total_amount_spent']
    
#     for col in numerical_columns:
#         q1 = df2[col].quantile(0.25)
#         q3 = df2[col].quantile(0.75)
#         iqr = q3 - q1
#         lower_bound = q1 - 1.5 * iqr
#         upper_bound = q3 + 1.5 * iqr
#         df2 = df2[(df2[col] >= lower_bound) & (df2[col] <= upper_bound)]
    
#     return df2

# df3 = remove_outliers(df2) # 37% of the data removed as outliers

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create bins/groups for numerical columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###CHECK BINS

# COMMAND ----------

# The lower and upper limits mentioned below are inclusive in the bins
# If a value (say 9.9) is above the upper limit of a bin (0-9) and below the lower limit of the next bin (10-19) then it is put in the current bin (0-9)

bin_edges = [0, 10, 20, 30, 40, 50, 60, 70, float('inf')]
bin_labels = ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70+']
df2['recency_bins'] = pd.cut(df2['recency'], bins=bin_edges, labels=bin_labels, include_lowest=True)

bin_edges = [0, 10, 20, 30, 40, 50, float('inf')]
bin_labels = ['0-9', '10-19', '20-29', '30-39', '40-49', '50+']
df2['IPT_bins'] = pd.cut(df2['IPT'], bins=bin_edges, labels=bin_labels, right=False)

bin_edges = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, float('inf')]
bin_labels = ['0-49', '50-99', '100-149', '150-199', '200-249', '250-299', '300-349', '350-399', '400-449', '450-499', '500-549', '550+']
df2['ATV_bins'] = pd.cut(df2['ATV'], bins=bin_edges, labels=bin_labels, include_lowest=True)

bin_edges = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, float('inf')]
bin_labels = ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79', '80-89', '90+']
df2['frequency_bins'] = pd.cut(df2['frequency'], bins=bin_edges, labels=bin_labels, include_lowest=True)

bin_edges = [0, 130, 160, 190, 220, 250, 280, 310, 340, 370]
bin_labels = ['0+', '130-159', '160-189', '190-219', '220-249', '250-279', '280-309', '310-339', '340-369']
df2['tsfp_bins'] = pd.cut(df2['time_since_first_purchase'], bins=bin_edges, labels=bin_labels, include_lowest=True)

bin_edges = [0, 2000, 4000, 6000, 8000, 10000, 12000, 14000, float('inf')]
bin_labels = ['0-1999', '2000-3999', '4000-5999', '6000-7999', '8000-9999', '10000-11999', '12000-13999', '14000+']
df2['total_amount_spent_bins'] = pd.cut(df2['total_amount_spent'], bins=bin_edges, labels=bin_labels, include_lowest=True)

# COMMAND ----------

# Display count plots for all the bins created

features = ['recency_bins', 'IPT_bins', 'ATV_bins', 'frequency_bins', 'tsfp_bins', 'total_amount_spent_bins']
for feature in features:
    df2['redeem_flag_percentage'] = df2.groupby(feature)['redeem_flag'].transform(lambda x: (x == 1).sum() / len(x))
    
    plt.figure(figsize=(18, 8))
    ax = sns.barplot(x=feature, y='redeem_flag_percentage', data=df2, color='#3374a0')

    # Add data labels
    for p in ax.patches:
        ax.annotate(f'{p.get_height():.2%}', (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center', xytext=(0, 10), textcoords='offset points')

    plt.title(f'Percentage of Redeem_Flag=1 for {feature}')
    plt.ylabel('Percentage')
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pairplot of numerical columns

# COMMAND ----------

# Pairplot of numerical columns

selected_features = ['recency', 'IPT', 'ATV', 'frequency', 'time_since_first_purchase', 'total_amount_spent', 'redeem_flag']
# df2['redeem_flag'] = df2['redeem_flag'].astype(object)
sns.pairplot(df2[selected_features], hue='redeem_flag', diag_kind='hist')
plt.show()

# COMMAND ----------

# # Imitation of pairplot using regular scatterplot

# numerical_columns = ['recency', 'IPT', 'ATV', 'frequency', 'time_since_first_purchase', 'total_amount_spent']
# fig, axes = plt.subplots(6, 6, figsize = (20, 20))
# axes = axes.flatten()
# k = 0
# j = 0
# for i, ax in enumerate(axes):
#     x = df2[numerical_columns].iloc[:, k]
#     y = df2[numerical_columns].iloc[:, j]
#     sns.scatterplot(x = x, y = y, ax = ax)
#     k = k + 1
#     if k == 6:
#         k = 0
#         j = j + 1
# plt.tight_layout()
# plt.show()

# COMMAND ----------

# Imitation of pairplot using regular scatterplot with the target column as the hue

numerical_columns = ['recency', 'IPT', 'ATV', 'frequency', 'time_since_first_purchase', 'total_amount_spent']
fig, axes = plt.subplots(6, 6, figsize = (20, 20))
axes = axes.flatten()
k = 0
j = 0

for i, ax in enumerate(axes):
    x = df2[numerical_columns].iloc[:, k]
    y = df2[numerical_columns].iloc[:, j]
    sns.scatterplot(x=x, y=y, hue=df2['redeem_flag'], ax=ax)
    k = k + 1
    if k == 6:
        k = 0
        j = j + 1

plt.tight_layout()
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


