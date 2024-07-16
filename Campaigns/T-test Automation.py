# Databricks notebook source
# MAGIC %md
# MAGIC <b>Note:</b> Number of customers in the sandbox table should be greater than the target audience.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Sandbox Requirements:</b>
# MAGIC 1. customer_key<br>
# MAGIC 2. card_key<br>
# MAGIC 3. variable of interest - atv/frequency/recency

# COMMAND ----------

# MAGIC %md
# MAGIC #Import Libraries

# COMMAND ----------

import pyspark.pandas as ps
from scipy.stats import ttest_ind
from scipy.stats import levene
import numpy as np
import pandas as pd
ps.set_option('compute.ops_on_diff_frames', True)
pd.set_option('display.max_columns', None)

# COMMAND ----------

# MAGIC %md
# MAGIC #Intializations for User Inputs

# COMMAND ----------

dbutils.widgets.text(name='Sandbox Table Name', defaultValue='null')
dbutils.widgets.text(name='Test Size', defaultValue='null')
dbutils.widgets.text(name='Control Size', defaultValue='null')
dbutils.widgets.text(name='Variable of Interest', defaultValue='null')
dbutils.widgets.text(name='Folder Path', defaultValue='null')
# dbutils.widgets.text(name='significance_level', defaultValue='null')
# dbutils.widgets.multiselect("filter")

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Dataframe From Sandbox Table

# COMMAND ----------

sandbox_name = dbutils.widgets.get('Sandbox Table Name')
sdf = spark.sql(f"select * from {sandbox_name}") ######## Parameter-1
df = sdf.toPandas()
df.shape

# COMMAND ----------

# df = df.dropna(subset=['card_key', 'customer_key'])
df = df.drop_duplicates(subset=['customer_key'], keep = 'first', ignore_index=True)
df['customer_key'] = df['customer_key'].astype(str)
# df['customer_key'] = df['customer_key'].apply(lambda x: f'{x:016d}')
# df['card_key'] = df['card_key'].astype('int64')
# df['card_key'] = df['card_key'].astype('object')
df.nunique()

# COMMAND ----------

suggested_test_size = round(len(df)*0.95)
suggested_control_size = round(len(df)*0.05)

if int(str(len(df)*0.95).split('.')[1]) == 5:
    suggested_test_size += 1

print(f"Suggested test size (95%): {suggested_test_size}")
print(f"Suggested control size (5%): {suggested_control_size}")

# COMMAND ----------

df.info()

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #Test-Control split and T-test

# COMMAND ----------

test_size = int(dbutils.widgets.get('Test Size'))  ######## Parameter-2
np.random.seed(7) # Change the seed of the test set here, if necessary
test_data = df.sample(n=test_size)
test_id_list=test_data.customer_key.tolist()
control_available_for_sel=df[~df['customer_key'].isin(test_id_list)]

control_size=int(dbutils.widgets.get('Control Size')) ############# Parameter-3

max_iterations = 1000 ############### Optional Parameter-4
current_iteration = 0 ################## Optional Parameter-5
significance_level = 0.001 ################## Optional Parameter-6
variable_of_interest = str(dbutils.widgets.get('Variable of Interest')) ############## Parameter-7
seed_value = 1 ################ Optional Parameter-8

while current_iteration < max_iterations:
    np.random.seed(seed_value)

    # Randomly sample data for each iteration
    control_data = control_available_for_sel.sample(n=control_size)

    # Perform levene test
    statistic_lv, p_value_lv = levene(test_data[variable_of_interest], control_data[variable_of_interest])
    
    if p_value_lv < significance_level:
        print(f"Levene test: Null hypothesis rejected, p value is {round(p_value_lv,5)}. Population variation is not equal.")
        var_pop = False
    else:
        print(f"Levene test: Null hypothesis accepted, p value is {round(p_value_lv,5)}. Population variation is equal.")
        var_pop = True
    
    # Perform t-test
    t_statistic, p_value = ttest_ind(test_data[variable_of_interest], control_data[variable_of_interest], equal_var = var_pop)

    # Check if the p-value is greater than the significance level
    if p_value > significance_level:
        print(f"Iteration {current_iteration + 1}: T-statistic = {round(t_statistic,5)}, P-value = {round(p_value,5)}")
        break  # Exit the loop if significance is achieved

    current_iteration += 1
    seed_value += 1

# If the loop completes without breaking, print a message
if current_iteration == max_iterations:
    print(f"The significance level was not achieved after {max_iterations} iterations. Please change the seed of the test set to resample.")
else:
    print(f"The significance level was achieved at {seed_value} seed level")

# COMMAND ----------

# MAGIC %md
# MAGIC #Customer Keys should be mutually exclusive

# COMMAND ----------

control_id_list = control_data.customer_key.tolist()
common = set(test_id_list).intersection(set(control_id_list))
if common:
    print("There are intersecting customer keys numbers!!")
else:
    print("Test and control set are mutually exclusive")

# COMMAND ----------

# MAGIC %md
# MAGIC #Manual Check for Variable of Interest

# COMMAND ----------

print("Test data shape:", test_data.shape)
print("Control data shape:", control_data.shape)
print(f"Test data mean {variable_of_interest}", test_data[variable_of_interest].mean())
print(f"Control data mean {variable_of_interest}", control_data[variable_of_interest].mean())

# COMMAND ----------

# MAGIC %md
# MAGIC #Final Test and Control Tables

# COMMAND ----------

test_data.display()

# COMMAND ----------

# segment_counts = test_data.groupby('segment').size()
# print(segment_counts)

# COMMAND ----------

control_data.display()

# COMMAND ----------

# segment_counts = control_data.groupby('segment').size()
# print(segment_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write CSV to DBFS

# COMMAND ----------

directory = dbutils.widgets.get('Folder Path')  ######## Parameter-9

# COMMAND ----------

test_data[['customer_key']].to_csv(directory + 'freq_rpc_increase_test_customerkey.csv' , sep = '|', index = False)
# test_data[['customer_key', 'card_key']].to_csv(directory + 'lapser_win_back_test_cardkey.csv' , sep = '|', index = False)
control_data[['customer_key']].to_csv(directory + 'freq_rpc_increase_control_customerkey.csv' , sep = '|', index = False)
