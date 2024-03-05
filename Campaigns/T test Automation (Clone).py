# Databricks notebook source
# MAGIC %md
# MAGIC <b>Note:</b> Number of customers in the sandbox table should be greater than the target audience.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Sandbox Requirements:</b>
# MAGIC 1. mobile<br>
# MAGIC 2. card_key<br>
# MAGIC 3. (variable of interest - atv/frequency/recency)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Intializations for Spark

# COMMAND ----------

import pyspark.pandas as ps

from scipy.stats import ttest_ind
from scipy.stats import levene
import numpy as np
import pandas as pd

# COMMAND ----------

ps.set_option('compute.ops_on_diff_frames', True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Intializations for Python

# COMMAND ----------

pd.set_option('display.max_columns', None)

# COMMAND ----------

dbutils.widgets.text(name='sandbox_table_name', defaultValue='null')
dbutils.widgets.text(name='test_size', defaultValue='null')
dbutils.widgets.text(name='control_size', defaultValue='null')
dbutils.widgets.text(name='variable_of_interest', defaultValue='null')
dbutils.widgets.text(name='folder_path', defaultValue='null')
# dbutils.widgets.text(name='significance_level', defaultValue='null')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dataframe From Sandbox Table

# COMMAND ----------

### assuming we are running a campaign on all vips
sandbox_name = dbutils.widgets.get('sandbox_table_name')
sdf = spark.sql(f"select * from {sandbox_name}") ######## Parameter-1

# COMMAND ----------

df = sdf.toPandas()
df.shape

# COMMAND ----------

df.head()

# COMMAND ----------

df = df.dropna(subset=['card_key'])
df['card_key'] = df['card_key'].astype('int64')
df['card_key'] = df['card_key'].astype('object')

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test-Control split and T-test

# COMMAND ----------

test_size = int(dbutils.widgets.get('test_size'))  ######## Parameter-2
np.random.seed(2) # Change the seed of the test set here, if necessary
test_data = df.sample(n=test_size)
test_id_list=test_data.mobile.tolist()
control_available_for_sel=df[~df['mobile'].isin(test_id_list)]

control_size=int(dbutils.widgets.get('control_size')) ############# Parameter-3

# Initialize loop parameters
max_iterations = 1000 ############### Optional Parameter-4
current_iteration = 0 ################## Optional Parameter-5
significance_level = 0.001 ################## Optional Parameter-6
variable_of_interest = str(dbutils.widgets.get('variable_of_interest')) ############## Parameter-7
seed_value = 1 ################ Optional Parameter-8

while current_iteration < max_iterations:
    np.random.seed(seed_value)

    # Randomly sample data for each iteration
    control_data = control_available_for_sel.sample(n=control_size)

    # Perform levene test
    statistic_lv, p_value_lv = levene(test_data[variable_of_interest], control_data[variable_of_interest])
    
    # print(p_value_lv)
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
# MAGIC ##Mobiles should be mutually exclusive

# COMMAND ----------

control_id_list = control_data.mobile.tolist()
common = set(test_id_list).intersection(set(control_id_list))
if common:
    print("There are intersecting mobile numbers!!")
else:
    print("Test and control set are mutually exclusive")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Manual Check for Variable of Interest

# COMMAND ----------

print("Test data shape:", test_data.shape)
print("Control data shape:", control_data.shape)
print("Test data mean ATV", test_data[variable_of_interest].mean())
print("Control data mean ATV", control_data[variable_of_interest].mean())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final Test and Control Tables

# COMMAND ----------

test_data.display()

# COMMAND ----------

control_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write CSV to DBFS

# COMMAND ----------

# directory = dbutils.widgets.get('folder_path')  ######## Parameter-9

# test_data[['mobile']].to_csv(directory + 'test_mobile.csv' , sep = '|', index = False)

# test_data[['mobile', 'card_key']].to_csv(directory + 'test_cardkey.csv' , sep = '|', index = False)

# control_data[['mobile']].to_csv(directory + 'control_mobile.csv' , sep = '|', index = False)

# COMMAND ----------


