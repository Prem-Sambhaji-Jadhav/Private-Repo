# Databricks notebook source
# MAGIC %md
# MAGIC <b>Note:</b><br>
# MAGIC 1. Customers in the sandbox table should be an exhaustive list of the target audience.<br>
# MAGIC 2. Please make sure that the Trigger ID is null at all times except when customer keys need to be pushed to swan.<br>
# MAGIC
# MAGIC <b>Sandbox Requirements:</b>
# MAGIC 1. customer_key<br>
# MAGIC 2. card_key<br>
# MAGIC 3. variable of interest - rpc/atv/frequency/recency

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

dbutils.widgets.text(name='Campaign ID', defaultValue='null')
dbutils.widgets.text(name='Campaign Name', defaultValue='null')
dbutils.widgets.text(name='Sandbox Table Name', defaultValue='null')
dbutils.widgets.text(name='Test-Control Split (% format)', defaultValue='null')
dbutils.widgets.text(name='Variable of Interest', defaultValue='null')
dbutils.widgets.text(name='Trigger ID', defaultValue='null')

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Dataframe From Sandbox Table

# COMMAND ----------

sandbox_name = dbutils.widgets.get('Sandbox Table Name')
sdf = spark.sql(f"select * from {sandbox_name}") # Parameter-3
df = sdf.toPandas()
df.shape

# COMMAND ----------

df = df.dropna(subset=['card_key', 'customer_key'])
df = df.drop_duplicates(subset=['customer_key'], keep = 'first', ignore_index=True)
df['customer_key'] = df['customer_key'].astype('object')
df['card_key'] = df['card_key'].astype('int64')
df['card_key'] = df['card_key'].astype('object')
df.nunique()

# COMMAND ----------

df.info()

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #Test-Control split and T-test

# COMMAND ----------

split_perc = int(dbutils.widgets.get('Test-Control Split (% format)'))

test_size = round(len(df)*split_perc/100)

if int(str(len(df)*split_perc/100).split('.')[1]) == 5:
    test_size += 1

print(f"Test size ({split_perc}%): {test_size}")
print(f"Control size ({100 - split_perc}%): {len(df) - test_size}")

# COMMAND ----------

variable_of_interest = str(dbutils.widgets.get('Variable of Interest')) # Parameter-4

# COMMAND ----------

current_iteration = 0
significance_level = 0.001
seed_value = 1

while True:
    np.random.seed(seed_value)

    # Randomly sample data for each iteration
    test_data = df.sample(n = test_size)
    test_id_list = test_data.customer_key.tolist()
    control_data = df[~df['customer_key'].isin(test_id_list)]

    # Perform levene test
    statistic_lv, p_value_lv = levene(test_data[variable_of_interest], control_data[variable_of_interest])
    
    if p_value_lv < significance_level:
        var_pop = False
        # print(f"Levene test: Null hypothesis rejected, p value is {round(p_value_lv,5)}. Population variation is not equal.")
    else:
        var_pop = True
        # print(f"Levene test: Null hypothesis accepted, p value is {round(p_value_lv,5)}. Population variation is equal.")
    
    # Perform t-test
    t_statistic, p_value = ttest_ind(test_data[variable_of_interest], control_data[variable_of_interest], equal_var = var_pop)

    # Check if the p-value is greater than the significance level
    if p_value > significance_level:
        print(f"Iteration {current_iteration + 1}: T-statistic = {round(t_statistic,5)}, P-value = {round(p_value,5)}")
        achieved = True
        break  # Exit the loop if significance is achieved

    current_iteration += 1
    seed_value += 1

# If the loop completes without breaking, print a message
if achieved:
    print(f"The significance level was achieved at {seed_value} seed level")
else:
    print(f"The significance level was not achieved even after {current_iteration} iterations.")

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

# directory = dbutils.widgets.get('Folder Path')

# COMMAND ----------

# test_data[['customer_key']].to_csv(directory + 'freq_rpc_increase_test_customerkey.csv' , sep = '|', index = False)
# test_data[['customer_key', 'card_key']].to_csv(directory + 'lapser_win_back_test_cardkey.csv' , sep = '|', index = False)
# control_data[['customer_key']].to_csv(directory + 'freq_rpc_increase_control_customerkey.csv' , sep = '|', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Save to Sandbox

# COMMAND ----------

campaign_id = int(dbutils.widgets.get('Campaign ID')) # Parameter-1
campaign_name = str(dbutils.widgets.get('Campaign Name')) # Parameter-2

query = f"""
SELECT *
FROM dev.sandbox.qatar_campaign_customer_details
WHERE campaign_id = {campaign_id}
AND campaign_name = '{campaign_name}'
"""

data_empty = spark.sql(query).toPandas().empty

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct campaign_id,campaign_name from dev.sandbox.campaign_customer_details  

# COMMAND ----------

if data_empty:
    test_data = test_data.drop(columns = variable_of_interest)
    control_data = control_data.drop(columns = variable_of_interest)
    test_data['campaign_set'] = "test"
    control_data['campaign_set'] = "control"

    sandbox_df = pd.concat([test_data, control_data], ignore_index=True)
    sandbox_df['campaign_id'] = campaign_id
    sandbox_df['campaign_name'] = campaign_name

    test_cardkeys_df = sandbox_df[sandbox_df['campaign_set'] == 'test'][['campaign_id','campaign_name','customer_key', 'card_key']]
    test_cardkeys_df['customer_key|card_key'] = test_cardkeys_df['customer_key'].astype(str) + '|' + test_cardkeys_df['card_key'].astype(str)
    test_cardkeys_df = test_cardkeys_df[['campaign_id','campaign_name','customer_key|card_key']]

    sandbox_df = sandbox_df[['campaign_id','campaign_name','campaign_set','customer_key']]

# COMMAND ----------

if data_empty:
    spark_df = spark.createDataFrame(sandbox_df)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.qatar_campaign_customer_details")

    spark_df = spark.createDataFrame(test_cardkeys_df)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.qatar_campaign_test_cardkeys")

# COMMAND ----------

print(data_empty)

# COMMAND ----------

# MAGIC %md
# MAGIC #Push to Swan

# COMMAND ----------

if data_empty == False:
    customer_key_lst = test_data['customer_key'].astype(str).tolist()

# COMMAND ----------

if data_empty == False:
    all_strings = all(isinstance(item, str) for item in customer_key_lst)

    # Check if all elements are integers
    all_integers = all(isinstance(item, int) for item in customer_key_lst)

    print(f"All elements are strings: {all_strings}")
    print(f"All elements are integers: {all_integers}")

# COMMAND ----------

if data_empty == False:
    print(len(customer_key_lst))

# COMMAND ----------

import pandas as pd
query = """select a.customer_key
                    from dev.sandbox.qatar_campaign_customer_details a
                    join gold.customer.vynamic_customer_profile b on a.customer_key = b.customer_key
                    where b.nationality IN ('QATAR','TUNISIA','JORDAN','EGYPT')
                    and a.campaign_set = 'test'
                    group by 1"""

df = spark.sql(query).toPandas()
customer_key_lst = df['customer_key'].astype(str).tolist()
all_strings = all(isinstance(item, str) for item in customer_key_lst)

# Check if all elements are integers
all_integers = all(isinstance(item, int) for item in customer_key_lst)

print(f"All elements are strings: {all_strings}")
print(f"All elements are integers: {all_integers}")
print(len(customer_key_lst))
##data_empty = False

# COMMAND ----------

if data_empty == False:
    trigger_id = int(dbutils.widgets.get('Trigger ID')) # Parameter-7
    print(trigger_id)

# COMMAND ----------

if data_empty == False:
    import requests
    import json

    # API URL
    url = f"https://connect.swan.cx/webhook/comms/bulk-trigger/api/{trigger_id}"

    # Headers
    headers = {
        "ingageapikey": "571f161edc011fe405563857684568",
        "Content-Type": "application/json"
    }

    while True:
        if len(customer_key_lst) > 99900:
            customer_key_lst_split = customer_key_lst[:99900]
            customer_key_lst = customer_key_lst[99900:]
        else:
            customer_key_lst_split = customer_key_lst.copy()
            customer_key_lst = []

        # Request Body
        body = {
            "attrKey": "CDID",
            "attrValue": customer_key_lst_split
        }

        # Convert body to JSON
        body_json = json.dumps(body)

        # Make the POST request
        response = requests.post(url, headers=headers, data=body_json)

        # Check the response
        if response.status_code == 200:
            print("Success:", response.json())
        else:
            print("Error:", response.json())
        
        if len(customer_key_lst) > 0:
            continue
        else:
            break
