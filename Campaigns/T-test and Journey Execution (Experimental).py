# Databricks notebook source
# MAGIC %md
# MAGIC #Important Notes

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Note:</b><br>
# MAGIC 1. Customers in the sandbox table should be an exhaustive list of the target audience.<br>
# MAGIC 2. Please make sure that the Trigger ID is null at all times except when customer keys need to be pushed to swan.<br>
# MAGIC
# MAGIC <b>Sandbox Requirements:</b>
# MAGIC 1. customer_key<br>
# MAGIC 2. card_key<br>
# MAGIC 3. variable of interest - rpc/atv/frequency/recency/etc

# COMMAND ----------

# MAGIC %md
# MAGIC #Function Initializations

# COMMAND ----------

import pyspark.pandas as ps
from scipy.stats import ttest_ind
from scipy.stats import levene
import numpy as np
import pandas as pd
ps.set_option('compute.ops_on_diff_frames', True)
pd.set_option('display.max_columns', None)

# COMMAND ----------

dbutils.widgets.text(name='Campaign ID', defaultValue='null')
dbutils.widgets.text(name='Campaign Name', defaultValue='null')
dbutils.widgets.text(name='Sandbox Table Name', defaultValue='null')
dbutils.widgets.text(name='Test Size (% format)', defaultValue='null')
dbutils.widgets.text(name='Variable of Interest', defaultValue='null')
dbutils.widgets.text(name='Trigger ID', defaultValue='null')

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Data

# COMMAND ----------

sandbox_name = dbutils.widgets.get('Sandbox Table Name')
df = spark.sql(f"SELECT * FROM {sandbox_name}").toPandas() # Parameter-3
df.shape

# COMMAND ----------

# Drop any null values of card key and customer key
df = df.dropna(subset = ['card_key', 'customer_key'])

# Convert customer key and card key to string values
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
# MAGIC #Loyalty Customers Check

# COMMAND ----------

query = f"""
SELECT
    t1.customer_key,
    t1.card_key
FROM {sandbox_name} AS t1
JOIN gold.customer.maxxing_customer_profile AS t2
    ON t1.customer_key = t2.customer_key
    AND t1.card_key = t2.card_key
WHERE
    t2.LHRDATE IS NULL
    AND (t2.LHP IS NULL OR t2.LHP = 0)
"""
non_lhp_customers_flag = spark.sql(query).toPandas().empty

if not non_lhp_customers_flag:
    raise ValueError('Data consists of non-loyalty customers!')
else:
    print("All customers are loyalty customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #Test-Control Split and T-test

# COMMAND ----------

split_perc = int(dbutils.widgets.get('Test Size (% format)'))

test_size = round(len(df)*split_perc/100)

if int(str(len(df)*split_perc/100).split('.')[1]) == 5:
    test_size += 1

print(f"Test size ({split_perc}%): {test_size}")
print(f"Control size ({100 - split_perc}%): {len(df) - test_size}")

# COMMAND ----------

variable_of_interest = str(dbutils.widgets.get('Variable of Interest')) # Parameter-4

# COMMAND ----------

# Set the significance level and the number of iterations to try out
significance_level = 0.05
iterations = 1000

p_values_dct = {}
for i in range(iterations):
    np.random.seed(i)

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

    # Store the t-statistic and p-value in a dictionary
    p_values_dct[i] = [t_statistic, p_value]

# Extract the seed which gave the best p-value
max_value = max(p_values_dct, key=lambda k: p_values_dct[k][1])
t_statistic = p_values_dct[max_value][0]
p_value = p_values_dct[max_value][1]

# Check if the significance level was achieved or not
if p_value > significance_level:
    achieved = True
else:
    achieved = False

# Display the seed having the best sample split
print(f"Best sample split found at iteration {max_value}: t-statistic = {round(t_statistic, 4)}, p-value = {round(p_value, 4)}")

# If significance level was achieved, then recreate the test and control sets
if achieved:
    print("The significance level was achieved with the p-value")
    np.random.seed(max_value)
    test_data = df.sample(n = test_size)
    test_id_list = test_data.customer_key.tolist()
    control_data = df[~df['customer_key'].isin(test_id_list)]

# If significance level was not achieved, then raise an error to run more iterations
else:
    raise ValueError('More iterations are required to achieve the significance level!')

# COMMAND ----------

# MAGIC %md
# MAGIC #Test-Control Similarity Check

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

control_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Save to Sandbox

# COMMAND ----------

campaign_id = int(dbutils.widgets.get('Campaign ID')) # Parameter-1
campaign_name = str(dbutils.widgets.get('Campaign Name')) # Parameter-2

query = f"""
SELECT *
FROM dev.sandbox.campaign_customer_details
WHERE campaign_id = {campaign_id}
AND campaign_name = '{campaign_name}'
"""

data_empty = spark.sql(query).toPandas().empty

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
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.campaign_customer_details")

    spark_df = spark.createDataFrame(test_cardkeys_df)
    spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.campaign_test_cardkeys")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT campaign_id, campaign_name, COUNT(*)
# MAGIC FROM dev.sandbox.campaign_customer_details
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT campaign_id, campaign_name, COUNT(*)
# MAGIC FROM dev.sandbox.campaign_test_cardkeys
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #Push to Swan

# COMMAND ----------

if data_empty == False:
    customer_key_lst = test_data['customer_key'].astype(str).tolist()

    all_strings = all(isinstance(item, str) for item in customer_key_lst)

    # Check if all elements are integers
    all_integers = all(isinstance(item, int) for item in customer_key_lst)

    print(f"All elements are strings: {all_strings}")
    print(f"All elements are integers: {all_integers}")
    print(f"\nNumber of Customers: {len(customer_key_lst)}")

# COMMAND ----------

if data_empty == False:
    trigger_id = int(dbutils.widgets.get('Trigger ID')) # Parameter-7
    print(f"Trigger ID: {trigger_id}")

# COMMAND ----------

ingage_api_key = dbutils.secrets.get("lulucdp-secret-scope", "ingageapikey")

# COMMAND ----------

if data_empty == False:
    import requests
    import json

    # API URL
    url = f"https://connect.swan.cx/webhook/comms/bulk-trigger/api/{trigger_id}"

    # Headers
    headers = {
        "ingageapikey":ingage_api_key,
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
