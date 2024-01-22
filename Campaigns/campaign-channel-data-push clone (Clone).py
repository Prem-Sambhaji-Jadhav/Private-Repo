# Databricks notebook source
# MAGIC %md
# MAGIC # package imports -SH

# COMMAND ----------

from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import ast

print('---------------------------')
print('package imports done!!')
print('---------------------------')

# COMMAND ----------

# MAGIC %md
# MAGIC # Swan Campaign Data
# MAGIC analytics.campaign_delivery_stats (future table name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## swanlake connection

# COMMAND ----------

# mounting source location
ACCOUNT = "swanlake"
CONTAINER = "cold-storage"
PROPERTY = f"fs.azure.account.key.{ACCOUNT}.blob.core.windows.net"
SOURCE = f"wasbs://{CONTAINER}@{ACCOUNT}.blob.core.windows.net"
MOUNT_POINT = f"/mnt/" + CONTAINER
EXTRA_CONFIGS = {f"fs.azure.account.key.{ACCOUNT}.blob.core.windows.net": 'F3KAGhl9XmR6suvdIjBqms2bQgruhjXct73KPDcdE3dsYJoUBVEnmMXMGOse3eMU3Wku8GikZ9m+JGtRYUDFpA=='}
try:
    dbutils.fs.mount(source=SOURCE, mount_point=MOUNT_POINT, extra_configs=EXTRA_CONFIGS)
except Exception as ex:
    print("Database already mounted!")
    print("Ignoring the mounting part...")

# COMMAND ----------

# MAGIC %sql
# MAGIC select campaign_id, campaign_desc, month_year, count(CDID)
# MAGIC from analytics.email_campaign_delivery
# MAGIC group by 1,2,3
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## campaign attributes

# COMMAND ----------

# all campaign attributes go here

CAMP_START_DATE = date(2023,12,22)
CAMP_END_DATE = date(2023,12,29)
month_yr = '202312'
CAMP_JOURNEY_ID = '319f4b6b-8490-4521-9181-b3175c6174a6'
CAMPAIGN_ID = 86
CAMPAIGN_DESC = 'Splurger Non Convert trip Increase followup 10pct'

# COMMAND ----------

# MAGIC %md
# MAGIC ## driver code

# COMMAND ----------

# driver function to get the swan data
def get_campaign_email_data(start_date, end_date, journeyId):
    """takes start_date: campaign start date,
       end_date: campaign end date,
       journey_id: journey identifier (available from swan journey URL)
       and returns all the 'CDID' (CDP customer_key) level 'delivery event entries'
       for a particular journey"""

    base_path = f"dbfs:/mnt/{CONTAINER}/swan/lulu/comms"

    # today = datetime.today().date()
    print(f"campaign start_date: {start_date}, end_date: {end_date}\n")

    # Generate a list of dates from start_date until end_date (inclusive)
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    dfs = []

    COLS = ['id', 'customerEmail', 'status', 'type', 'subType', 'journeyId', 'date', 'CDID']

    for dt in date_list:

        date_folder_path = dt.strftime("%d-%m-%Y")
        date_folder = dt.strftime("%Y-%m-%d")

        date_folder_path = f"{base_path}/{date_folder_path}"

        print(f"\t\t Reading path:{date_folder_path}")

        # read all parquet files in the date folder
        df = spark.read.parquet(f"{date_folder_path}/*.parquet")

        # filter and select specific columns
        df = df.filter(df.journeyId == journeyId).select(COLS)\
               .withColumn('folder_date',lit(date_folder)).distinct()
    

        dfs.append(df)

    # combine all DataFrames into a single DataFrame
    final_df = dfs[0] if dfs else None
    for df in dfs[1:]:
        final_df = final_df.union(df)

    return final_df

# COMMAND ----------

# reading swan data and assigning to a dataframe
print(f"fetching data for campaign: {CAMPAIGN_DESC}\n")

df_campaign_data_raw = get_campaign_email_data(start_date=CAMP_START_DATE, end_date=CAMP_END_DATE,\
                                               journeyId=CAMP_JOURNEY_ID)
pdf_campaign_data_raw = df_campaign_data_raw.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## data transformation

# COMMAND ----------

# status identification and transformations

def determine_final_status(status):
    
    if 'bounce' in status:
        return 'bounced'
    elif 'unsubscribe' in status or 'group_unsubscribe' in status:
        return 'unsubscribed'
    elif 'click' in status:
        return 'clicked'
    elif status == {'sent', 'delivered'}:
        return 'not_opened'
    elif status == {'sent', 'open', 'delivered'} or status == {'sent', 'open'}:
        return 'opened'
    else:
        return 'not_delivered' #for the {'sent'} value



# change status list to set data type
pdf_campaign_data = pdf_campaign_data_raw.copy()
pdf_campaign_data['status_set'] = pdf_campaign_data['status'].apply(lambda x: set(ast.literal_eval(x)))

# grouping by CDID and getting the last updated date and status_set
final_status_agg = pdf_campaign_data.groupby(['CDID', 'date'])\
                                    .agg(last_updated=('folder_date', 'max'),\
                                         campaign_sent_date = ('folder_date', 'min'),\
                                         campaign_channel = ('subType', 'min'),\
                                         email = ('customerEmail', 'min'),\
                                         status_set=('status_set', lambda x: set.union(*x)))
                                    
final_status_agg['final_status'] = final_status_agg['status_set']\
                                   .apply(lambda x: determine_final_status(x))

final_status_agg['email'] = final_status_agg['email'].str.lower()                                   
final_status_agg['status_set_str'] = final_status_agg.status_set.astype(str)

final_status_agg['swan_journey_id'] = CAMP_JOURNEY_ID
final_status_agg['campaign_id'] = CAMPAIGN_ID
final_status_agg['campaign_desc'] = CAMPAIGN_DESC


# final_status_agg.sort_values(by='last_updated', ascending=False).reset_index().head()

final_campaign_status = final_status_agg.reset_index().copy()
final_campaign_status.head(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## validation: swan
# MAGIC validate total sent,delivered,opened,not opened data from swan <br>

# COMMAND ----------

# count validations (to be checked from swan dashboard)
TOTAL_SENT = final_campaign_status.shape[0]
TOTAL_DELIVERED = final_campaign_status[~final_campaign_status.final_status\
                                                              .isin(["not_delivered","bounced"])].shape[0]
print(f"Total Sent:\t\t {TOTAL_SENT}")
print(f"Total Delivered:\t {TOTAL_DELIVERED}\n")

full_stats = final_campaign_status.final_status.value_counts().to_frame().reset_index()\
                                  .rename(columns={'index':'final_status','final_status':'num_customers'})\
                                  .sort_values(by='num_customers', ascending=False)

print(f"Full Stats:\n {full_stats}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## test block

# COMMAND ----------

# MAGIC %md
# MAGIC ## final dataframe
# MAGIC to be appended to gold

# COMMAND ----------

# building final dataframe

CHANNEL_POS = 'pos'
country = 'uae'
SCHEMA = 'gold'

df = spark.createDataFrame(final_campaign_status)


df = df.withColumn("channel", lit(CHANNEL_POS)) \
        .withColumn("month_year", lit(month_yr)) \
        .withColumn("country", lit(country)) \
        .withColumn("insert_utc_timestamp", date_format(current_timestamp(),"yyyyMMddhhmmss"))\
        .withColumn("key", lit("campaign"))


display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## validation: analytics table

# COMMAND ----------

# gold target and current source dataframe column validation
df = df.select("CDID", "last_updated", "campaign_sent_date",
                                "campaign_channel", "email", "status_set","final_status",
                                "status_set_str", "swan_journey_id", "campaign_id",
                                "campaign_desc", "channel", "month_year", "country",
                                "insert_utc_timestamp", "key")

column_list_target = spark.sql("""show columns from analytics.email_campaign_delivery""")\
                                 .rdd.flatMap(lambda x: x).collect()

column_list_source = df.columns

set_target, set_source = set(column_list_target), set(column_list_source)

column_check_1 = set_target-set_source
column_check_2 = set_source-set_target


if len(column_check_1) + len(column_check_2) != 0:

    print(f"missing from source: {column_check_1}")
    print(f"missing from target: {column_check_2}\n")
    raise TypeError("Column set mismatch between source and target tables, exited process!!")
else:
    print('Target/Source column check PASSED!!\n')



print(f"Total Entries to be appended: {TOTAL_SENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC --total customers before data append
# MAGIC select 
# MAGIC count(*) as totalcustomers
# MAGIC from analytics.email_campaign_delivery 

# COMMAND ----------

# MAGIC %md
# MAGIC ## write to gold

# COMMAND ----------

# # writing to gold delta table
# GOLD_PATH = 'dbfs:/mnt/cdp-customers/analytics-layer'
# tableName = 'email_campaign_delivery'


# # appending new campaign data
# df.write.format("delta")\
#         .mode("append")\
#         .partitionBy("country","channel","month_year") \
#         .option("mergeSchema", "false") \
#         .save(f"{GOLD_PATH}/{tableName}")

# COMMAND ----------

tableName = "email_campaign_delivery"
viewName = "campaign_append"
df.createOrReplaceTempView(viewName)
query = f""" MERGE INTO analytics.{tableName}
             USING {viewName}
             ON analytics.{tableName}.`CDID` = {viewName}.`CDID`
             AND analytics.{tableName}.`campaign_id` = {viewName}.`campaign_id`
             WHEN NOT MATCHED 
             THEN INSERT *"""
spark.sql(query)
print("complete..")

# COMMAND ----------

# MAGIC %sql
# MAGIC --total customers after data append
# MAGIC select 
# MAGIC count(*) as totalcustomers
# MAGIC from analytics.email_campaign_delivery

# COMMAND ----------

# MAGIC %sql
# MAGIC -- analytics table validation
# MAGIC
# MAGIC select campaign_id, campaign_channel, count(*) as num_customers
# MAGIC from analytics.email_campaign_delivery
# MAGIC group by campaign_id, campaign_channel
# MAGIC order by campaign_id desc
