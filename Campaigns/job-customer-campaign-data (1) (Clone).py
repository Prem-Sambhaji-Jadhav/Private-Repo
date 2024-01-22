# Databricks notebook source
# MAGIC %md
# MAGIC #Setup

# COMMAND ----------

print("-------------------------------------------------------")
print("\t Initial setup - in progress..")

# COMMAND ----------

# MAGIC %md
# MAGIC ##importing packages

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.pandas as ps
ps.set_option('compute.ops_on_diff_frames', True)
# import pandas as pd

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# MAGIC %md ## azure connection

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# COMMAND ----------

# mounting source location
ACCOUNT = "luludatalake"
CONTAINER = "cdp-customers"

PROPERTY = f"fs.azure.account.key.{ACCOUNT}.blob.core.windows.net"

SOURCE = f"wasbs://{CONTAINER}@{ACCOUNT}.blob.core.windows.net"
MOUNT_POINT = f"/mnt/" + CONTAINER
EXTRA_CONFIGS = {f"fs.azure.account.key.{ACCOUNT}.blob.core.windows.net": dbutils.secrets.get('lulucdp-secret-scope', 'lulucdp-adb-storage-key')}

try:
    dbutils.fs.mount(source=SOURCE, mount_point=MOUNT_POINT, extra_configs=EXTRA_CONFIGS)
except Exception as ex:
    print("Database already mounted!")
    print("Ignoring the mounting part...")

# COMMAND ----------

# MAGIC %md ## input parameters

# COMMAND ----------

# MAGIC %md ### dates

# COMMAND ----------

# today = datetime(2022,12,12)
today = date.today()

CURR_DATE = today.strftime("%Y%m%d")
LAST_YEAR_MONTH = today.strftime("%Y%m")

print("\t Current date: {}".format(CURR_DATE))
print("\t Processing data year month: {}".format(LAST_YEAR_MONTH))

# COMMAND ----------

# MAGIC %md ### keys

# COMMAND ----------

KEY_TMS = 'tms'
KEY_RFM = 'rfm'
COUNTRY = 'uae'

# COMMAND ----------

# MAGIC %md ### paths

# COMMAND ----------

LAYER = 'gold-layer'
SOURCE_SUB_LAYER = 'staging'
TARGET_SUB_LAYER = 'customer-profiling'
DELTA_SUB_LAYER = 'analytics'
MASTER_SUB_LAYER = 'master'
CHANNEL = 'pos'

INPUT_PATH = f"dbfs:/mnt/{CONTAINER}/{LAYER}/{SOURCE_SUB_LAYER}/pos/"
MASTER_PATH = f"dbfs:/mnt/{CONTAINER}/{LAYER}/{MASTER_SUB_LAYER}/"
OUTPUT_PATH = f"dbfs:/mnt/{CONTAINER}/{LAYER}/{TARGET_SUB_LAYER}/{CHANNEL}/{LAST_YEAR_MONTH}/"
DELTA_PATH = f"dbfs:/mnt/{CONTAINER}/{LAYER}/{DELTA_SUB_LAYER}/"
GOLD_PATH = 'dbfs:/mnt/cdp-customers/analytics-layer'

print("\t Input Path: {}".format(INPUT_PATH))
print("\t Master Path: {}".format(MASTER_PATH))
print("\t Output Path: {}".format(OUTPUT_PATH))
print("\t Delta Path: {}".format(DELTA_PATH))

# COMMAND ----------

# MAGIC %md # Process

# COMMAND ----------

print("{} Process Starts".format(datetime.now().strftime("%d-%m-%Y %H:%M:%S")))
process_start_time = datetime.now()

# COMMAND ----------

# MAGIC %md ## read rfm segments
# MAGIC Use RFM segments of month which you used in your population control analysis <br>

# COMMAND ----------

LAST_YEAR_MONTH = '202310'

# COMMAND ----------

print("{} reading rfm segments - in progress..".format(datetime.now().strftime("%d-%m-%Y %H:%M:%S")), 
      end = ' ')
tableName = 'customer_segments'
query = f""" SELECT * FROM analytics.{tableName}
             WHERE analytics.{tableName}.`key` = '{KEY_RFM}'
             AND  analytics.{tableName}.`country` = '{COUNTRY}'
             AND  analytics.{tableName}.`month_year` = '{LAST_YEAR_MONTH}'
             AND  analytics.{tableName}.`channel` = '{CHANNEL}'
         """
sdf_rfm_segments = spark.sql(query)
sdf_rfm_segments = sdf_rfm_segments.withColumnRenamed("total_orders", "frequency") \
                                    .withColumnRenamed("total_spend", "monetary") \
                                    .withColumnRenamed("average_order_value", "average_spend")
df_rfm_segment = sdf_rfm_segments.pandas_api()
print("complete..")

# COMMAND ----------

# MAGIC %md ## read tms segments
# MAGIC Use TMS segments of month which you used in your population control analysis <br>

# COMMAND ----------

LAST_YEAR_MONTH = '202310'

# COMMAND ----------

print("{} reading rfm segments - in progress..".format(datetime.now().strftime("%d-%m-%Y %H:%M:%S")), 
      end = ' ')
tableName = 'customer_segments'
query = f""" SELECT * FROM analytics.{tableName}
             WHERE analytics.{tableName}.`key` = '{KEY_TMS}'
             AND  analytics.{tableName}.`country` = '{COUNTRY}'
             AND  analytics.{tableName}.`month_year` = '{LAST_YEAR_MONTH}'
             AND  analytics.{tableName}.`channel` = '{CHANNEL}'
         """
sdf_rfm_segments = spark.sql(query)
sdf_rfm_segments = sdf_rfm_segments.withColumnRenamed("total_orders", "frequency") \
                                    .withColumnRenamed("total_spend", "monetary") \
                                    .withColumnRenamed("average_order_value", "average_spend")
df_tms_segments = sdf_rfm_segments.pandas_api()
print("complete..")

df_tms_segments.rename(columns = {'segment': 'tms_segment'}, inplace = True)

# COMMAND ----------

# MAGIC %md ## reading customer master

# COMMAND ----------

df_master = ps.read_delta(MASTER_PATH + 'customer/customer_profile/')

# COMMAND ----------

# MAGIC %md ## reading pos txn

# COMMAND ----------

print("{} \t read data - in progress..".format(datetime.now().strftime("%d-%m-%Y %H:%M:%S")), end = ' ')
df_material_master = ps.read_delta(MASTER_PATH + "material/material_master")
df_store_master = ps.read_delta(MASTER_PATH + "store/store_master")

df_pos_data_ = ps.read_delta(INPUT_PATH + 'pos_transactions')
print("complete..")

# COMMAND ----------

LOYALTY_DATE = '2023-01-06'
LOYALTY_END = '2023-04-06'

# COMMAND ----------

df_pos_data = df_pos_data_[(df_pos_data_['transaction_date']>=LOYALTY_DATE) &
                                (df_pos_data_['customer_id'].notna()) &
                                (df_pos_data_['country'] == COUNTRY.upper())].reset_index()

# COMMAND ----------

# MAGIC %md ## one segment per mobile

# COMMAND ----------

df_rfm_segments = df_rfm_segment.merge(df_tms_segments[['customer_id', 'tms_segment']], on = 'customer_id', how = 'left')

# COMMAND ----------

# df_rfm_segments = df_rfm_segments2.merge(df_tms_segments2[['customer_id', 'tms_segment']], on = 'customer_id', how = 'left')

# COMMAND ----------

df_rfm_segments.loc[df_rfm_segments['segment'] == 'VIP', 'priority'] = 1
df_rfm_segments.loc[df_rfm_segments['segment'] == 'Slipping Loyalist', 'priority'] = 2
df_rfm_segments.loc[df_rfm_segments['segment'] == 'Splurger', 'priority'] = 3
df_rfm_segments.loc[df_rfm_segments['segment'] == 'Frequentist', 'priority'] = 4
df_rfm_segments.loc[df_rfm_segments['segment'] == 'Moderate', 'priority'] = 5
df_rfm_segments.loc[df_rfm_segments['segment'] == 'Newbie', 'priority'] = 6
df_rfm_segments.loc[df_rfm_segments['segment'] == 'Lapser', 'priority'] = 7

df_mob_segs = df_master.merge(df_rfm_segments[['customer_id','segment', 'tms_segment', 'priority']],
                             left_on ='account_key', right_on = 'customer_id',
                             how = 'left').reset_index(drop = True)
df_mob_segs.loc[df_mob_segs['segment'].isna(), 'segment'] = 'No Mapping'
df_mob_segs.loc[df_mob_segs['priority'].isna(), 'priority'] = 8

## taking the highest segment in case of multiple  account keys
df_mob_seg = df_mob_segs.sort_values('priority', ascending = True)
df_one_seg = df_mob_seg.drop_duplicates('mobile', keep = 'first').reset_index(drop = True)
df_one_seg.rename(columns = {'segment': 'rfm_segment'}, inplace = True)

# COMMAND ----------

# df_one_seg.to_parquet('dbfs:/mnt/cdp-customers/gold-layer/adhoc/campaigns/enrollment_ca1/' + 'one_seg.paruqet', engine = 'pyarrow')

# COMMAND ----------

# df_sd = df_mob_seg.groupby('mobile').agg(segms = ('priority', 'nunique')).reset_index()
# display(df_sd[df_sd['segms']>1])

# COMMAND ----------

# display(df_mob_seg[df_mob_seg['mobile'] == 971563180007][['mobile', 'account_key', 'segment']])            

# COMMAND ----------

# display(df_one_seg[df_one_seg['mobile'] == 971563180007][['mobile', 'account_key', 'segment']])

# COMMAND ----------

df_one_seg.shape, df_master.shape, df_master.mobile.nunique()

# COMMAND ----------

# MAGIC %md ## table 
# MAGIC  Check the customer count in 'tb_results' before adding new data.<br>
# MAGIC  Helps ensure data integrity before appending records.<br>
# MAGIC  

# COMMAND ----------

# df_emailm1 = ps.read_csv('dbfs:/mnt/cdp-customers/gold-layer/adhoc/campaigns/enrollment_ca1/o1kr1c2_audience.csv', sep = '|', index = False)

df_emailm1 = ps.read_csv('dbfs:/mnt/cdp-customers/gold-layer/adhoc/campaigns/dec_campaigns/Splurger trip increase followup/' + 'control_mobile_nonconverted_10per.csv', index = False)

# df_emailm2.rename(columns = {'Email':'email'}, inplace = True)
df_emailm2 = df_emailm1[['mobile']]
# df_emailm2 = df_emailm1[['mobile']].merge(df_master[['mobile']], on = ['mobile'], how = 'left').reset_index(drop = True)
print(df_emailm2.shape)
df_emailm2.head()

# COMMAND ----------

df_emailm2['key'] = 'campaign'
df_emailm2['country'] = 'uae'
df_emailm2['channel'] = 'pos'
df_emailm2['month_year'] = '202312'
df_emailm2['start_date'] = '2023-12-23' #start date of campaign
df_emailm2['end_date'] = '2023-12-29' #end date of campaign

df_emailm2['customer_type'] = 'enrolled' # unenrolled, enrolled, all
df_emailm2['campaign_type'] = 'whatsapp' # whatsapp, email, sms
df_emailm2['campaign_nature'] = 'promotional' # promotional, survey
df_emailm2['campaign_set'] = 'control' # test, control

df_emailm2['campaign_id'] = 86
df_emailm2['campaign_desc'] = 'Splurger Non Convert trip Increase followup 10pct'

# df_req = df_emailm2[df_emailm2['customer_key'].notna()]

LIST_COL = ['key', 'country', 'channel', 'month_year', 'start_date', 'end_date',
            'customer_type', 'campaign_type', 'campaign_nature', 'campaign_id', 'campaign_desc',
            'mobile', 'email', 'customer_key', 'customer_id', 'rfm_segment', 'tms_segment',
            'campaign_set']
LIST_COL2 = ['key', 'country', 'channel', 'month_year', 'start_date', 'end_date',
            'customer_type', 'campaign_type', 'campaign_nature', 'campaign_id', 'campaign_desc',
            'mobile', 'campaign_set']

LIST_COL_ACC = ['mobile', 'email', 'customer_key', 'account_key', 'rfm_segment', 'tms_segment']

df_final = df_emailm2.merge(df_one_seg[LIST_COL_ACC], on = 'mobile',
                                          how = 'inner').reset_index(drop = True)
df_final.rename(columns = {'account_key': 'customer_id'}, inplace = True)

# df_final.loc[df_final['email'].notna(), 'campaign_set'] = 'test'

df_final['mobile'] = df_final['mobile'].apply(int)
sdf_write = df_final[LIST_COL].to_spark()

# COMMAND ----------

# MAGIC %md ##Data validation

# COMMAND ----------

# DBTITLE 1,Customers in campaign set
df_final.shape, df_emailm2.shape, sdf_write.count()

# COMMAND ----------

# MAGIC %md ### Check table that will get append

# COMMAND ----------

display(sdf_write)

# COMMAND ----------

# MAGIC %md ### Checking RFM distribution of customers

# COMMAND ----------

df_ag = df_final.groupby('rfm_segment').agg(customers = ('customer_id', 'nunique')).reset_index()
df_ag

# COMMAND ----------

# MAGIC %md
# MAGIC ###Total Customers Before data append <br>

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(mobile)
# MAGIC from analytics.campaign

# COMMAND ----------

# MAGIC %md ## write

# COMMAND ----------

tableName = "campaign"
viewName = "campaign_append"
sdf_write.createOrReplaceTempView(viewName)
query = f""" MERGE INTO analytics.{tableName}
             USING {viewName}
             ON analytics.{tableName}.`mobile` = {viewName}.`mobile`
             AND analytics.{tableName}.`campaign_id` = {viewName}.`campaign_id`
             WHEN NOT MATCHED 
             THEN INSERT *"""
spark.sql(query)
print("complete..")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Total Customers after data append
# MAGIC  Verify that the change in data before and after the append matches the number of customer in the campaign <br>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(mobile)
# MAGIC from analytics.campaign
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Campaign wise Customer count verifications

# COMMAND ----------

# MAGIC %sql
# MAGIC select campaign_id,
# MAGIC campaign_set,
# MAGIC count(mobile) from analytics.campaign 
# MAGIC  group by 1,2
# MAGIC  order by campaign_id desc

# COMMAND ----------

# dataframe=ps.read_delta('dbfs:/mnt/cdp-customers/analytics-layer/campaign')
# display(dataframe[dataframe['campaign_id']==95])
