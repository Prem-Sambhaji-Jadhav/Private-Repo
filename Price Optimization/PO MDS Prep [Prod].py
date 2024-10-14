# Databricks notebook source
# MAGIC %md
# MAGIC #Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as psf

import os
import numpy as np
import pandas as pd
import re
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from itertools import combinations

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Azure Connection

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

# MAGIC %md
# MAGIC ##Dates

# COMMAND ----------

today = date.today()
CURR_DATE = today.strftime("%Y%m%d")

# date boundaries for the MDS run
START_DATE = '2022-08-01'
END_DATE = '2024-07-31'

print("\t Current date: {}".format(CURR_DATE))
print("\t Processing data from: {}".format(START_DATE))
print("\t Processing data till: {}".format(END_DATE))

print("\t Number of days: {}".format((datetime.strptime(END_DATE, "%Y-%m-%d") - datetime.strptime(START_DATE, "%Y-%m-%d")).days+1))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Fetch

# COMMAND ----------

df_hhc_cluster_sku_info = spark.table('analytics.pricing.hhc_cluster_sku_info_remapped')

# pending MG df except completed MGs: ['DISINFECTANTS', 'ALL PURPOSE CLEANER', 'WASHING UP']
df_hhc_cluster_sku_info_pending = df_hhc_cluster_sku_info.filter(~col('material_group_name').isin(['DISINFECTANTS', 'ALL PURPOSE CLEANER', 'WASHING UP']))

# count number of distinct cluster_idx in each material group
df_hhc_cluster_sku_info_pending.groupBy('material_group_name').agg(countDistinct('cluster_idx').alias('distinct_cluster_count'),
count('material_id').alias('total_SKUs')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## POC DG (INPUTS)

# COMMAND ----------

# select the MG for the MDS run
material_group_name = 'DISHWASHER DETERGENT'

# printing total distinct cluster_idx in the material group
cluster_idx_list = df_hhc_cluster_sku_info.filter(df_hhc_cluster_sku_info.material_group_name == material_group_name).select('cluster_idx').distinct().collect()

cluster_idx_list = sorted([row[0] for row in cluster_idx_list])

print(f'Current MG: {material_group_name}, Clusters: {cluster_idx_list}')

# COMMAND ----------

# select the dg (cluster_idx), and the mg_abbr. for the MDS run
mg_name_abbr = 'dishwasher'
cluster_idx = 3
weight_scheme = 'w1'

print(f'Current MG: {material_group_name}, MG Abbr: {mg_name_abbr}, Cluster: {cluster_idx}, Weight Scheme: {weight_scheme}')

# COMMAND ----------

cluster_table_name = 'analytics.pricing.hhc_cluster_sku_info_remapped'

print(f'Fetching Material IDs for: {material_group_name}, DG:{cluster_idx}\n')

query = f"""
SELECT
    material_id,
    material_name
FROM {cluster_table_name}
WHERE
    material_group_name = '{material_group_name}'
    AND cluster_idx = '{cluster_idx}'
    AND weight_scheme = '{weight_scheme}'
"""

df_poc_group_apc = spark.sql(query)

table_name_sub = mg_name_abbr + "_dg" + str(cluster_idx)
dg_base_table_name = f"dev.sandbox.pj_hhc_{table_name_sub}"

df_poc_group_apc.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(dg_base_table_name)

print(f'Material IDs saved to: {dg_base_table_name}')

df_poc_group_apc.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SKU Mapping Dict

# COMMAND ----------

# Mapping dict for the SKUs

query = f"""
SELECT
    material_id,
    SUM(amount) AS total_sales
FROM {cluster_table_name} AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.material_id = t2.product_id
WHERE
    t2.business_day BETWEEN '{START_DATE}' AND '{END_DATE}'
    AND t2.transaction_type IN ("SALE", "SELL_MEDIA")
    AND t2.amount > 0
    AND t2.quantity > 0
    AND t1.cluster_idx = '{cluster_idx}'
    AND t1.weight_scheme = '{weight_scheme}'
    AND t1.material_group_name = '{material_group_name}'
GROUP BY 1
ORDER BY 2 DESC
"""

dg_item_mapping_df = spark.sql(query).toPandas()[['material_id']]
dg_item_mapping_df['material_id'] = dg_item_mapping_df['material_id'].astype(str)

dg_item_mapping_dict = {value: f'p_{i}' for i, value in enumerate(dg_item_mapping_df['material_id'])}
print(dg_item_mapping_dict)

# COMMAND ----------

MATERIAL_GROUP_POC = material_group_name

demand_group_item_list = [row.material_id for row in spark.sql(f"select * from {dg_base_table_name}") \
    .select('material_id').distinct().collect()]

demand_group_item_tuple = tuple(demand_group_item_list)

print('POC MG Name: ', MATERIAL_GROUP_POC)
print('Total Items in the selected demand group: ', len(demand_group_item_tuple))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Intermediate Tables

# COMMAND ----------

# Setting table names for all sandboxes

txn_data_raw_dg_poc_name = f"dev.sandbox.pj_txn_data_poc_hhc_{table_name_sub}"
store_product_first_txn_table_name = f"dev.sandbox.pj_store_material_first_txn_hhc_{table_name_sub}"
dg_master_table_name = f"dev.sandbox.pj_poc_master_info_hhc_{table_name_sub}"
dg_mds_base_table_name = f"dev.sandbox.pj_poc_mds_base_hhc_{table_name_sub}"
dg_mds_final_table_name = f"dev.sandbox.pj_poc_mds_final_hhc_{table_name_sub}"
temp_sandbox_table_name = 'dev.sandbox.txn_agg_discount_view_pos_temp_poc'

# COMMAND ----------

# MAGIC %md
# MAGIC ## POS Disc. Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### txn agg

# COMMAND ----------

# query_txn_agg_disc_pos = f"""
# WITH promotion_info_raw AS (
#     SELECT     
#         transaction_id,
#         to_date(CAST(business_date AS STRING), 'yyyyMMdd') as transaction_date,
#         product_id,
#         item_quantity,
#         item_amount,
#         campaign_id,
#         pm_discount_amount,
#         pm_discount_action,
#         item_discount_amount,
#         line_item_sequence_number,
#         pm_reason_code,
#         pm_campaign_group,
#         store_id,
#         pm_discount_media_type as discount_media_type,
#         case
#             when pm_discount_media_type is null then 'NA'
#             when UPPER(TRIM(pm_discount_media_type)) = 'SPECIAL OFFER0' then 'special_offer'
#             when UPPER(TRIM(pm_discount_media_type)) = 'COUNTER_POINT' then 'point_based'
#             else 'voucher_code_based'
#         end as discount_media_type_cln
#     FROM gold.marketing.uae_pos_sales_campaign
#     WHERE 1=1
#     AND to_date(CAST(business_date AS STRING), 'yyyyMMdd') BETWEEN '{START_DATE}' AND '{END_DATE}'
#     AND void_flag IS NULL
#     AND product_id in (select material_id from {dg_base_table_name})
# ),

# price_poc_pilot_products as (
# select distinct material_id, material_name, material_group_name, category_name
# from gold.material.material_master
# where UPPER(material_group_name) = '{MATERIAL_GROUP_POC}'
# ),

# txn_discount_agg as (
# select
# transaction_id,
# campaign_id,
# product_id,
# min(transaction_date) as transaction_date,
# sum(pm_discount_amount*item_quantity) as campaign_discount,
# min(pm_reason_code) as reason_code,
# min(pm_campaign_group) as campaign_group,
# min(store_id) as store_id,
# min(discount_media_type) as discount_media_type,
# min(discount_media_type_cln) as discount_media_type_cln
# from promotion_info_raw
# WHERE 1=1
# AND campaign_id IS NOT NULL
# AND discount_media_type_cln not in ('voucher_code_based', 'special_offer')
# group by transaction_id, campaign_id, product_id
# )

# select * from txn_discount_agg

# """

# # print(query_txn_agg_disc_pos)

# df_txn_agg_discount_pos = spark.sql(query_txn_agg_disc_pos)

# txn_agg_discount_view_name_pos  = 'txn_agg_discount_view_pos_poc'
# df_txn_agg_discount_pos.createOrReplaceTempView(txn_agg_discount_view_name_pos)

# # writing to a temp sandbox table (to be removed later)
# df_txn_agg_discount_pos.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(temp_sandbox_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### weekly agg

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct product_id from dev.sandbox.txn_agg_discount_view_pos_temp_poc
# MAGIC
# MAGIC -- select
# MAGIC -- product_id,
# MAGIC -- min(transaction_date) as min_txn_date,
# MAGIC -- max(transaction_date) as max_txn_date
# MAGIC -- from dev.sandbox.txn_agg_discount_view_pos_temp_poc
# MAGIC -- group by product_id
# MAGIC -- order by min_txn_date

# COMMAND ----------

# query_weekly_agg_disc_pos = f"""
# with daily_product_discounts as (
# select
# transaction_date,
# year(transaction_date) as txn_year,
# CONCAT(YEAR(transaction_date), '-W', LPAD(WEEKOFYEAR(transaction_date), 2, '0')) AS year_week,
# product_id,
# campaign_id,
# case when campaign_group is null then 'NA' else campaign_group end as campaign_group,
# reason_code,
# sum(case when discount_media_type_cln = 'point_based' then campaign_discount/400
# else campaign_discount end) as campaign_discount
# from {temp_sandbox_table_name}
# group by transaction_date, product_id, campaign_id, campaign_group, reason_code
# ),

# weekly_product_discounts_cid as (
# select
# txn_year,
# year_week,
# product_id,
# campaign_id,
# campaign_group,
# reason_code,
# round(sum(campaign_discount),3) as campaign_discount
# from daily_product_discounts
# group by txn_year, year_week, product_id, campaign_id, campaign_group, reason_code
# ),

# weekly_product_discounts_cgroup as (
# select
# txn_year,
# year_week,
# product_id,
# UPPER(campaign_group) as campaign_group,
# reason_code,
# round(sum(campaign_discount),2) as campaign_discount
# from daily_product_discounts
# group by txn_year, year_week, product_id, campaign_group, reason_code
# )

# select *
# from weekly_product_discounts_cgroup

# """


# df_weekly_agg_disc_pos = spark.sql(query_weekly_agg_disc_pos)

# weekly_agg_discount_view_name_pos  = 'weekly_agg_discount_view_pos'
# df_weekly_agg_disc_pos.createOrReplaceTempView(weekly_agg_discount_view_name_pos)

# COMMAND ----------

# MAGIC %md
# MAGIC # Holiday Dates

# COMMAND ----------

# updated with 2022 data (from akif)
spark.sql("select * from dev.sandbox.pg_uae_holiday_list").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Info (2 years)

# COMMAND ----------

# MAGIC %md
# MAGIC ## daily txn data (base)
# MAGIC (full 2 years txn level data for poc demand group (dg))

# COMMAND ----------

raw_txn_query = f"""
  with trans_data_raw as (
    select
      transaction_id,
      business_day,
      CONCAT(YEAR(business_day), '-W', LPAD(weekofyear(business_day), 2, '0')) as week_number,
      customer_id,
      product_id as material_id,
      quantity,
      amount,
      store_id,
      store_name,
      region_name,
      category_name,
      material_group_name,
      material_name,
      unit_price,
      regular_unit_price,
      actual_unit_price,
      coalesce(item_discount_amount,0) as item_discount_amount,
      ean
    from(
        select
          t1.*,
          t2.*,
          t3.store_name,
          t3.region_name
        from
          gold.transaction.uae_pos_transactions t1
          join gold.material.material_master t2 on t1.product_id = t2.material_id
          join gold.store.store_master t3 on t1.store_id = t3.store_id
        where
          t1.business_day >= '{START_DATE}'
          and t1.business_day <= '{END_DATE}'
          and round(t1.amount, 2) > 0
          and t1.quantity > 0
          and t1.product_id in {demand_group_item_tuple}
          -- exclude days which dont form a full 7 days week
          and t1.business_day not between '2024-07-29' and '2024-07-31'
          and t3.region_name in ('DUBAI','SHARJAH','ABU DHABI','AL AIN')
      )
  )

select * from trans_data_raw
"""

df_trans_level_raw = spark.sql(raw_txn_query)

print('txn table name: ', txn_data_raw_dg_poc_name)

df_trans_level_raw.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(txn_data_raw_dg_poc_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##1st txn (store-product)

# COMMAND ----------

store_product_first_txn_query = f"""
select
region_name,
store_id,
material_id,
min(business_day) as first_txn_day,
CONCAT(YEAR(first_txn_day), '-W', LPAD(weekofyear(first_txn_day), 2, '0')) as first_txn_week,
case when first_txn_day between '{START_DATE}' and date_add('{START_DATE}', 6)
     then 1 else 0
     end as existing_product_flag
from {txn_data_raw_dg_poc_name}
group by region_name, store_id, material_id
"""

spark.sql(store_product_first_txn_query).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(store_product_first_txn_table_name)

print("created table name: ", store_product_first_txn_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Sales (extended)

# COMMAND ----------

# calendar_table_name = 'dev.sandbox.pg_calendar_dates_lookup_202407'

# print(f"{txn_data_raw_dg_poc_name}\n"
#       f"{calendar_table_name}\n"
#       f"{store_product_first_txn_table_name}")

# COMMAND ----------

# product_daily_sales_extended_query = f"""

# with daily_sales_store_agg as (
#     select store_id, business_day, material_id, sum(amount) as sales, sum(quantity) as quantity
#     from {txn_data_raw_dg_poc_name}
#     group by store_id, business_day, material_id
# ),

# merged_daily_sales_info as (
# SELECT
#     c.business_day,
#     p.store_id,
#     p.material_id,
#     COALESCE(p2.sales, 0) AS sales,  -- Replace NULL sales with zero
#     COALESCE(p2.quantity, 0) AS quantity  -- Replace NULL quantity with zero
# FROM
#     (SELECT DISTINCT business_day FROM {calendar_table_name}) as c
# CROSS JOIN
#     (SELECT DISTINCT store_id, material_id FROM daily_sales_store_agg) as p
# LEFT JOIN
#     daily_sales_store_agg as p2
# ON
#     p.store_id = p2.store_id AND p.material_id = p2.material_id AND c.business_day = p2.business_day
# ),

# first_txn_day_info as (
#     SELECT material_id, store_id, first_txn_day, first_txn_week, existing_product_flag
#     from {store_product_first_txn_table_name}
# ),

# daily_sales_info_ext as (
# select
# business_day,
# CONCAT(YEAR(business_day), '-W', LPAD(weekofyear(business_day), 2, '0')) as business_week,
# store_id,
# material_id,
# round(sales,2) as sales,
# round(quantity,2) as quantity,
# round(AVG(sales) OVER (PARTITION BY store_id, material_id ORDER BY business_day ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),2) as MA_15_sales,
# round(AVG(quantity) OVER (PARTITION BY store_id, material_id ORDER BY business_day ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),2) as MA_15_quantity,
# round(AVG(sales) OVER (PARTITION BY store_id, material_id ORDER BY business_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) as MA_7_sales,
# round(AVG(quantity) OVER (PARTITION BY store_id, material_id ORDER BY business_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) as MA_7_quantity

# from merged_daily_sales_info
# )

# -- FINAL DRIVER CODE
# select t1.*, t2.first_txn_day
# FROM daily_sales_info_ext t1
# JOIN first_txn_day_info t2
#     on t1.material_id = t2.material_id
#     and t1.store_id = t2.store_id
#     and t1.business_day >= t2.first_txn_day
# ORDER BY t1.material_id, t1.store_id, t1.business_day
# """

# df_daily_sales_ext = spark.sql(product_daily_sales_extended_query)

# daily_sales_ext_view_name = 'daily_sales_ext_view'
# df_daily_sales_ext.createOrReplaceTempView(daily_sales_ext_view_name)

# df_daily_sales_ext.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Data Prep

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekly Regional Sales

# COMMAND ----------

weekly_sales_info_query = f"""
with weekly_sales_info as (
  SELECT
    week_number,
    region_name,
    material_id,
    round(sum(amount),2) as sales,
    sum(quantity) as quantity,
    round(sum(amount)/sum(quantity),2) as avg_unit_price,
    count(distinct store_id) as num_stores
  FROM
    {txn_data_raw_dg_poc_name}
  GROUP BY
    week_number,
    region_name,
    material_id
    ),

new_store_launch_counts as (
  SELECT
    region_name,
    first_txn_week,
    material_id,
    count(distinct store_id) as num_new_stores
    from {store_product_first_txn_table_name}
  GROUP BY
    region_name,
    first_txn_week,
    material_id
),

holiday_info as (
  select holiday_week, count(holiday_date) as num_holidays
  from (select holiday_year,
        cast(holiday_date as date) as holiday_date,
        CONCAT(YEAR(holiday_date), '-W', LPAD(weekofyear(holiday_date), 2, '0')) as holiday_week,
        holiday_desc
        from dev.sandbox.pg_uae_holiday_list)
  group by holiday_week
)

select t1.*, coalesce(t2.num_new_stores,0) as num_new_stores,
coalesce(round(t2.num_new_stores/t1.num_stores,4),0) as new_launch_idx,
case when t4.num_holidays is null then 0 else 1 end as has_holidays
from weekly_sales_info t1
left join new_store_launch_counts t2
on t1.week_number = t2.first_txn_week
and t1.region_name = t2.region_name
and t1.material_id = t2.material_id
left join holiday_info t4
on t1.week_number = t4.holiday_week
order by t1.material_id, t1.week_number, t1.region_name 
"""

df_weekly_regional_sales = spark.sql(weekly_sales_info_query)

df_weekly_regional_sales.createOrReplaceTempView('region_weekly_sales_info')
df_weekly_regional_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Discount Calculation

# COMMAND ----------

query = f"""
create or replace temp view region_weekly_discount_info as (

with region_weekly_unit_prices as (
  select
    region_name,
    week_number,
    material_id,
    unit_price_adj as unit_price,
    sum(num_txn) as num_txn,
    sum(total_sales) / sum(total_qty) as avg_unit_sales_price,
    min(week_first_day) as week_first_day
  from(
      select
        region_name,
        week_number,
        material_id,
        unit_price,
        FLOOR((unit_price * 2) + 0.5) / 2 AS unit_price_adj,
        sum(amount) as total_sales,
        sum(quantity) as total_qty,
        count(distinct transaction_id) as num_txn,
        min(business_day) as week_first_day
      from
        {txn_data_raw_dg_poc_name}
      group by
        region_name,
        week_number,
        material_id,
        unit_price
    )
  group by
    region_name,
    week_number,
    material_id,
    unit_price_adj
),

region_weekly_unit_sale_price as (
  select
    region_name,
    week_number,
    material_id,
    sum(amount) as total_sales,
    sum(quantity) as total_qty,
    count(distinct transaction_id) as num_txn,
    total_sales / total_qty as unit_sales_price,
    min(business_day) as week_first_day
  from
    {txn_data_raw_dg_poc_name}
  group by
    region_name,
    week_number,
    material_id
),


merged_data_base as (
select
  t1.region_name, t1.week_number, t1.material_id,
  t1.total_sales, t1.total_qty, t1.unit_sales_price,
  t2.week_number as week_number_hist, t2.unit_price, CAST(t2.num_txn AS INT) as num_txn
from
  region_weekly_unit_sale_price t1
  join region_weekly_unit_prices t2
on t1.region_name = t2.region_name
and t1.material_id = t2.material_id
and datediff(t1.week_first_day, t2.week_first_day) between 0 and 90
order by t1.region_name, t1.material_id, t1.week_number, t2.week_number
),

merged_data_expanded as (
  select *, EXPLODE(ARRAY_REPEAT(unit_price, num_txn)) AS unit_price_hist_exp
  from merged_data_base
)

select
region_name,
week_number,
material_id,
round(avg(total_sales),2) as total_sales_amount,
avg(total_qty) as total_quantity,
round(avg(unit_sales_price),2) as avg_unit_sales_price,
percentile_approx(unit_price_hist_exp, 0.5) as base_price_hist,
case when (avg_unit_sales_price - base_price_hist) / base_price_hist >= 0.1 then 1 else 0 end as price_inc_flag,
case when (base_price_hist - avg_unit_sales_price ) / base_price_hist >= 0.03 then 1 else 0 end as discount_flag,
case when discount_flag = 1 then (base_price_hist - avg_unit_sales_price)*total_quantity else 0 end as discount_amount,
round(discount_amount/total_sales_amount, 4) as discount_perc,
case when discount_perc = 0 then 'No discount'
     when discount_perc between 0.01 and 0.05 then '<5%'
     when discount_perc between 0.051 and 0.10 then '5-10%'
     when discount_perc between 0.101 and 0.20 then '10-20%'
     else '>20%'
     end as discount_perc_cat
from merged_data_expanded
group by region_name, week_number, material_id
order by region_name, material_id, week_number
)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Master Data Table

# COMMAND ----------

master_query_1 = """
select
t2.week_number,
t2.region_name,
t2.material_id,
t2.sales,
t2.quantity,
t2.avg_unit_price,
t2.new_launch_idx,
t2.has_holidays,
t1.price_inc_flag,
t1.discount_flag,
t1.discount_amount,
t1.discount_perc,
t1.discount_perc_cat
from region_weekly_discount_info t1
join region_weekly_sales_info t2
on t1.region_name = t2.region_name
and t1.week_number = t2.week_number
and t1.material_id = t2.material_id
"""

spark.sql(master_query_1).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(dg_master_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##New Launch Index & MA

# COMMAND ----------

# Function to derive the price ratios for each material
def calculate_price_ratios(df, mapping_dict):

    reverse_mapping = {v: k for k, v in mapping_dict.items()}
    proxy_ids = mapping_dict.values()
    
    def calculate_group_ratios(group):
        prices = group.set_index('material_id')['avg_unit_price'].to_dict()
        
        for proxy in proxy_ids:
            material_id = reverse_mapping[proxy]
            if material_id in prices:
                group[f'price_ratio_{proxy}'] = group['avg_unit_price'] / prices[material_id]
            else:
                group[f'price_ratio_{proxy}'] = np.nan
        
        return group
    
    result = df.groupby(['week_number', 'region_name'], group_keys=False).apply(calculate_group_ratios)

    result['material_proxy_identifier'] = result['material_id'].map(mapping_dict)
    
    return result

print('Total Materials in the DG: ', len(dg_item_mapping_dict))

query_product_lookup = f"""
select distinct material_id, material_name
from {dg_base_table_name}
"""

df_product_lookup = spark.sql(query_product_lookup).toPandas()
df_product_lookup['material_id'] = df_product_lookup['material_id'].astype(str)
product_lookup_dict = df_product_lookup.set_index('material_id')['material_name'].to_dict()

# Adding MA columns to the base data
query_ma = f"""
with ma_added_sales as (
select *,
sum(sales) over(partition by region_name, material_id order by week_number rows between 2 preceding and 1 preceding) as MA_15D_SALES,
sum(quantity) over(partition by region_name, material_id order by week_number rows between 2 preceding and 1 preceding) as MA_15D_QTY,
MA_15D_SALES/MA_15D_QTY as avg_unit_price_ma_15d,
sum(sales) over(partition by region_name, material_id order by week_number rows between 4 preceding and 1 preceding) as MA_30D_SALES,
sum(quantity) over(partition by region_name, material_id order by week_number rows between 4 preceding and 1 preceding) as MA_30D_QTY,
MA_30D_SALES/MA_30D_QTY as avg_unit_price_ma_30d,
coalesce(round(avg_unit_price/avg_unit_price_ma_15d,3),1.0) as price_ratio_ma_15d,
coalesce(round(avg_unit_price/avg_unit_price_ma_30d,3),1.0) as price_ratio_ma_30d
from {dg_master_table_name}
order by region_name, material_id, week_number
)

select * from ma_added_sales
"""

df_source_ma_added = spark.sql(query_ma).toPandas()

# correcting for discount perc
df_source_ma_added['discount_perc'] = df_source_ma_added['discount_amount'] / (df_source_ma_added['discount_amount'] + df_source_ma_added['sales'])
df_source_ma_added['discount_perc'] = df_source_ma_added['discount_perc'].replace([np.inf, -np.inf], 0)
df_source_ma_added['discount_perc'] = df_source_ma_added['discount_perc'].round(4)

# Adding price ratios
df_source_final = calculate_price_ratios(df_source_ma_added, dg_item_mapping_dict)

# Adjusting for the new launch index
df_source_final['new_launch_idx'] = df_source_final.apply(
    lambda row: row['new_launch_idx'] if row['new_launch_idx'] >= 0.1 and row['week_number'] != '2022-W31' else 0,
    axis=1
)

# Dataframe rows mismatch check!
if df_source_final.shape[0] == df_source_ma_added.shape[0]:
    print('Input/Transformed Dataframes are equal in row size, Output Shape: ', df_source_final.shape)
    df_model_input_base = df_source_final.copy()
    df_model_input_base['material_id'] = df_model_input_base['material_id'].astype(str)

    sdf_model_input_base = spark.createDataFrame(df_model_input_base)
    sdf_model_input_base.createOrReplaceTempView('hhc_dg_model_base_data_final')

    sdf_model_input_base.display()
else:
    raise ValueError('Input/Transformed Dataframes are not equal')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Weather Data

# COMMAND ----------

END_DATE = '2024-07-31'
START_DATE = '2022-08-01'

file_path = "/Workspace/Repos/piyush@loyalytics.in/lulu_notebooks/temp_data_files/uae_weather_data_22_24_v2.xlsx"

xls = pd.ExcelFile(file_path)

df_list = []

for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name)
    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')
    df_list.append(df)

df_uae_weather_data_daily_raw = pd.concat(df_list, ignore_index=True)
df_uae_weather_data_daily = df_uae_weather_data_daily_raw[(df_uae_weather_data_daily_raw['date'] >= START_DATE) & (df_uae_weather_data_daily_raw['date'] <= END_DATE)].copy()

df_uae_weather_data_daily['rainfall'] = df_uae_weather_data_daily['rainfall'].round(2)
df_uae_weather_data_daily['temp'] = df_uae_weather_data_daily['temp'].round(2)

sdf_uae_weather_data_daily = spark.createDataFrame(df_uae_weather_data_daily)
sdf_uae_weather_data_daily.createOrReplaceTempView('uae_weather_data_daily_view')

# aggregating for weekly data
query_weather = """
select
region as region_name,
CONCAT(YEAR(date), '-W', LPAD(weekofyear(date), 2, '0')) as week_number,
round(avg(temp),2) as avg_temp,
round(sum(rainfall),2) as total_rainfall
from uae_weather_data_daily_view
group by region_name, week_number
"""

df_uae_weather_data_weekly = spark.sql(query_weather).toPandas()

df_uae_weather_data_weekly.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Promo Events Data

# COMMAND ----------

file_path = '/Workspace/Repos/piyush@loyalytics.in/lulu_notebooks/temp_data_files/promo_events_master_23_24.csv'

df_promo_events_raw = pd.read_csv(file_path)
df_promo_events = df_promo_events_raw.copy()

df_promo_events['start_date'] = pd.to_datetime(df_promo_events['start_date'], format='%Y-%m-%d')
df_promo_events['end_date'] = pd.to_datetime(df_promo_events['end_date'], format='%Y-%m-%d')
df_promo_events['event_date'] = df_promo_events.apply(lambda row: pd.date_range(row['start_date'], row['end_date']), axis=1)


df_promo_events_exploded = df_promo_events.explode('event_date')
df_promo_events_final = df_promo_events_exploded[['event_date', 'event_theme']].rename(columns={'event_theme': 'active_event_theme'})
df_promo_events_final['week_number'] = df_promo_events_final['event_date'].dt.strftime('%Y-W%V')
df_promo_events_final['event_date'] = df_promo_events_final['event_date'].dt.strftime('%Y-%m-%d')


sdf_promo_events_final = spark.createDataFrame(df_promo_events_final)
sdf_promo_events_final.createOrReplaceTempView('promo_events_final_view')


query_event_index = """
WITH weekly_event_days AS (
    SELECT
    week_number,
    active_event_theme,
    COUNT(DISTINCT event_date) AS num_days
    FROM promo_events_final_view
    GROUP BY week_number, active_event_theme
),

event_metrics AS (
    SELECT 
        week_number,
        COUNT(DISTINCT active_event_theme) AS num_events,
        SUM(num_days) AS total_event_days,
        MAX(num_days) AS max_event_duration
    FROM weekly_event_days
    GROUP BY week_number
),

week_stats AS (
    SELECT
        AVG(num_events) AS avg_events,
        AVG(total_event_days) AS avg_total_days,
        AVG(max_event_duration) AS avg_max_duration
    FROM event_metrics
),

event_presence_indexed as (
SELECT 
    em.week_number,
    em.num_events,
    em.total_event_days,
    em.max_event_duration,
    round((em.num_events / ws.avg_events * 0.45 +
     em.total_event_days / ws.avg_total_days * 0.45 +
     em.max_event_duration / ws.avg_max_duration * 0.10),3) AS event_presence_idx
FROM event_metrics em, week_stats ws)

select week_number, event_presence_idx
FROM event_presence_indexed

"""

df_event_index = spark.sql(query_event_index).toPandas()
df_event_index.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final Merged Data

# COMMAND ----------

import pandas as pd

df_ab_merged = pd.merge(
    df_model_input_base,
    df_uae_weather_data_weekly[['region_name', 'week_number', 'avg_temp', 'total_rainfall']],
    on=['region_name', 'week_number'],
    how='left'
)

df_mds_base_merged = pd.merge(
    df_ab_merged,
    df_event_index[['week_number', 'event_presence_idx']],
    on='week_number',
    how='left'
)

df_mds_base_merged['event_presence_idx'] = df_mds_base_merged['event_presence_idx'].fillna(0)

print(df_mds_base_merged.info())
print(df_mds_base_merged.head())
# print(df_mds_base_merged.isnull().sum())

# COMMAND ----------

sdf_base_merged = spark.createDataFrame(df_mds_base_merged)

sdf_base_merged.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(dg_mds_base_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Price Ratio Comp

# COMMAND ----------

import pandas as pd
import numpy as np

def create_comp_price_ratio(df):

    grouped = df.groupby(['week_number', 'region_name'], group_keys=True)

    def calculate_ratio(group):

        total_sales = group['sales'].sum()
        total_quantity = group['quantity'].sum()
        # calculate complementary price
        group['comp_price'] = (total_sales - group['sales']) / (total_quantity - group['quantity'])
        
        # complementary price ratio
        group['price_ratio_comp'] = group['avg_unit_price'] / group['comp_price']
        
        return group
    
    result = grouped.apply(calculate_ratio)
    result = result.reset_index(drop=True)
    
    return result

# reading the master base mds
df_final_mds_base_raw = spark.sql(f"select * from {dg_mds_base_table_name}").toPandas()

# adding the complementary price ratio column
df_final_mds_base_raw = create_comp_price_ratio(df_final_mds_base_raw)

base_cols = ['week_number', 'region_name', 'material_id', 'quantity',
'avg_unit_price', 'new_launch_idx', 'has_holidays',
'price_inc_flag', 'discount_amount', 'discount_perc', 'event_presence_idx', 'price_ratio_ma_15d', 'price_ratio_ma_30d']

for i in range(len(dg_item_mapping_dict)):
    base_cols.append(f'price_ratio_p_{i}')

base_cols = base_cols + ['price_ratio_comp', 'avg_temp', 'total_rainfall']

# selecting the relevant base columns
df_final_mds_base = df_final_mds_base_raw[base_cols].copy()

# basic cleanups
float_cols = df_final_mds_base.select_dtypes(include=['float64']).columns
df_final_mds_base[float_cols] = df_final_mds_base[float_cols].round(3)

price_ratio_cols = [col for col in df_final_mds_base.columns
                    if col.startswith('price_ratio_p')]

for col in price_ratio_cols:
    df_final_mds_base[col].fillna(df_final_mds_base[col].mean(), inplace=True)

null_counts = df_final_mds_base.isnull().sum()
null_columns = null_counts[null_counts > 0].to_dict()

if null_columns:
    print(null_columns)
else:
    print("No column with null values")

print('Shape of the final MDS:', df_final_mds_base.shape)

# show the final mds dataframe
df_final_mds_base.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Promo Event Flags

# COMMAND ----------

promo_events_df = spark.sql("SELECT * FROM dev.sandbox.pg_po_weekly_events_features").toPandas()

df_final_mds_base = pd.merge(df_final_mds_base, promo_events_df, on = 'week_number', how = 'left')

#Impute null values with 0
cols = promo_events_df.columns[1:]
df_final_mds_base[cols] = df_final_mds_base[cols].fillna(0)

df_final_mds_base.rename(columns={col: 'ev_' + col for col in cols}, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Numeric Distribution

# COMMAND ----------

import pandas as pd
import numpy as np
def get_numeric_dist_base_data(txn_table_dg, tnx_table_cat):
    
    query_numeric_dist = f"""
    with sku_store_counts as (
    select region_name, week_number, material_id, count(distinct store_id) as num_stores_sku
    from {txn_table_dg}
    group by region_name, week_number, material_id
    ),
    cat_store_counts as (
    select region_name, week_number, count(distinct store_id) as num_stores_cat
    from {tnx_table_cat}
    group by region_name, week_number
    )
    select t1.region_name, t1.week_number, t1.material_id,
    t1.num_stores_sku, t2.num_stores_cat,
    round(t1.num_stores_sku/t2.num_stores_cat,3) as numeric_dist
    from sku_store_counts t1
    join cat_store_counts t2
    on t1.region_name = t2.region_name
    and t1.week_number = t2.week_number
    """
    
    sdf_numeric_dist = spark.sql(query_numeric_dist)
    pdf_numeric_dist = sdf_numeric_dist.toPandas()
    return pdf_numeric_dist
def get_numeric_dist_features(pdf_numeric_dist, dg_item_mapping_dict):
    df = pdf_numeric_dist.copy()
    
    pivoted = df.pivot(index=['region_name', 'week_number'], 
                       columns='material_id', 
                       values='numeric_dist')
    
    sort_dict = {k: int(v.split('_')[1]) for k, v in dg_item_mapping_dict.items()}
    
    pivoted = pivoted[sorted(pivoted.columns, key=lambda x: sort_dict[str(x)])]
    pivoted.columns = [f'numeric_dist_{dg_item_mapping_dict[str(col)]}' for col in pivoted.columns]
    pivoted = pivoted.reset_index()
    
    all_columns = [f'numeric_dist_p_{i}' for i in range(len(dg_item_mapping_dict))]
    
    for col in all_columns:
        if col not in pivoted.columns:
            pivoted[col] = pd.np.nan
    
    reordered_columns = ['region_name', 'week_number'] + all_columns
    pivoted = pivoted.reindex(columns=reordered_columns)
    
    return pivoted

# source tables names
txn_table_dg = txn_data_raw_dg_poc_name # txn table for dg
tnx_table_cat = 'dev.sandbox.pg_txn_data_hhc_raw' #txn table for overall category

# get the base numeric data
pdf_numeric_dist = get_numeric_dist_base_data(txn_table_dg, tnx_table_cat)

# get the features (pivoted)
df_numeric_dist_long = get_numeric_dist_features(pdf_numeric_dist, dg_item_mapping_dict)
df_numeric_dist_long.display()

# COMMAND ----------

df_final_mds_base = pd.merge(df_final_mds_base, df_numeric_dist_long, on = ['region_name', 'week_number'], how = 'left')

cols = df_numeric_dist_long.columns[2:]
df_final_mds_base[cols] = df_final_mds_base[cols].fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lag Quantity

# COMMAND ----------

df_final_mds_base = df_final_mds_base.sort_values(by = ['region_name', 'material_id', 'week_number']).reset_index(drop = True)

df_final_mds_base['lag_quantity_1w'] = df_final_mds_base.groupby('region_name')['quantity'].shift(1)
df_final_mds_base['lag_quantity_2w'] = df_final_mds_base.groupby('region_name')['quantity'].shift(2)
df_final_mds_base['lag_quantity_1w'] = df_final_mds_base.groupby('region_name', group_keys=False)['lag_quantity_1w'].apply(lambda x: x.fillna(x.mean()))
df_final_mds_base['lag_quantity_2w'] = df_final_mds_base.groupby('region_name', group_keys=False)['lag_quantity_2w'].apply(lambda x: x.fillna(x.mean()))

df_final_mds_base[['week_number', 'region_name', 'material_id', 'quantity', 'lag_quantity_1w', 'lag_quantity_2w']].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Material Identifiers

# COMMAND ----------

# adding a column for proxy identifier
df_final_mds_base['material_proxy_identifier'] = df_final_mds_base['material_id'].map(dg_item_mapping_dict)

# adding a column for material name
df_final_mds_base['material_name'] = df_final_mds_base['material_id'].map(product_lookup_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Seasonality Index

# COMMAND ----------

def get_dg_seasonality_idx(df_final_mds_base):

    import pandas as pd
    import numpy as np

    df_final_mds_base['year'] = df_final_mds_base['week_number'].str[:4]
    df_final_mds_base['week_number_val'] = df_final_mds_base['week_number'].str[-3:]

    df_seasonality_idx = df_final_mds_base.pivot_table(index=['region_name', 'material_id', 'week_number_val'], columns='year', values='quantity')

    df_seasonality_idx.columns = [f'year_{str(col)[-2:]}' for col in df_seasonality_idx.columns]

    df_seasonality_idx.reset_index(inplace=True)

    year_columns = [col for col in df_seasonality_idx.columns if col.startswith('year_')]

    def calculate_overall_avg(group):
        # Flatten all year columns into a single series
        all_values = group[year_columns].values.flatten()
        all_values = all_values[~np.isnan(all_values)]
        return np.mean(all_values) if len(all_values) > 0 else np.nan

    overall_avg = df_seasonality_idx.groupby(['region_name', 'material_id']).apply(calculate_overall_avg)

    # overall avg_qty
    df_seasonality_idx['overall_avg_qty'] = df_seasonality_idx.set_index(['region_name', 'material_id']).index.map(overall_avg)

    # yearly avg_qty
    df_seasonality_idx['yearly_avg_qty'] = df_seasonality_idx[year_columns].mean(axis=1, skipna=True)

    # Calculate seasonality_idx
    df_seasonality_idx['seasonality_idx'] = np.round(df_seasonality_idx['yearly_avg_qty'] / df_seasonality_idx['overall_avg_qty'],3)

    # final seasonality idx dataframe
    final_cols = ['region_name', 'material_id', 'week_number_val', 'seasonality_idx']
    df_seasonality_idx_final = df_seasonality_idx[final_cols].copy()

    return df_seasonality_idx_final

df_seasonality_idx = get_dg_seasonality_idx(df_final_mds_base=df_final_mds_base)

# adding the week_number val and the seasonality idx
df_final_mds_base['week_number_val'] = df_final_mds_base['week_number'].str.split('-').str[1]
df_final_mds_base = pd.merge(df_final_mds_base, df_seasonality_idx, 
                     on=['region_name', 'material_id', 'week_number_val'], 
                     how='inner')

df_seasonality_idx.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Edge Case Imputations
# MAGIC
# MAGIC (No Price Var. + Null Price Ratios SKUs)

# COMMAND ----------

# For any SKU in a given region, if the distinct avg_unit_price is 1, add 0.1 to a random record in the column

temp = df_final_mds_base.groupby(['material_id', 'region_name'])['avg_unit_price'].nunique().reset_index()
temp = temp[temp['avg_unit_price'] == 1].reset_index(drop = True)

materials = temp['material_id'].unique()

for material in materials:
    regions = temp[temp['material_id'] == material]['region_name'].unique()

    for region in regions:
        random_index = np.random.choice(df_final_mds_base[(df_final_mds_base['material_id'] == material) & (df_final_mds_base['region_name'] == region)].index)

        df_final_mds_base.loc[random_index, 'avg_unit_price'] += 0.1



# impute for those records where price_ratio_comp is null
df_final_mds_base['price_ratio_comp'] = df_final_mds_base.groupby(['material_id', 'region_name'])['price_ratio_comp'].transform(lambda x: x.fillna(x.mean()))


# impute for those records where price_ratio_comp is null for all rows for a given material-region pair (no other item from same DG present)

df_final_mds_base['price_ratio_comp'].fillna(1, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save to Sandbox

# COMMAND ----------

# debug code

# temp_cols = ['week_number', 'region_name', 'material_id', 'quantity', 'avg_unit_price','price_ratio_comp', 'numeric_dist_comp']
# df_nulls = df_final_mds_base[df_final_mds_base.price_ratio_comp.isna()][temp_cols]

# df_nulls.groupby(['material_id', 'region_name']).agg({'week_number': 'count'})

# COMMAND ----------

# df_final_mds_base[df_final_mds_base.region_name=='AL AIN'].material_id.unique()

# df_final_mds_base[df_final_mds_base.region_name=='AL AIN'][temp_cols+['numeric_dist_p_0','numeric_dist_p_1']].numeric_dist_p_1.sum()

# COMMAND ----------

# null check and manual intervention

null_counts = df_final_mds_base.isnull().sum()
null_col_dict = null_counts[null_counts > 0].to_dict()


if null_col_dict:
    raise ValueError(f'Null Values Found, {null_col_dict}')
else:
    print("No column with null values")

# COMMAND ----------

spark.createDataFrame(df_final_mds_base).write.mode("overwrite").option("overWriteSchema",True).saveAsTable(dg_mds_final_table_name)

print("Data written to table: " + dg_mds_final_table_name)

# COMMAND ----------

query = f"""
select *,
'{material_group_name}' as material_group_name, 'DG-{cluster_idx}' as demand_group
from
{dg_mds_final_table_name}
"""

df_final_mds_base = spark.sql(query)

df_final_mds_base.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to master

# COMMAND ----------

from pyspark.sql import functions as F

mds_master_table_name = 'analytics.pricing.hhc_mds_master'
sdf_target = spark.table(mds_master_table_name)

existing_mg_dg_pairs = [(row[0], row[1]) for row in sdf_target.select('material_group_name', 'demand_group').distinct().collect()]

current_mg_dg_pair = (material_group_name, f'DG-{cluster_idx}')

# Schema mismatch check
target_columns = set(sdf_target.columns)
current_columns = set(df_final_mds_base.columns)

# Find columns missing in the current dataframe
missing_columns = target_columns - current_columns
extra_columns = current_columns - target_columns


# if missing_columns:
#     print(f'missing columns: {missing_columns}\n')
# if extra_columns:
#     print(f'extra columns: {extra_columns}\n')

missing_or_extra = missing_columns.union(extra_columns)


if all(col.startswith('price_ratio_') or col.startswith('numeric_dist_') for col in missing_or_extra):
    print("All Missing Columns Either Price Ratio or Numeric Dist. Columns")
else:
    print('Some Non Price Ratio/Numeric Dist. Cols are also missing. ')


# append operation
if current_mg_dg_pair not in existing_mg_dg_pairs:
    df_final_mds_base.write.mode("append").option("mergeSchema", "true").saveAsTable(mds_master_table_name)
    spark.sql(f"drop table if exists {dg_mds_final_table_name}")
    print(f"Data for {current_mg_dg_pair} written to Analytics Master Table: {mds_master_table_name}")
    print("Temp. Sandbox Deleted!!")
else:
    print(f"MG-DG {current_mg_dg_pair} already exists in the master table")
    print(f"\tSkipping write to {mds_master_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DG Append Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC material_group_name,
# MAGIC demand_group,
# MAGIC count(distinct material_id) as num_skus
# MAGIC from
# MAGIC   analytics.pricing.hhc_mds_master
# MAGIC group by
# MAGIC   material_group_name,
# MAGIC   demand_group
# MAGIC order by
# MAGIC   material_group_name,
# MAGIC   demand_group
