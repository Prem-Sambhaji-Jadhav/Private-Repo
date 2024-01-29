-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Campaign Data: VIP40AED
-- MAGIC Desc: Top 55k (approx.) VIP Customers who has shopped within departments 1-13 on the basis of their average_order_value, total_orders and recency. Need to divide the data into test and control sets of 5-95% and get their mobile and card_key numbers.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Data Collection

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.rb_dep1to13 AS
SELECT t1.mobile, t1.customer_id, t1.product_id, t2.department_id
FROM gold.pos_transactions AS t1
LEFT JOIN (SELECT department_id, material_id
            FROM gold.material_master
            WHERE department_id BETWEEN 1 AND 13) AS t2
ON t1.product_id = t2.material_id
WHERE t1.business_day BETWEEN '2023-01-01' AND '2023-12-31'
AND LHPRDate IS NOT NULL

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.rb_rfm AS
SELECT t2.*, t1.card_key
FROM gold.customer_profile t1
RIGHT JOIN (SELECT t1.customer_id,
                    ROUND(ROUND(SUM(amount),0) / COUNT(DISTINCT transaction_id),2) AS average_order_value,
                    COUNT(DISTINCT transaction_id) AS total_orders,
                    DATEDIFF("2023-12-31", DATE(MAX(last_purchase_date))) AS recency
            FROM analytics.customer_segments AS t1
            JOIN gold.pos_transactions AS t2
            ON t1.customer_id = t2.customer_id
            JOIN gold.material_master AS t3
            ON t2.product_id = t3.material_id
            WHERE key = 'rfm'
            AND LHPRDate IS NOT NULL
            AND channel = 'pos'
            AND t1.country = 'uae'
            AND month_year = '202312'
            AND segment = 'VIP'
            AND department_id BETWEEN 1 AND 13
            AND business_day BETWEEN '2023-01-01' AND '2023-12-31'
            GROUP BY t1.customer_id) AS t2
ON t1.account_key = t2.customer_id

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.rb_table AS
SELECT t1.*, t2.department_id,t2.mobile
FROM sandbox.rb_rfm AS t1
LEFT JOIN sandbox.rb_dep1to13 AS t2
ON t1.customer_id = t2.customer_id
WHERE t2.department_id IS NOT NULL

-- COMMAND ----------

SELECT COUNT(*)
FROM (SELECT DISTINCT customer_id, mobile
      FROM sandbox.rb_table)

-- check this number to match approx. with the count of VIP in the RFV dashboard. (164,124)

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.rb_vip40 AS
SELECT DISTINCT customer_id, mobile, card_key,
        average_order_value, total_orders, recency
FROM sandbox.rb_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Customer Behaviour Distribution

-- COMMAND ----------

-- MAGIC %python
-- MAGIC a = """
-- MAGIC      select * from sandbox.rb_vip40
-- MAGIC      """
-- MAGIC data = spark.sql(a)
-- MAGIC df_vip40 = data.toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_vip40[['average_order_value', 'total_orders', 'recency']].describe(percentiles=[.1,.2,.3,.4,.5,.6,.7,.75,.8,.85,.9,.95,.96,.97,.98,.99])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Thresholds to Limit 55k Customers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Threshold ATV<=600, total_orders >=25, recency <=15
-- MAGIC
-- MAGIC filtered_df_vip40 = df_vip40[(df_vip40['average_order_value'] <= 800) & \
-- MAGIC                             (df_vip40['average_order_value'] >= 200) & \
-- MAGIC                             (df_vip40['total_orders'] <= 50) & \
-- MAGIC                             (df_vip40['total_orders'] >= 15) & \
-- MAGIC                             (df_vip40['recency'] <= 30) & \
-- MAGIC                             (df_vip40['recency'] >= 1)].reset_index(drop = True)
-- MAGIC filtered_df_vip40.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC filtered_df_vip40.info()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC filtered_df_vip40['card_key'] = filtered_df_vip40['card_key'].astype(int)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC filtered_df_vip40 = filtered_df_vip40.dropna(subset = ['mobile']).reset_index(drop=True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Convert DataFrame to Sandbox

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # converting pd to SQL
-- MAGIC filtered_df_vip40 = spark.createDataFrame(filtered_df_vip40) # to convert to SPARK
-- MAGIC
-- MAGIC # writting spark as a temp table in sandbox
-- MAGIC filtered_df_vip40.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("sandbox.rb_filtered_df_vip40")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test-Control Split

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.rb_VIP40AED_customers AS 
SELECT * ,
      CASE WHEN split_num <= 19 THEN 'test'
      ELSE 'control'
      END AS sample
FROM
      (SELECT *, ntile(20) OVER (ORDER BY rand()) AS split_num
      FROM
            (
            SELECT * FROM sandbox.rb_filtered_df_vip40
            )
      )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test-Control Similarity Check

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Converting it back to PD dataframe
-- MAGIC q1 = """
-- MAGIC      select * from sandbox.rb_VIP40AED_customers
-- MAGIC      """
-- MAGIC data = spark.sql(q1)
-- MAGIC df = data.pandas_api()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Filter the DataFrame where 'sample' is equal to 'test'
-- MAGIC test_df = df.loc[df['sample'] == 'test']
-- MAGIC control_df = df.loc[df['sample'] == 'control']
-- MAGIC
-- MAGIC mean_sales_test = test_df['average_order_value'].mean()
-- MAGIC mean_sales_control = control_df['average_order_value'].mean()
-- MAGIC
-- MAGIC mean_orders_test = test_df['total_orders'].mean()
-- MAGIC mean_orders_control = control_df['total_orders'].mean()
-- MAGIC
-- MAGIC print(mean_sales_test, mean_sales_control, mean_orders_test, mean_orders_control)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Write CSVs to DBFS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- saving mobile data of control
-- MAGIC
-- MAGIC df = spark.sql("select distinct mobile from sandbox.rb_VIP40AED_customers where sample='control' ")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/VIP40AED/' + 'control_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_final.shape

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- saving mobile data of test
-- MAGIC df = spark.sql("select distinct mobile from sandbox.rb_VIP40AED_customers where sample='test' ")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC # df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/VIP40AED/' + 'test_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_final.shape

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- saving card key and mobile data of test
-- MAGIC
-- MAGIC df = spark.sql("select distinct mobile, card_key from sandbox.rb_VIP40AED_customers where sample='test' ")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/VIP40AED/' + 'test_card_key.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ad-Hoc

-- COMMAND ----------

select max(month_year) from analytics.customer_segments
where  key = 'tms'  AND channel = 'pos'
            AND country = 'uae'
            -- AND month_year = '202312'
            -- AND segment = 'VIP'

-- COMMAND ----------

select campaign_id , count(*)
from analytics.campaign
where campaign_id IN (80, 81, 82, 83, 84, 85, 86) 
and tms_segment is not null
group by 1
sort by 1

-- COMMAND ----------

select campaign_id , count(*)
from analytics.campaign
where campaign_id IN (80, 81, 82, 83, 84, 85, 86) 
-- and tms_segment is not null
group by 1
sort by 1

-- COMMAND ----------

select c.customer_id, c.tms_segment, cs.month_year, cs.segment
from analytics.campaign c
inner join (select segment, customer_id, month_year
from analytics.customer_segments
where month_year = '202310' 
and key = 'tms'
AND channel = 'pos'
AND country = 'uae') cs
on c.customer_id = cs.customer_id
where c.campaign_id in (81)
and cs.segment is not null
limit 10



-- COMMAND ----------

select * from analytics.campaign_details sort by campaign_id desc
