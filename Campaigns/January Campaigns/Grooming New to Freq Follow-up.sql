-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### O5KR1C4- (15% back as points for offer converts while 10% for non offer converts	65 AED for offer converts while 50 AED for offer non-converts) [Dept. Super Market]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Desc: Here we will be creating 2 distinct journey flows where we will have 
-- MAGIC 1. offer converts Newbie who will be offered a new disc of 15% upto 65AED and this will be divided into test and control groups.
-- MAGIC 2. non offer converts Newbie who will be offered the same disc of 10% upto 50 AED which will be again divided into test and control groups.
-- MAGIC

-- COMMAND ----------

-- -- Current Newbies
-- Select count(distinct cdid) from(
-- select distinct pt.customer_id cdid, pt.customer_card_key card_key, pt.mobile mobile, pt.business_day, cs.segment, cs.average_order_value atv
-- from gold.pos_transactions pt
-- left join analytics.customer_segments cs on pt.customer_id = cs.customer_id
-- left join gold.customer_profile cp on pt.customer_id = cp.account_key
-- where  cs.key = 'rfm' 
-- and cs.channel = 'pos' 
-- and cs.country = 'uae' 
-- and cs.month_year = '202312'
-- and cp.LHRDATE is not null
-- and cs.first_purchase_date >= '2023-12-16'
-- and cs.last_purchase_date <= '2023-12-31'
-- and cs.segment = 'Newbie'
-- and cp.card_key is not null
-- )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Reading data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = pd.read_csv("/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/Newbie_to_freq/test_mobile.csv")
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("sandbox.pj_newbie_to_freq_test_mobile")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Converts

-- COMMAND ----------

-- 1304 converted customers at 3:26 PM
DROP TABLE IF EXISTS sandbox.pj_newbie_to_freq_followup_converts;

CREATE TABLE sandbox.pj_newbie_to_freq_followup_converts AS (
  SELECT DISTINCT t1.mobile, customer_card_key,
          ROUND(ROUND(SUM(amount),0) / COUNT(DISTINCT transaction_id),2) AS average_order_value,
          ROUND(SUM(amount),0) AS amount_spent
  FROM sandbox.pj_newbie_to_freq_test_mobile AS t1
  JOIN gold.pos_transactions AS t2
  ON t1.mobile = t2.mobile
  JOIN gold.material_master AS t4
  ON t4.material_id = t2.product_id
  WHERE business_day BETWEEN "2024-01-13" AND "2024-01-19"
  AND department_id BETWEEN 1 and 13
  GROUP BY t1.mobile, customer_card_key
  HAVING amount_spent >= 50
)

-- COMMAND ----------

SELECT COUNT(*) FROM sandbox.pj_newbie_to_freq_followup_converts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Non-Converts

-- COMMAND ----------

--  non-converts
DROP TABLE IF EXISTS sandbox.pj_newbie_to_freq_followup_nonconverts;

CREATE TABLE sandbox.pj_newbie_to_freq_followup_nonconverts AS (
  SELECT DISTINCT t1.mobile, customer_card_key
  FROM sandbox.pj_newbie_to_freq_test_mobile AS t1
  JOIN gold.pos_transactions AS t2
  ON t1.mobile = t2.mobile
  WHERE t1.mobile NOT IN (SELECT DISTINCT mobile
                          FROM sandbox.pj_newbie_to_freq_followup_converts)
)

-- COMMAND ----------

SELECT COUNT(DISTINCT mobile) FROM sandbox.pj_newbie_to_freq_followup_nonconverts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test-Control Split

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_newbie_to_freq_followup_converts_split AS 
SELECT * ,
      CASE WHEN split_num <= 19 THEN 'test'
      ELSE 'control'
      END AS sample
FROM
      (SELECT *, ntile(20) OVER (ORDER BY rand()) AS split_num
      FROM
            (
            SELECT DISTINCT mobile, customer_card_key, average_order_value
            FROM sandbox.pj_newbie_to_freq_followup_converts
            WHERE customer_card_key IS NOT NULL
            AND mobile IS NOT NULL
            )
      )

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_newbie_to_freq_followup_nonconverts_split AS 
SELECT * ,
      CASE WHEN split_num <= 19 THEN 'test'
      ELSE 'control'
      END AS sample
FROM
      (SELECT *, ntile(20) OVER (ORDER BY rand()) AS split_num
      FROM
            (
            SELECT * FROM sandbox.pj_newbie_to_freq_followup_nonconverts
            WHERE customer_card_key IS NOT NULL
            AND mobile IS NOT NULL
            )
      )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test-Control Similarity Check

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT *
-- MAGIC FROM sandbox.pj_newbie_to_freq_followup_converts_split
-- MAGIC """
-- MAGIC converts_df = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC # Filter the DataFrame where 'sample' is equal to 'test'
-- MAGIC converts_test_df = converts_df.loc[converts_df['sample'] == 'test']
-- MAGIC converts_control_df = converts_df.loc[converts_df['sample'] == 'control']
-- MAGIC
-- MAGIC # Checking for uniformity in test and control sets
-- MAGIC mean_atv_test = converts_test_df['average_order_value'].mean()
-- MAGIC mean_atv_control = converts_control_df['average_order_value'].mean()
-- MAGIC
-- MAGIC print(mean_atv_test, mean_atv_control)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Write CSVs to DBFS

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("select distinct mobile from sandbox.pj_newbie_to_freq_followup_converts_split where sample='control'")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/Newbie_to_freq_followup/' + 'converts_control_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("select distinct mobile from sandbox.pj_newbie_to_freq_followup_converts_split where sample='test'")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/Newbie_to_freq_followup/' + 'converts_test_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("select distinct mobile, customer_card_key from sandbox.pj_newbie_to_freq_followup_converts_split where sample='test'")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/Newbie_to_freq_followup/' + 'converts_test_card_key.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("select distinct mobile from sandbox.pj_newbie_to_freq_followup_nonconverts_split where sample='control'")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/Newbie_to_freq_followup/' + 'nonconverts_control_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("select distinct mobile from sandbox.pj_newbie_to_freq_followup_nonconverts_split where sample='test'")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/Newbie_to_freq_followup/' + 'nonconverts_test_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("select distinct mobile, customer_card_key from sandbox.pj_newbie_to_freq_followup_nonconverts_split where sample='test'")
-- MAGIC df_final=df.pandas_api()
-- MAGIC
-- MAGIC df_final.to_pandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/Newbie_to_freq_followup/' + 'nonconverts_test_card_key.csv' , sep = '|', index = False)

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


