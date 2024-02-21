-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##ATV Stretch (16/02/24)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - max trans value is 50 (e.g. for frequentist find mean and median ATV).
-- MAGIC - offer of 10,15,20%.
-- MAGIC - stretch these to threshold of 20%
-- MAGIC - (MPP member pricing promotion ?? Ask Piyush abt this)
-- MAGIC
-- MAGIC Desc. - We are performing a tier based ATV stretch (A|B testing is required for figuring out what's the least offer for similar results)
-- MAGIC 1. Find 2 tiers of ATV ranges as per analysis, and then send seperate campaigns for 15%-20% or 10%-15% campaigns.
-- MAGIC 2. Threshold will be different for both tier, stretch of 20 and/or 25%.
-- MAGIC 3. folks who are non-converts and the ones who rolled back to pre campaign ATV, send the same campaign again. (This is for follow-up)
-- MAGIC 4. folks who have gone up, even by 50% of offer threshold when we consider their ATV post offer, we need to run a campaign which makes them stick to this habbit. (This is for follow-up)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Finding statistical metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##ATV 50-60 Stretched upto 70

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer base atv_stretch_50_60
-- MAGIC - Customers spending a min of atleast 70 in lifetime.

-- COMMAND ----------


select count(cust) from(
select cust,
       sum(abv70_flag) as 70_flag,
       SUM(amt) / COUNT(trans) atv 
from(
select cust,
      trans, 
      amt,
      case when amt>70 then 1 else 0 end as abv70_flag
from(
select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
FROM gold.pos_transactions pt
    JOIN gold.material_master mm ON pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
  WHERE mm.department_id BETWEEN 1 AND 13
        AND pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
      --   AND pt.business_day <= '2024-01-31'
        AND pt.amount > 0
        AND cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = 202401
        AND cs.segment = 'Frequentist'
        AND pt.LHPRDate IS NOT NULL
  group by 1,2
)
)
group by 1
having sum(abv70_flag) > 0 and SUM(amt) / COUNT(trans) BETWEEN 50 and 60)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Customers who have spent min 70 at least, and are in the atv range of 50-60.
-- MAGIC
-- MAGIC query = """
-- MAGIC select cust from(
-- MAGIC select cust,
-- MAGIC        sum(abv70_flag) as 70_flag,
-- MAGIC        SUM(amt) / COUNT(trans) atv 
-- MAGIC from(
-- MAGIC select cust,
-- MAGIC       trans, 
-- MAGIC       amt,
-- MAGIC       case when amt>70 then 1 else 0 end as abv70_flag
-- MAGIC from(
-- MAGIC select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
-- MAGIC FROM gold.pos_transactions pt
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC   WHERE mm.department_id BETWEEN 1 AND 13
-- MAGIC         AND pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
-- MAGIC         AND pt.amount > 0
-- MAGIC         AND cs.key = 'rfm'
-- MAGIC         AND cs.channel = 'pos'
-- MAGIC         AND cs.country = 'uae'
-- MAGIC         AND cs.month_year = 202401
-- MAGIC         AND cs.segment = 'Frequentist'
-- MAGIC         AND pt.LHPRDate IS NOT NULL
-- MAGIC   group by 1,2
-- MAGIC )
-- MAGIC )
-- MAGIC group by 1
-- MAGIC having sum(abv70_flag) > 0 and SUM(amt) / COUNT(trans) BETWEEN 50 and 60)
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_atv5060')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Generic Conversion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC NOTE: Initially we had taken at most 1ce, hence the generic conversion stands at 154/1766 = 8.72%
-- MAGIC Whereas it should have been 20.57% as we can see below.

-- COMMAND ----------

-- num = wow footfall above 70 for 50-60
-- denom = yearly (2023) footfall above 70 for 50-60
-- denom
select count(*) from(
select cust,
       sum(abv70_flag) as 70_flag,
       SUM(amt) / COUNT(trans) atv 
from(
select cust,
      trans, 
      amt,
      case when amt>70 then 1 else 0 end as abv70_flag
from(
select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
FROM gold.pos_transactions pt
    JOIN gold.material_master mm ON pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
  WHERE mm.department_id BETWEEN 1 AND 13
        AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
        AND pt.amount > 0
        AND cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = 202312
        AND cs.segment = 'Frequentist'
        AND pt.LHPRDate IS NOT NULL
  group by 1,2
)
)
group by 1
having sum(abv70_flag) > 0 and SUM(amt) / COUNT(trans) BETWEEN 50 and 60)


-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC select cust from(
-- MAGIC select cust,
-- MAGIC        sum(abv70_flag) as 70_flag,
-- MAGIC        SUM(amt) / COUNT(trans) atv 
-- MAGIC from(
-- MAGIC select cust,
-- MAGIC       trans, 
-- MAGIC       amt,
-- MAGIC       case when amt>70 then 1 else 0 end as abv70_flag
-- MAGIC from(
-- MAGIC select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
-- MAGIC FROM gold.pos_transactions pt
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC   WHERE mm.department_id BETWEEN 1 AND 13
-- MAGIC         AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
-- MAGIC         AND pt.amount > 0
-- MAGIC         AND cs.key = 'rfm'
-- MAGIC         AND cs.channel = 'pos'
-- MAGIC         AND cs.country = 'uae'
-- MAGIC         AND cs.month_year = 202312
-- MAGIC         AND cs.segment = 'Frequentist'
-- MAGIC         AND pt.LHPRDate IS NOT NULL
-- MAGIC   group by 1,2
-- MAGIC )
-- MAGIC )
-- MAGIC group by 1
-- MAGIC having sum(abv70_flag) > 0 and SUM(amt) / COUNT(trans) BETWEEN 50 and 60)
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_atv5060_23')

-- COMMAND ----------

-- num = wow footfall above 70 for 50-60 out of the defined segment.

with WeeklyMetrics AS (
select WEEKOFYEAR(bday) AS week_number,
       COUNT(distinct cust) AS weekly_footfall
from(
select cs.customer_id cust, pt.transaction_id trans, pt.business_day bday, sum(pt.amount) amt
FROM gold.pos_transactions pt
    JOIN gold.material_master mm ON pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    join cust_base_atv5060_23 cb on cb.cust = pt.customer_id
  WHERE mm.department_id BETWEEN 1 AND 13
        AND pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
        AND pt.amount > 0
        AND cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = 202401
        AND cs.segment = 'Frequentist'
        AND pt.LHPRDate IS NOT NULL
  group by 1,2,3
  having sum(pt.amount)>70
)
group by 1
)
SELECT round(mean(weekly_footfall), 0) AS weekly_avg_Jan
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LFL Campaigns
-- MAGIC (76,81,82,83)

-- COMMAND ----------

--read offer converts/ total reads of prev campaign

-- campaign metrics RB edition

-- CTE1
WITH final_merged_campaign_customer_data AS (
    SELECT
        t1.campaign_id,
        t1.customer_id,
        t1.campaign_set,
        COALESCE(t2.delivery_status, 'NA') AS delivery_status,
        COALESCE(t3.num_trans, 0) AS num_trans,
        COALESCE(t3.num_offer_trans, 0) AS num_offer_trans,
        COALESCE(t3.total_spend, 0) AS total_spend,
        COALESCE(t3.total_offer_spend, 0) AS total_offer_spend
    FROM
        analytics.campaign_audience t1
        LEFT JOIN analytics.campaign_delivery t2 ON t1.campaign_id = t2.campaign_id AND t1.customer_key = t2.customer_key
        LEFT JOIN analytics.campaign_sales t3 ON t1.campaign_id = t3.campaign_id AND t1.customer_id = t3.customer_id
),

-- CTE2
final_campaign_data AS (
    SELECT
        campaign_id,
        campaign_set,
        COUNT(customer_id) AS total_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN 1 ELSE 0 END) AS read_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'not_opened' THEN 1 ELSE 0 END) AS notread_customers,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' AND num_trans > 0 THEN 1
                 WHEN campaign_set = 'control' AND num_trans > 0 THEN 1
                 ELSE 0 END) AS read_returning_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'not_opened' AND num_trans > 0 THEN 1
                 WHEN campaign_set = 'control' AND num_trans > 0 THEN 1
                 ELSE 0 END) AS notread_returning_customers,

        SUM(CASE WHEN  delivery_status = 'opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS read_offer_customers,
        SUM(CASE WHEN  delivery_status = 'not_opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS notread_offer_customers,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN total_spend
                 WHEN campaign_set = 'control' THEN total_spend ELSE 0 END) AS total_spend,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN total_offer_spend
                 WHEN campaign_set = 'control' THEN 0 ELSE 0 END) AS total_offer_spend,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN num_trans
                 WHEN campaign_set = 'control' THEN num_trans ELSE 0 END) AS total_transactions,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN num_offer_trans
                 WHEN campaign_set = 'control' THEN 0 ELSE 0 END) AS total_offer_transactions
    FROM
        final_merged_campaign_customer_data
    GROUP BY
        campaign_id, campaign_set
)

-- Extraction Table
SELECT
    t1.campaign_id,
    t2.campaign_desc,
    t2.offer_threshold,
    t1.campaign_set,
    t1.total_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.read_customers END AS read_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.notread_customers END AS notread_customers,
    t1.read_returning_customers,
    t1.notread_returning_customers,
    t1.read_offer_customers AS read_offer_customers,
    t1.notread_offer_customers AS notread_offer_customers

FROM
    final_campaign_data t1
    JOIN analytics.campaign_details t2 ON t1.campaign_id = t2.campaign_id
WHERE
    t1.campaign_id IN (76, 81, 82, 83)
ORDER BY
    t1.campaign_id, t1.campaign_set DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Storing the Customer_id for campaigning.
-- MAGIC Use this snippet for finding mobile | card-key

-- COMMAND ----------

select * from cust_base_atv5060

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Weekly ATV of the selected cohort

-- COMMAND ----------

--weekly ATV
with WeeklyMetrics AS (
  SELECT
    WEEKOFYEAR(pt.business_day) AS week_number,
    ROUND(ROUND(SUM(pt.amount), 0) / COUNT(DISTINCT pt.transaction_id), 2) AS weekly_atv
  FROM 
    analytics.customer_segments cs
    JOIN gold.pos_transactions pt ON cs.customer_id = pt.customer_id
    JOIN gold.material_master mm on pt.product_id = mm.material_id
    JOIN cust_base_atv5060 cb on cb.cust = pt.customer_id
    WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202401'
    AND cs.segment = 'Frequentist'
    AND pt.amount > 0
    AND pt.LHPRDate IS NOT NULL
    AND mm.department_id BETWEEN 1 and 13
  GROUP BY week_number
)
SELECT round(AVG(weekly_atv),0) AS avg_weekly_atv
FROM WeeklyMetrics;

-- COMMAND ----------

--ATV
SELECT atv
FROM(
  SELECT
    ROUND(ROUND(SUM(pt.amount), 0) / COUNT(DISTINCT pt.transaction_id), 2) AS atv
  FROM 
    gold.pos_transactions pt 
    JOIN gold.material_master mm on pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs on cs.customer_id = pt.customer_id
    JOIN cust_base_atv5060 cb on cb.cust = pt.customer_id
    WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202401
    AND cs.segment = 'Frequentist'
    AND pt.amount > 0
    AND pt.LHPRDate IS NOT NULL
    AND mm.department_id BETWEEN 1 and 13
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ATV 60-70 Stretched upto 80

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer base atv_stretch_60_70
-- MAGIC - Customers below 84 in lifetime as well as the once who have shopped just once above 80

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Customers have shopped above 80 at least 1ce, and are in the atv range of 60-70.
-- MAGIC
-- MAGIC query = """
-- MAGIC select cust from(
-- MAGIC select cust,
-- MAGIC        sum(abv80_flag) as 80_flag,
-- MAGIC        SUM(amt) / COUNT(trans) atv 
-- MAGIC from(
-- MAGIC select cust,
-- MAGIC       trans, 
-- MAGIC       amt,
-- MAGIC       case when amt>80 then 1 else 0 end as abv80_flag
-- MAGIC from(
-- MAGIC select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
-- MAGIC FROM gold.pos_transactions pt
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC   WHERE mm.department_id BETWEEN 1 AND 13
-- MAGIC         AND pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
-- MAGIC         AND pt.amount > 0
-- MAGIC         AND cs.key = 'rfm'
-- MAGIC         AND cs.channel = 'pos'
-- MAGIC         AND cs.country = 'uae'
-- MAGIC         AND cs.month_year = 202401
-- MAGIC         AND cs.segment = 'Frequentist'
-- MAGIC         AND pt.LHPRDate IS NOT NULL
-- MAGIC   group by 1,2
-- MAGIC )
-- MAGIC )
-- MAGIC group by 1
-- MAGIC having sum(abv80_flag) > 0 and SUM(amt) / COUNT(trans) BETWEEN 60 and 70)
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_atv6070')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Generic Conversion

-- COMMAND ----------

-- Denom
select count(*) from(
select cust,
       sum(abv84_flag) as 84_flag,
       SUM(amt) / COUNT(trans) atv 
from(
select cust,
      trans, 
      amt,
      case when amt>84 then 1 else 0 end as abv84_flag
from(
select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
FROM gold.pos_transactions pt
    JOIN gold.material_master mm ON pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
  WHERE mm.department_id BETWEEN 1 AND 13
        AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
        AND pt.amount > 0
        AND cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = 202312
        AND cs.segment = 'Frequentist'
        AND pt.LHPRDate IS NOT NULL
  group by 1,2
)
)
group by 1
having sum(abv84_flag) in (0,1) and SUM(amt) / COUNT(trans) BETWEEN 60 and 70)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC select cust from(
-- MAGIC select cust,
-- MAGIC        sum(abv84_flag) as 84_flag,
-- MAGIC        SUM(amt) / COUNT(trans) atv 
-- MAGIC from(
-- MAGIC select cust,
-- MAGIC       trans, 
-- MAGIC       amt,
-- MAGIC       case when amt>84 then 1 else 0 end as abv84_flag
-- MAGIC from(
-- MAGIC select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
-- MAGIC FROM gold.pos_transactions pt
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC   WHERE mm.department_id BETWEEN 1 AND 13
-- MAGIC         AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
-- MAGIC         AND pt.amount > 0
-- MAGIC         AND cs.key = 'rfm'
-- MAGIC         AND cs.channel = 'pos'
-- MAGIC         AND cs.country = 'uae'
-- MAGIC         AND cs.month_year = 202312
-- MAGIC         AND cs.segment = 'Frequentist'
-- MAGIC         AND pt.LHPRDate IS NOT NULL
-- MAGIC   group by 1,2
-- MAGIC )
-- MAGIC )
-- MAGIC group by 1
-- MAGIC having sum(abv84_flag) in (0,1) and SUM(amt) / COUNT(trans) BETWEEN 60 and 70)
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_atv6070_23')

-- COMMAND ----------

-- num 

with WeeklyMetrics AS (
select WEEKOFYEAR(bday) AS week_number,
       COUNT(distinct cust) AS weekly_footfall
from(
select cs.customer_id cust, pt.transaction_id trans, pt.business_day bday, sum(pt.amount) amt
FROM gold.pos_transactions pt
    JOIN gold.material_master mm ON pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    join cust_base_atv6070_23 cb on cb.cust = pt.customer_id
  WHERE mm.department_id BETWEEN 1 AND 13
        AND pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
        AND pt.amount > 0
        AND cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = 202401
        AND cs.segment = 'Frequentist'
        AND pt.LHPRDate IS NOT NULL
  group by 1,2,3
  having sum(pt.amount)>84
)
group by 1
)
SELECT round(mean(weekly_footfall), 0) AS weekly_avg_Jan
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Storing the Customer_id for campaigning.
-- MAGIC Use this snippet for finding mobile | card-key

-- COMMAND ----------

select * from cust_base_atv6070

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Weekly ATV for the selected cohort 

-- COMMAND ----------

--weekly ATV
with WeeklyMetrics AS (
  SELECT
    WEEKOFYEAR(pt.business_day) AS week_number,
    ROUND(ROUND(SUM(pt.amount), 0) / COUNT(DISTINCT pt.transaction_id), 2) AS weekly_atv
  FROM 
    analytics.customer_segments cs
    JOIN gold.pos_transactions pt ON cs.customer_id = pt.customer_id
    JOIN gold.material_master mm on pt.product_id = mm.material_id
    JOIN cust_base_atv6070 cb on cb.cust = pt.customer_id
    WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202401'
    AND cs.segment = 'Frequentist'
    AND pt.amount > 0
    AND pt.LHPRDate IS NOT NULL
    AND mm.department_id BETWEEN 1 and 13
  GROUP BY week_number
)
SELECT round(AVG(weekly_atv),0) AS avg_weekly_atv
FROM WeeklyMetrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ATV 70-80 Stretcheed upto 90

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customer base

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Customers who have shopped at least 1ce above 90, and are in the atv range of 70-80.
-- MAGIC
-- MAGIC query = """
-- MAGIC select cust from(
-- MAGIC select cust,
-- MAGIC        sum(abv90_flag) as 90_flag,
-- MAGIC        SUM(amt) / COUNT(trans) atv 
-- MAGIC from(
-- MAGIC select cust,
-- MAGIC       trans, 
-- MAGIC       amt,
-- MAGIC       case when amt>90 then 1 else 0 end as abv90_flag
-- MAGIC from(
-- MAGIC select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
-- MAGIC FROM gold.pos_transactions pt
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC   WHERE mm.department_id BETWEEN 1 AND 13
-- MAGIC         AND pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
-- MAGIC         AND pt.amount > 0
-- MAGIC         AND cs.key = 'rfm'
-- MAGIC         AND cs.channel = 'pos'
-- MAGIC         AND cs.country = 'uae'
-- MAGIC         AND cs.month_year = 202401
-- MAGIC         AND cs.segment = 'Frequentist'
-- MAGIC         AND pt.LHPRDate IS NOT NULL
-- MAGIC   group by 1,2
-- MAGIC )
-- MAGIC )
-- MAGIC group by 1
-- MAGIC having sum(abv90_flag) > 0 and SUM(amt) / COUNT(trans) BETWEEN 70 and 80)
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_atv7080')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Generic Conversion

-- COMMAND ----------

-- Denom
select count(*) from(
select cust,
       sum(abv90_flag) as 90_flag,
       SUM(amt) / COUNT(trans) atv 
from(
select cust,
      trans, 
      amt,
      case when amt>90 then 1 else 0 end as abv90_flag
from(
select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
FROM gold.pos_transactions pt
    JOIN gold.material_master mm ON pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
  WHERE mm.department_id BETWEEN 1 AND 13
        AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
        AND pt.amount > 0
        AND cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = 202312
        AND cs.segment = 'Frequentist'
        AND pt.LHPRDate IS NOT NULL
  group by 1,2
)
)
group by 1
having sum(abv90_flag) in (0,1) and SUM(amt) / COUNT(trans) BETWEEN 70 and 80)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC
-- MAGIC select cust from(
-- MAGIC select cust,
-- MAGIC        sum(abv90_flag) as 90_flag,
-- MAGIC        SUM(amt) / COUNT(trans) atv 
-- MAGIC from(
-- MAGIC select cust,
-- MAGIC       trans, 
-- MAGIC       amt,
-- MAGIC       case when amt>90 then 1 else 0 end as abv90_flag
-- MAGIC from(
-- MAGIC select cs.customer_id cust, pt.transaction_id trans, sum(pt.amount) amt
-- MAGIC FROM gold.pos_transactions pt
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC   WHERE mm.department_id BETWEEN 1 AND 13
-- MAGIC         AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
-- MAGIC         AND pt.amount > 0
-- MAGIC         AND cs.key = 'rfm'
-- MAGIC         AND cs.channel = 'pos'
-- MAGIC         AND cs.country = 'uae'
-- MAGIC         AND cs.month_year = 202312
-- MAGIC         AND cs.segment = 'Frequentist'
-- MAGIC         AND pt.LHPRDate IS NOT NULL
-- MAGIC   group by 1,2
-- MAGIC )
-- MAGIC )
-- MAGIC group by 1
-- MAGIC having sum(abv90_flag) in (0,1) and SUM(amt) / COUNT(trans) BETWEEN 70 and 80)
-- MAGIC """
-- MAGIC df= spark.sql(query).toPandas()
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_atv7080_23')

-- COMMAND ----------

-- num 

with WeeklyMetrics AS (
select WEEKOFYEAR(bday) AS week_number,
       COUNT(distinct cust) AS weekly_footfall
from(
select cs.customer_id cust, pt.transaction_id trans, pt.business_day bday, sum(pt.amount) amt
FROM gold.pos_transactions pt
    JOIN gold.material_master mm ON pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    join cust_base_atv7080_23 cb on cb.cust = pt.customer_id
  WHERE mm.department_id BETWEEN 1 AND 13
        AND pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
        AND pt.amount > 0
        AND cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = 202401
        AND cs.segment = 'Frequentist'
        AND pt.LHPRDate IS NOT NULL
  group by 1,2,3
  having sum(pt.amount)>90
)
group by 1
)
SELECT round(mean(weekly_footfall), 0) AS weekly_avg_Jan
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Weekly ATV

-- COMMAND ----------

--ATV
SELECT atv
FROM(
  SELECT
    ROUND(ROUND(SUM(pt.amount), 0) / COUNT(DISTINCT pt.transaction_id), 2) AS atv
  FROM 
    gold.pos_transactions pt 
    JOIN gold.material_master mm on pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs on cs.customer_id = pt.customer_id
    JOIN cust_base_atv7080 cb on cb.cust = pt.customer_id
    WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202401
    AND cs.segment = 'Frequentist'
    AND pt.amount > 0
    AND pt.LHPRDate IS NOT NULL
    AND mm.department_id BETWEEN 1 and 13
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # OT to Repeaters (16/02/24)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - find the typical/ generic conversion rate for OT to Repeaters, this is the mean of number of days which OT took to visit the store for the second time.
-- MAGIC - E.g., if 3 weeks is the avg, then in the 4th week we can send a communication based campaign to recall them.
-- MAGIC
-- MAGIC Desc. -
-- MAGIC 1. Find out irrspective of segments (excluding splurgers), what is the avg conversion days for customers.
-- MAGIC 2. Do the same exercise for OT as well. OT = single transaction only.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Overall 2nd Visit days

-- COMMAND ----------

SELECT 
round(avg(avg_gap_days),2) overall_2nd_visit
FROM(
  SELECT
    t2.segment,
    t2.customer_id,
    round(AVG(gap_days),0) AS avg_gap_days
  FROM (
    SELECT
      t1.segment,
      t1.customer_id,
      dense_rank() OVER (PARTITION BY t1.customer_id ORDER BY t1.business_day) AS rank,
      DATEDIFF(
        day,
        LAG(t1.business_day, 1) OVER (PARTITION BY t1.customer_id ORDER BY t1.business_day),
        t1.business_day
      ) AS gap_days
    FROM(
      SELECT DISTINCT
        cs.segment,
        pt.customer_id,
        pt.business_day
      FROM analytics.customer_segments cs
      JOIN gold.pos_transactions pt ON cs.customer_id = pt.customer_id
      JOIN gold.material_master mm ON pt.product_id = mm.material_id
      WHERE pt.business_day BETWEEN '2023-06-06' AND '2024-01-06'
        AND cs.key = 'rfm'
        AND cs.country = 'uae'
        AND cs.channel = 'pos'
        AND cs.month_year = '202401'
        AND mm.department_id BETWEEN 1 AND 13
    ) t1
  ) t2
  WHERE rank <= 2
  GROUP BY t2.segment, t2.customer_id
) AS t3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Segment_wise 2nd Visit Days

-- COMMAND ----------

SELECT t3.segment, 
round(avg(avg_gap_days),2) segment_wise_2nd_visit
FROM(
  SELECT
    t2.segment,
    t2.customer_id,
    round(AVG(gap_days),0) AS avg_gap_days
  FROM (
    SELECT
      t1.segment,
      t1.customer_id,
      dense_rank() OVER (PARTITION BY t1.customer_id ORDER BY t1.business_day) AS rank,
      DATEDIFF(
        day,
        LAG(t1.business_day, 1) OVER (PARTITION BY t1.customer_id ORDER BY t1.business_day),
        t1.business_day
      ) AS gap_days
    FROM(
      SELECT DISTINCT
        cs.segment,
        pt.customer_id,
        pt.business_day
      FROM analytics.customer_segments cs
      JOIN gold.pos_transactions pt ON cs.customer_id = pt.customer_id
      JOIN gold.material_master mm ON pt.product_id = mm.material_id
      WHERE pt.business_day BETWEEN '2023-01-08' AND '2024-02-08'
        AND cs.key = 'rfm'
        AND cs.country = 'uae'
        AND cs.channel = 'pos'
        AND cs.month_year = '202401'
        AND mm.department_id BETWEEN 1 AND 13
    ) t1
  ) t2
  WHERE rank <= 2
  GROUP BY t2.segment, t2.customer_id
) AS t3
GROUP BY t3.segment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Count of customers per segment who shopped 1ce in the 6 months period

-- COMMAND ----------

select cs.segment, count(distinct t1.customer_id)
from(
      select pt.customer_id,
          count(distinct pt.transaction_id) total_orders
      from gold.pos_transactions pt
      JOIN gold.material_master mm on pt.product_id = mm.material_id
      where 
      pt.business_day BETWEEN '2023-08-01' AND '2024-01-31'
      and pt.LHPRDate is not null
      and pt.amount>0
      AND mm.department_id BETWEEN 1 AND 13
      group by 1
    ) t1
join analytics.customer_segments cs on t1.customer_id = cs.customer_id
where t1.total_orders = 1
  and cs.key = 'rfm'
  AND cs.country = 'uae'
  AND cs.channel = 'pos'
  AND cs.month_year = '202401'
group by 1
  



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Moderates - Recency distribution

-- COMMAND ----------

select count(distinct t1.customer_id), 
percentile(cs.recency, 0.25), 
percentile(cs.recency, 0.5), 
percentile(cs.recency, 0.75)
from(
      select pt.customer_id,
          count(distinct pt.transaction_id) total_orders
      from gold.pos_transactions pt
      JOIN gold.material_master mm on pt.product_id = mm.material_id
      where 
      pt.business_day BETWEEN '2023-08-01' AND '2024-01-31'
      and pt.LHPRDate is not null
      and pt.amount>0
      AND mm.department_id BETWEEN 1 AND 13
      group by 1
    ) t1
join analytics.customer_segments cs on t1.customer_id = cs.customer_id
where t1.total_orders = 1
  and cs.key = 'rfm'
  AND cs.country = 'uae'
  AND cs.channel = 'pos'
  AND cs.month_year = '202401'
  and cs.segment = 'Moderate'
  


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Count of moderates below p25

-- COMMAND ----------

select count(distinct t1.customer_id)
from(
      select pt.customer_id,
          count(distinct pt.transaction_id) total_orders
      from gold.pos_transactions pt
      JOIN gold.material_master mm on pt.product_id = mm.material_id
      where 
      pt.business_day BETWEEN '2023-08-01' AND '2024-01-31'
      and pt.LHPRDate is not null
      and pt.amount>0
      AND mm.department_id BETWEEN 1 AND 13
      group by 1
    ) t1
join analytics.customer_segments cs on t1.customer_id = cs.customer_id
where t1.total_orders = 1
  and cs.key = 'rfm'
  AND cs.country = 'uae'
  AND cs.channel = 'pos'
  AND cs.month_year = '202401'
  and cs.segment = 'Moderate'
  and cs.recency <=31
  


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Storing the cust_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """SELECT DISTINCT t1.customer_id
-- MAGIC FROM (
-- MAGIC     SELECT pt.customer_id,
-- MAGIC         count(DISTINCT pt.transaction_id) AS total_orders,
-- MAGIC         cs.recency
-- MAGIC     FROM gold.pos_transactions pt
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC     WHERE pt.business_day BETWEEN '2023-08-01' AND '2024-01-31'
-- MAGIC         AND pt.LHPRDate IS NOT NULL
-- MAGIC         AND pt.amount > 0
-- MAGIC         AND mm.department_id BETWEEN 1 AND 13
-- MAGIC         AND cs.key = 'rfm'
-- MAGIC         AND cs.country = 'uae'
-- MAGIC         AND cs.channel = 'pos'
-- MAGIC         AND cs.month_year = '202401'
-- MAGIC         AND cs.segment = 'Moderate'
-- MAGIC         AND cs.recency <= 31
-- MAGIC     GROUP BY pt.customer_id, cs.recency
-- MAGIC ) t1
-- MAGIC WHERE t1.total_orders = 1
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # Overall moderates who shopped 1ce in last 6 months
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base1')

-- COMMAND ----------

select count(distinct t1.customer_id)
from(
      select pt.customer_id,
          count(distinct pt.transaction_id) total_orders
      from gold.pos_transactions pt
      JOIN gold.material_master mm on pt.product_id = mm.material_id
      JOIN cust_base1 cb on cb.customer_id = pt.customer_id
      where 
      pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
      and pt.LHPRDate is not null
      and pt.amount>0
      AND mm.department_id BETWEEN 1 AND 13
      group by 1
    ) t1
join analytics.customer_segments cs on t1.customer_id = cs.customer_id
where t1.total_orders = 1
  and cs.key = 'rfm'
  AND cs.country = 'uae'
  AND cs.channel = 'pos'
  AND cs.month_year = '202401'
  and cs.segment = 'Moderate'
  and cs.recency <=31
  


-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC select distinct t1.customer_id
-- MAGIC from(
-- MAGIC       select pt.customer_id,
-- MAGIC           count(distinct pt.transaction_id) total_orders
-- MAGIC       from gold.pos_transactions pt
-- MAGIC       JOIN gold.material_master mm on pt.product_id = mm.material_id
-- MAGIC       JOIN cust_base1 cb on cb.customer_id = pt.customer_id
-- MAGIC       where 
-- MAGIC       pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
-- MAGIC       and pt.LHPRDate is not null
-- MAGIC       and pt.amount>0
-- MAGIC       AND mm.department_id BETWEEN 1 AND 13
-- MAGIC       group by 1
-- MAGIC     ) t1
-- MAGIC join analytics.customer_segments cs on t1.customer_id = cs.customer_id
-- MAGIC where t1.total_orders = 1
-- MAGIC   and cs.key = 'rfm'
-- MAGIC   AND cs.country = 'uae'
-- MAGIC   AND cs.channel = 'pos'
-- MAGIC   AND cs.month_year = '202401'
-- MAGIC   and cs.segment = 'Moderate'
-- MAGIC   and cs.recency <=31
-- MAGIC   """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # Overall moderates who shopped 1ce in Jan
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base2')

-- COMMAND ----------


-- have to remove these folks as they have shopped already.
select count(distinct pt.customer_id)
from cust_base2 cb
join gold.pos_transactions pt on cb.customer_id = pt.customer_id
where pt.business_day>='2024-02-01'


-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC select distinct pt.customer_id
-- MAGIC from cust_base2 cb
-- MAGIC join gold.pos_transactions pt on cb.customer_id = pt.customer_id
-- MAGIC where pt.business_day>='2024-02-01'
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # Out of the Ja customers those who shopped again in Feb
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base3')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Caustomer Base

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- removing the 6086 customers who have shopeed in Feb already
-- MAGIC
-- MAGIC query = """
-- MAGIC select distinct t1.customer_id
-- MAGIC from(
-- MAGIC       select pt.customer_id,
-- MAGIC           count(distinct pt.transaction_id) total_orders
-- MAGIC       from gold.pos_transactions pt
-- MAGIC       JOIN gold.material_master mm on pt.product_id = mm.material_id
-- MAGIC       JOIN cust_base1 cb on cb.customer_id = pt.customer_id
-- MAGIC       where 
-- MAGIC       pt.business_day BETWEEN '2024-01-01' AND '2024-01-31'
-- MAGIC       and pt.LHPRDate is not null
-- MAGIC       and pt.amount>0
-- MAGIC       AND mm.department_id BETWEEN 1 AND 13
-- MAGIC       group by 1
-- MAGIC     ) t1
-- MAGIC join analytics.customer_segments cs on t1.customer_id = cs.customer_id
-- MAGIC left anti join cust_base3 cb on t1.customer_id = cb.customer_id
-- MAGIC where t1.total_orders = 1
-- MAGIC   and cs.key = 'rfm'
-- MAGIC   AND cs.country = 'uae'
-- MAGIC   AND cs.channel = 'pos'
-- MAGIC   AND cs.month_year = '202401'
-- MAGIC   and cs.segment = 'Moderate'
-- MAGIC   and cs.recency <=31
-- MAGIC   """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # customers who have shopped once in Jan - customers out of these who shopped in Feb again
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base4')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Overall Moderates who are OT

-- COMMAND ----------

-- %python
-- query = """
select count(distinct t1.customer_id)
from(
      select pt.customer_id,
          count(distinct pt.transaction_id) total_orders
      from gold.pos_transactions pt
      JOIN gold.material_master mm on pt.product_id = mm.material_id
      where 
      pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
      and pt.LHPRDate is not null
      and pt.amount>0
      AND mm.department_id BETWEEN 1 AND 13
      group by 1
    ) t1
join analytics.customer_segments cs on t1.customer_id = cs.customer_id
where t1.total_orders = 1
  and cs.key = 'rfm'
  AND cs.country = 'uae'
  AND cs.channel = 'pos'
  AND cs.month_year = 202312
  and cs.segment = 'Moderate'
-- """
-- df = spark.sql(query).toPandas()
-- df.shape
  


-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC select distinct t1.customer_id
-- MAGIC from(
-- MAGIC       select pt.customer_id,
-- MAGIC           count(distinct pt.transaction_id) total_orders
-- MAGIC       from gold.pos_transactions pt
-- MAGIC       JOIN gold.material_master mm on pt.product_id = mm.material_id
-- MAGIC       where 
-- MAGIC       pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
-- MAGIC       and pt.LHPRDate is not null
-- MAGIC       and pt.amount>0
-- MAGIC       AND mm.department_id BETWEEN 1 AND 13
-- MAGIC       group by 1
-- MAGIC     ) t1
-- MAGIC join analytics.customer_segments cs on t1.customer_id = cs.customer_id
-- MAGIC where t1.total_orders = 1
-- MAGIC   and cs.key = 'rfm'
-- MAGIC   AND cs.country = 'uae'
-- MAGIC   AND cs.channel = 'pos'
-- MAGIC   AND cs.month_year = 202312
-- MAGIC   and cs.segment = 'Moderate'
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC   
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Overall last year's moderates
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base5')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Generic Conversion of OT Moderates

-- COMMAND ----------

WITH WeeklyMetrics AS (
  SELECT
    WEEKOFYEAR(pt.business_day) AS week_number,
    COUNT(DISTINCT pt.customer_id) AS weekly_footfall
  FROM cust_base5 cb
  JOIN gold.pos_transactions pt on cb.customer_id = pt.customer_id
  JOIN gold.material_master mm ON pt.product_id = mm.material_id
  JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
  WHERE
    pt.business_day BETWEEN '2024-01-01' AND '2024-01-31' 
    AND pt.LHPRDate IS NOT NULL
    AND pt.amount > 0
    AND mm.department_id BETWEEN 1 AND 13
    AND cs.key = 'rfm' 
    AND cs.country = 'uae' 
    AND cs.channel = 'pos' 
    AND cs.month_year = '202401' 
    AND cs.segment = 'Moderate'  
  GROUP BY WEEKOFYEAR(pt.business_day)
)
SELECT ROUND(AVG(weekly_footfall), 0) AS avg_weekly_footfall
FROM WeeklyMetrics;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ATV of the selected cohort

-- COMMAND ----------

--weekly ATV
SELECT atv
FROM(
  SELECT
    ROUND(ROUND(SUM(pt.amount), 0) / COUNT(DISTINCT pt.transaction_id), 2) AS atv
  FROM 
    gold.pos_transactions pt 
    JOIN gold.material_master mm on pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs on cs.customer_id = pt.customer_id
    JOIN cust_base4 cb on cb.customer_id = pt.customer_id
    WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202401
    AND cs.segment = 'Moderate'
    AND pt.amount > 0
    AND pt.LHPRDate IS NOT NULL
    AND mm.department_id BETWEEN 1 and 13
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LFL Campaign

-- COMMAND ----------

--read offer converts/ total reads of prev campaign

-- campaign metrics RB edition

-- CTE1
WITH final_merged_campaign_customer_data AS (
    SELECT
        t1.campaign_id,
        t1.customer_id,
        t1.campaign_set,
        COALESCE(t2.delivery_status, 'NA') AS delivery_status,
        COALESCE(t3.num_trans, 0) AS num_trans,
        COALESCE(t3.num_offer_trans, 0) AS num_offer_trans,
        COALESCE(t3.total_spend, 0) AS total_spend,
        COALESCE(t3.total_offer_spend, 0) AS total_offer_spend
    FROM
        analytics.campaign_audience t1
        LEFT JOIN analytics.campaign_delivery t2 ON t1.campaign_id = t2.campaign_id AND t1.customer_key = t2.customer_key
        LEFT JOIN analytics.campaign_sales t3 ON t1.campaign_id = t3.campaign_id AND t1.customer_id = t3.customer_id
),

-- CTE2
final_campaign_data AS (
    SELECT
        campaign_id,
        campaign_set,
        COUNT(customer_id) AS total_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN 1 ELSE 0 END) AS read_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'not_opened' THEN 1 ELSE 0 END) AS notread_customers,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' AND num_trans > 0 THEN 1
                 WHEN campaign_set = 'control' AND num_trans > 0 THEN 1
                 ELSE 0 END) AS read_returning_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'not_opened' AND num_trans > 0 THEN 1
                 WHEN campaign_set = 'control' AND num_trans > 0 THEN 1
                 ELSE 0 END) AS notread_returning_customers,

        SUM(CASE WHEN  delivery_status = 'opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS read_offer_customers,
        SUM(CASE WHEN  delivery_status = 'not_opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS notread_offer_customers,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN total_spend
                 WHEN campaign_set = 'control' THEN total_spend ELSE 0 END) AS total_spend,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN total_offer_spend
                 WHEN campaign_set = 'control' THEN 0 ELSE 0 END) AS total_offer_spend,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN num_trans
                 WHEN campaign_set = 'control' THEN num_trans ELSE 0 END) AS total_transactions,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN num_offer_trans
                 WHEN campaign_set = 'control' THEN 0 ELSE 0 END) AS total_offer_transactions
    FROM
        final_merged_campaign_customer_data
    GROUP BY
        campaign_id, campaign_set
)

-- Extraction Table
SELECT
    t1.campaign_id,
    t2.campaign_desc,
    t2.offer_threshold,
    t1.campaign_set,
    t1.total_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.read_customers END AS read_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.notread_customers END AS notread_customers,
    t1.read_returning_customers,
    t1.notread_returning_customers,
    t1.read_offer_customers AS read_offer_customers,
    t1.notread_offer_customers AS notread_offer_customers

FROM
    final_campaign_data t1
    JOIN analytics.campaign_details t2 ON t1.campaign_id = t2.campaign_id
WHERE
    t1.campaign_id IN (53)
ORDER BY
    t1.campaign_id, t1.campaign_set DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SL Revival
-- MAGIC - In the SL revival campaign we are trying to send a campaign to the folks who have very recently (Dec/ Jan) turned into a SL from VIP. We are not expecting an ATV Stretch but just a visit back to the store around the same ATV range. SL comes back every 25 days and currently we should find SL who have a recency over 25/ 1 month and campaign for them.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customer Base

-- COMMAND ----------

with VIP as (
select distinct cs.customer_id
from 
analytics.customer_segments cs
JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202312
    AND cs.segment = 'VIP'
    AND cp.LHRDate IS NOT NULL
)
select count(cs.customer_id)
from VIP
JOIN analytics.customer_segments cs on VIP.customer_id = cs.customer_id
JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202401
    AND cs.segment = 'Slipping Loyalist'

-- COMMAND ----------

with VIP as (
select distinct cs.customer_id
from 
analytics.customer_segments cs
JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202311
    AND cs.segment = 'VIP'
    AND cp.LHRDate IS NOT NULL
)
select count(cs.customer_id)
from VIP
JOIN analytics.customer_segments cs on VIP.customer_id = cs.customer_id
JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202312
    AND cs.segment = 'Slipping Loyalist'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #DEC
-- MAGIC query = """
-- MAGIC with VIP as (
-- MAGIC select distinct cs.customer_id
-- MAGIC from 
-- MAGIC analytics.customer_segments cs
-- MAGIC JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
-- MAGIC WHERE
-- MAGIC     cs.key = 'rfm'
-- MAGIC     AND cs.channel = 'pos'
-- MAGIC     AND cs.country = 'uae'
-- MAGIC     AND cs.month_year = 202312
-- MAGIC     AND cs.segment = 'VIP'
-- MAGIC     AND cp.LHRDate IS NOT NULL
-- MAGIC )
-- MAGIC select cs.customer_id
-- MAGIC from VIP
-- MAGIC JOIN analytics.customer_segments cs on VIP.customer_id = cs.customer_id
-- MAGIC JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
-- MAGIC WHERE
-- MAGIC     cs.key = 'rfm'
-- MAGIC     AND cs.channel = 'pos'
-- MAGIC     AND cs.country = 'uae'
-- MAGIC     AND cs.month_year = 202401
-- MAGIC     AND cs.segment = 'Slipping Loyalist'
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_SL12')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Nov
-- MAGIC query = """
-- MAGIC with VIP as (
-- MAGIC select distinct cs.customer_id
-- MAGIC from 
-- MAGIC analytics.customer_segments cs
-- MAGIC JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
-- MAGIC WHERE
-- MAGIC     cs.key = 'rfm'
-- MAGIC     AND cs.channel = 'pos'
-- MAGIC     AND cs.country = 'uae'
-- MAGIC     AND cs.month_year = 202311
-- MAGIC     AND cs.segment = 'VIP'
-- MAGIC     AND cp.LHRDate IS NOT NULL
-- MAGIC )
-- MAGIC select cs.customer_id
-- MAGIC from VIP
-- MAGIC JOIN analytics.customer_segments cs on VIP.customer_id = cs.customer_id
-- MAGIC JOIN gold.customer_profile cp on cs.customer_id = cp.account_key
-- MAGIC WHERE
-- MAGIC     cs.key = 'rfm'
-- MAGIC     AND cs.channel = 'pos'
-- MAGIC     AND cs.country = 'uae'
-- MAGIC     AND cs.month_year = 202312
-- MAGIC     AND cs.segment = 'Slipping Loyalist'
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_SL11')

-- COMMAND ----------

SELECT DISTINCT count(customer_id)
FROM cust_base_SL11
UNION
SELECT DISTINCT count(customer_id)
FROM cust_base_SL12

-- COMMAND ----------

SELECT count(*)
FROM cust_base_SL11 t1
JOIN cust_base_SL12 t2 ON t1.customer_id = t2.customer_id

-- COMMAND ----------

SELECT DISTINCT count(customer_id)
FROM cust_base_SL11
UNION ALL
SELECT DISTINCT count(customer_id)
FROM cust_base_SL12
WHERE customer_id NOT IN (SELECT customer_id FROM cust_base_SL11)

-- COMMAND ----------

SELECT COUNT(DISTINCT customer_id)
FROM (
    SELECT customer_id FROM cust_base_SL11
    UNION
    SELECT customer_id FROM cust_base_SL12
) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Storing the customer cohort

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC SELECT customer_id
-- MAGIC FROM cust_base_SL11
-- MAGIC UNION ALL
-- MAGIC SELECT customer_id
-- MAGIC FROM cust_base_SL12
-- MAGIC WHERE customer_id NOT IN (SELECT customer_id FROM cust_base_SL11)
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_SL')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ATV of the selected cohort

-- COMMAND ----------

--weekly ATV
SELECT atv
FROM(
  SELECT
    ROUND(ROUND(SUM(pt.amount), 0) / COUNT(DISTINCT pt.transaction_id), 2) AS atv
  FROM 
    gold.pos_transactions pt 
    JOIN gold.material_master mm on pt.product_id = mm.material_id
    JOIN analytics.customer_segments cs on cs.customer_id = pt.customer_id
    JOIN cust_base_SL cb on cb.customer_id = pt.customer_id
    WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = 202401
    AND cs.segment = 'Slipping Loyalist'
    AND pt.amount > 0
    AND pt.LHPRDate IS NOT NULL
    AND mm.department_id BETWEEN 1 and 13
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overall SL in 2023

-- COMMAND ----------

-- denom = in the month_year 202312, the SL shopping from dep 1-13
select count(distinct cust)
from(
      select cs.customer_id cust
      from analytics.customer_segments cs
      JOIN gold.pos_transactions pt on pt.customer_id = cs.customer_id
      JOIN gold.material_master mm on pt.product_id = mm.material_id
      where 
      cs.month_year = 202312
      and cs.key = 'rfm'
      AND cs.country = 'uae'
      AND cs.channel = 'pos'
      and cs.segment = 'Slipping Loyalist'
      and pt.LHPRDate is not null
      AND mm.department_id BETWEEN 1 AND 13
      group by 1
    )
  



-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC       select distinct cs.customer_id
-- MAGIC       from analytics.customer_segments cs
-- MAGIC       JOIN gold.pos_transactions pt on pt.customer_id = cs.customer_id
-- MAGIC       JOIN gold.material_master mm on pt.product_id = mm.material_id
-- MAGIC       where 
-- MAGIC       cs.month_year = 202312
-- MAGIC       and cs.key = 'rfm'
-- MAGIC       AND cs.country = 'uae'
-- MAGIC       AND cs.channel = 'pos'
-- MAGIC       and cs.segment = 'Slipping Loyalist'
-- MAGIC       and pt.LHPRDate is not null
-- MAGIC       AND mm.department_id BETWEEN 1 AND 13
-- MAGIC       group by 1
-- MAGIC       """
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base_SL23')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Generic SL Conversion WoW in Jan 

-- COMMAND ----------

--Num = wow SL entering store in Jan
WITH WeeklyMetrics AS (
  SELECT
    WEEKOFYEAR(pt.business_day) AS week_number,
    COUNT(DISTINCT pt.customer_id) AS weekly_footfall
  FROM cust_base_SL23 cb
  JOIN gold.pos_transactions pt on cb.customer_id = pt.customer_id
  JOIN gold.material_master mm ON pt.product_id = mm.material_id
  JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
  WHERE
    pt.business_day >= '2024-01-01' 
    AND pt.LHPRDate IS NOT NULL
    AND mm.department_id BETWEEN 1 AND 13
    AND cs.key = 'rfm' 
    AND cs.country = 'uae' 
    AND cs.channel = 'pos' 
    AND cs.month_year = '202401' 
    AND cs.segment = 'Slipping Loyalist'  
  GROUP BY WEEKOFYEAR(pt.business_day)
)
SELECT ROUND(AVG(weekly_footfall), 0) AS avg_weekly_footfall
FROM WeeklyMetrics;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## LFL Campaigns

-- COMMAND ----------

-- 75,77,88
WITH final_merged_campaign_customer_data AS (
    SELECT
        t1.campaign_id,
        t1.customer_id,
        t1.campaign_set,
        COALESCE(t2.delivery_status, 'NA') AS delivery_status,
        COALESCE(t3.num_trans, 0) AS num_trans,
        COALESCE(t3.num_offer_trans, 0) AS num_offer_trans,
        COALESCE(t3.total_spend, 0) AS total_spend,
        COALESCE(t3.total_offer_spend, 0) AS total_offer_spend
    FROM
        analytics.campaign_audience t1
        LEFT JOIN analytics.campaign_delivery t2 ON t1.campaign_id = t2.campaign_id AND t1.customer_key = t2.customer_key
        LEFT JOIN analytics.campaign_sales t3 ON t1.campaign_id = t3.campaign_id AND t1.customer_id = t3.customer_id
),

-- CTE2
final_campaign_data AS (
    SELECT
        campaign_id,
        campaign_set,
        COUNT(customer_id) AS total_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN 1 ELSE 0 END) AS read_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'not_opened' THEN 1 ELSE 0 END) AS notread_customers,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' AND num_trans > 0 THEN 1
                 WHEN campaign_set = 'control' AND num_trans > 0 THEN 1
                 ELSE 0 END) AS read_returning_customers,
        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'not_opened' AND num_trans > 0 THEN 1
                 WHEN campaign_set = 'control' AND num_trans > 0 THEN 1
                 ELSE 0 END) AS notread_returning_customers,

        SUM(CASE WHEN  delivery_status = 'opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS read_offer_customers,
        SUM(CASE WHEN  delivery_status = 'not_opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS notread_offer_customers,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN total_spend
                 WHEN campaign_set = 'control' THEN total_spend ELSE 0 END) AS total_spend,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN total_offer_spend
                 WHEN campaign_set = 'control' THEN 0 ELSE 0 END) AS total_offer_spend,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN num_trans
                 WHEN campaign_set = 'control' THEN num_trans ELSE 0 END) AS total_transactions,

        SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' THEN num_offer_trans
                 WHEN campaign_set = 'control' THEN 0 ELSE 0 END) AS total_offer_transactions
    FROM
        final_merged_campaign_customer_data
    GROUP BY
        campaign_id, campaign_set
)

-- Extraction Table
SELECT
    t1.campaign_id,
    t2.campaign_desc,
    t2.offer_threshold,
    t1.campaign_set,
    t1.total_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.read_customers END AS read_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.notread_customers END AS notread_customers,
    t1.read_returning_customers,
    t1.notread_returning_customers,
    t1.read_offer_customers AS read_offer_customers,
    t1.notread_offer_customers AS notread_offer_customers

FROM
    final_campaign_data t1
    JOIN analytics.campaign_details t2 ON t1.campaign_id = t2.campaign_id
WHERE
    t1.campaign_id IN (75,77)
ORDER BY
    t1.campaign_id, t1.campaign_set DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
