-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Frequentist ATV Stretch
-- MAGIC 1. ATV between 45-55
-- MAGIC 2. Departments 1-13
-- MAGIC 3. Offer - 5% back as points upto 25-30 AED
-- MAGIC 4. Threshold - 70 AED
-- MAGIC 5. Audience - 40k

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Analysis

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT DISTINCT t1.mobile, card_key,
-- MAGIC         ROUND(ROUND(SUM(amount),0) / COUNT(DISTINCT transaction_id),2) AS atv,
-- MAGIC         COUNT(DISTINCT transaction_id) AS total_orders,
-- MAGIC         DATEDIFF("2023-12-31", DATE(MAX(last_purchase_date))) AS recency
-- MAGIC FROM gold.pos_transactions AS t1
-- MAGIC JOIN analytics.customer_segments AS t2
-- MAGIC ON t1.customer_id = t2.customer_id
-- MAGIC JOIN gold.customer_profile AS t3
-- MAGIC ON t1.customer_id = t3.account_key
-- MAGIC JOIN gold.material_master AS t4
-- MAGIC ON t1.product_id = t4.material_id
-- MAGIC WHERE key = 'rfm'
-- MAGIC AND channel = 'pos'
-- MAGIC AND t2.country = 'uae'
-- MAGIC AND month_year = '202312'
-- MAGIC AND segment = 'Frequentist'
-- MAGIC AND department_id BETWEEN 1 AND 13
-- MAGIC AND business_day BETWEEN "2023-04-20" AND "2024-01-20"
-- MAGIC AND loyalty_account_id IS NOT NULL
-- MAGIC AND card_key IS NOT NULL
-- MAGIC AND t1.mobile IS NOT NULL
-- MAGIC GROUP BY t1.mobile, card_key
-- MAGIC HAVING atv BETWEEN 45 AND 55
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(query).toPandas()
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('cust_base')

-- COMMAND ----------

WITH cte AS (SELECT t1.mobile, card_key, transaction_id,
                    ROUND(SUM(amount),0) AS spend
            FROM cust_base AS t1
            JOIN gold.pos_transactions AS t2
            ON t1.mobile = t2.mobile
            JOIN gold.material_master AS t3
            ON t2.product_id = t3.material_id
            WHERE business_day BETWEEN "2023-04-20" AND "2024-01-20"
            AND department_id BETWEEN 1 AND 13
            GROUP BY t1.mobile, card_key, transaction_id),

cte2 AS (SELECT mobile, card_key,
                COUNT(DISTINCT transaction_id) AS transaction_count
        FROM cte
        GROUP BY mobile, card_key)

SELECT ROUND(SUM(transaction_count)/COUNT(*), 0) AS avg_orders
FROM cte2

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC WITH cte AS (SELECT t1.mobile, card_key, transaction_id,
-- MAGIC                     ROUND(SUM(amount),0) AS spend,
-- MAGIC                     CASE WHEN spend >= 70 THEN 1 ELSE 0 END AS flag70
-- MAGIC             FROM cust_base AS t1
-- MAGIC             JOIN gold.pos_transactions AS t2
-- MAGIC             ON t1.mobile = t2.mobile
-- MAGIC             JOIN gold.material_master AS t3
-- MAGIC             ON t2.product_id = t3.material_id
-- MAGIC             WHERE business_day BETWEEN "2023-04-20" AND "2024-01-20"
-- MAGIC             AND department_id BETWEEN 1 AND 13
-- MAGIC             GROUP BY t1.mobile, card_key, transaction_id)
-- MAGIC
-- MAGIC SELECT t1.mobile, t1.card_key, atv, total_orders, recency,
-- MAGIC         SUM(flag70) AS spend70_count
-- MAGIC FROM cte AS t1
-- MAGIC JOIN cust_base AS t2
-- MAGIC ON t1.mobile = t2.mobile
-- MAGIC GROUP BY t1.mobile, t1.card_key, atv, total_orders, recency
-- MAGIC HAVING spend70_count > 10
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df.shape

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('freq_atv_stretch')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test-Control Split

-- COMMAND ----------

-- CREATE OR REPLACE TABLE sandbox.pj_freq_atv_stretch_split AS 
-- SELECT * ,
--       CASE WHEN split_num <= 19 THEN 'test'
--       ELSE 'control'
--       END AS sample
-- FROM
--       (SELECT *, ntile(20) OVER (ORDER BY rand()) AS split_num
--       FROM
--             (
--             SELECT DISTINCT mobile, card_key, atv, total_orders, recency
--             FROM freq_atv_stretch
--             )
--       )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test-Control Similarity Check

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT *
-- MAGIC FROM sandbox.pj_freq_atv_stretch_split
-- MAGIC """
-- MAGIC df = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC test_df = df.loc[df['sample'] == 'test']
-- MAGIC control_df = df.loc[df['sample'] == 'control']
-- MAGIC
-- MAGIC mean_atv_test = test_df['atv'].mean()
-- MAGIC mean_atv_control = control_df['atv'].mean()
-- MAGIC
-- MAGIC mean_total_orders_test = test_df['total_orders'].mean()
-- MAGIC mean_total_orders_control = control_df['total_orders'].mean()
-- MAGIC
-- MAGIC print(mean_atv_test, mean_atv_control, mean_total_orders_test, mean_total_orders_control)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Write CSVs to DBFS

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("SELECT DISTINCT mobile FROM sandbox.pj_freq_atv_stretch_split WHERE sample = 'control'").toPandas()
-- MAGIC
-- MAGIC df.to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/freq_atv_stretch/' + 'control_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("SELECT DISTINCT mobile FROM sandbox.pj_freq_atv_stretch_split WHERE sample = 'test'").toPandas()
-- MAGIC
-- MAGIC df.to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/freq_atv_stretch/' + 'test_mobile.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("SELECT DISTINCT mobile, card_key FROM sandbox.pj_freq_atv_stretch_split WHERE sample = 'test'").toPandas()
-- MAGIC
-- MAGIC df.to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/january_campaigns/freq_atv_stretch/' + 'test_card_key.csv' , sep = '|', index = False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Cost Estimation

-- COMMAND ----------

-- General conversion from Segment POV for 1-13 Dept.
SELECT
    WEEKOFYEAR(pt.business_day) AS week_number,
    ROUND(SUM(pt.amount),0) AS weekly_sales,
    COUNT(pt.transaction_id) AS weekly_freq,
    count(cs.customer_id) AS weekly_cust_count
FROM
    gold.pos_transactions pt
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    JOIN gold.material_master mm ON pt.product_id = mm.material_id 
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202312'
    AND cs.segment = 'Frequentist'
    AND pt.loyalty_account_id IS NOT NULL
    AND mm.department_id BETWEEN 1 AND 13
    AND pt.business_day BETWEEN '2023-04-20' AND '2024-01-20'
GROUP BY 1

-- COMMAND ----------

-- General conversion overall in selected dept.
with cte1 as (
select
WEEKOFYEAR(pt.business_day) AS week_number,
SUM(pt.amount) AS weekly_sales,
COUNT(pt.transaction_id) AS weekly_freq
from gold.pos_transactions pt
inner join gold.material_master mm on pt.product_id = mm.material_id
where mm.department_id between 1 and 13
and pt.business_day between '2023-04-20' and '2024-01-20'
group by 1
order by 1
)
select avg(weekly_sales) avg_weekly_sales, avg(weekly_freq) avg_weekly_footfall
from cte1

-- COMMAND ----------

WITH cte1 AS (SELECT CONCAT(YEAR(business_day), WEEKOFYEAR(business_day)) AS week_number,
                      COUNT(transaction_id) AS weekly_freq
              FROM gold.pos_transactions AS t1
              JOIN gold.material_master AS t2
              ON t1.product_id = t2.material_id
              JOIN analytics.customer_segments AS t3
              ON t1.customer_id = t3.customer_id
              WHERE t2.department_id BETWEEN 1 AND 13
              AND key = 'rfm'
              AND channel = 'pos'
              AND t3.country = 'uae'
              AND month_year = '202312'
              AND segment = 'Frequentist'
              AND loyalty_account_id IS NOT NULL
              AND business_day BETWEEN "2023-04-20" AND "2024-01-20" -- 4,365,463 / 1,873,601
              GROUP BY week_number
              ORDER BY week_number
)

SELECT ROUND(AVG(weekly_freq),0) AS avg_weekly_footfall
FROM cte1

-- COMMAND ----------

SELECT COUNT(DISTINCT mobile) AS tot_cust,
        COUNT(DISTINCT CASE WHEN department_id BETWEEN 1 AND 13 THEN mobile END) AS supermarket_cust,
        ROUND(supermarket_cust/tot_cust,4) AS perc_segment_in_supermarket
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2
ON t1.product_id = t2.material_id
JOIN analytics.customer_segments AS t3
ON t1.customer_id = t3.customer_id
AND key = 'rfm'
AND channel = 'pos'
AND t3.country = 'uae'
AND month_year = '202312'
AND segment = 'Frequentist'
AND business_day BETWEEN "2023-04-20" AND "2024-01-20"
AND loyalty_account_id IS NOT NULL

-- COMMAND ----------

SELECT *
FROM sandbox.pg_temp_dashboard_final_metrics_v2
WHERE campaign_id IN (76, 81, 82, 83)

-- COMMAND ----------

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

        -- SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS read_offer_customers,
        -- SUM(CASE WHEN campaign_set = 'test' AND delivery_status = 'not_opened' AND num_offer_trans > 0 THEN 1 ELSE 0 END) AS notread_offer_customers,

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
    t2.points_multiplier,
    t2.offer_threshold,
    t1.campaign_set,
    t1.total_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.read_customers END AS read_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.notread_customers END AS notread_customers,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.read_customers / t1.total_customers END AS open_perc,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.notread_customers / t1.total_customers END AS notopen_perc,

    t1.read_returning_customers,
    CASE WHEN t1.campaign_set = 'test' THEN t1.read_returning_customers / t1.read_customers
         ELSE t1.read_returning_customers / t1.total_customers END AS read_return_cust_perc,
    t1.notread_returning_customers,
    CASE WHEN t1.campaign_set = 'test' THEN t1.notread_returning_customers / t1.notread_customers
         ELSE t1.notread_returning_customers / t1.total_customers END AS notread_return_cust_perc,

    t1.read_offer_customers AS read_offer_customers,
    t1.read_offer_customers / t1.read_customers  AS read_offer_cust_perc,

    t1.notread_offer_customers AS notread_offer_customers,
    t1.notread_offer_customers / t1.notread_customers AS notread_offer_cust_perc,

    t1.total_spend,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.total_offer_spend END AS total_offer_spend,
    COALESCE(t1.total_spend / NULLIF(t1.read_returning_customers, 0), 0) AS read_revenue_per_customer,
    COALESCE(t1.total_spend / NULLIF(t1.notread_returning_customers, 0), 0) AS notread_revenue_per_customer,

    CASE WHEN t1.campaign_set = 'control' THEN 0 ELSE t1.total_offer_spend / NULLIF(t1.read_offer_customers, 0) END AS read_offer_revenue_per_customer,
    CASE WHEN t1.campaign_set = 'control' THEN 0 ELSE t1.total_offer_spend / NULLIF(t1.notread_offer_customers, 0) END AS notread_offer_revenue_per_customer,
    t1.total_spend / NULLIF(t1.total_transactions, 0) AS ATV,
    CASE WHEN t1.campaign_set = 'control' THEN 'NA' ELSE t1.total_offer_spend / NULLIF(t1.total_offer_transactions, 0) END AS offer_ATV
FROM
    final_campaign_data t1
    JOIN analytics.campaign_details t2 ON t1.campaign_id = t2.campaign_id
WHERE
    t1.campaign_id IN (76, 81, 82, 83)
ORDER BY
    t1.campaign_id, t1.campaign_set DESC;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

create or replace table sandbox.pj_temp_pos_transactions as (
  WITH cte AS (select transaction_id, business_day, mobile, customer_id,
                      SUM(amount) as spend,
                      CASE WHEN spend >= 70 THEN 1 ELSE 0 END AS flag70
              from gold.pos_transactions pt
              JOIN gold.material_master mm on pt.product_id = mm.material_id
              where pt.business_day BETWEEN '2023-04-20' AND '2024-01-20'
              and mm.department_id BETWEEN 1 and 13
              and pt.loyalty_account_id is not null
              GROUP BY pt.transaction_id, business_day, mobile, customer_id),

  cte2 AS (SELECT transaction_id, business_day, mobile, customer_id,
                  SUM(flag70) as spend70_count
           FROM cte
           GROUP BY transaction_id, business_day, mobile, customer_id
           HAVING spend70_count < 10)

  SELECT t1.transaction_id, t1.business_day, t1.mobile, t1.customer_id, spend
  FROM cte AS t1
  JOIN cte2 AS t2
  ON t1.transaction_id = t2.transaction_id
)

-- COMMAND ----------

create or replace table sandbox.pj_dump as (
  select transaction_id, business_day, mobile, customer_id,
          SUM(amount) as spend
  from gold.pos_transactions pt
  JOIN gold.material_master mm on pt.product_id = mm.material_id
  where pt.business_day BETWEEN '2023-04-20' AND '2024-01-20'
  and mm.department_id BETWEEN 1 and 13
  and pt.loyalty_account_id is not null
  GROUP BY pt.transaction_id, business_day, mobile, customer_id
)

-- COMMAND ----------

WITH cte AS (
  SELECT mobile, ROUND(ROUND(SUM(spend),0) / COUNT(DISTINCT transaction_id),2) AS atv
  FROM sandbox.pj_temp_pos_transactions
  GROUP BY mobile
  HAVING atv BETWEEN 45 AND 55
),

WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.spend) AS weekly_sales,
        sum(pt.spend)/count(pt.transaction_id) weekly_atv,
        COUNT(pt.transaction_id) AS weekly_freq,
        count(distinct pt.mobile) weekly_count,
        sum(cs.inter_purchase_time) IPT
    FROM
        cte
        JOIN sandbox.pj_temp_pos_transactions pt ON pt.mobile = cte.mobile
        JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        AND cs.segment = 'Frequentist'
    GROUP BY 1
)

SELECT ROUND(AVG(weekly_sales),2) avg_weekly_sales,
        ROUND(AVG(weekly_freq),2) avg_weekly_freq,
        ROUND(AVG(weekly_count),2) avg_weekly_count,
        ROUND(AVG(weekly_atv),2) avg_weekly_atv,
        ROUND(AVG(IPT),2) avg_weekly_IPT
FROM WeeklyMetrics

-- COMMAND ----------

-- total frequentist shopping in supermarket

SELECT
    cs.segment,COUNT(DISTINCT mobile)
FROM
    gold.pos_transactions pt
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    JOIN gold.material_master mm ON pt.product_id = mm.material_id 
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202312'
    AND cs.segment = 'Frequentist'
    -- AND pt.amount >= 50
    AND pt.loyalty_account_id IS NOT NULL
    AND mm.department_id BETWEEN 1 AND 13
    AND pt.business_day BETWEEN '2023-10-15' AND '2024-01-20'
    group by 1

-- COMMAND ----------



-- COMMAND ----------


