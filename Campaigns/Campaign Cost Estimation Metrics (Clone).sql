-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Objectives

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. O1	Maintain and grow our VIP customers	
-- MAGIC - KR1:	VIP retention to be increased to 50%
-- MAGIC - KR2:	Move 15% Slipping Loyalist to VIP's
-- MAGIC - KR3:	Move 5% Moderates to VIP's
-- MAGIC 2. O2	Reduce High Value Lapsers
-- MAGIC - KR1:	Prevent moderates slipping into Lapsers
-- MAGIC - KR2:	Reactivate 10% high value Lapsers
-- MAGIC 3. O3	Convert 20% "one timers" to repeat customers
-- MAGIC - KR1:	Reactivate one timers to make a 2nd purchase
-- MAGIC - KR2:	Convert 50% newbies into repeat within first 60 days
-- MAGIC - KR3:	Convert 70% newbies into repeat within first 90 days
-- MAGIC 4. O4	Increase revenue per customer for Frequentists & Splurgers
-- MAGIC - KR1:	Increase Frequentists ATV by 10%
-- MAGIC - KR2:	20% of splurgers to have one additional trip/year
-- MAGIC 5. O5	Increase trips of customers
-- MAGIC - KR1:	Grooming newbies to Frequentists
-- MAGIC - KR2:	Splurgers trip Increase 
-- MAGIC - KR3:	SL Reversion
-- MAGIC - KR4:	Grooming Moderates to Frequentists
-- MAGIC 6. O6	Customer Satisfaction
-- MAGIC - KR1:	Birthday Offer
-- MAGIC - KR2:	VIP 40 AED Saver
-- MAGIC - KR3:	VIP 40 AED Saver Converts
-- MAGIC 7. O7	ATV Stretch Customers
-- MAGIC - KR1:	Frequentist ATV Stretch
-- MAGIC 8. CA1	Customer Acquisition(Non Emirati)	
-- MAGIC - KR1:	Increase VIP Fashion penetration by 5%
-- MAGIC - KR2:	Increase Frequentist Fashion penetration by 5%
-- MAGIC 9. CA2	Customer Acquisition( Emirati)	
-- MAGIC - KR1:	Increase VIP/Frequentist Fashion penetration by 5%
-- MAGIC 10. O1	Increase fashion Repeat Rate(Non Emirati)	
-- MAGIC - KR1:	Convert  VIP 1 timers to Repeat
-- MAGIC - KR2:	Convert  Frequentists 1 timers to Repeat
-- MAGIC 11. O2	Increase fashion Repeat Rate( Emirati)	
-- MAGIC - KR1:	Emiratis 1 timers to Repeat
-- MAGIC 12. S1	Survey		
-- MAGIC - KR1:    VIP,SL and Frequentist  LHP Survey

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. O1: VIP, SL, Moderates

-- COMMAND ----------

--Generic Behavior:

WITH WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM
        gold.pos_transactions pt
        JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        AND cs.segment IN ('VIP')
        AND pt.loyalty_account_id IS NOT NULL -- have to confirm with Nik
        AND mm.department_id BETWEEN 1 AND 13 -- have to confirm with Nik
        -- AND pt.business_day >= '2024-01-01'
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br> (in general)
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 27,403,219<br>
-- MAGIC Avg Weekly Freq - 69,875<br>
-- MAGIC Avg Weekly Count - 47,145<br>
-- MAGIC Avg Weekly ATV - 391.85<br><br>
-- MAGIC
-- MAGIC Moderate:<br>
-- MAGIC Avg Weekly Sales - 5,572,658<br>
-- MAGIC Avg Weekly Freq - 58,213<br>
-- MAGIC Avg Weekly Count - 47,819<br>
-- MAGIC Avg Weekly ATV - 101<br><br>
-- MAGIC
-- MAGIC Slipping Loyalist:<br>
-- MAGIC Avg Weekly Sales - 974,802<br>
-- MAGIC Avg Weekly Freq - 2,576<br>
-- MAGIC Avg Weekly Count - 1,866<br>
-- MAGIC Avg Weekly ATV - 381.29

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>January 2024 Period</b><br>
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 31,404,034<br>
-- MAGIC Avg Weekly Freq - 81,247<br>
-- MAGIC Avg Weekly Count - 54,126<br>
-- MAGIC Avg Weekly ATV - 369.03<br><br>
-- MAGIC
-- MAGIC Moderate:<br>
-- MAGIC Avg Weekly Sales - 9,307,871<br>
-- MAGIC Avg Weekly Freq - 97,051<br>
-- MAGIC Avg Weekly Count - 74,368<br>
-- MAGIC Avg Weekly ATV - 95.04<br><br>
-- MAGIC
-- MAGIC Slipping Loyalist:<br>
-- MAGIC Avg Weekly Sales - 498,875<br>
-- MAGIC Avg Weekly Freq - 1,361<br>
-- MAGIC Avg Weekly Count - 1,100<br>
-- MAGIC Avg Weekly ATV - 350.87

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2. O2: Lapsers, Moderates

-- COMMAND ----------

--Generic Behavior:

WITH WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM
        gold.pos_transactions pt
        JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        AND cs.segment IN ('Lapser')
        AND pt.loyalty_account_id IS NOT NULL -- have to confirm with Nik
        AND mm.department_id BETWEEN 1 AND 13 -- have to confirm with Nik
        AND pt.business_day >= '2024-01-01'
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC Lapser:<br>
-- MAGIC Avg Weekly Sales - 568,048<br>
-- MAGIC Avg Weekly Freq - 5,332<br>
-- MAGIC Avg Weekly Count - 3,186<br>
-- MAGIC Avg Weekly ATV - 86.62<br><br>
-- MAGIC
-- MAGIC Moderate:<br>
-- MAGIC Avg Weekly Sales - 5,572,658<br>
-- MAGIC Avg Weekly Freq - 58,213<br>
-- MAGIC Avg Weekly Count - 47,819<br>
-- MAGIC Avg Weekly ATV - 101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Janauary 2024 Period</b><br>
-- MAGIC Lapser:<br>
-- MAGIC Avg Weekly Sales - 360,286<br>
-- MAGIC Avg Weekly Freq - 2,960<br>
-- MAGIC Avg Weekly Count - 2,337<br>
-- MAGIC Avg Weekly ATV - 120.66<br><br>
-- MAGIC
-- MAGIC Moderate:<br>
-- MAGIC Avg Weekly Sales - 9,307,871<br>
-- MAGIC Avg Weekly Freq - 97,051<br>
-- MAGIC Avg Weekly Count - 74,368<br>
-- MAGIC Avg Weekly ATV - 95.04

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3. O3: One Timer, Newbies

-- COMMAND ----------

--Generic Behavior:

WITH cte AS (
  SELECT mobile, COUNT(DISTINCT transaction_id) AS freq
  FROM gold.pos_transactions
  GROUP BY mobile
  HAVING freq = 1
),

WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM
        cte AS t1
        JOIN gold.pos_transactions pt ON t1.mobile = pt.mobile
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
    WHERE
        mm.department_id BETWEEN 1 AND 13 -- have to confirm with Nik
        AND pt.business_day >= '2024-01-01'
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC One Timers:<br>
-- MAGIC Avg Weekly Sales - 3,558,107<br>
-- MAGIC Avg Weekly Freq - 28,081<br>
-- MAGIC Avg Weekly Count - 28,081<br>
-- MAGIC Avg Weekly ATV - 128.83<br><br>
-- MAGIC
-- MAGIC <b>January 2024 Period</b><br>
-- MAGIC One Timers:<br>
-- MAGIC Avg Weekly Sales - 2,239,963<br>
-- MAGIC Avg Weekly Freq - 23,733<br>
-- MAGIC Avg Weekly Count - 23,733<br>
-- MAGIC Avg Weekly ATV - 92.96

-- COMMAND ----------

--Generic Behavior:

WITH cte AS (
  SELECT t1.mobile, COUNT(transaction_id) AS freq
  FROM gold.pos_transactions AS t1
  JOIN analytics.customer_segments AS t2
  ON t1.customer_id = t2.customer_id
  WHERE DATE(first_purchase_date) BETWEEN "2023-10-17" AND "2023-10-31"
  AND business_day >= "2023-10-17"
  GROUP BY t1.mobile
  HAVING freq > 1
),

WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM cte AS t1
        JOIN gold.pos_transactions pt ON t1.mobile = pt.mobile
        -- JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
    WHERE
        -- cs.key = 'rfm'
        -- AND cs.channel = 'pos'
        -- AND cs.country = 'uae'
        -- AND cs.month_year = '202312'
        -- AND cs.segment IN ('Newbie')
        pt.loyalty_account_id IS NOT NULL -- have to confirm with Nik
        AND mm.department_id BETWEEN 1 AND 13 -- have to confirm with Nik
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Newbies (60 days):<br>
-- MAGIC Avg Weekly Sales - 307,793<br>
-- MAGIC Avg Weekly Freq - 2,864<br>
-- MAGIC Avg Weekly Count - 1,512<br>
-- MAGIC Avg Weekly ATV - 121.99<br><br>
-- MAGIC
-- MAGIC Newbies (90 days):<br>
-- MAGIC Avg Weekly Sales - 286,765<br>
-- MAGIC Avg Weekly Freq - 2,946<br>
-- MAGIC Avg Weekly Count - 1,572<br>
-- MAGIC Avg Weekly ATV - 129.74

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###4. O4: Frequentists, Splurgers

-- COMMAND ----------

--Generic Behavior:

WITH WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM
        gold.pos_transactions pt
        JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        AND cs.segment IN ('Frequentist')
        AND pt.loyalty_account_id IS NOT NULL -- have to confirm with Nik
        AND mm.department_id BETWEEN 1 AND 13 -- have to confirm with Nik
        -- AND pt.business_day >= '2024-01-01'
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 39,224,115<br>
-- MAGIC Avg Weekly Freq - 533,841<br>
-- MAGIC Avg Weekly Count - 242,867<br>
-- MAGIC Avg Weekly ATV - 74.35<br><br>
-- MAGIC
-- MAGIC Splurger:<br>
-- MAGIC Avg Weekly Sales - 1,807,458<br>
-- MAGIC Avg Weekly Freq - 2,622<br>
-- MAGIC Avg Weekly Count - 2,347<br>
-- MAGIC Avg Weekly ATV - 708.45

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Janauary 2024 Period</b><br>
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 40,107,311<br>
-- MAGIC Avg Weekly Freq - 541,757<br>
-- MAGIC Avg Weekly Count - 252,953<br>
-- MAGIC Avg Weekly ATV - 72.97<br><br>
-- MAGIC
-- MAGIC Splurger:<br>
-- MAGIC Avg Weekly Sales - 2,669,577<br>
-- MAGIC Avg Weekly Freq - 4,752<br>
-- MAGIC Avg Weekly Count - 3,979<br>
-- MAGIC Avg Weekly ATV - 541.53

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###5. O5: Newbie, Splurger, SL, Moderate

-- COMMAND ----------

--Generic Behavior:

WITH WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM
        gold.pos_transactions pt
        JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        AND cs.segment IN ('Newbie')
        AND pt.loyalty_account_id IS NOT NULL -- have to confirm with Nik
        AND mm.department_id BETWEEN 1 AND 13 -- have to confirm with Nik
        -- AND pt.business_day >= '2024-01-01'
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC Newbie:<br>
-- MAGIC Avg Weekly Sales - 416,145<br>
-- MAGIC Avg Weekly Freq - 4,935<br>
-- MAGIC Avg Weekly Count - 2,886<br>
-- MAGIC Avg Weekly ATV - 87.67<br><br>
-- MAGIC
-- MAGIC Moderate:<br>
-- MAGIC Avg Weekly Sales - 5,572,658<br>
-- MAGIC Avg Weekly Freq - 58,213<br>
-- MAGIC Avg Weekly Count - 47,819<br>
-- MAGIC Avg Weekly ATV - 101<br><br>
-- MAGIC
-- MAGIC Slipping Loyalist:<br>
-- MAGIC Avg Weekly Sales - 974,802<br>
-- MAGIC Avg Weekly Freq - 2,576<br>
-- MAGIC Avg Weekly Count - 1,866<br>
-- MAGIC Avg Weekly ATV - 381.29<br><br>
-- MAGIC
-- MAGIC Splurger:<br>
-- MAGIC Avg Weekly Sales - 1,807,458<br>
-- MAGIC Avg Weekly Freq - 2,622<br>
-- MAGIC Avg Weekly Count - 2,347<br>
-- MAGIC Avg Weekly ATV - 708.45

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>January 2024 Period</b><br>
-- MAGIC Newbie:<br>
-- MAGIC Avg Weekly Sales - 323,849<br>
-- MAGIC Avg Weekly Freq - 4,597<br>
-- MAGIC Avg Weekly Count - 2,350<br>
-- MAGIC Avg Weekly ATV - 70.08<br><br>
-- MAGIC
-- MAGIC Moderate:<br>
-- MAGIC Avg Weekly Sales - 9,307,871<br>
-- MAGIC Avg Weekly Freq - 97,051<br>
-- MAGIC Avg Weekly Count - 74,368<br>
-- MAGIC Avg Weekly ATV - 95.04<br><br>
-- MAGIC
-- MAGIC Slipping Loyalist:<br>
-- MAGIC Avg Weekly Sales - 498,875<br>
-- MAGIC Avg Weekly Freq - 1,361<br>
-- MAGIC Avg Weekly Count - 1,100<br>
-- MAGIC Avg Weekly ATV - 350.87<br><br>
-- MAGIC
-- MAGIC Splurger:<br>
-- MAGIC Avg Weekly Sales - 2,669,577<br>
-- MAGIC Avg Weekly Freq - 4,752<br>
-- MAGIC Avg Weekly Count - 3,979<br>
-- MAGIC Avg Weekly ATV - 541.53

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6. O6: VIP

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 27,403,219<br>
-- MAGIC Avg Weekly Freq - 69,875<br>
-- MAGIC Avg Weekly Count - 47,145<br>
-- MAGIC Avg Weekly ATV - 391.85<br><br>
-- MAGIC
-- MAGIC <b>January 2024 Period</b><br>
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 31,404,034<br>
-- MAGIC Avg Weekly Freq - 81,247<br>
-- MAGIC Avg Weekly Count - 54,126<br>
-- MAGIC Avg Weekly ATV - 369.03

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###7. O7: Frequentist

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 39,224,115<br>
-- MAGIC Avg Weekly Freq - 533,841<br>
-- MAGIC Avg Weekly Count - 242,867<br>
-- MAGIC Avg Weekly ATV - 74.35<br><br>
-- MAGIC
-- MAGIC <b>Janauary 2024 Period</b><br>
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 40,107,311<br>
-- MAGIC Avg Weekly Freq - 541,757<br>
-- MAGIC Avg Weekly Count - 252,953<br>
-- MAGIC Avg Weekly ATV - 72.97

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###8. CA1: VIP, Frequentist (Non-Emirati)

-- COMMAND ----------

--Generic Behavior:

WITH WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM
        gold.pos_transactions pt
        JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
        JOIN gold.customer_profile cp ON pt.customer_id = cp.account_key
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        AND cs.segment IN ('VIP')
        AND cp.nationality_group != "EMIRATI"
        AND category_name IN ('LINGERIE', 'LADIES INDIAN DRESSES', 'LADIES BLOUSES', 'SAREE', 'LADIES DRESSES', 'MENS SHIRTS', 'MENS T SHIRTS', 'OTHER MENSWEAR', 'MENS TROUSERS') -- have to confirm with Nik
        AND pt.loyalty_account_id IS NOT NULL -- have to confirm with Nik
        AND pt.business_day >= '2024-01-01'
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 264,636<br>
-- MAGIC Avg Weekly Freq - 2,076<br>
-- MAGIC Avg Weekly Count - 1,673<br>
-- MAGIC Avg Weekly ATV - 126.2<br><br>
-- MAGIC
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 1,379,385<br>
-- MAGIC Avg Weekly Freq - 17,356<br>
-- MAGIC Avg Weekly Count - 14,065<br>
-- MAGIC Avg Weekly ATV - 77.99

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>January 2024 Period</b><br>
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 159,240<br>
-- MAGIC Avg Weekly Freq - 1,586<br>
-- MAGIC Avg Weekly Count - 1,417<br>
-- MAGIC Avg Weekly ATV - 100.66<br><br>
-- MAGIC
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 854,683<br>
-- MAGIC Avg Weekly Freq - 13,551<br>
-- MAGIC Avg Weekly Count - 11,847<br>
-- MAGIC Avg Weekly ATV - 62.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###9. CA2: VIP, Frequentist (Emirati)

-- COMMAND ----------

--Generic Behavior:

WITH WeeklyMetrics AS (
    SELECT
        WEEKOFYEAR(pt.business_day) AS week_number,
        SUM(pt.amount) AS weekly_sales,
        SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id) weekly_atv,
        COUNT(DISTINCT pt.transaction_id) AS weekly_freq,
        COUNT(DISTINCT pt.mobile) weekly_count
    FROM
        gold.pos_transactions pt
        JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        JOIN gold.material_master mm on pt.product_id = mm.material_id 
        JOIN gold.customer_profile cp ON pt.customer_id = cp.account_key
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        AND cs.segment IN ('Frequentist')
        AND cp.nationality_group = "EMIRATI"
        AND category_name IN ('LINGERIE', 'LADIES INDIAN DRESSES', 'LADIES BLOUSES', 'SAREE', 'LADIES DRESSES', 'MENS SHIRTS', 'MENS T SHIRTS', 'OTHER MENSWEAR', 'MENS TROUSERS') -- have to confirm with Nik
        AND pt.loyalty_account_id IS NOT NULL -- have to confirm with Nik
        AND pt.business_day >= '2024-01-01'
    GROUP BY 1
)

SELECT ROUND(SUM(weekly_sales)/COUNT(*),0) avg_weekly_sales,
        ROUND(SUM(weekly_freq)/COUNT(*),0) avg_weekly_freq,
        ROUND(SUM(weekly_count)/COUNT(*),0) avg_weekly_count,
        ROUND(SUM(weekly_atv)/COUNT(*),2) avg_weekly_atv
FROM WeeklyMetrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Overall Period</b><br>
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 36,147<br>
-- MAGIC Avg Weekly Freq - 412<br>
-- MAGIC Avg Weekly Count - 373<br>
-- MAGIC Avg Weekly ATV - 90.12<br><br>
-- MAGIC
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 18,275<br>
-- MAGIC Avg Weekly Freq - 281<br>
-- MAGIC Avg Weekly Count - 247<br>
-- MAGIC Avg Weekly ATV - 65.66

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>January 2024 Period</b><br>
-- MAGIC VIP:<br>
-- MAGIC Avg Weekly Sales - 30,396<br>
-- MAGIC Avg Weekly Freq - 413<br>
-- MAGIC Avg Weekly Count - 391<br>
-- MAGIC Avg Weekly ATV - 74.09<br><br>
-- MAGIC
-- MAGIC Frequentist:<br>
-- MAGIC Avg Weekly Sales - 17,285<br>
-- MAGIC Avg Weekly Freq - 286<br>
-- MAGIC Avg Weekly Count - 261<br>
-- MAGIC Avg Weekly ATV - 61.18

-- COMMAND ----------

select * from sandbox.pg_temp_dashboard_metrics_v2 where campaign_id in (76,81,82,83)

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Salary Distribution over a Month
-- MAGIC

-- COMMAND ----------

-- mocd ko nikalna hai and check karna hai salary pe kya scene hai

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###ATV Breach

-- COMMAND ----------

SELECT segment,
    COUNT(DISTINCT pt.mobile) customers,
    ROUND(SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id),2) atv
FROM
    gold.pos_transactions pt
    JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    JOIN gold.material_master mm on pt.product_id = mm.material_id 
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202304'
    AND pt.loyalty_account_id IS NOT NULL
    AND mm.department_id BETWEEN 1 AND 13
    AND pt.business_day BETWEEN '2023-04-01' AND '2023-04-30'
GROUP BY 1

-- COMMAND ----------

-- MAGIC %py
-- MAGIC segments = ['VIP', 'Slipping Loyalist', 'Splurger', 'Frequentist', 'Newbie', 'Lapser', 'Moderate']
-- MAGIC ATV = [319.08, 0, 23.25, 68.18, 81.98, 256.8, 120.57]
-- MAGIC p = []
-- MAGIC for i in range(7):
-- MAGIC     query = """
-- MAGIC     WITH cte AS (SELECT pt.mobile,
-- MAGIC                     ROUND(SUM(pt.amount)/COUNT(DISTINCT pt.transaction_id),2) atv
-- MAGIC                 FROM
-- MAGIC                     gold.pos_transactions pt
-- MAGIC                     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC                     JOIN gold.material_master mm on pt.product_id = mm.material_id 
-- MAGIC                 WHERE
-- MAGIC                     cs.key = 'rfm'
-- MAGIC                     AND cs.channel = 'pos'
-- MAGIC                     AND cs.country = 'uae'
-- MAGIC                     AND cs.month_year = '202302'
-- MAGIC                     AND cs.segment = '{}'
-- MAGIC                     AND pt.loyalty_account_id IS NOT NULL
-- MAGIC                     AND mm.department_id BETWEEN 1 AND 13
-- MAGIC                     AND pt.business_day BETWEEN '2023-02-01' AND '2023-02-28'
-- MAGIC                 GROUP BY 1
-- MAGIC                 HAVING atv > {})
-- MAGIC
-- MAGIC     SELECT COUNT(*)
-- MAGIC     FROM cte""".format(segments[i], ATV[i])
-- MAGIC
-- MAGIC     p.append(spark.sql(query).toPandas().iloc[0,0])
-- MAGIC
-- MAGIC p

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Avg Sales per Period

-- COMMAND ----------

--1. find segment-wise foot fall on avg per period. 7 segments * 3 periods. 21 rows.

WITH cte AS (
  SELECT
      cs.segment,
      month(pt.business_day) AS month_year,
      CASE
          WHEN DAY(pt.business_day) BETWEEN 1 AND 10 THEN '1'
          WHEN DAY(pt.business_day) BETWEEN 11 AND 20 THEN '2'
          WHEN DAY(pt.business_day) BETWEEN 21 AND 31 THEN '3'
          END AS period_number,
      ROUND(SUM(pt.amount), 2) AS periodly_sales,
      COUNT(cs.customer_id) AS periodly_cust_count
  FROM
      gold.pos_transactions pt
      LEFT JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
      LEFT JOIN gold.material_master mm ON pt.product_id = mm.material_id
  WHERE
      cs.key = 'rfm'
      AND cs.channel = 'pos'
      AND cs.country = 'uae'
      AND cs.month_year = '202312'
      AND pt.loyalty_account_id IS NOT NULL
      AND mm.department_id BETWEEN 1 AND 13
      AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
  GROUP BY 1, 2, 3
  ORDER BY 1, 2, 3
)

SELECT
    segment,
    period_number,
    ROUND(AVG(periodly_sales),0) AS avg_periodly_sales,
    ROUND(AVG(periodly_cust_count),0) AS avg_periodly_cust_count
FROM cte
GROUP BY segment, period_number

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Sales distribution of Segments over Periods

-- COMMAND ----------

SELECT
    cs.segment,
    CASE
        WHEN DAY(pt.business_day) BETWEEN 1 AND 10 THEN '1'
        WHEN DAY(pt.business_day) BETWEEN 11 AND 20 THEN '2'
        WHEN DAY(pt.business_day) BETWEEN 21 AND 31 THEN '3'
        END AS period_number,
    ROUND(SUM(pt.amount), 2) AS periodly_sales,
    COUNT(cs.customer_id) AS periodly_cust_count
FROM
    gold.pos_transactions pt
    LEFT JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    LEFT JOIN gold.material_master mm ON pt.product_id = mm.material_id
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202312'
    AND pt.loyalty_account_id IS NOT NULL
    AND mm.department_id BETWEEN 1 AND 13
    AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Percentile Sales by Segment

-- COMMAND ----------

--2. p25, p50, p75, p95 of sales in the same period for Frequentist and VIP.

SELECT
    cs.segment,
    DAY(pt.business_day),
    CASE
        WHEN DAY(pt.business_day) BETWEEN 1 AND 10 THEN '1-10'
        WHEN DAY(pt.business_day) BETWEEN 11 AND 20 THEN '11-20'
        WHEN DAY(pt.business_day) BETWEEN 21 AND 31 THEN '21-31'
        END AS period_number,
    ROUND(SUM(pt.amount), 0) AS daily_sales,
    COUNT(cs.customer_id) AS daily_cust_count
FROM
    gold.pos_transactions pt
    LEFT JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    LEFT JOIN gold.material_master mm ON pt.product_id = mm.material_id
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202312'
    AND pt.loyalty_account_id IS NOT NULL
    AND mm.department_id BETWEEN 1 AND 13
    AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Sales and Customer Count WoW

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC     cs.segment,
-- MAGIC     FLOOR(DATEDIFF(pt.business_day, "2023-01-01") / 7) + 1 AS week_number,
-- MAGIC     ROUND(SUM(pt.amount),0) AS sales,
-- MAGIC     COUNT(DISTINCT cs.customer_id) AS cust_count
-- MAGIC FROM gold.pos_transactions pt
-- MAGIC     JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
-- MAGIC     JOIN gold.material_master mm ON pt.product_id = mm.material_id
-- MAGIC WHERE
-- MAGIC     cs.key = 'rfm'
-- MAGIC     AND cs.channel = 'pos'
-- MAGIC     AND cs.country = 'uae'
-- MAGIC     AND cs.month_year = '202312'
-- MAGIC     AND pt.loyalty_account_id IS NOT NULL
-- MAGIC     AND mm.department_id BETWEEN 1 AND 13
-- MAGIC     AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
-- MAGIC GROUP BY 1, 2
-- MAGIC ORDER BY 1, 2
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC sale_WoW = []
-- MAGIC cust_count_WoW = []
-- MAGIC
-- MAGIC segments = df['segment'].unique()
-- MAGIC for segment in segments:
-- MAGIC     weeks = df[df['segment'] == segment]['week_number'].values
-- MAGIC     sales = df[df['segment'] == segment]['sales'].values
-- MAGIC     cust_count = df[df['segment'] == segment]['cust_count'].values
-- MAGIC
-- MAGIC     for i in range(len(weeks)):
-- MAGIC         if i == 0:
-- MAGIC             sale_WoW.append(0)
-- MAGIC             cust_count_WoW.append(0)
-- MAGIC             sale_prev = sales[i]
-- MAGIC             cust_prev = cust_count[i]
-- MAGIC         
-- MAGIC         else:
-- MAGIC             sale_WoW.append((sales[i] - sale_prev)/sale_prev)
-- MAGIC             cust_count_WoW.append((cust_count[i] - cust_prev)/cust_prev)
-- MAGIC             sale_prev = sales[i]
-- MAGIC             cust_prev = cust_count[i]
-- MAGIC
-- MAGIC df['sale_WoW'] = sale_WoW
-- MAGIC df['cust_count_WoW'] = cust_count_WoW

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.createDataFrame(df)
-- MAGIC df.createOrReplaceTempView('weekly_growth')

-- COMMAND ----------

SELECT * FROM weekly_growth

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

--Salary
-- The number of folks in each segment vs the number of folks in each segment who are not in the mocd category.
select cs.segment, count(distinct cs.customer_id) cust_count
from analytics.customer_segments cs
--join gold.pos_transactions pt on cs.customer_id=pt.customer_id
where cs.key = 'rfm'
and cs.channel = 'pos'
and cs.country = 'uae'
and cs.month_year = 202312
--and pt.mocd_flag = 0
group by 1

-- COMMAND ----------

-- These are the folks in BPL
select cs.segment, count(distinct cs.customer_id) cust_count
from analytics.customer_segments cs
join gold.pos_transactions pt on cs.customer_id=pt.customer_id
where cs.key = 'rfm'
and cs.channel = 'pos'
and cs.country = 'uae'
and cs.month_year = 202312
and pt.mocd_flag = 1
group by 1

-- COMMAND ----------

-- The amount spent and footfall on a monthly avg basis by all segments vs the week-wise contribution.
with cte1 as (
SELECT
        cs.segment,
        SUM(pt.amount)/12 AS monthly_sales,
        count(cs.customer_id)/12 as monthly_cust_count
    FROM
        gold.pos_transactions pt
        LEFT JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
        LEFT JOIN gold.material_master mm on pt.product_id = mm.material_id 
    WHERE
        cs.key = 'rfm'
        AND cs.channel = 'pos'
        AND cs.country = 'uae'
        AND cs.month_year = '202312'
        --AND cs.segment IN ('Moderate')
        and pt.loyalty_account_id is not null
        AND mm.department_id BETWEEN 1 and 13 
        --AND pt.business_day BETWEEN '2023-10-15' AND '2024-01-20'
    GROUP BY 1
    )
    
select segment,avg(monthly_sales), avg(monthly_cust_count)
from cte1
group by 1

-- COMMAND ----------

WITH min_date_cte AS (
    SELECT MIN(business_day) AS min_business_day
    FROM gold.pos_transactions pt
    WHERE pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
)

SELECT
    cs.segment,
    month(pt.business_day) AS month_year,
    CEIL(DATEDIFF(pt.business_day, min_business_day) / 7) AS week_number,
    round(SUM(pt.amount),2) AS monthly_sales,
    COUNT(cs.customer_id) AS monthly_cust_count
FROM
    min_date_cte
    CROSS JOIN gold.pos_transactions pt
    LEFT JOIN analytics.customer_segments cs ON pt.customer_id = cs.customer_id
    LEFT JOIN gold.material_master mm ON pt.product_id = mm.material_id
WHERE
    cs.key = 'rfm'
    AND cs.channel = 'pos'
    AND cs.country = 'uae'
    AND cs.month_year = '202312'
    AND pt.loyalty_account_id IS NOT NULL
    AND mm.department_id BETWEEN 1 AND 13
    AND pt.business_day BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3


-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


