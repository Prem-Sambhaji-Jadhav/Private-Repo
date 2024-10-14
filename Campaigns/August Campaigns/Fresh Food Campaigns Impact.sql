-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Bakery RPC Boost

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN dev.sandbox.campaign_customer_details AS t3 ON t1.customer_key = t3.customer_key
    JOIN analytics.campaign.campaign_delivery AS t4
        ON t3.customer_key = t4.customer_key
        AND t3.campaign_id = t4.campaign_id
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (7009004, 7003004, 7005002, 7006001, 7003006, 7005007)
        AND campaign_set = 'test'
        AND t3.campaign_id = 143
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        and t4.delivery_status = 'opened'
    GROUP BY 1
),

opens AS (
    SELECT COUNT(DISTINCT customer_key) as opened_customers
    FROM analytics.campaign.campaign_delivery
    WHERE delivery_status = 'opened'
    AND campaign_id = 143
)

SELECT
    max(opened_customers) as opened_customers_a,
    COUNT(CASE WHEN rpc >= 30 THEN customer_key END) AS test_offer_customers,
    ROUND((test_offer_customers/opened_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 30 THEN rpc END), 1) AS test_rpc
FROM customers, opens

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.campaign.campaign_audience AS t3 ON t1.customer_key = t3.customer_key
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (7009004, 7003004, 7005002, 7006001, 7003006, 7005007)
        AND campaign_set = 'control'
        AND t3.campaign_id = 143
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
),

opens AS (
    SELECT COUNT(*) as total_customers
    FROM analytics.campaign.campaign_audience
    WHERE
        campaign_id = 143
        AND campaign_set = 'control'
)

SELECT
    MAX(total_customers) AS total_customers_a,
    COUNT(CASE WHEN rpc >= 30 THEN customer_key END) AS organic_customers,
    ROUND((organic_customers/total_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 30 THEN rpc END), 1) AS control_rpc
FROM customers, opens

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delicatessen RPC Boost

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN dev.sandbox.campaign_customer_details AS t3 ON t1.customer_key = t3.customer_key
    JOIN analytics.campaign.campaign_delivery AS t4
        ON t3.customer_key = t4.customer_key
        AND t3.campaign_id = t4.campaign_id
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (9009001, 9001003, 9003002, 9006001, 9002001, 9010001, 9010005, 9010003, 9006002, 9001001, 9003001, 9010006, 9001002, 9004001, 9002002, 9001004, 9010004)
        AND campaign_set = 'test'
        AND t3.campaign_id = 144
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        and t4.delivery_status = 'opened'
    GROUP BY 1
),

opens AS (
    SELECT COUNT(DISTINCT customer_key) as opened_customers
    FROM analytics.campaign.campaign_delivery
    WHERE delivery_status = 'opened'
    AND campaign_id = 144
)

SELECT
    max(opened_customers) as opened_customers_a,
    COUNT(CASE WHEN rpc >= 50 THEN customer_key END) AS test_offer_customers,
    ROUND((test_offer_customers/opened_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 50 THEN rpc END), 1) AS test_rpc
FROM customers, opens

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.campaign.campaign_audience AS t3 ON t1.customer_key = t3.customer_key
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (9009001, 9001003, 9003002, 9006001, 9002001, 9010001, 9010005, 9010003, 9006002, 9001001, 9003001, 9010006, 9001002, 9004001, 9002002, 9001004, 9010004)
        AND campaign_set = 'control'
        AND t3.campaign_id = 144
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
),

opens AS (
    SELECT COUNT(*) as total_customers
    FROM analytics.campaign.campaign_audience
    WHERE
        campaign_id = 144
        AND campaign_set = 'control'
)

SELECT
    MAX(total_customers) AS total_customers_a,
    COUNT(CASE WHEN rpc >= 50 THEN customer_key END) AS organic_customers,
    ROUND((organic_customers/total_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 50 THEN rpc END), 1) AS control_rpc
FROM customers, opens

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Fish and Meat RPC Boost

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN dev.sandbox.campaign_customer_details AS t3 ON t1.customer_key = t3.customer_key
    JOIN analytics.campaign.campaign_delivery AS t4
        ON t3.customer_key = t4.customer_key
        AND t3.campaign_id = t4.campaign_id
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (12001003, 12001004)
        AND campaign_set = 'test'
        AND t3.campaign_id = 145
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        and t4.delivery_status = 'opened'
    GROUP BY 1
),

opens AS (
    SELECT COUNT(DISTINCT customer_key) as opened_customers
    FROM analytics.campaign.campaign_delivery
    WHERE delivery_status = 'opened'
    AND campaign_id = 145
)

SELECT
    max(opened_customers) as opened_customers_a,
    COUNT(CASE WHEN rpc >= 100 THEN customer_key END) AS test_offer_customers,
    ROUND((test_offer_customers/opened_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 100 THEN rpc END), 1) AS test_rpc
FROM customers, opens

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.campaign.campaign_audience AS t3 ON t1.customer_key = t3.customer_key
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (12001003, 12001004)
        AND campaign_set = 'control'
        AND t3.campaign_id = 145
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
),

opens AS (
    SELECT COUNT(*) as total_customers
    FROM analytics.campaign.campaign_audience
    WHERE
        campaign_id = 145
        AND campaign_set = 'control'
)

SELECT
    MAX(total_customers) AS total_customers_a,
    COUNT(CASE WHEN rpc >= 100 THEN customer_key END) AS organic_customers,
    ROUND((organic_customers/total_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 100 THEN rpc END), 1) AS control_rpc
FROM customers, opens

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #IHKP RPC Boost

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN dev.sandbox.campaign_customer_details AS t3 ON t1.customer_key = t3.customer_key
    JOIN analytics.campaign.campaign_delivery AS t4
        ON t3.customer_key = t4.customer_key
        AND t3.campaign_id = t4.campaign_id
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (10001001, 10002001, 10007001, 10005004, 10003002, 10005001, 10007003, 10003001, 10006001, 10008004, 10005005)
        AND campaign_set = 'test'
        AND t3.campaign_id = 146
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
        and t4.delivery_status = 'opened'
    GROUP BY 1
),

opens AS (
    SELECT COUNT(DISTINCT customer_key) as opened_customers
    FROM analytics.campaign.campaign_delivery
    WHERE delivery_status = 'opened'
    AND campaign_id = 146
)

SELECT
    max(opened_customers) as opened_customers_a,
    COUNT(CASE WHEN rpc >= 50 THEN customer_key END) AS test_offer_customers,
    ROUND((test_offer_customers/opened_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 50 THEN rpc END), 1) AS test_rpc
FROM customers, opens

-- COMMAND ----------

WITH customers AS (
    SELECT
        t3.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.campaign.campaign_audience AS t3 ON t1.customer_key = t3.customer_key
    WHERE
        t1.transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
        AND material_group_id IN (10001001, 10002001, 10007001, 10005004, 10003002, 10005001, 10007003, 10003001, 10006001, 10008004, 10005005)
        AND campaign_set = 'control'
        AND t3.campaign_id = 146
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
),

opens AS (
    SELECT COUNT(*) as total_customers
    FROM analytics.campaign.campaign_audience
    WHERE
        campaign_id = 146
        AND campaign_set = 'control'
)

SELECT
    MAX(total_customers) AS total_customers_a,
    COUNT(CASE WHEN rpc >= 50 THEN customer_key END) AS organic_customers,
    ROUND((organic_customers/total_customers_a) * 100, 2) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 50 THEN rpc END), 1) AS control_rpc
FROM customers, opens
