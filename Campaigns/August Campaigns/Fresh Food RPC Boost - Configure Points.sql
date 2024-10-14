-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Bakery RPC Boost

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW bakery_rpc_points AS (
    WITH customers AS (
        SELECT
            CONCAT(t1.customer_key, '|', t1.card_key) AS `customer_key|card_key`,
            SUM(amount) AS total_spend
        FROM dev.sandbox.pj_bakery_rpc_boost_final AS t1
        JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
        JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
        JOIN analytics.campaign.email_campaign_delivery AS t4 ON t1.customer_key = t4.CDID
        WHERE
            transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
            AND department_name = 'BAKERY'
            AND material_group_name IN ('BOUGHT IN CAKES', 'DRY CAKES', 'SAVOURIES', 'DOUGHNUTS', 'COOKIES', 'ARABIC SAVOURIES')
            AND final_status = "opened"
            AND campaign_id = 143
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
        HAVING total_spend > 30
    )

    SELECT
        143 AS campaign_id,
        "Bakery RPC Boost" AS campaign_name,
        `customer_key|card_key`,
        CASE WHEN ROUND(total_spend * 19) > 3800 THEN 3800
            ELSE ROUND(total_spend * 19)
            END AS points_earned
    FROM customers
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delicatessen RPC Boost

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW delicatessen_rpc_points AS (
    WITH customers AS (
        SELECT
            CONCAT(t1.customer_key, '|', t1.card_key) AS `customer_key|card_key`,
            SUM(amount) AS total_spend
        FROM dev.sandbox.pj_delicatessen_rpc_boost_final AS t1
        JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
        JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
        JOIN analytics.campaign.email_campaign_delivery AS t4 ON t1.customer_key = t4.CDID
        WHERE
            transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
            AND department_name = 'DELICATESSEN'
            AND material_group_name IN ('WHITE CHEESE (BULK)', 'IDLY/DOSA BATTER', 'COOKED  TURKEY', 'GREEN OLIVES', 'COOKED BEEF', 'CHAPPATHI', 'HARD CHEESE DELI', 'MALABAR PARATHA', 'PICKLED VEGETABLES', 'SEMI-HARD CHEESE', 'IH SALAD & MARINATED', 'BEEF PREPACKED', 'WHEAT PARATHA', 'BLACK OLIVES', 'IDIYAPPAM', 'SOFT & GRATED CHEESE', 'COOKED CHICKEN', 'IH CHEESE')
            AND final_status = "opened"
            AND campaign_id = 144
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
        HAVING total_spend > 50
    )

    SELECT
        144 AS campaign_id,
        "Delicatessen RPC Boost" AS campaign_name,
        `customer_key|card_key`,
        CASE WHEN ROUND(total_spend * 19) > 3800 THEN 3800
            ELSE ROUND(total_spend * 19)
            END AS points_earned
    FROM customers
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Fish and Meat RPC Boost

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW fish_meat_rpc_points AS (
    WITH customers AS (
        SELECT
            CONCAT(t1.customer_key, '|', t1.card_key) AS `customer_key|card_key`,
            SUM(amount) AS total_spend
        FROM dev.sandbox.pj_fish_meat_rpc_boost_final AS t1
        JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
        JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
        JOIN analytics.campaign.email_campaign_delivery AS t4 ON t1.customer_key = t4.CDID
        WHERE
            transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
            AND department_name IN ('FISH', 'MEAT')
            AND material_group_name IN ('WHOLE FISH', 'SHELL FISH')
            AND final_status = "opened"
            AND campaign_id = 145
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
        HAVING total_spend > 100
    )

    SELECT
        145 AS campaign_id,
        "Fish and Meat RPC Boost" AS campaign_name,
        `customer_key|card_key`,
        CASE WHEN ROUND(total_spend * 19) > 3800 THEN 3800
            ELSE ROUND(total_spend * 19)
            END AS points_earned
    FROM customers
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #IHKP RPC Boost

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW ihkp_rpc_points AS (
    WITH customers AS (
        SELECT
            CONCAT(t1.customer_key, '|', t1.card_key) AS `customer_key|card_key`,
            SUM(amount) AS total_spend
        FROM dev.sandbox.pj_ihkp_rpc_boost_final AS t1
        JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
        JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
        JOIN analytics.campaign.email_campaign_delivery AS t4 ON t1.customer_key = t4.CDID
        WHERE
            transaction_date BETWEEN "2024-08-25" AND "2024-08-31"
            AND department_name = 'IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)'
            AND material_group_name IN ('INDIAN MEALS', 'ROTISERRIE', 'ORIENTAL MEALS', 'INDIAN SNACKS', 'INDIAN SWEETS DELI', 'BROASTS', 'INDIAN NAMKEEN', 'INDIAN BREAKFAST', 'TANDOOR', 'SANDWICHES', 'BOUGHT IN SAVOURIES')
            AND final_status = "opened"
            AND campaign_id = 146
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
        HAVING total_spend > 50
    )

    SELECT
        146 AS campaign_id,
        "IHKP RPC Boost" AS campaign_name,
        `customer_key|card_key`,
        CASE WHEN ROUND(total_spend * 19) > 3800 THEN 3800
            ELSE ROUND(total_spend * 19)
            END AS points_earned
    FROM customers
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Final Table

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.campaign_points_configure AS (
    SELECT * FROM bakery_rpc_points
    UNION
    SELECT * FROM delicatessen_rpc_points
    UNION
    SELECT * FROM fish_meat_rpc_points
    UNION
    SELECT * FROM ihkp_rpc_points
)

-- COMMAND ----------

SELECT *
FROM dev.sandbox.campaign_points_configure

-- COMMAND ----------

SELECT campaign_id, campaign_name, COUNT(*) AS customers
FROM dev.sandbox.campaign_points_configure
GROUP BY 1, 2
ORDER BY 1

-- COMMAND ----------

select * from dev.sandbox.campaign_points_configure

-- COMMAND ----------


