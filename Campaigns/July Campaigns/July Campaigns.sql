-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Lapser Win Back Follow-up

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_lapser_win_back_followup AS (
    WITH lapsers AS (
        SELECT
            t3.customer_key,
            card_key,
            SUM(amount) AS rpc
        FROM gold.transaction.uae_pos_transactions AS t1
        LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        LEFT JOIN gold.customer.maxxing_customer_profile AS t3 ON t1.customer_id = t3.account_key
        LEFT JOIN analytics.segment.customer_segments AS t4 ON t1.customer_id = t4.customer_id
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-08-20"
            AND department_id BETWEEN 1 AND 13
            AND LHRDATE IS NOT NULL
            AND t3.customer_key IS NOT NULL
            AND card_key IS NOT NULL
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
            AND key = 'rfm'
            AND channel = 'pos'
            AND t4.country = 'uae'
            AND month_year = 202407
            AND segment IN ('Lapser - Repeater', 'Lapser - One Timer')
        GROUP BY 1, 2
    ),

    non_mods AS (
        SELECT
            DISTINCT t1.*
        FROM lapsers AS t1
        LEFT JOIN gold.customer.maxxing_customer_profile AS t2 ON t1.customer_key = t2.customer_key
        LEFT JOIN analytics.segment.customer_segments AS t3 ON t2.account_key = t3.customer_id
        WHERE
            key = 'rfm'
            AND t3.country = 'uae'
            AND channel = 'pos'
            AND month_year BETWEEN 202302 AND 202405
            AND t3.segment IN ('VIP', 'Frequentist', 'Slipping Loyalist')
    ),

    mods AS (
        SELECT
            DISTINCT t1.*
        FROM lapsers AS t1
        LEFT JOIN gold.customer.maxxing_customer_profile AS t2 ON t1.customer_key = t2.customer_key
        LEFT JOIN analytics.segment.customer_segments AS t3 ON t2.account_key = t3.customer_id
        WHERE
            key = 'rfm'
            AND t3.country = 'uae'
            AND channel = 'pos'
            AND month_year BETWEEN 202302 AND 202405
            AND t3.segment IN ('Moderate')
        ORDER BY rpc DESC
    )

    SELECT DISTINCT customer_key, card_key, rpc
    FROM non_mods
    ORDER BY rpc DESC
    LIMIT 35000
    -- UNION
    -- SELECT DISTINCT customer_key, card_key, rpc
    -- FROM mods
)

-- COMMAND ----------

SELECT COUNT(*), COUNT(DISTINCT customer_key)
FROM dev.sandbox.pj_lapser_win_back_followup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Newbie Trip Increase Follow-up

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_newbie_trip_increase_followup AS (
    WITH loyaltynewbie AS (
        SELECT
            t2.customer_key,
            t2.card_key,
            MIN(transaction_date) AS mindate
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.customer.maxxing_customer_profile AS t2 ON t1.customer_id = t2.account_key
        WHERE
            business_day BETWEEN '2023-08-01' AND '2024-08-14'
            AND (LHRDATE IS NOT NULL
                OR t2.LHP = 1)
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND t2.customer_key IS NOT NULL
            AND card_key IS NOT NULL
            AND amount > 0
            AND quantity > 0
        GROUP BY 1, 2
        HAVING mindate >= '2024-07-01'
    ),

    last_week_transactions AS (
        SELECT DISTINCT a.customer_key
        FROM gold.transaction.uae_pos_transactions AS a
        JOIN gold.material.material_master AS b on a.product_id = b.material_id
        WHERE
            a.business_day BETWEEN '2024-08-07' AND '2024-08-14'
            AND b.department_id BETWEEN 1 AND 13
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
    ),

    customers AS (
        SELECT
            a.customer_key,
            a.card_key
        FROM loyaltynewbie AS a
        LEFT ANTI JOIN last_week_transactions AS b ON a.customer_key = b.customer_key
    ),

    june_transactions_2 AS (
        SELECT
            a.customer_key,
            SUM(a.amount) AS rpc
        FROM gold.transaction.uae_pos_transactions AS a
        JOIN gold.material.material_master AS b ON a.product_id = b.material_id
        WHERE a.business_day BETWEEN '2024-07-01' AND '2024-08-14'
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
        GROUP BY 1
    )

    SELECT
        a.*,
        b.rpc
    FROM customers AS a 
    JOIN june_transactions_2 AS b ON a.customer_key = b.customer_key
)

-- COMMAND ----------

SELECT COUNT(*), COUNT(DISTINCT customer_key)
FROM dev.sandbox.pj_newbie_trip_increase_followup

-- COMMAND ----------

select * from dev.sandbox.pj_newbie_trip_increase_followup

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


