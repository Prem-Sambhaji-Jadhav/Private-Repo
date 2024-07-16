-- Databricks notebook source
-- MAGIC %md
-- MAGIC #MOCD Acquisition

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_mocd_acquisition AS (
    WITH main_data AS (
        SELECT
            DISTINCT t1.mobile,
            t1.customer_id,
            segment,
            card_key,
            SUM(amount) AS rpc,
            SUM(amount)/COUNT(DISTINCT transaction_id) AS atv
        FROM sandbox.am_acquisition AS t1
        LEFT JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
        LEFT JOIN gold.pos_transactions AS t3 ON t1.customer_id = t3.customer_id
        LEFT JOIN gold.material_master AS t4 ON t3.product_id = t4.material_id
        LEFT JOIN analytics.customer_segments AS t5 ON t1.customer_id = t5.customer_id
        WHERE
            business_day BETWEEN "2024-01-01" AND "2024-06-26"
            AND department_id BETWEEN 1 AND 13
            AND key = 'rfm'
            AND channel = 'pos'
            AND t5.country = 'uae'
            AND month_year = 202405
            AND LHRDATE IS NULL
        GROUP BY 1, 2, 3, 4
        ORDER BY rpc DESC
    ),

    non_mods AS (
        SELECT *
        FROM main_data
        WHERE segment IN ("VIP", "Frequentist", "Slipping Loyalist")
    ),

    mods AS (
        SELECT *
        FROM main_data
        WHERE segment = "Moderate"
        LIMIT 14779
    )

    SELECT *
    FROM non_mods
    UNION
    SELECT *
    FROM mods
)

-- COMMAND ----------

SELECT COUNT(*), COUNT(DISTINCT mobile)
FROM sandbox.pj_mocd_acquisition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #MOCD Engagement

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_mocd_engagement AS (
    SELECT
        DISTINCT t1.mobile,
        t1.customer_id,
        card_key,
        SUM(amount) AS rpc,
        SUM(amount)/COUNT(DISTINCT transaction_id) AS atv
    FROM sandbox.am_engagement AS t1
    LEFT JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
    LEFT JOIN gold.pos_transactions AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2023-05-01" AND "2024-06-26"
        AND LHRDATE IS NOT NULL
    GROUP BY 1, 2, 3
)

-- COMMAND ----------

SELECT COUNT(*), COUNT(DISTINCT mobile)
FROM sandbox.pj_mocd_engagement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Newbie Trip Increase

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_newbie_trip_increase AS (
    WITH loyaltynewbie AS (
        SELECT
            t2.mobile,
            t2.card_key,
            MIN(transaction_date) AS mindate
        FROM gold.pos_transactions AS t1
        JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
        WHERE
            business_day BETWEEN '2023-06-01' AND '2024-06-26'
            AND (LHRDATE IS NOT NULL
                OR t2.LHP = 1)
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1, 2
        HAVING mindate >= '2024-05-01'
    ),

    last_week_transactions AS (
        SELECT DISTINCT a.mobile
        FROM gold.pos_transactions AS a
        JOIN gold.material_master AS b on a.product_id = b.material_id
        WHERE
            a.business_day BETWEEN '2024-06-20' AND '2024-06-26'
            AND b.department_id BETWEEN 1 AND 13
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
    ),

    customers AS (
        SELECT
            a.mobile,
            a.card_key
        FROM loyaltynewbie AS a
        LEFT ANTI JOIN last_week_transactions AS b ON a.mobile = b.mobile
    ),

    june_transactions_2 AS (
        SELECT
            a.mobile,
            SUM(a.amount) AS rpc
        FROM gold.pos_transactions AS a
        JOIN gold.material_master AS b ON a.product_id=b.material_id
        WHERE a.business_day BETWEEN '2024-05-01' AND '2024-06-26'
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
        GROUP BY 1
    )

    SELECT
        a.*,
        b.rpc
    FROM customers AS a 
    JOIN june_transactions_2 AS b ON a.mobile = b.mobile
)

-- COMMAND ----------

SELECT COUNT(*), COUNT(DISTINCT mobile)
FROM sandbox.pj_newbie_trip_increase

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Lapser Win Back

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_lapser_win_back AS (
    WITH lapsers AS (
        SELECT
            t3.mobile,
            t3.account_key AS customer_id,
            card_key,
            SUM(amount) AS rpc,
            SUM(amount)/COUNT(DISTINCT transaction_id) AS atv
        FROM gold.pos_transactions AS t1
        LEFT JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
        LEFT JOIN gold.customer_profile AS t3 ON t1.customer_id = t3.account_key
        LEFT JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
        WHERE
            business_day BETWEEN "2023-06-01" AND "2024-06-26"
            AND department_id BETWEEN 1 AND 13
            AND LHRDATE IS NOT NULL
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
            AND key = 'rfm'
            AND channel = 'pos'
            AND t4.country = 'uae'
            AND month_year = 202405
            AND segment IN ('Lapser - Repeater', 'Lapser - One Timer')
        GROUP BY 1, 2, 3
    ),

    non_mods AS (
        SELECT
            DISTINCT t1.*
        FROM lapsers AS t1
        LEFT JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
        WHERE
            key = 'rfm'
            AND country = 'uae'
            AND channel = 'pos'
            AND month_year BETWEEN 202302 AND 202404
            AND t2.segment IN ('VIP', 'Frequentist', 'Slipping Loyalist') -- 24,317
    ),

    mods AS (
        SELECT
            DISTINCT t1.*
        FROM lapsers AS t1
        LEFT JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
        WHERE
            key = 'rfm'
            AND country = 'uae'
            AND channel = 'pos'
            AND month_year BETWEEN 202302 AND 202404
            AND t2.segment IN ('Moderate')
        ORDER BY rpc DESC
        LIMIT 49000
    )

    SELECT *
    FROM non_mods
    UNION
    SELECT *
    FROM mods
)

-- COMMAND ----------

SELECT COUNT(*), COUNT(DISTINCT mobile)
FROM sandbox.pj_lapser_win_back

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


