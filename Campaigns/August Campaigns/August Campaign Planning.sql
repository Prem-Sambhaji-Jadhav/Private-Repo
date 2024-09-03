-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Audience Sizes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Total Customers

-- COMMAND ----------

WITH all_customers AS (
    SELECT customer_key, "Bakery Introduction" AS campaign_name FROM dev.sandbox.pj_bakery_intro
    UNION
    SELECT customer_key, "Delicatessen Introduction" AS campaign_name FROM dev.sandbox.pj_delicatessen_intro
    UNION
    SELECT customer_key, "Fish and Meat Introduction" AS campaign_name FROM dev.sandbox.pj_fish_meat_intro
    UNION
    SELECT customer_key, "IHKP Introduction" AS campaign_name FROM dev.sandbox.pj_ihkp_intro
    UNION
    SELECT customer_key, "Bakery RPC Increase" AS campaign_name FROM dev.sandbox.pj_bakery_rpc_increase
    UNION
    SELECT customer_key, "Delicatessen RPC Increase" AS campaign_name FROM dev.sandbox.pj_delicatessen_rpc_increase
    UNION
    SELECT customer_key, "Fish and Meat RPC Increase" AS campaign_name FROM dev.sandbox.pj_fish_meat_rpc_increase
    UNION
    SELECT customer_key, "IHKP RPC Increase" AS campaign_name FROM dev.sandbox.pj_ihkp_rpc_increase
)

SELECT
    campaign_name,
    COUNT(DISTINCT customer_key) AS customers
FROM all_customers
GROUP BY 1
ORDER BY 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Mutually Exclusive (Intro & RPC)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW unique_customers AS (
    WITH all_customers AS (
        SELECT customer_key, "Bakery Introduction" AS campaign_name FROM dev.sandbox.pj_bakery_intro
        UNION
        SELECT customer_key, "Delicatessen Introduction" AS campaign_name FROM dev.sandbox.pj_delicatessen_intro
        UNION
        SELECT customer_key, "Fish and Meat Introduction" AS campaign_name FROM dev.sandbox.pj_fish_meat_intro
        UNION
        SELECT customer_key, "IHKP Introduction" AS campaign_name FROM dev.sandbox.pj_ihkp_intro
        UNION
        SELECT customer_key, "Bakery RPC Increase" AS campaign_name FROM dev.sandbox.pj_bakery_rpc_increase
        UNION
        SELECT customer_key, "Delicatessen RPC Increase" AS campaign_name FROM dev.sandbox.pj_delicatessen_rpc_increase
        UNION
        SELECT customer_key, "Fish and Meat RPC Increase" AS campaign_name FROM dev.sandbox.pj_fish_meat_rpc_increase
        UNION
        SELECT customer_key, "IHKP RPC Increase" AS campaign_name FROM dev.sandbox.pj_ihkp_rpc_increase
    )


    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "Bakery Introduction"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Bakery Introduction"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "Delicatessen Introduction"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Delicatessen Introduction"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "Fish and Meat Introduction"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Fish and Meat Introduction"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "IHKP Introduction"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "IHKP Introduction"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "Bakery RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Bakery RPC Increase"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "Delicatessen RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Delicatessen RPC Increase"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "Fish and Meat RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Fish and Meat RPC Increase"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, campaign_name
    FROM all_customers
    WHERE campaign_name = "IHKP RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "IHKP RPC Increase"
        GROUP BY 1
    )
);

SELECT
    campaign_name,
    COUNT(DISTINCT customer_key) AS customers
FROM unique_customers
GROUP BY 1
ORDER BY 2

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW ranked_customers AS (
    WITH all_customers AS (
        SELECT customer_key, "Bakery Introduction" AS campaign_name FROM dev.sandbox.pj_bakery_intro
        UNION
        SELECT customer_key, "Delicatessen Introduction" AS campaign_name FROM dev.sandbox.pj_delicatessen_intro
        UNION
        SELECT customer_key, "Fish and Meat Introduction" AS campaign_name FROM dev.sandbox.pj_fish_meat_intro
        UNION
        SELECT customer_key, "IHKP Introduction" AS campaign_name FROM dev.sandbox.pj_ihkp_intro
        UNION
        SELECT customer_key, "Bakery RPC Increase" AS campaign_name FROM dev.sandbox.pj_bakery_rpc_increase
        UNION
        SELECT customer_key, "Delicatessen RPC Increase" AS campaign_name FROM dev.sandbox.pj_delicatessen_rpc_increase
        UNION
        SELECT customer_key, "Fish and Meat RPC Increase" AS campaign_name FROM dev.sandbox.pj_fish_meat_rpc_increase
        UNION
        SELECT customer_key, "IHKP RPC Increase" AS campaign_name FROM dev.sandbox.pj_ihkp_rpc_increase
    )

    SELECT
        campaign_name,
        customer_key,
        ROW_NUMBER() OVER (PARTITION BY campaign_name ORDER BY RAND()) AS rn
    FROM all_customers
);

CREATE OR REPLACE TEMP VIEW campaign_customers AS (
    WITH bakery_intro AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "Bakery Introduction"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE campaign_name = 'Bakery Introduction' AND rn <= 27000 -- 72,648
    ),

    fish_meat_intro AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "Fish and Meat Introduction"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE
            campaign_name = 'Fish and Meat Introduction' AND rn <= 32500 -- 74,801
            AND customer_key NOT IN (SELECT customer_key FROM bakery_intro)
    ),

    fish_meat_rpc_increase AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "Fish and Meat RPC Increase"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE
            campaign_name = 'Fish and Meat RPC Increase' AND rn <= 35000 -- 106869
            AND customer_key NOT IN (SELECT customer_key FROM bakery_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_intro)
    ),

    ihkp_intro AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "IHKP Introduction"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE
            campaign_name = 'IHKP Introduction' AND rn <= 48000 -- 109,250
            AND customer_key NOT IN (SELECT customer_key FROM bakery_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
    ),

    delicatessen_intro AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "Delicatessen Introduction"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE
            campaign_name = 'Delicatessen Introduction' AND rn <= 75000 -- 131,934
            AND customer_key NOT IN (SELECT customer_key FROM bakery_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
            AND customer_key NOT IN (SELECT customer_key FROM ihkp_intro)
    ),

    ihkp_rpc_increase AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "IHKP RPC Increase"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE
            campaign_name = 'IHKP RPC Increase' AND rn <= 52000 -- 146702
            AND customer_key NOT IN (SELECT customer_key FROM bakery_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
            AND customer_key NOT IN (SELECT customer_key FROM ihkp_intro)
            AND customer_key NOT IN (SELECT customer_key FROM delicatessen_intro)
    ),

    delicatessen_rpc_increase AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "Delicatessen RPC Increase"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE
            campaign_name = 'Delicatessen RPC Increase' AND rn <= 100000 -- 151,084
            AND customer_key NOT IN (SELECT customer_key FROM bakery_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
            AND customer_key NOT IN (SELECT customer_key FROM ihkp_intro)
            AND customer_key NOT IN (SELECT customer_key FROM delicatessen_intro)
            AND customer_key NOT IN (SELECT customer_key FROM ihkp_rpc_increase)
    ),

    bakery_rpc_increase AS (
        SELECT campaign_name, customer_key
        FROM unique_customers
        WHERE campaign_name = "Bakery RPC Increase"
        UNION
        SELECT campaign_name, customer_key
        FROM ranked_customers
        WHERE
            campaign_name = 'Bakery RPC Increase' AND rn <= 157505 -- 157,505
            AND customer_key NOT IN (SELECT customer_key FROM bakery_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_intro)
            AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
            AND customer_key NOT IN (SELECT customer_key FROM ihkp_intro)
            AND customer_key NOT IN (SELECT customer_key FROM delicatessen_intro)
            AND customer_key NOT IN (SELECT customer_key FROM ihkp_rpc_increase)
            AND customer_key NOT IN (SELECT customer_key FROM delicatessen_rpc_increase)
    )

    SELECT campaign_name, COUNT(customer_key) AS customers FROM bakery_intro GROUP BY 1
    UNION ALL
    SELECT campaign_name, COUNT(customer_key) AS customers FROM delicatessen_intro GROUP BY 1
    UNION ALL
    SELECT campaign_name, COUNT(customer_key) AS customers FROM fish_meat_intro GROUP BY 1
    UNION ALL
    SELECT campaign_name, COUNT(customer_key) AS customers FROM ihkp_intro GROUP BY 1
    UNION ALL
    SELECT campaign_name, COUNT(customer_key) AS customers FROM bakery_rpc_increase GROUP BY 1
    UNION ALL
    SELECT campaign_name, COUNT(customer_key) AS customers FROM delicatessen_rpc_increase GROUP BY 1
    UNION ALL
    SELECT campaign_name, COUNT(customer_key) AS customers FROM fish_meat_rpc_increase GROUP BY 1
    UNION ALL
    SELECT campaign_name, COUNT(customer_key) AS customers FROM ihkp_rpc_increase GROUP BY 1
);

SELECT
    *,
    SUM(customers) OVER() AS total_customers
FROM campaign_customers

-- Bakery Intro
-- Fish and Meat Intro
-- Fish and Meat RPC Increase
-- IHKP Intro
-- Delicatessen Intro
-- IHKP RPC Increase
-- Delicatessen RPC Increase
-- Bakery RPC Increase

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Mutually Exclusive (RPC)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW unique_customers_2 AS (
    WITH all_customers AS (
        SELECT customer_key, rpc, "Bakery RPC Increase" AS campaign_name FROM dev.sandbox.pj_bakery_rpc_increase
        UNION
        SELECT customer_key, rpc, "Delicatessen RPC Increase" AS campaign_name FROM dev.sandbox.pj_delicatessen_rpc_increase
        UNION
        SELECT customer_key, rpc, "Fish and Meat RPC Increase" AS campaign_name FROM dev.sandbox.pj_fish_meat_rpc_increase
        UNION
        SELECT customer_key, rpc, "IHKP RPC Increase" AS campaign_name FROM dev.sandbox.pj_ihkp_rpc_increase
    )


    SELECT customer_key, rpc, campaign_name
    FROM all_customers
    WHERE campaign_name = "Bakery RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Bakery RPC Increase"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, rpc, campaign_name
    FROM all_customers
    WHERE campaign_name = "Delicatessen RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Delicatessen RPC Increase"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, rpc, campaign_name
    FROM all_customers
    WHERE campaign_name = "Fish and Meat RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "Fish and Meat RPC Increase"
        GROUP BY 1
    )
    UNION
    SELECT customer_key, rpc, campaign_name
    FROM all_customers
    WHERE campaign_name = "IHKP RPC Increase"
    AND customer_key NOT IN (
        SELECT customer_key
        FROM all_customers
        WHERE campaign_name != "IHKP RPC Increase"
        GROUP BY 1
    )
);

SELECT
    campaign_name,
    COUNT(DISTINCT customer_key) AS customers
FROM unique_customers_2
GROUP BY 1
ORDER BY 2

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW ranked_customers_2 AS (
    WITH all_customers AS (
        SELECT customer_key, rpc, "Bakery RPC Increase" AS campaign_name FROM dev.sandbox.pj_bakery_rpc_increase
        UNION
        SELECT customer_key, rpc, "Delicatessen RPC Increase" AS campaign_name FROM dev.sandbox.pj_delicatessen_rpc_increase
        UNION
        SELECT customer_key, rpc, "Fish and Meat RPC Increase" AS campaign_name FROM dev.sandbox.pj_fish_meat_rpc_increase
        UNION
        SELECT customer_key, rpc, "IHKP RPC Increase" AS campaign_name FROM dev.sandbox.pj_ihkp_rpc_increase
    )

    SELECT
        campaign_name,
        customer_key,
        rpc,
        ROW_NUMBER() OVER (PARTITION BY campaign_name ORDER BY RAND()) AS rn
    FROM all_customers
);

CREATE OR REPLACE TEMP VIEW campaign_customers_2 AS (
    WITH fish_meat_rpc_increase AS (
        SELECT * FROM (
            SELECT campaign_name, customer_key, rpc
            FROM unique_customers_2
            WHERE campaign_name = "Fish and Meat RPC Increase"
            UNION
            SELECT campaign_name, customer_key, rpc
            FROM ranked_customers_2
            WHERE campaign_name = 'Fish and Meat RPC Increase' AND rn <= 27000 -- 106869
        ) LIMIT 28421
    ),

    ihkp_rpc_increase AS (
        SELECT * FROM (
            SELECT campaign_name, customer_key, rpc
            FROM unique_customers_2
            WHERE campaign_name = "IHKP RPC Increase"
            UNION
            SELECT campaign_name, customer_key, rpc
            FROM ranked_customers_2
            WHERE
                campaign_name = 'IHKP RPC Increase' AND rn <= 19000 -- 146702
                AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
        ) LIMIT 28421
    ),

    delicatessen_rpc_increase AS (
        SELECT * FROM (
            SELECT campaign_name, customer_key, rpc
            FROM unique_customers_2
            WHERE campaign_name = "Delicatessen RPC Increase"
            UNION
            SELECT campaign_name, customer_key, rpc
            FROM ranked_customers_2
            WHERE
                campaign_name = 'Delicatessen RPC Increase' AND rn <= 29000 -- 151,084
                AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
                AND customer_key NOT IN (SELECT customer_key FROM ihkp_rpc_increase)
        ) LIMIT 28421
    ),

    bakery_rpc_increase AS (
        SELECT * FROM (
            SELECT campaign_name, customer_key, rpc
            FROM unique_customers_2
            WHERE campaign_name = "Bakery RPC Increase"
            UNION
            SELECT campaign_name, customer_key, rpc
            FROM ranked_customers_2
            WHERE
                campaign_name = 'Bakery RPC Increase' AND rn <= 29000 -- 157,505
                AND customer_key NOT IN (SELECT customer_key FROM fish_meat_rpc_increase)
                AND customer_key NOT IN (SELECT customer_key FROM ihkp_rpc_increase)
                AND customer_key NOT IN (SELECT customer_key FROM delicatessen_rpc_increase)
        ) LIMIT 28421
    )

    SELECT campaign_name, customer_key, rpc FROM bakery_rpc_increase
    UNION
    SELECT campaign_name, customer_key, rpc FROM delicatessen_rpc_increase
    UNION
    SELECT campaign_name, customer_key, rpc FROM fish_meat_rpc_increase
    UNION
    SELECT campaign_name, customer_key, rpc FROM ihkp_rpc_increase
);

SELECT
    campaign_name,
    COUNT(customer_key) AS customers,
    SUM(COUNT(customer_key)) OVER() AS total_customers
FROM campaign_customers_2
GROUP BY 1

-- Fish and Meat RPC Increase
-- IHKP RPC Increase
-- Delicatessen RPC Increase
-- Bakery RPC Increase

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_bakery_rpc_boost_final AS (
    SELECT t1.customer_key, t2.card_key, rpc
    FROM campaign_customers_2 AS t1
    JOIN gold.customer.maxxing_customer_profile AS t2 ON t1.customer_key = t2.customer_key
    WHERE campaign_name = "Bakery RPC Increase"
    AND card_key IS NOT NULL
);

CREATE OR REPLACE TABLE dev.sandbox.pj_delicatessen_rpc_boost_final AS (
    SELECT t1.customer_key, t2.card_key, rpc
    FROM campaign_customers_2 AS t1
    JOIN gold.customer.maxxing_customer_profile AS t2 ON t1.customer_key = t2.customer_key
    WHERE campaign_name = "Delicatessen RPC Increase"
    AND card_key IS NOT NULL
);

CREATE OR REPLACE TABLE dev.sandbox.pj_fish_meat_rpc_boost_final AS (
    SELECT t1.customer_key, t2.card_key, rpc
    FROM campaign_customers_2 AS t1
    JOIN gold.customer.maxxing_customer_profile AS t2 ON t1.customer_key = t2.customer_key
    WHERE campaign_name = "Fish and Meat RPC Increase"
    AND card_key IS NOT NULL
);

CREATE OR REPLACE TABLE dev.sandbox.pj_ihkp_rpc_boost_final AS (
    SELECT t1.customer_key, t2.card_key, rpc
    FROM campaign_customers_2 AS t1
    JOIN gold.customer.maxxing_customer_profile AS t2 ON t1.customer_key = t2.customer_key
    WHERE campaign_name = "IHKP RPC Increase"
    AND card_key IS NOT NULL
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Bakery Introduction

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_bakery_intro AS (
    WITH all_customers AS (
        SELECT
            t3.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t3 ON t1.customer_id = t3.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t2.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Moderate | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "Frequentist | Appliances & Laundry Care Shoppers")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    ),

    engaged_customers AS (
        SELECT
            t4.customer_key
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND department_name = "BAKERY"
            AND material_group_name IN ("PRE PACK CAKES", "ARABIC SWEETS", "BOUGHT IN ARABIC", "SPECIALITY BREADS", "WHITE SOFT ROLLS", "SEASONAL CAKES&SPCLS", "BOUGHT IN SAVORIES", "DANISH PASTRIES", "DESSERTS", "OTHER INGREDIENTS", "WHOLE MEAL ROLLS")
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Moderate | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "Frequentist | Appliances & Laundry Care Shoppers")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM all_customers
    WHERE
        customer_key NOT IN (SELECT customer_key FROM engaged_customers)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_VIP_rpc)
        AND customer_key NOT IN (SELECT customer_key FROM dev.sandbox.sss_moderate_trip_increase_aug24)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_frequentist_rpc)
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_bakery_intro

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH new_customers AS (
    SELECT
        t1.customer_key,
        MIN(business_day) AS first_purchase_date
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_name = "BAKERY"
        AND material_group_name IN ("PRE PACK CAKES", "ARABIC SWEETS", "BOUGHT IN ARABIC", "SPECIALITY BREADS", "WHITE SOFT ROLLS", "SEASONAL CAKES&SPCLS", "BOUGHT IN SAVORIES", "DANISH PASTRIES", "DESSERTS", "OTHER INGREDIENTS", "WHOLE MEAL ROLLS")
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Moderate | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "Frequentist | Appliances & Laundry Care Shoppers")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
    HAVING first_purchase_date >= "2024-05-01"
),

first_amount AS (
    SELECT
        t1.customer_key,
        first_purchase_date,
        SUM(amount) AS first_spend
    FROM new_customers AS t1
    JOIN gold.transaction.uae_pos_transactions AS t2
        ON t1.first_purchase_date = t2.business_day
        AND t1.customer_key = t2.customer_key
    JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
    WHERE
        department_name = "BAKERY"
        AND material_group_name IN ("PRE PACK CAKES", "ARABIC SWEETS", "BOUGHT IN ARABIC", "SPECIALITY BREADS", "WHITE SOFT ROLLS", "SEASONAL CAKES&SPCLS", "BOUGHT IN SAVORIES", "DANISH PASTRIES", "DESSERTS", "OTHER INGREDIENTS", "WHOLE MEAL ROLLS")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2
),

weekly_new_customers AS (
    SELECT
        MONTH(first_purchase_date) AS month,
        CASE WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        customer_key,
        first_spend
    FROM first_amount
    GROUP BY 1, 2, 3, 4
)

SELECT
    week,
    ROUND(COUNT(customer_key)/COUNT(DISTINCT month), 1) AS avg_new_customers,
    ROUND(AVG(first_spend), 1) AS avg_first_spend
FROM weekly_new_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name = "BAKERY"
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_bakery_intro AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name = "BAKERY"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_bakery_intro AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name = "BAKERY"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delicatessen Introduction

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_delicatessen_intro AS (
    WITH all_customers AS (
        SELECT
            t3.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t3 ON t1.customer_id = t3.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t2.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Frequentist | Appliances & Laundry Care Shoppers", "Moderate | Baking Enthusiasts")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    ),

    engaged_customers AS (
        SELECT
            t4.customer_key
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND department_name = "DELICATESSEN"
            AND material_group_name IN ('CHEESE DELI', 'CHINESE-KOREAN FOOD', 'BOUGHT IN PREPACKED', 'IH  PASTE&BUTTER NUT', 'OTHER B-IN PREPACKED', 'MARINATED & SALAD PP', 'FILIPINO FOOD', 'IH  PLATTER&HAMPERS')
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Frequentist | Appliances & Laundry Care Shoppers", "Moderate | Baking Enthusiasts")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM all_customers
    WHERE
        customer_key NOT IN (SELECT customer_key FROM engaged_customers)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_VIP_rpc)
        AND customer_key NOT IN (SELECT customer_key FROM dev.sandbox.sss_moderate_trip_increase_aug24)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_frequentist_rpc)
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_delicatessen_intro

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH new_customers AS (
    SELECT
        t1.customer_key,
        MIN(business_day) AS first_purchase_date
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_name = "DELICATESSEN"
        AND material_group_name IN ('CHEESE DELI', 'CHINESE-KOREAN FOOD', 'BOUGHT IN PREPACKED', 'IH  PASTE&BUTTER NUT', 'OTHER B-IN PREPACKED', 'MARINATED & SALAD PP', 'FILIPINO FOOD', 'IH  PLATTER&HAMPERS')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Frequentist | Appliances & Laundry Care Shoppers", "Moderate | Baking Enthusiasts")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
    HAVING first_purchase_date >= "2024-05-01"
),

first_amount AS (
    SELECT
        t1.customer_key,
        first_purchase_date,
        SUM(amount) AS first_spend
    FROM new_customers AS t1
    JOIN gold.transaction.uae_pos_transactions AS t2
        ON t1.first_purchase_date = t2.business_day
        AND t1.customer_key = t2.customer_key
    JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
    WHERE
        department_name = "DELICATESSEN"
        AND material_group_name IN ('CHEESE DELI', 'CHINESE-KOREAN FOOD', 'BOUGHT IN PREPACKED', 'IH  PASTE&BUTTER NUT', 'OTHER B-IN PREPACKED', 'MARINATED & SALAD PP', 'FILIPINO FOOD', 'IH  PLATTER&HAMPERS')
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2
),

weekly_new_customers AS (
    SELECT
        MONTH(first_purchase_date) AS month,
        CASE WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        customer_key,
        first_spend
    FROM first_amount
    GROUP BY 1, 2, 3, 4
)

SELECT
    week,
    ROUND(COUNT(customer_key)/COUNT(DISTINCT month), 1) AS avg_new_customers,
    ROUND(AVG(first_spend), 1) AS avg_first_spend
FROM weekly_new_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name = "DELICATESSEN"
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_delicatessen_intro AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name = "DELICATESSEN"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_delicatessen_intro AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name = "DELICATESSEN"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Fish and Meat Introduction

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_fish_meat_intro AS (
    WITH all_customers AS (
        SELECT
            t3.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t3 ON t1.customer_id = t3.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t2.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "Frequentist | Appliances & Laundry Care Shoppers", "VIP | Grocery & Household Needs")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    ),

    engaged_customers AS (
        SELECT
            t4.customer_key
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND department_name IN ("FISH", "MEAT")
            AND material_group_name IN ('DE FROZEN', 'SMOKED FISH', 'POULTRY OFFALS', 'VALUE ADDED (MEAT)')
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "Frequentist | Appliances & Laundry Care Shoppers", "VIP | Grocery & Household Needs")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM all_customers
    WHERE
        customer_key NOT IN (SELECT customer_key FROM engaged_customers)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_VIP_rpc)
        AND customer_key NOT IN (SELECT customer_key FROM dev.sandbox.sss_moderate_trip_increase_aug24)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_frequentist_rpc)
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_fish_meat_intro

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH new_customers AS (
    SELECT
        t1.customer_key,
        MIN(business_day) AS first_purchase_date
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_name IN ("FISH", "MEAT")
        AND material_group_name IN ('DE FROZEN', 'SMOKED FISH', 'POULTRY OFFALS', 'VALUE ADDED (MEAT)')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "Frequentist | Appliances & Laundry Care Shoppers", "VIP | Grocery & Household Needs")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
    HAVING first_purchase_date >= "2024-05-01"
),

first_amount AS (
    SELECT
        t1.customer_key,
        first_purchase_date,
        SUM(amount) AS first_spend
    FROM new_customers AS t1
    JOIN gold.transaction.uae_pos_transactions AS t2
        ON t1.first_purchase_date = t2.business_day
        AND t1.customer_key = t2.customer_key
    JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
    WHERE
        department_name IN ("FISH", "MEAT")
        AND material_group_name IN ('DE FROZEN', 'SMOKED FISH', 'POULTRY OFFALS', 'VALUE ADDED (MEAT)')
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2
),

weekly_new_customers AS (
    SELECT
        MONTH(first_purchase_date) AS month,
        CASE WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        customer_key,
        first_spend
    FROM first_amount
    GROUP BY 1, 2, 3, 4
)

SELECT
    week,
    ROUND(COUNT(customer_key)/COUNT(DISTINCT month), 1) AS avg_new_customers,
    ROUND(AVG(first_spend), 1) AS avg_first_spend
FROM weekly_new_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name IN ("FISH", "MEAT")
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_fish_meat_intro AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name IN ("FISH", "MEAT")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_fish_meat_intro AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name IN ("FISH", "MEAT")
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #IHKP Introduction

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_ihkp_intro AS (
    WITH all_customers AS (
        SELECT
            t3.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t3 ON t1.customer_id = t3.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t2.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Appliances & Laundry Care Shoppers", "Frequentist | Fruits & Vegetables Lovers", "Moderate | Baking Enthusiasts", "VIP | Busy Millennials", "Frequentist | No Mission Buyers")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    ),

    engaged_customers AS (
        SELECT
            t4.customer_key
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
            AND material_group_name IN ('FRUIT & SALAD BAR', 'SUSHI', 'INTERNATIONAL MEALS', 'INTL BREAKFAST', 'CHARCOAL GRILLS', 'DESSERTS', 'PASTA STATION', 'BOUGHT IN CHILLFOOD', 'CHILLED MEALS', 'ARABIC BREAKFAST', 'IRANIAN CUISINE')
            AND LHRDATE IS NOT NULL
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Appliances & Laundry Care Shoppers", "Frequentist | Fruits & Vegetables Lovers", "Moderate | Baking Enthusiasts", "VIP | Busy Millennials", "Frequentist | No Mission Buyers")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM all_customers
    WHERE
        customer_key NOT IN (SELECT customer_key FROM engaged_customers)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_VIP_rpc)
        AND customer_key NOT IN (SELECT customer_key FROM dev.sandbox.sss_moderate_trip_increase_aug24)
        AND customer_key NOT IN (SELECT customer_key FROM sandbox.nm_frequentist_rpc)
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_ihkp_intro

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH new_customers AS (
    SELECT
        t1.customer_key,
        MIN(business_day) AS first_purchase_date
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
        AND material_group_name IN ('FRUIT & SALAD BAR', 'SUSHI', 'INTERNATIONAL MEALS', 'INTL BREAKFAST', 'CHARCOAL GRILLS', 'DESSERTS', 'PASTA STATION', 'BOUGHT IN CHILLFOOD', 'CHILLED MEALS', 'ARABIC BREAKFAST', 'IRANIAN CUISINE')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Appliances & Laundry Care Shoppers", "Frequentist | Fruits & Vegetables Lovers", "Moderate | Baking Enthusiasts", "VIP | Busy Millennials", "Frequentist | No Mission Buyers")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
    HAVING first_purchase_date >= "2024-05-01"
),

first_amount AS (
    SELECT
        t1.customer_key,
        first_purchase_date,
        SUM(amount) AS first_spend
    FROM new_customers AS t1
    JOIN gold.transaction.uae_pos_transactions AS t2
        ON t1.first_purchase_date = t2.business_day
        AND t1.customer_key = t2.customer_key
    JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
    WHERE
        department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
        AND material_group_name IN ('FRUIT & SALAD BAR', 'SUSHI', 'INTERNATIONAL MEALS', 'INTL BREAKFAST', 'CHARCOAL GRILLS', 'DESSERTS', 'PASTA STATION', 'BOUGHT IN CHILLFOOD', 'CHILLED MEALS', 'ARABIC BREAKFAST', 'IRANIAN CUISINE')
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2
),

weekly_new_customers AS (
    SELECT
        MONTH(first_purchase_date) AS month,
        CASE WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(first_purchase_date, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        customer_key,
        first_spend
    FROM first_amount
    GROUP BY 1, 2, 3, 4
)

SELECT
    week,
    ROUND(COUNT(customer_key)/COUNT(DISTINCT month), 1) AS avg_new_customers,
    ROUND(AVG(first_spend), 1) AS avg_first_spend
FROM weekly_new_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_ihkp_intro AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_ihkp_intro AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Bakery RPC Increase

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_bakery_rpc_increase AS (
    WITH top_buyers AS (
        SELECT
            t4.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND department_name = "BAKERY"
            AND material_group_name IN ('BOUGHT IN CAKES', 'DRY CAKES', 'SAVOURIES', 'DOUGHNUTS', 'COOKIES', 'ARABIC SAVOURIES')
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Moderate | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "Frequentist | Appliances & Laundry Care Shoppers")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM top_buyers
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_bakery_rpc_increase

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH weekly_customers AS (
    SELECT
        MONTH(business_day) AS month,
        CASE WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND department_name = "BAKERY"
        AND material_group_name IN ('BOUGHT IN CAKES', 'DRY CAKES', 'SAVOURIES', 'DOUGHNUTS', 'COOKIES', 'ARABIC SAVOURIES')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Moderate | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "Frequentist | Appliances & Laundry Care Shoppers")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1, 2, 3
)

SELECT
    week,
    ROUND(AVG(rpc), 1) AS avg_rpc,
    ROUND(AVG(atv), 1) AS avg_atv
FROM weekly_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

WITH customers AS (
    SELECT
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.segment.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28
        AND department_name = "BAKERY"
        AND material_group_name IN ('BOUGHT IN CAKES', 'DRY CAKES', 'SAVOURIES', 'DOUGHNUTS', 'COOKIES', 'ARABIC SAVOURIES')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Moderate | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "Frequentist | Appliances & Laundry Care Shoppers")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
)

SELECT
    COUNT(customer_key) AS total_customers,
    COUNT(CASE WHEN rpc >= 30 THEN customer_key END) AS organic_customers,
    ROUND(organic_customers/total_customers, 4) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 30 THEN rpc END), 1) AS test_rpc
FROM customers

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name = "BAKERY"
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_bakery_rpc_increase AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name = "BAKERY"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_bakery_rpc_increase AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name = "BAKERY"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delicatessen RPC Increase

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_delicatessen_rpc_increase AS (
    WITH top_buyers AS (
        SELECT
            t4.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND department_name = "DELICATESSEN"
            AND material_group_name IN ('WHITE CHEESE (BULK)', 'IDLY/DOSA BATTER', 'COOKED  TURKEY', 'GREEN OLIVES', 'COOKED BEEF', 'CHAPPATHI', 'HARD CHEESE DELI', 'MALABAR PARATHA', 'PICKLED VEGETABLES', 'SEMI-HARD CHEESE', 'IH SALAD & MARINATED', 'BEEF PREPACKED', 'WHEAT PARATHA', 'BLACK OLIVES', 'IDIYAPPAM', 'SOFT & GRATED CHEESE', 'COOKED CHICKEN', 'IH CHEESE')
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Frequentist | Appliances & Laundry Care Shoppers", "Moderate | Baking Enthusiasts")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM top_buyers
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_delicatessen_rpc_increase

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH weekly_customers AS (
    SELECT
        MONTH(business_day) AS month,
        CASE WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND department_name = "DELICATESSEN"
        AND material_group_name IN ('WHITE CHEESE (BULK)', 'IDLY/DOSA BATTER', 'COOKED  TURKEY', 'GREEN OLIVES', 'COOKED BEEF', 'CHAPPATHI', 'HARD CHEESE DELI', 'MALABAR PARATHA', 'PICKLED VEGETABLES', 'SEMI-HARD CHEESE', 'IH SALAD & MARINATED', 'BEEF PREPACKED', 'WHEAT PARATHA', 'BLACK OLIVES', 'IDIYAPPAM', 'SOFT & GRATED CHEESE', 'COOKED CHICKEN', 'IH CHEESE')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Frequentist | Appliances & Laundry Care Shoppers", "Moderate | Baking Enthusiasts")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1, 2, 3
)

SELECT
    week,
    ROUND(AVG(rpc), 1) AS avg_rpc,
    ROUND(AVG(atv), 1) AS avg_atv
FROM weekly_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

WITH customers AS (
    SELECT
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.segment.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28
        AND department_name = "DELICATESSEN"
        AND material_group_name IN ('WHITE CHEESE (BULK)', 'IDLY/DOSA BATTER', 'COOKED  TURKEY', 'GREEN OLIVES', 'COOKED BEEF', 'CHAPPATHI', 'HARD CHEESE DELI', 'MALABAR PARATHA', 'PICKLED VEGETABLES', 'SEMI-HARD CHEESE', 'IH SALAD & MARINATED', 'BEEF PREPACKED', 'WHEAT PARATHA', 'BLACK OLIVES', 'IDIYAPPAM', 'SOFT & GRATED CHEESE', 'COOKED CHICKEN', 'IH CHEESE')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "VIP | Grocery & Household Needs", "Frequentist | Appliances & Laundry Care Shoppers", "Moderate | Baking Enthusiasts")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
)

SELECT
    COUNT(customer_key) AS total_customers,
    COUNT(CASE WHEN rpc >= 50 THEN customer_key END) AS organic_customers,
    ROUND(organic_customers/total_customers, 4) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 50 THEN rpc END), 1) AS test_rpc
FROM customers

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name = "DELICATESSEN"
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_delicatessen_rpc_increase AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name = "DELICATESSEN"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_delicatessen_rpc_increase AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name = "DELICATESSEN"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Fish and Meat RPC Increase

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_fish_meat_rpc_increase AS (
    WITH top_buyers AS (
        SELECT
            t4.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND department_name IN ('FISH', 'MEAT')
            AND material_group_name IN ('WHOLE FISH', 'SHELL FISH')
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "Frequentist | Appliances & Laundry Care Shoppers", "VIP | Grocery & Household Needs")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM top_buyers
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_fish_meat_rpc_increase

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH weekly_customers AS (
    SELECT
        MONTH(business_day) AS month,
        CASE WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND department_name IN ("FISH", "MEAT")
        AND material_group_name IN ('WHOLE FISH', 'SHELL FISH')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "Frequentist | Appliances & Laundry Care Shoppers", "VIP | Grocery & Household Needs")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1, 2, 3
)

SELECT
    week,
    ROUND(AVG(rpc), 1) AS avg_rpc,
    ROUND(AVG(atv), 1) AS avg_atv
FROM weekly_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

WITH customers AS (
    SELECT
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.segment.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28
        AND department_name IN ("FISH", "MEAT")
        AND material_group_name IN ('WHOLE FISH', 'SHELL FISH')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Fruits & Vegetables Lovers", "VIP | Busy Millennials", "Frequentist | Appliances & Laundry Care Shoppers", "VIP | Grocery & Household Needs")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
)

SELECT
    COUNT(customer_key) AS total_customers,
    COUNT(CASE WHEN rpc >= 100 THEN customer_key END) AS organic_customers,
    ROUND(organic_customers/total_customers, 4) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 100 THEN rpc END), 1) AS test_rpc
FROM customers

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name IN ("FISH", "MEAT")
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_fish_meat_rpc_increase AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name IN ("FISH", "MEAT")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_fish_meat_rpc_increase AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name IN ("FISH", "MEAT")
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #IHKP RPC Increase

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_ihkp_rpc_increase AS (
    WITH top_buyers AS (
        SELECT
            t4.customer_key,
            ROUND(SUM(amount), 2) AS rpc,
            ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
        JOIN gold.customer.maxxing_customer_profile AS t4 ON t1.customer_id = t4.account_key
        WHERE
            business_day BETWEEN "2023-08-01" AND "2024-07-31"
            AND LHRDATE IS NOT NULL
            AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
            AND material_group_name IN ('INDIAN MEALS', 'ROTISERRIE', 'ORIENTAL MEALS', 'INDIAN SNACKS', 'INDIAN SWEETS DELI', 'BROASTS', 'INDIAN NAMKEEN', 'INDIAN BREAKFAST', 'TANDOOR', 'SANDWICHES', 'BOUGHT IN SAVOURIES')
            AND key = 'micro-segment'
            AND channel = 'pos'
            AND t3.country = 'uae'
            AND month_year = 202404
            AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Appliances & Laundry Care Shoppers", "Frequentist | Fruits & Vegetables Lovers", "Moderate | Baking Enthusiasts", "VIP | Busy Millennials", "Frequentist | No Mission Buyers")
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1
    )

    SELECT *
    FROM top_buyers
)

-- COMMAND ----------

SELECT COUNT(*) FROM dev.sandbox.pj_ihkp_rpc_increase

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Organic Customers

-- COMMAND ----------

WITH weekly_customers AS (
    SELECT
        MONTH(business_day) AS month,
        CASE WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 1 AND 7 THEN "W1"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 8 AND 14 THEN "W2"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 15 AND 21 THEN "W3"
            WHEN INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28 THEN "W4"
            ELSE "W5" END AS week,
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
        AND material_group_name IN ('INDIAN MEALS', 'ROTISERRIE', 'ORIENTAL MEALS', 'INDIAN SNACKS', 'INDIAN SWEETS DELI', 'BROASTS', 'INDIAN NAMKEEN', 'INDIAN BREAKFAST', 'TANDOOR', 'SANDWICHES', 'BOUGHT IN SAVOURIES')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Appliances & Laundry Care Shoppers", "Frequentist | Fruits & Vegetables Lovers", "Moderate | Baking Enthusiasts", "VIP | Busy Millennials", "Frequentist | No Mission Buyers")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1, 2, 3
)

SELECT
    week,
    ROUND(AVG(rpc), 1) AS avg_rpc,
    ROUND(AVG(atv), 1) AS avg_atv
FROM weekly_customers
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

WITH customers AS (
    SELECT
        t1.customer_key,
        ROUND(SUM(amount), 2) AS rpc,
        ROUND(SUM(amount)/COUNT(DISTINCT transaction_id), 2) AS atv
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN analytics.segment.customer_segments AS t3 ON t1.customer_id = t3.customer_id
    WHERE
        business_day BETWEEN "2024-05-01" AND "2024-07-31"
        AND INT(SUBSTRING(business_day, 9, 10)) BETWEEN 22 AND 28
        AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
        AND material_group_name IN ('INDIAN MEALS', 'ROTISERRIE', 'ORIENTAL MEALS', 'INDIAN SNACKS', 'INDIAN SWEETS DELI', 'BROASTS', 'INDIAN NAMKEEN', 'INDIAN BREAKFAST', 'TANDOOR', 'SANDWICHES', 'BOUGHT IN SAVOURIES')
        AND key = 'micro-segment'
        AND channel = 'pos'
        AND t3.country = 'uae'
        AND month_year = 202404
        AND segment IN ("Frequentist | Baking Enthusiasts", "Frequentist | Appliances & Laundry Care Shoppers", "Frequentist | Fruits & Vegetables Lovers", "Moderate | Baking Enthusiasts", "VIP | Busy Millennials", "Frequentist | No Mission Buyers")
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND quantity > 0
        AND amount > 0
    GROUP BY 1
)

SELECT
    COUNT(customer_key) AS total_customers,
    COUNT(CASE WHEN rpc >= 50 THEN customer_key END) AS organic_customers,
    ROUND(organic_customers/total_customers, 4) AS organic_conversion_perc,
    ROUND(AVG(CASE WHEN rpc >= 50 THEN rpc END), 1) AS test_rpc
FROM customers

-- COMMAND ----------

-- WITH spends as (
--     SELECT
--         customer_key,
--         transaction_id,
--         product_id,
--         SUM(amount) AS spend
--     FROM gold.transaction.uae_pos_transactions AS t1
--     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
--     WHERE
--         business_day BETWEEN "2024-02-01" AND "2024-07-31"
--         AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
--         AND transaction_type IN ("SALE", "SELL_MEDIA")
--         AND amount > 0
--         AND quantity > 0
--     GROUP BY 1, 2, 3
-- ),

-- promo_table AS (
--     SELECT
--         transaction_id,
--         product_id,
--         CONCAT(SUBSTRING(business_date, 1, 4), '-',
--             SUBSTRING(business_date, 5, 2), '-',
--             SUBSTRING(business_date, 7, 2)) AS formatted_date
--     FROM gold.marketing.uae_pos_sales_campaign
--     WHERE
--         void_flag IS NULL
--         AND campaign_id IS NOT NULL
--         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
--         AND (pm_reason_code != "HAPPINESS VOUCHER")
--         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
--         AND business_date BETWEEN "20240201" AND "20240731"
--     GROUP BY transaction_id, product_id, business_date
-- ),

-- final_cust AS (
--     SELECT
--         t1.customer_key,
--         SUM(spend) AS total_spend,
--         SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
--         ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
--     FROM dev.sandbox.pj_ihkp_rpc_increase AS t1
--     JOIN spends AS t2 ON t1.customer_key = t2.customer_key
--     LEFT JOIN promo_table AS t3
--         ON t2.transaction_id = t3.transaction_id
--         AND t2.product_id = t3.product_id
--     GROUP BY 1
-- )

-- SELECT
--     COUNT(customer_key) AS total_customers,
--     COUNT(CASE WHEN organic_spend_perc >= 50 THEN customer_key END) AS organic_customers
-- FROM final_cust

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Highest Weekday Orders

-- COMMAND ----------

WITH spends as (
    SELECT
        customer_key,
        transaction_id,
        product_id,
        SUM(amount) AS spend
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE
        business_day BETWEEN "2024-02-01" AND "2024-07-31"
        AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
    GROUP BY 1, 2, 3
),

promo_table AS (
    SELECT
        transaction_id,
        product_id,
        CONCAT(SUBSTRING(business_date, 1, 4), '-',
            SUBSTRING(business_date, 5, 2), '-',
            SUBSTRING(business_date, 7, 2)) AS formatted_date
    FROM gold.marketing.uae_pos_sales_campaign
    WHERE
        void_flag IS NULL
        AND campaign_id IS NOT NULL
        AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
        AND (pm_reason_code != "HAPPINESS VOUCHER")
        AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
        AND business_date BETWEEN "20240201" AND "20240731"
    GROUP BY transaction_id, product_id, business_date
),

final_cust AS (
    SELECT
        t1.customer_key,
        SUM(spend) AS total_spend,
        SUM(CASE WHEN formatted_date IS NOT NULL THEN spend ELSE 0 END) AS organic_spend,
        ROUND(organic_spend/total_spend*100, 2) AS organic_spend_perc
    FROM dev.sandbox.pj_ihkp_rpc_increase AS t1
    JOIN spends AS t2 ON t1.customer_key = t2.customer_key
    LEFT JOIN promo_table AS t3
        ON t2.transaction_id = t3.transaction_id
        AND t2.product_id = t3.product_id
    GROUP BY 1
    HAVING organic_spend_perc >= 50
)

SELECT
    DAYOFWEEK(business_day) AS weekday,
    COUNT(DISTINCT transaction_id) AS orders,
    ROUND(SUM(amount)) AS sales
FROM final_cust AS t1
JOIN gold.transaction.uae_pos_transactions AS t2 ON t1.customer_key = t2.customer_key
JOIN gold.material.material_master AS t3 ON t2.product_id = t3.material_id
WHERE
    business_day BETWEEN "2024-02-01" AND "2024-07-31"
    AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1
ORDER BY 1

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


