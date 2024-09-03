-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW mgs AS (
    WITH sales AS (
        SELECT
            region_name,
            business_day,
            INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
            material_id,
            material_group_name,
            SUM(amount) AS sales
        FROM gold.transaction.uae_pos_transactions AS t1
        LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
        WHERE
            business_day BETWEEN "2023-08-09" AND "2024-08-08"
            AND material_group_name IN ("PASTA", "INSTANT NOODLE", "CUP NOODLE", "COCONUT OIL")
            AND tayeb_flag = 0
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1, 2, 3, 4, 5
    ),

    gp AS (
        SELECT
            CASE WHEN region = "AUH" THEN "ABU DHABI"
                WHEN region = "ALN" THEN "AL AIN"
                WHEN region = "DXB" THEN "DUBAI"
                ELSE "SHARJAH" END AS region_name,
            year_month,
            material_id,
            gp_wth_chargeback
        FROM gold.business.gross_profit
        WHERE country = 'AE'
        AND year_month BETWEEN 202308 AND 202408
    ),

    combined AS (
        SELECT
            t1.*,
            COALESCE(sales*gp_wth_chargeback/100, 0) AS abs_gp
        FROM sales AS t1
        LEFT JOIN gp AS t2
            ON t1.region_name = t2.region_name
            AND t1.year_month = t2.year_month
            AND t1.material_id = t2.material_id
    )

    SELECT
        material_group_name AS mg_catg,
        ROUND(SUM(CASE
            WHEN material_group_name = "PASTA"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN sales
            WHEN material_group_name = "INSTANT NOODLE"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN sales
            WHEN material_group_name = "CUP NOODLE"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN sales
            WHEN material_group_name = "COCONUT OIL"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN sales
            ELSE 0 END)) AS sales_MAT,
        ROUND(SUM(CASE
            WHEN material_group_name = "PASTA"
                AND business_day BETWEEN "2024-06-08" AND "2024-08-08"
                THEN sales
            WHEN material_group_name = "INSTANT NOODLE"
                AND business_day BETWEEN "2024-06-01" AND "2024-08-08"
                THEN sales
            WHEN material_group_name = "CUP NOODLE"
                AND business_day BETWEEN "2024-06-01" AND "2024-08-08"
                THEN sales
            WHEN material_group_name = "COCONUT OIL"
                AND business_day BETWEEN "2024-06-15" AND "2024-08-08"
                THEN sales
            ELSE 0 END)) AS sales_post_period,
        ROUND(SUM(CASE
            WHEN material_group_name = "PASTA"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN abs_gp
            WHEN material_group_name = "INSTANT NOODLE"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN abs_gp
            WHEN material_group_name = "CUP NOODLE"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN abs_gp
            WHEN material_group_name = "COCONUT OIL"
                AND business_day BETWEEN "2023-08-09" AND "2024-08-08"
                THEN abs_gp
            ELSE 0 END)) AS gp_MAT,
        CASE
            WHEN material_group_name = "PASTA" THEN 0.0150
            WHEN material_group_name = "INSTANT NOODLE" THEN -0.0037
            WHEN material_group_name = "CUP NOODLE" THEN 0.0099
            WHEN material_group_name = "COCONUT OIL" THEN 0.0333
        END AS gp_margin_delta,
        ROUND(gp_margin_delta*sales_post_period) AS incremental_gp
    FROM combined
    GROUP BY 1
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW catg AS (
    WITH sales AS (
        SELECT
            region_name,
            business_day,
            INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
            material_id,
            category_name,
            SUM(amount) AS sales
        FROM gold.transaction.uae_pos_transactions AS t1
        LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
        LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
        WHERE
            business_day BETWEEN "2023-06-05" AND "2024-06-04"
            AND category_name = "WATER"
            AND tayeb_flag = 0
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
        GROUP BY 1, 2, 3, 4, 5
    ),

    gp AS (
        SELECT
            CASE WHEN region = "AUH" THEN "ABU DHABI"
                WHEN region = "ALN" THEN "AL AIN"
                WHEN region = "DXB" THEN "DUBAI"
                ELSE "SHARJAH" END AS region_name,
            year_month,
            material_id,
            gp_wth_chargeback
        FROM gold.business.gross_profit
        WHERE country = 'AE'
        AND year_month BETWEEN 202306 AND 202406
    ),

    combined AS (
        SELECT
            t1.*,
            COALESCE(sales*gp_wth_chargeback/100, 0) AS abs_gp
        FROM sales AS t1
        LEFT JOIN gp AS t2
            ON t1.region_name = t2.region_name
            AND t1.year_month = t2.year_month
            AND t1.material_id = t2.material_id
    )

    SELECT
        category_name AS mg_catg,
        ROUND(SUM(sales)) AS sales_MAT,
        ROUND(SUM(CASE WHEN business_day >= "2024-04-01" THEN sales ELSE 0 END)) AS sales_post_period,
        ROUND(SUM(abs_gp)) AS gp_MAT,
        0.0050 AS gp_margin_delta,
        ROUND(gp_margin_delta*sales_post_period) AS incremental_gp
    FROM combined
    GROUP BY 1
)

-- COMMAND ----------

CREATE OR REPLACE TABLE dev.sandbox.pj_assortment_gp_generated_check AS (
    SELECT * FROM catg
    UNION
    SELECT * FROM mgs
)

-- COMMAND ----------

SELECT * FROM dev.sandbox.pj_assortment_gp_generated_check
