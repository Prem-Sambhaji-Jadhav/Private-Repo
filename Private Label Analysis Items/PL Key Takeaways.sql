-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Creating sandbox table with the data till November

-- COMMAND ----------

-- drop table if exists sandbox.pj_pl_transactions_info_nov;

-- create table sandbox.pj_pl_transactions_info_nov as (

-- select t1.transaction_id, t1.transaction_date, t1.business_day, t1.billed_amount, t1.product_id,
--         t1.amount, t1.quantity, t1.unit_price, t1.regular_unit_price, t1.actual_unit_price,
--         t1.customer_id, t2.material_name, t2.material_group_id, t2.material_group_name,
--         t2.category_id, t2.category_name, t2.department_id, t2.department_name, t2.brand,
--         t4.store_id, t4.store_name, t4.region_name, t4.broad_segment, t4.sub_segment,
--         case when t3.material_id is null then 0 else 1 end as pl
-- from gold.pos_transactions t1
-- join gold.material_master t2
-- on t1.product_id = t2.material_id
-- left join sandbox.pj_private_label_material_id_raw t3
-- on t1.product_id = t3.material_id
-- join gold.store_master t4
-- on t1.store_id = t4.store_id
-- where t1.business_day between "2022-01-01" and "2023-11-30"
-- and round(t1.amount, 0) > 0
-- and t1.quantity > 0
-- and department_name not in ('018','EX.EKPHK','ADVANCE / EXCHANGE DEPOSITS','FRUIT & VEGETABLES','HARD GOODS','PERFUMES COSMETICS & JEWELLERY','MALL LEASE','GIFT CARD','CHARITY','RESTAURANT','FOUNDATION PROGRAM','016','THE KITCHEN','HUDA SHIPPING')
-- )

-- COMMAND ----------

WITH cte AS (SELECT YEAR(business_day) AS year, category_name,
                    COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS pl_materials,
                    COUNT(DISTINCT product_id) AS num_materials,
                    ROUND(pl_materials/num_materials, 4) AS SKU_contribution
            FROM sandbox.pj_pl_transactions_info_sept
            WHERE MONTH(business_day) < 10
            GROUP BY year, category_name)

SELECT category_name,
        MAX(CASE WHEN year = 2022 THEN SKU_contribution END) AS 2022_,
        MAX(CASE WHEN year = 2023 THEN SKU_contribution END) AS 2023_
FROM cte
GROUP BY category_name
HAVING 2022_ > 0
AND 2023_ > 0
ORDER BY 2023_ DESC

-- COMMAND ----------

WITH cte AS (SELECT YEAR(business_day) AS year_info, category_name,
                    ROUND(SUM(amount),0) AS total_sales,
                    ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales
            FROM sandbox.pj_pl_transactions_info_sept
            WHERE MONTH(business_day) < 10
            AND customer_id IS NOT NULL
            GROUP BY year_info, category_name
            HAVING pl_sales > 0)

SELECT category_name,
        MAX(CASE WHEN year_info = 2022 THEN total_sales END) AS 2022_sales,
        MAX(CASE WHEN year_info = 2022 THEN pl_sales END) AS 2022_pl_sales,
        MAX(CASE WHEN year_info = 2023 THEN total_sales END) AS 2023_sales,
        MAX(CASE WHEN year_info = 2023 THEN pl_sales END) AS 2023_pl_sales,
        ROUND((2023_pl_sales - 2022_pl_sales)/2022_pl_sales, 4) AS YoY
FROM cte
GROUP BY category_name
ORDER BY category_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Data Models for PPT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Slide 4

-- COMMAND ----------

WITH cte AS (SELECT YEAR(business_day) AS year_info, region_name, store_name,
                    ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
                    ROUND(SUM(amount),0) AS total_sales
              FROM sandbox.pj_pl_transactions_info_nov
              WHERE MONTH(business_day) < 12
              GROUP BY year_info, region_name, store_name
              HAVING pl_sales > 0)

SELECT region_name, store_name,
        MAX(CASE WHEN year_info = "2022" THEN pl_sales END) AS 2022_pl_sales,
        MAX(CASE WHEN year_info = "2022" THEN total_sales END) AS 2022_total_sales,
        MAX(CASE WHEN year_info = "2023" THEN pl_sales END) AS 2023_pl_sales,
        MAX(CASE WHEN year_info = "2023" THEN total_sales END) AS 2023_total_sales,
        ROUND(2022_pl_sales/2022_total_sales,4) AS 2022_pl_sales_contribution,
        ROUND(2023_pl_sales/2023_total_sales,4) AS 2023_pl_sales_contribution,
        (2023_pl_sales_contribution - 2022_pl_sales_contribution) AS contribution_change,
        ROUND((2023_pl_sales - 2022_pl_sales)/2022_pl_sales,4) AS sales_change_YoY
FROM cte
GROUP BY region_name, store_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Slide 9-13

-- COMMAND ----------

SELECT category_name,
        ROUND(SUM(amount),0) AS total_sales,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        COUNT(DISTINCT customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS pl_customers
FROM sandbox.pj_pl_transactions_info_nov
WHERE YEAR(business_day) = 2023
AND customer_id IS NOT NULL
GROUP BY category_name
HAVING pl_sales > 0
ORDER BY category_name

-- COMMAND ----------

WITH cte AS (SELECT segment, category_name,
                    ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
                    ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS non_pl_sales
            FROM sandbox.pj_pl_transactions_info_nov AS t1
            JOIN analytics.customer_segments AS t2
            ON t1.customer_id = t2.customer_id
            WHERE key = 'rfm'
            AND channel = 'pos'
            AND country = 'uae'
            AND month_year = '202311'
            AND YEAR(business_day) = 2023
            GROUP BY segment, category_name
            HAVING pl_sales > 0)

SELECT category_name,
        MAX(CASE WHEN segment = "VIP" THEN pl_sales END) AS VIP_pl_sales,
        MAX(CASE WHEN segment = "VIP" THEN non_pl_sales END) AS VIP_non_pl_sales,
        MAX(CASE WHEN segment = "Frequentist" THEN pl_sales END) AS Frequentist_pl_sales,
        MAX(CASE WHEN segment = "Frequentist" THEN non_pl_sales END) AS Frequentist_non_pl_sales,
        MAX(CASE WHEN segment = "Splurger" THEN pl_sales END) AS Splurger_pl_sales,
        MAX(CASE WHEN segment = "Splurger" THEN non_pl_sales END) AS Splurger_non_pl_sales,
        MAX(CASE WHEN segment = "Moderate" THEN pl_sales END) AS Moderate_pl_sales,
        MAX(CASE WHEN segment = "Moderate" THEN non_pl_sales END) AS Moderate_non_pl_sales
FROM cte
GROUP BY category_name
ORDER BY category_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Slide 14-16

-- COMMAND ----------

WITH cte AS (SELECT nationality_group, category_name,
                    ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
                    ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS non_pl_sales
            FROM sandbox.pj_pl_transactions_info_nov AS t1
            JOIN gold.customer_profile AS t2
            ON t1.customer_id = t2.account_key
            WHERE nationality_group NOT IN ("NA", "OTHERS")
            AND YEAR(business_day) = 2023
            GROUP BY nationality_group, category_name
            HAVING pl_sales > 0)

SELECT category_name,
        MAX(CASE WHEN nationality_group = "ARABS" THEN pl_sales END) AS Arab_pl_sales,
        MAX(CASE WHEN nationality_group = "ARABS" THEN non_pl_sales END) AS Arab_non_pl_sales,
        MAX(CASE WHEN nationality_group = "EMIRATI" THEN pl_sales END) AS Emirati_pl_sales,
        MAX(CASE WHEN nationality_group = "EMIRATI" THEN non_pl_sales END) AS Emirati_non_pl_sales,
        MAX(CASE WHEN nationality_group = "INDIAN" THEN pl_sales END) AS Indian_pl_sales,
        MAX(CASE WHEN nationality_group = "INDIAN" THEN non_pl_sales END) AS Indian_non_pl_sales,
        MAX(CASE WHEN nationality_group = "FILIPINO" THEN pl_sales END) AS Filipino_pl_sales,
        MAX(CASE WHEN nationality_group = "FILIPINO" THEN non_pl_sales END) AS Filipino_non_pl_sales
FROM cte
GROUP BY category_name
ORDER BY category_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Slide 7

-- COMMAND ----------

WITH cte AS (SELECT YEAR(business_day) AS year_info, department_name, category_name,
                    ROUND(SUM(amount),0) AS total_sales,
                    ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
                    COUNT(DISTINCT customer_id) AS total_customers,
                    COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS pl_customers,
                    ROUND(pl_customers/total_customers,4) AS pl_customer_penetration,
                    COUNT(DISTINCT product_id) AS num_materials,
                    COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS pl_materials,
                    ROUND(pl_materials/num_materials,4) AS pl_materials_penetration
            FROM sandbox.pj_pl_transactions_info_nov
            WHERE MONTH(business_day) < 12
            GROUP BY year_info, department_name, category_name
            HAVING pl_sales > 0
            ORDER BY category_name)

SELECT department_name, category_name,
        MAX(CASE WHEN year_info = 2022 THEN total_sales END) AS 2022_sales,
        MAX(CASE WHEN year_info = 2022 THEN pl_sales END) AS 2022_pl_sales,
        MAX(CASE WHEN year_info = 2023 THEN total_sales END) AS 2023_sales,
        MAX(CASE WHEN year_info = 2023 THEN pl_sales END) AS 2023_pl_sales,
        ROUND((2023_pl_sales - 2022_pl_sales)/2022_pl_sales,4) AS YoY_sales_change,
        ROUND(2023_pl_sales/2023_sales,4) AS 2023_pl_sales_penetration,
        MAX(CASE WHEN year_info = 2023 THEN pl_customer_penetration END) AS 2023_pl_customer_penetration,
        MAX(CASE WHEN year_info = 2023 THEN pl_materials_penetration END) AS 2023_pl_materials_penetration
FROM cte
GROUP BY department_name, category_name
ORDER BY category_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Slide 17

-- COMMAND ----------

SELECT SUM(is_one_timer) AS total_one_timers,
        SUM(is_pl_one_timer) AS pl_one_timers

FROM (SELECT customer_id,
              CASE WHEN COUNT(DISTINCT transaction_id) = 1 THEN 1 ELSE 0 END AS is_one_timer,
              CASE WHEN COUNT(DISTINCT transaction_id) FILTER(WHERE pl=1) = 1 THEN 1 ELSE 0 END AS is_pl_one_timer
      FROM sandbox.pj_pl_transactions_info_nov
      WHERE customer_id IS NOT NULL
      AND YEAR(business_day) = 2023
      GROUP BY customer_id)

-- COMMAND ----------

SELECT COUNT(DISTINCT customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS total_pl_customers
FROM sandbox.pj_pl_transactions_info_nov
WHERE YEAR(business_day) = 2023

-- COMMAND ----------

WITH customer_num_purchases AS (SELECT DISTINCT customer_id,
                                        COUNT(DISTINCT product_id) AS pl_items_purchased,
                                        COUNT(DISTINCT transaction_id) AS num_transactions
                                FROM sandbox.pj_pl_transactions_info_nov
                                WHERE pl = 1
                                AND customer_id IS NOT NULL
                                AND YEAR(business_day) = 2023
                                GROUP BY customer_id)

SELECT COUNT(CASE WHEN num_transactions = 1 THEN customer_id END) AS num_transactions_once,
        COUNT(CASE WHEN num_transactions = 2 THEN customer_id END) AS num_transactions_twice,
        COUNT(CASE WHEN num_transactions = 3 THEN customer_id END) AS num_transactions_thrice,
        COUNT(CASE WHEN num_transactions > 3 THEN customer_id END) AS num_transactions_4Above
FROM customer_num_purchases

-- COMMAND ----------

-- Creating a new table similar to the master table which will have data from february to november

WITH general_cust_counts AS (SELECT department_name, category_name,
                                    COUNT(DISTINCT customer_id) AS total_customers,
                                    COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS total_pl_customers
                            FROM sandbox.pj_pl_transactions_info_nov
                            WHERE YEAR(business_day) = 2023
                            GROUP BY department_name, category_name
                            HAVING total_customers >= 100
                            AND total_pl_customers >= 100),

one_timer_flagged AS (SELECT department_name, category_name, SUM(is_one_timer) AS total_one_timers,
                              SUM(is_pl_one_timer) AS pl_one_timers

                      FROM (SELECT department_name, category_name, customer_id,
                                    CASE WHEN COUNT(DISTINCT transaction_id) = 1 THEN 1 ELSE 0 END AS is_one_timer,
                                    CASE WHEN COUNT(DISTINCT transaction_id) FILTER(WHERE pl=1) = 1 THEN 1 ELSE 0 END AS is_pl_one_timer
                            FROM sandbox.pj_pl_transactions_info_nov
                            WHERE customer_id IS NOT NULL
                            AND YEAR(business_day) = 2023
                            GROUP BY department_name, category_name, customer_id)
                      
                      GROUP BY department_name, category_name)

SELECT t1.*, t2.total_one_timers, t2.pl_one_timers,
        (t1.total_customers - t2.total_one_timers) AS total_repeat_customers,
        (t1.total_pl_customers - t2.pl_one_timers) AS pl_repeat_customers,
        ROUND(pl_repeat_customers/total_pl_customers,4) AS pl_repeat_rate,
        ROUND(pl_one_timers/total_pl_customers,4) AS pl_one_timer_rate,
        ROUND(total_repeat_customers/total_customers,4) AS total_repeat_rate,
        ROUND(total_one_timers/total_customers,4) AS total_one_timer_rate
FROM general_cust_counts AS t1
JOIN one_timer_flagged AS t2
ON t1.category_name = t2.category_name
AND t1.department_name = t2.department_name

-- COMMAND ----------

WITH general_cust_counts AS (SELECT nationality_group,
                                    COUNT(DISTINCT t1.customer_id) AS total_customers,
                                    COUNT(DISTINCT CASE WHEN pl = 1 THEN t1.customer_id END) AS total_pl_customers
                            FROM sandbox.pj_pl_transactions_info_nov AS t1
                            JOIN gold.customer_profile AS t2
                            ON t1.customer_id = t2.account_key
                            WHERE t2.nationality_group NOT IN ("NA", "OTHERS")
                            AND YEAR(business_day) = 2023
                            GROUP BY nationality_group),

one_timer_flagged AS (SELECT nationality_group, SUM(is_one_timer) AS total_one_timers,
                              SUM(is_pl_one_timer) AS pl_one_timers

                      FROM (SELECT nationality_group, t1.customer_id,
                                    CASE WHEN COUNT(DISTINCT transaction_id) = 1 THEN 1 ELSE 0 END AS is_one_timer,
                                    CASE WHEN COUNT(DISTINCT transaction_id) FILTER(WHERE pl=1) = 1 THEN 1 ELSE 0 END AS is_pl_one_timer
                            FROM sandbox.pj_pl_transactions_info_nov AS t1
                            JOIN gold.customer_profile AS t2
                            ON t1.customer_id = t2.account_key
                            WHERE t2.nationality_group NOT IN ("NA", "OTHERS")
                            AND YEAR(business_day) = 2023
                            GROUP BY nationality_group, t1.customer_id)
                      
                      GROUP BY nationality_group)

SELECT t1.nationality_group, t1.total_customers,
        t1.total_pl_customers, t2.total_one_timers, t2.pl_one_timers,
        (t1.total_customers - t2.total_one_timers) AS total_repeat_customers,
        (t1.total_pl_customers - t2.pl_one_timers) AS pl_repeat_customers,
        ROUND(pl_repeat_customers/total_pl_customers,4) AS pl_repeat_rate,
        ROUND(pl_one_timers/total_pl_customers,4) AS pl_one_timer_rate,
        ROUND(total_repeat_customers/total_customers,4) AS total_repeat_rate,
        ROUND(total_one_timers/total_customers,4) AS total_one_timer_rate
FROM general_cust_counts AS t1
JOIN one_timer_flagged AS t2
ON t1.nationality_group = t2.nationality_group

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Slide 18

-- COMMAND ----------

SELECT category_name,
        ROUND(ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0)/COUNT(DISTINCT CASE WHEN pl = 1 THEN transaction_id END),2) AS pl_ATV,
        ROUND(ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0)/COUNT(DISTINCT CASE WHEN pl = 0 THEN transaction_id END),2) AS non_pl_ATV,
        ROUND(pl_ATV - non_pl_ATV,2) AS Delta
FROM sandbox.pj_pl_transactions_info_nov
WHERE YEAR(business_day) = 2023
AND customer_id IS NOT NULL
GROUP BY category_name
HAVING pl_ATV IS NOT NULL

-- COMMAND ----------

WITH mg_atv AS (
    SELECT category_name, material_group_name,
            ROUND(ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0)/COUNT(DISTINCT CASE WHEN pl = 1 THEN transaction_id END),2) AS pl_ATV,
            ROUND(ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0)/COUNT(DISTINCT CASE WHEN pl = 0 THEN transaction_id END),2) AS non_pl_ATV,
            ROUND(pl_ATV - non_pl_ATV,2) AS Delta
    FROM sandbox.pj_pl_transactions_info_nov
    WHERE YEAR(business_day) = 2023
    AND customer_id IS NOT NULL
    GROUP BY category_name, material_group_name
    HAVING pl_ATV IS NOT NULL
),

RankedMaterialGroups AS (
    SELECT category_name, material_group_name, pl_ATV, non_pl_ATV, Delta,
            ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY pl_ATV DESC) AS rnk
    FROM mg_atv
)

SELECT category_name, material_group_name, pl_ATV, non_pl_ATV, ROUND(pl_ATV - non_pl_ATV,2) AS Delta
FROM RankedMaterialGroups
WHERE rnk = 1

-- COMMAND ----------

WITH cte AS (SELECT category_name, segment,
                    ROUND(ROUND(SUM(amount),0)/COUNT(DISTINCT transaction_id),2) AS ATV
            FROM sandbox.pj_pl_transactions_info_nov AS t1
            JOIN analytics.customer_segments AS t2
            ON t1.customer_id = t2.customer_id
            WHERE key = 'rfm'
            AND channel = 'pos'
            AND country = 'uae'
            AND month_year = '202311'
            AND pl = 1
            AND YEAR(business_day) = 2023
            GROUP BY category_name, segment)

SELECT category_name,
        MAX(CASE WHEN segment = 'Moderate' THEN ATV END) AS moderate_atv,
        MAX(CASE WHEN segment = 'VIP' THEN ATV END) AS vip_atv,
        MAX(CASE WHEN segment = 'Frequentist' THEN ATV END) AS freq_atv,
        MAX(CASE WHEN segment = 'Splurger' THEN ATV END) AS splurger_atv,
        MAX(CASE WHEN segment = 'Lapser' THEN ATV END) AS lapser_atv,
        MAX(CASE WHEN segment = 'Newbie' THEN ATV END) AS newbie_atv,
        MAX(CASE WHEN segment = 'Slipping Loyalist' THEN ATV END) AS sl_atv
FROM cte
GROUP BY category_name

-- COMMAND ----------

WITH cte AS (SELECT category_name, nationality_group,
                    ROUND(ROUND(SUM(amount),0)/COUNT(DISTINCT transaction_id),2) AS ATV
            FROM sandbox.pj_pl_transactions_info_nov AS t1
            JOIN gold.customer_profile AS t2
            ON t1.customer_id = t2.account_key
            WHERE nationality_group NOT IN ("NA", "OTHERS")
            AND pl = 1
            AND YEAR(business_day) = 2023
            GROUP BY category_name, nationality_group)

SELECT category_name,
        MAX(CASE WHEN nationality_group = 'SRI-LANKAN' THEN ATV END) AS srilankan_atv,
        MAX(CASE WHEN nationality_group = 'ARABS' THEN ATV END) AS arabs_atv,
        MAX(CASE WHEN nationality_group = 'ASIANS' THEN ATV END) AS asians_atv,
        MAX(CASE WHEN nationality_group = 'EUROPEAN' THEN ATV END) AS european_atv,
        MAX(CASE WHEN nationality_group = 'BANGLADESHI' THEN ATV END) AS bangladeshi_atv,
        MAX(CASE WHEN nationality_group = 'AFRICAN' THEN ATV END) AS african_atv,
        MAX(CASE WHEN nationality_group = 'EMIRATI' THEN ATV END) AS emirati_atv,
        MAX(CASE WHEN nationality_group = 'EGYPTIAN' THEN ATV END) AS egyptian_atv,
        MAX(CASE WHEN nationality_group = 'INDIAN' THEN ATV END) AS indian_atv,
        MAX(CASE WHEN nationality_group = 'PAKISTANI' THEN ATV END) AS pakistani_atv,
        MAX(CASE WHEN nationality_group = 'FILIPINO' THEN ATV END) AS filipino_atv
FROM cte
GROUP BY category_name

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

SELECT segment,
        ROUND(SUM(amount),0) AS total_sales,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        COUNT(DISTINCT t1.customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN t1.customer_id END) AS pl_customers
FROM sandbox.pj_pl_transactions_info_nov AS t1
JOIN analytics.customer_segments AS t2
ON t1.customer_id = t2.customer_id
WHERE key = 'rfm'
AND channel = 'pos'
AND country = 'uae'
AND month_year = '202311'
AND YEAR(business_day) = 2023
GROUP BY segment

-- COMMAND ----------

SELECT nationality_group,
        ROUND(SUM(amount),0) AS total_sales,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        COUNT(DISTINCT t1.customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN t1.customer_id END) AS pl_customers
FROM sandbox.pj_pl_transactions_info_nov AS t1
JOIN gold.customer_profile AS t2
ON t1.customer_id = t2.account_key
WHERE nationality_group NOT IN ("NA", "OTHERS")
AND YEAR(business_day) = 2023
GROUP BY nationality_group

-- COMMAND ----------

WITH cte AS (SELECT segment,
                    ROUND(ROUND(SUM(amount),0)/COUNT(DISTINCT transaction_id),2) AS ATV
            FROM sandbox.pj_pl_transactions_info_nov AS t1
            JOIN analytics.customer_segments AS t2
            ON t1.customer_id = t2.customer_id
            WHERE key = 'rfm'
            AND channel = 'pos'
            AND country = 'uae'
            AND month_year = '202311'
            AND pl = 1
            AND YEAR(business_day) = 2023
            GROUP BY segment)

SELECT MAX(CASE WHEN segment = 'Moderate' THEN ATV END) AS moderate_atv,
        MAX(CASE WHEN segment = 'VIP' THEN ATV END) AS vip_atv,
        MAX(CASE WHEN segment = 'Frequentist' THEN ATV END) AS freq_atv,
        MAX(CASE WHEN segment = 'Splurger' THEN ATV END) AS splurger_atv,
        MAX(CASE WHEN segment = 'Lapser' THEN ATV END) AS lapser_atv,
        MAX(CASE WHEN segment = 'Newbie' THEN ATV END) AS newbie_atv,
        MAX(CASE WHEN segment = 'Slipping Loyalist' THEN ATV END) AS sl_atv
FROM cte

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

SELECT category_name,
        COUNT(DISTINCT product_id) AS total_materials,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS pl_materials,
        COUNT(DISTINCT material_group_id) AS num_mgs,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN material_group_id END) AS pl_mgs,
        ROUND(SUM(amount),0) AS sales,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        ROUND(pl_sales/sales,4) AS pl_sales_contri
FROM sandbox.pj_pl_transactions_info_nov
WHERE category_name = 'JUICE & DRINKS (LONG LIFE)'
AND YEAR(business_day) = 2023
GROUP BY category_name

-- COMMAND ----------

SELECT category_name, material_group_name, COUNT(DISTINCT product_id)
FROM sandbox.pj_pl_transactions_info_nov
WHERE category_name = 'JUICE & DRINKS (LONG LIFE)'
AND YEAR(business_day) = 2023
AND pl = 1
GROUP BY category_name, material_group_name
