-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Saving Sandbox Table

-- COMMAND ----------

-- %py

-- df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/prem@loyalytics.in/pl_product_list.csv")
-- df1.createOrReplaceTempView('private_labels')

-- COMMAND ----------

-- DROP TABLE IF EXISTS sandbox.pj_private_label_material_id_raw;

-- CREATE TABLE sandbox.pj_private_label_material_id_raw AS (SELECT * FROM private_labels)

-- COMMAND ----------

-- drop table if exists sandbox.pj_pl_transactions_info_sept;

-- create table sandbox.pj_pl_transactions_info_sept as (

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
-- where t1.business_day between "2022-01-01" and "2023-09-30"
-- and round(t1.amount, 0) > 0
-- and t1.quantity > 0
-- and department_name not in ('018','EX.EKPHK','ADVANCE / EXCHANGE DEPOSITS','FRUIT & VEGETABLES','HARD GOODS','PERFUMES COSMETICS & JEWELLERY','MALL LEASE','GIFT CARD','CHARITY','RESTAURANT','FOUNDATION PROGRAM','016','THE KITCHEN','HUDA SHIPPING')
-- )

-- COMMAND ----------

-- drop table if exists sandbox.pj_pl_transactions_info_feb_to_nov;

-- create table sandbox.pj_pl_transactions_info_feb_to_nov as (

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
-- where t1.business_day between "2022-02-01" and "2023-11-30"
-- and round(t1.amount, 0) > 0
-- and t1.quantity > 0
-- and department_name not in ('018','EX.EKPHK','ADVANCE / EXCHANGE DEPOSITS','FRUIT & VEGETABLES','HARD GOODS','PERFUMES COSMETICS & JEWELLERY','MALL LEASE','GIFT CARD','CHARITY','RESTAURANT','FOUNDATION PROGRAM','016','THE KITCHEN','HUDA SHIPPING')
-- )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Overview

-- COMMAND ----------

SELECT department_name, category_name,
        COUNT(t1.material_id) AS num_materials,
        COUNT(t2.material_id) AS num_pl_materials
FROM gold.material_master AS t1
LEFT JOIN sandbox.pj_private_label_material_id_raw AS t2
ON t1.material_id = t2.material_id
GROUP BY department_name, category_name

-- COMMAND ----------

-- 2023 Transactions, Overall

SELECT department_name, category_name,
        COUNT(DISTINCT product_id) AS num_materials,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS num_pl_materials,
        ROUND(SUM(amount),0) AS sales, ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        COUNT(DISTINCT customer_id) AS num_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers
FROM sandbox.pj_pl_transactions_info_sept
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
GROUP BY department_name, category_name

-- COMMAND ----------

SELECT COUNT(DISTINCT customer_id) AS num_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers,
        ROUND(num_pl_customers/num_customers,4) AS pl_customer_percentage
FROM sandbox.pj_pl_transactions_info_sept
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"

-- COMMAND ----------

WITH customer_num_purchases AS (SELECT DISTINCT customer_id,
                                        COUNT(DISTINCT product_id) AS pl_items_purchased,
                                        COUNT(DISTINCT transaction_id) AS num_transactions
                                FROM sandbox.pj_pl_transactions_info_sept
                                WHERE pl = 1
                                AND customer_id IS NOT NULL
                                AND business_day BETWEEN '2023-01-01' AND '2023-09-30'
                                GROUP BY customer_id)

SELECT COUNT(CASE WHEN pl_items_purchased = 1 THEN customer_id END) AS pl_items_purchased_once,
        COUNT(CASE WHEN pl_items_purchased = 2 THEN customer_id END) AS pl_items_purchased_twice,
        COUNT(CASE WHEN pl_items_purchased = 3 THEN customer_id END) AS pl_items_purchased_thrice,
        COUNT(CASE WHEN pl_items_purchased > 3 THEN customer_id END) AS pl_items_purchased_4Above,
        COUNT(CASE WHEN num_transactions = 1 THEN customer_id END) AS num_transactions_once,
        COUNT(CASE WHEN num_transactions = 2 THEN customer_id END) AS num_transactions_twice,
        COUNT(CASE WHEN num_transactions = 3 THEN customer_id END) AS num_transactions_thrice,
        COUNT(CASE WHEN num_transactions > 3 THEN customer_id END) AS num_transactions_4Above
FROM customer_num_purchases

-- COMMAND ----------

WITH customer_categories_tried AS (SELECT DISTINCT customer_id,
                                        COUNT(DISTINCT category_id) AS pl_categories_tried
                                FROM sandbox.pj_pl_transactions_info_sept
                                WHERE pl = 1
                                AND customer_id IS NOT NULL
                                AND business_day BETWEEN '2023-01-01' AND '2023-09-30'
                                GROUP BY customer_id)

SELECT COUNT(CASE WHEN pl_categories_tried = 1 THEN customer_id END) AS pl_categories_tried_once,
        COUNT(CASE WHEN pl_categories_tried = 2 THEN customer_id END) AS pl_categories_tried_twice,
        COUNT(CASE WHEN pl_categories_tried = 3 THEN customer_id END) AS pl_categories_tried_thrice,
        COUNT(CASE WHEN pl_categories_tried > 3 THEN customer_id END) AS pl_categories_tried_4Above
FROM customer_categories_tried

-- COMMAND ----------

WITH customer_material_groups_tried AS (SELECT DISTINCT customer_id,
                                        COUNT(DISTINCT material_group_id) AS pl_material_groups_tried
                                FROM sandbox.pj_pl_transactions_info_sept
                                WHERE pl = 1
                                AND customer_id IS NOT NULL
                                AND business_day BETWEEN '2023-01-01' AND '2023-09-30'
                                GROUP BY customer_id)

SELECT COUNT(CASE WHEN pl_material_groups_tried = 1 THEN customer_id END) AS pl_material_groups_tried_once,
        COUNT(CASE WHEN pl_material_groups_tried = 2 THEN customer_id END) AS pl_material_groups_tried_twice,
        COUNT(CASE WHEN pl_material_groups_tried = 3 THEN customer_id END) AS pl_material_groups_tried_thrice,
        COUNT(CASE WHEN pl_material_groups_tried > 3 THEN customer_id END) AS pl_material_groups_tried_4Above
FROM customer_material_groups_tried

-- COMMAND ----------

WITH customer_num_purchases AS (SELECT category_name, customer_id,
                                        COUNT(DISTINCT product_id) AS pl_items_purchased
                                FROM sandbox.pj_pl_transactions_info_sept
                                WHERE pl = 1
                                AND customer_id IS NOT NULL
                                AND business_day BETWEEN '2023-01-01' AND '2023-09-30'
                                GROUP BY category_name, customer_id)

SELECT category_name, COUNT(CASE WHEN pl_items_purchased = 1 THEN customer_id END) AS pl_items_purchased_once,
        COUNT(CASE WHEN pl_items_purchased = 2 THEN customer_id END) AS pl_items_purchased_twice,
        COUNT(CASE WHEN pl_items_purchased = 3 THEN customer_id END) AS pl_items_purchased_thrice,
        COUNT(CASE WHEN pl_items_purchased > 3 THEN customer_id END) AS pl_items_purchased_4Above
FROM customer_num_purchases
GROUP BY category_name

-- COMMAND ----------

-- 2023 Transactions, Monthly

-- Calculating pl_sales contribution

CTE1 AS (SELECT MONTH(business_day) AS month,
                      department_name, category_name,
                      ROUND(SUM(amount),0) AS total_sales,
                      ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales
              FROM sandbox.pj_pl_transactions_info_feb_to_nov
              GROUP BY month, department_name, category_name),

-- Calculating Revenue Per Customer

CTE2 AS (SELECT MONTH(business_day) AS month, department_name, category_name,
                ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS loyalty_customers_pl_sales,
                COUNT(DISTINCT customer_id) AS num_customers,
                COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers
        FROM sandbox.pj_pl_transactions_info_feb_to_nov
        AND customer_id IS NOT NULL
        GROUP BY month, department_name, category_name)

SELECT t1.month, t1.department_name, t1.category_name,
        t1.total_sales, t1.pl_sales, t2.loyalty_customers_pl_sales,
        t2.num_customers, t2.num_pl_customers
FROM CTE1 AS t1
LEFT JOIN CTE2 AS t2
ON t1.month = t2.month
AND t1.department_name = t2.department_name
AND t1.category_name = t2.category_name

-- COMMAND ----------

-- Creating a new table similar to the master table which will have data from february to november

WITH general_cust_counts AS (SELECT department_name, category_name,
                                    COUNT(DISTINCT customer_id) AS total_customers,
                                    COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS total_pl_customers
                            FROM sandbox.pj_pl_transactions_info_feb_to_nov
                            GROUP BY department_name, category_name
                            HAVING total_customers >= 100
                            AND total_pl_customers >= 100),

one_timer_flagged AS (SELECT department_name, category_name, SUM(is_one_timer) AS total_one_timers,
                              SUM(is_pl_one_timer) AS pl_one_timers

                      FROM (SELECT department_name, category_name, customer_id,
                                    CASE WHEN COUNT(DISTINCT transaction_id) = 1 THEN 1 ELSE 0 END AS is_one_timer,
                                    CASE WHEN COUNT(DISTINCT transaction_id) FILTER(WHERE pl=1) = 1 THEN 1 ELSE 0 END AS is_pl_one_timer
                            FROM sandbox.pj_pl_transactions_info_feb_to_nov
                            WHERE customer_id IS NOT NULL
                            GROUP BY department_name, category_name, customer_id)
                      
                      GROUP BY department_name, category_name)

SELECT t1.*, t2.total_one_timers, t2.pl_one_timers,
        (t1.total_customers - t2.total_one_timers) AS total_repeat_customers,
        (t1.total_pl_customers - t2.pl_one_timers) AS pl_repeat_customers
FROM general_cust_counts AS t1
JOIN one_timer_flagged AS t2
ON t1.category_name = t2.category_name
AND t1.department_name = t2.department_name

-- COMMAND ----------

WITH customer_num_purchases AS (SELECT DISTINCT customer_id, category_name,
                                        COUNT(DISTINCT product_id) AS pl_items_purchased
                                FROM sandbox.pj_pl_transactions_info_sept
                                WHERE pl = 1
                                AND customer_id IS NOT NULL
                                AND business_day BETWEEN '2023-01-01' AND '2023-09-30'
                                GROUP BY customer_id, category_name)

SELECT category_name, COUNT(CASE WHEN pl_items_purchased = 1 THEN customer_id END) AS pl_items_purchased_once,
        COUNT(CASE WHEN pl_items_purchased = 2 THEN customer_id END) AS pl_items_purchased_twice,
        COUNT(CASE WHEN pl_items_purchased = 3 THEN customer_id END) AS pl_items_purchased_thrice,
        COUNT(CASE WHEN pl_items_purchased > 3 THEN customer_id END) AS pl_items_purchased_4Above
FROM customer_num_purchases
GROUP BY category_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 1
-- MAGIC Private Label Size and Growth Analysis

-- COMMAND ----------

-- From 'Material Master' Table

SELECT department_name, category_name, material_group_name,
        COUNT(t1.material_id) AS num_materials,
        COUNT(t2.material_id) AS num_pl_materials
FROM gold.material_master AS t1
LEFT JOIN sandbox.pj_private_label_material_id_raw AS t2
ON t1.material_id = t2.material_id
GROUP BY department_name, category_name, material_group_name

-- COMMAND ----------

-- 2023 Transactions, Overall

SELECT department_name, category_name,
        COUNT(DISTINCT product_id) AS num_materials,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS num_pl_materials,
        ROUND(SUM(amount),0) AS sales, ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        ROUND(SUM(quantity),0) AS total_quantity, ROUND(SUM(CASE WHEN pl = 1 THEN quantity ELSE 0 END),0) AS pl_quantity,
        COUNT(DISTINCT customer_id) AS num_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers
FROM sandbox.pj_pl_transactions_info_sept
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
GROUP BY department_name, category_name

-- COMMAND ----------

-- Last 2 Years Transactions, Quarterly

SELECT CONCAT(YEAR(business_day), " Q", QUARTER(business_day)) as quarter_info,
        department_name, category_name, material_group_name,
        COUNT(DISTINCT product_id) AS num_materials,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS num_pl_materials,
        ROUND(SUM(amount),0) AS total_sales,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        ROUND(SUM(quantity),0) AS total_quantity,
        ROUND(SUM(CASE WHEN pl = 1 THEN quantity ELSE 0 END),0) AS pl_quantity,
        COUNT(DISTINCT customer_id) AS num_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers,
        ROUND(pl_sales/num_pl_customers, 4) AS pl_RPC
FROM sandbox.pj_pl_transactions_info_sept
GROUP BY department_name, quarter_info, category_name, material_group_name

-- COMMAND ----------

-- 2023 Transactions, Monthly

-- Calculating pl_sales contribution, num_pl_materials_contribution, pl_quantity_contribution

WITH CTE1 AS (SELECT MONTH(business_day) AS month,
                      department_name, category_name, material_group_name,
                      COUNT(DISTINCT product_id) AS num_materials,
                      COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS num_pl_materials,
                      ROUND(SUM(amount),0) AS total_sales,
                      ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
                      ROUND(SUM(quantity),0) AS total_quantity,
                      ROUND(SUM(CASE WHEN pl = 1 THEN quantity ELSE 0 END),0) AS pl_quantity,
                      CONCAT(month, department_name, category_name, material_group_name) AS joinkey
              FROM sandbox.pj_pl_transactions_info_sept
              WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
              GROUP BY month, department_name, category_name, material_group_name),

-- Calculating Revenue Per Customer

CTE2 AS (SELECT MONTH(business_day) AS month,
                department_name, category_name, material_group_name,
                ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS loyalty_customers_pl_sales,
                COUNT(DISTINCT customer_id) AS num_customers,
                COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers,
                CONCAT(month, department_name, category_name, material_group_name) AS joinkey
        FROM sandbox.pj_pl_transactions_info_sept
        WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
        AND customer_id IS NOT NULL
        GROUP BY month, department_name, category_name, material_group_name)

SELECT t1.month, t1.department_name, t1.category_name, t1.material_group_name,
        t1.num_materials, t1.num_pl_materials, t1.total_sales, t1.pl_sales,
        t2.loyalty_customers_pl_sales, t1.total_quantity, t1.pl_quantity, t2.num_customers, t2.num_pl_customers
FROM CTE1 AS t1
LEFT JOIN CTE2 AS t2
ON t1.joinkey = t2.joinkey

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 2
-- MAGIC Major Private Label Categories and Material Groups

-- COMMAND ----------

-- 2023 Transactions, Monthly

WITH pl_sales_contribution_table AS (SELECT MONTH(business_day) AS month, department_name, category_name, 
                                          material_group_name, ROUND(SUM(amount),0) AS total_sales,
                                          ROUND(SUM(CASE WHEN pl = 1 THEN amount END),0) AS pl_sales,
                                          ROUND(pl_sales/total_sales,3) AS pl_sales_contribution
                                    FROM sandbox.pj_pl_transactions_info_sept
                                    WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
                                    GROUP BY month, department_name, category_name, material_group_name)

SELECT *,
      CASE WHEN pl_sales_contribution <= (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pl_sales_contribution) FROM pl_sales_contribution_table) THEN 'Low'
            WHEN pl_sales_contribution <= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pl_sales_contribution) FROM pl_sales_contribution_table) THEN 'Medium'
            WHEN pl_sales_contribution <= (SELECT PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY pl_sales_contribution) FROM pl_sales_contribution_table) THEN 'High'
      END AS contribution_label
FROM pl_sales_contribution_table

-- COMMAND ----------

-- Last 2 Years Transactions, Quarterly

WITH pl_sales_contribution_table AS (SELECT CONCAT(YEAR(business_day), " Q", QUARTER(business_day)) as quarter_info,
                                          department_name, category_name, material_group_name, ROUND(SUM(amount),0) AS total_sales,
                                          ROUND(SUM(CASE WHEN pl = 1 THEN amount END),0) AS pl_sales,
                                          ROUND(pl_sales/total_sales,3) AS pl_sales_contribution
                                    FROM sandbox.pj_pl_transactions_info_sept
                                    GROUP BY quarter_info, department_name, category_name, material_group_name)

SELECT *,
      CASE WHEN pl_sales_contribution <= (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pl_sales_contribution) FROM pl_sales_contribution_table) THEN 'Low'
            WHEN pl_sales_contribution <= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pl_sales_contribution) FROM pl_sales_contribution_table) THEN 'Medium'
            WHEN pl_sales_contribution <= (SELECT PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY pl_sales_contribution) FROM pl_sales_contribution_table) THEN 'High'
      END AS contribution_label
FROM pl_sales_contribution_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 3
-- MAGIC Customer Engagement with Private Label

-- COMMAND ----------

-- 2023 Transactions, Monthly

SELECT MONTH(business_day) AS month, department_name, category_name,
      COUNT(DISTINCT customer_id) AS num_customers,
      COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers,
      ROUND(SUM(amount),0) AS loyalty_sales,
      ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS loyalty_pl_sales
FROM sandbox.pj_pl_transactions_info_sept
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
AND customer_id IS NOT NULL
GROUP BY month, department_name, category_name

-- COMMAND ----------

-- Last 2 Years Transactions, Yearly

SELECT YEAR(business_day) AS year, department_name, category_name,
      COUNT(DISTINCT customer_id) AS num_customers,
      COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS num_pl_customers
FROM sandbox.pj_pl_transactions_info_sept
GROUP BY year, department_name, category_name

-- COMMAND ----------

-- 2023 September Transactions, Overall

SELECT department_name, category_name, segment, COUNT(DISTINCT t2.customer_id) AS num_customers,
      COUNT(DISTINCT CASE WHEN pl = 1 THEN t2.customer_id END) AS num_pl_customers
FROM sandbox.pj_pl_transactions_info_sept AS t1
JOIN analytics.customer_segments AS t2
ON t1.customer_id = t2.customer_id
WHERE key = 'rfm'
AND channel = 'pos'
AND country = 'uae'
AND month_year = '202309'
GROUP BY department_name, category_name, segment
HAVING category_name IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 4
-- MAGIC Private Label Performance in the Last Year

-- COMMAND ----------

-- Last 2 Years Transactions, Overall

WITH pl_year_on_year_change AS (SELECT department_name, category_name, material_group_name,
                                        CASE WHEN transaction_date BETWEEN '2022-01-02' AND '2022-11-01' THEN 'period_22'
                                            WHEN transaction_date BETWEEN '2023-01-01' AND '2023-09-30' THEN 'period_23'
                                            END AS period_type,
                                        COUNT(DISTINCT product_id) AS num_materials,
                                        ROUND(SUM(amount),0) AS total_sales,
                                        ROUND(SUM(quantity),0) AS quantity_sold,
                                        COUNT(DISTINCT customer_id) AS num_customer
                                FROM sandbox.pj_pl_transactions_info_sept
                                WHERE pl = 1
                                GROUP BY department_name, category_name, material_group_name, period_type)

SELECT department_name, category_name, material_group_name,
      MAX(CASE WHEN period_type = 'period_22' THEN total_sales END) AS sales_period_22,
      MAX(CASE WHEN period_type = 'period_23' THEN total_sales END) AS sales_period_23,
      ROUND((sales_period_23 - sales_period_22)/sales_period_22, 3) AS sales_delta,

      MAX(CASE WHEN period_type = 'period_22' THEN quantity_sold END) AS quantity_period_22,
      MAX(CASE WHEN period_type = 'period_23' THEN quantity_sold END) AS quantity_period_23,
      ROUND((quantity_period_23 - quantity_period_22)/quantity_period_22, 3) AS quantity_delta,

      MAX(CASE WHEN period_type = 'period_22' THEN num_customer END) AS customers_period_22,
      MAX(CASE WHEN period_type = 'period_23' THEN num_customer END) AS customers_period_23,
      ROUND((customers_period_23 - customers_period_22)/customers_period_22, 3) AS customers_delta,

      MAX(CASE WHEN period_type = 'period_22' THEN num_materials END) AS materials_period_22,
      MAX(CASE WHEN period_type = 'period_23' THEN num_materials END) AS materials_period_23,
      ROUND((materials_period_23 - materials_period_22)/materials_period_22, 3) AS materials_delta
FROM pl_year_on_year_change
GROUP BY department_name, category_name, material_group_name
HAVING sales_period_22 > 0
AND sales_period_23 > 0
AND quantity_period_22 > 0
AND quantity_period_23 > 0
AND customers_period_22 > 0
AND customers_period_23 > 0
AND materials_period_22 > 0
AND materials_period_23 > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 5
-- MAGIC Related Products Growth and Contribution Changes

-- COMMAND ----------

-- Last 2 Years Transactions, Quarterly

WITH to_remove_mg AS (SELECT material_group_name,
                              COUNT(DISTINCT category_name) AS cnt
                      FROM gold.material_master
                      GROUP BY material_group_name
                      HAVING cnt > 1)

SELECT CONCAT(YEAR(business_day), " ", "Q", QUARTER(business_day)) AS quarter_info,
        department_name, category_name, material_group_name,
        COUNT(DISTINCT product_id) AS num_materials,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS num_pl_materials,
        ROUND(SUM(amount),0) AS sales,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount END),0) AS pl_sales
FROM sandbox.pj_pl_transactions_info_sept
WHERE material_group_name NOT IN (SELECT material_group_name FROM to_remove_mg)
GROUP BY quarter_info, department_name, category_name, material_group_name
ORDER BY quarter_info

-- COMMAND ----------

-- Last 2 Years Transactions, Quarterly

WITH to_remove_mg AS (SELECT material_group_name,
                              COUNT(DISTINCT category_name) AS cnt
                      FROM gold.material_master
                      GROUP BY material_group_name
                      HAVING cnt > 1)

SELECT CONCAT(YEAR(business_day), " ", "Q", QUARTER(business_day)) AS quarter_info,
        department_name, category_name, material_group_name,
        CASE WHEN pl = 1 THEN 'PRIVATE LABEL' ELSE brand END AS brand_type,
        COUNT(DISTINCT product_id) AS num_materials,
        ROUND(SUM(amount),0) AS sales
FROM sandbox.pj_pl_transactions_info_sept
WHERE material_group_name NOT IN (SELECT material_group_name FROM to_remove_mg)
GROUP BY quarter_info, department_name, category_name, material_group_name, brand_type
HAVING brand_type IS NOT NULL
ORDER BY quarter_info

-- COMMAND ----------

-- 2023 Transactions, Monthly

WITH to_remove_mg AS (SELECT material_group_name,
                              COUNT(DISTINCT category_name) AS cnt
                      FROM gold.material_master
                      GROUP BY material_group_name
                      HAVING cnt > 1)

SELECT MONTH(business_day) AS month,
        department_name, category_name, material_group_name,
        CASE WHEN pl = 1 THEN 'PRIVATE LABEL' ELSE brand END AS brand_type,
        COUNT(DISTINCT customer_id) AS num_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN customer_id END) AS pl_customers
FROM sandbox.pj_pl_transactions_info_sept
WHERE material_group_name NOT IN (SELECT material_group_name FROM to_remove_mg)
AND business_day BETWEEN "2023-01-01" AND "2023-09-30"
GROUP BY month, department_name, category_name, material_group_name, brand_type
HAVING brand_type IS NOT NULL
ORDER BY month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 6
-- MAGIC Customer Segmentation Preferences

-- COMMAND ----------

-- Last 2 Years Transactions

WITH category_customers AS (SELECT category_name, COUNT(DISTINCT t2.customer_id) AS total_category_customers,
                                    COUNT(DISTINCT CASE WHEN pl = 1 THEN t2.customer_id END) AS total_category_pl_customers,
                                    (total_category_customers - total_category_pl_customers) AS total_category_non_pl_customers
                            FROM sandbox.pj_pl_transactions_info_sept AS t1
                            JOIN analytics.customer_segments AS t2
                            ON t1.customer_id = t2.customer_id
                            WHERE key = 'rfm'
                            AND channel = 'pos'
                            AND country = 'uae'
                            AND month_year = '202309'
                            GROUP BY category_name)

SELECT department_name, t1.category_name, segment, COUNT(DISTINCT t1.customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN t1.customer_id END) AS num_pl_customers,
        (total_customers - num_pl_customers) AS num_non_pl_customers,
        total_category_pl_customers, total_category_non_pl_customers,
        ROUND(num_pl_customers/total_category_pl_customers,4) AS pl_segment_penetration_of_category,
        ROUND(num_non_pl_customers/total_category_non_pl_customers,4) AS non_pl_segment_penetration_of_category,
        ROUND(pl_segment_penetration_of_category/non_pl_segment_penetration_of_category,4) AS index
FROM sandbox.pj_pl_transactions_info_sept AS t1
JOIN analytics.customer_segments AS t2
ON t1.customer_id = t2.customer_id
JOIN category_customers AS t3
ON t1.category_name = t3.category_name
WHERE key = 'rfm'
AND channel = 'pos'
AND country = 'uae'
AND month_year = '202309'
AND total_category_pl_customers > 100
GROUP BY department_name, t1.category_name, segment, total_category_pl_customers, total_category_non_pl_customers

-- COMMAND ----------

-- Last 2 Years Transactions

WITH segment_sales AS (SELECT segment,
                                  ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS total_segment_pl_sales,
                                  ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS total_segment_non_pl_sales
                          FROM sandbox.pj_pl_transactions_info_sept AS t1
                          JOIN analytics.customer_segments AS t2
                          ON t1.customer_id = t2.customer_id
                          WHERE key = 'rfm'
                          AND channel = 'pos'
                          AND country = 'uae'
                          AND month_year = '202309'
                          GROUP BY segment)

SELECT t2.segment, t1.category_name, department_name,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS non_pl_sales,
        total_segment_pl_sales, total_segment_non_pl_sales,
        ROUND(pl_sales/total_segment_pl_sales,4) AS pl_wallet_share,
        ROUND(non_pl_sales/total_segment_non_pl_sales,4) AS non_pl_wallet_share,
        ROUND(pl_wallet_share/non_pl_wallet_share,4) AS index
FROM sandbox.pj_pl_transactions_info_sept AS t1
JOIN analytics.customer_segments AS t2
ON t1.customer_id = t2.customer_id
JOIN segment_sales AS t3
ON t2.segment = t3.segment
WHERE key = 'rfm'
AND channel = 'pos'
AND country = 'uae'
AND month_year = '202309'
GROUP BY t2.segment, t1.category_name, department_name, total_segment_pl_sales, total_segment_non_pl_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 7
-- MAGIC Rate of Sale Variation Across Categories

-- COMMAND ----------

-- 2023 Transactions, Daily

SELECT business_day, department_name, category_name,
      ROUND(SUM(CASE WHEN pl = 1 THEN amount END),0) AS pl_sales,
      ROUND(SUM(CASE WHEN pl = 0 THEN amount END),0) AS non_pl_sales
FROM sandbox.pj_pl_transactions_info_sept
WHERE business_day >= "2023-01-01"
GROUP BY business_day, department_name, category_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 9
-- MAGIC Geographical and Store Format Impact

-- COMMAND ----------

-- Last 2 Years Transactions, Overall

SELECT region_name, store_name,
      ROUND(SUM(CASE WHEN pl = 1 THEN amount END),0) AS pl_sales,
      ROUND(SUM(CASE WHEN pl = 0 THEN amount END),0) AS non_pl_sales,
      COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS num_pl_materials,
      COUNT(DISTINCT CASE WHEN pl = 0 THEN product_id END) AS num_non_pl_materials
FROM sandbox.pj_pl_transactions_info_sept
GROUP BY region_name, store_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Item 10
-- MAGIC Nationality Preferences for Private Labels

-- COMMAND ----------

-- 2023 Transactions

WITH nationality_pl_customers AS (SELECT nationality_group, COUNT(DISTINCT t2.customer_id) AS total_national_customers,
                                          COUNT(DISTINCT CASE WHEN pl = 1 THEN t2.customer_id END) AS total_national_pl_customers,
                                          (total_national_customers - total_national_pl_customers) AS total_national_non_pl_customers
                                  FROM sandbox.pj_pl_transactions_info_sept AS t1
                                  JOIN analytics.customer_segments AS t2
                                  ON t1.customer_id = t2.customer_id
                                  JOIN gold.customer_profile AS t3
                                  ON t1.customer_id = t3.account_key
                                  WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
                                  AND key = 'rfm'
                                  AND channel = 'pos'
                                  AND t2.country = 'uae'
                                  AND month_year = '202309'
                                  AND t3.nationality_group NOT IN ("NA", "OTHERS")
                                  GROUP BY nationality_group)

SELECT t1.nationality_group, segment, COUNT(DISTINCT t3.customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN t3.customer_id END) AS num_pl_customers,
        (total_customers - num_pl_customers) AS num_non_pl_customers,
        total_national_pl_customers, total_national_non_pl_customers, total_national_customers,
        ROUND(num_pl_customers/total_national_pl_customers,4) AS pl_segment_penetration_of_nationality,
        ROUND(num_non_pl_customers/total_national_non_pl_customers,4) AS non_pl_segment_penetration_of_nationality,
        ROUND(pl_segment_penetration_of_nationality/non_pl_segment_penetration_of_nationality,4) AS index
FROM gold.customer_profile AS t1
JOIN analytics.customer_segments AS t2
ON t1.account_key = t2.customer_id
JOIN sandbox.pj_pl_transactions_info_sept AS t3
ON t1.account_key = t3.customer_id
JOIN nationality_pl_customers AS t4
ON t1.nationality_group = t4.nationality_group
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
AND key = 'rfm'
AND channel = 'pos'
AND t2.country = 'uae'
AND month_year = '202309'
AND t1.nationality_group NOT IN ("NA", "OTHERS")
GROUP BY t1.nationality_group, segment, total_national_pl_customers, total_national_non_pl_customers, total_national_customers

-- COMMAND ----------

-- 2023 Transactions

WITH segment_pl_sales AS (SELECT segment,
                                  ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS total_segment_pl_sales,
                                  ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS total_segment_non_pl_sales,
                                  ROUND(SUM(amount),0) AS total_segment_sales
                          FROM sandbox.pj_pl_transactions_info_sept AS t1
                          JOIN analytics.customer_segments AS t2
                          ON t1.customer_id = t2.customer_id
                          JOIN gold.customer_profile AS t3
                          ON t1.customer_id = t3.account_key
                          WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
                          AND key = 'rfm'
                          AND channel = 'pos'
                          AND t2.country = 'uae'
                          AND month_year = '202309'
                          AND t3.nationality_group NOT IN ("NA", "OTHERS")
                          GROUP BY segment)

SELECT t2.segment, t1.nationality_group,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS non_pl_sales,
        ROUND(SUM(amount),0) AS total_sales,
        total_segment_pl_sales, total_segment_non_pl_sales, total_segment_sales,
        ROUND(pl_sales/total_segment_pl_sales,4) AS pl_wallet_share,
        ROUND(non_pl_sales/total_segment_non_pl_sales,4) AS non_pl_wallet_share,
        ROUND(pl_wallet_share/non_pl_wallet_share,4) AS index
FROM gold.customer_profile AS t1
JOIN analytics.customer_segments AS t2
ON t1.account_key = t2.customer_id
JOIN sandbox.pj_pl_transactions_info_sept AS t3
ON t1.account_key = t3.customer_id
JOIN segment_pl_sales AS t4
ON t2.segment = t4.segment
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
AND key = 'rfm'
AND channel = 'pos'
AND t2.country = 'uae'
AND month_year = '202309'
AND t1.nationality_group NOT IN ("NA", "OTHERS")
GROUP BY t2.segment, t1.nationality_group, total_segment_pl_sales, total_segment_non_pl_sales, total_segment_sales

-- COMMAND ----------

-- 2023 Transactions

WITH category_customers AS (SELECT category_name, COUNT(DISTINCT t2.account_key) AS total_category_customers,
                                    COUNT(DISTINCT CASE WHEN pl = 1 THEN t2.account_key END) AS total_category_pl_customers,
                                    (total_category_customers - total_category_pl_customers) AS total_category_non_pl_customers
                            FROM sandbox.pj_pl_transactions_info_sept AS t1
                            JOIN gold.customer_profile AS t2
                            ON t1.customer_id = t2.account_key
                            WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
                            AND nationality_group NOT IN ("NA", "OTHERS")
                            GROUP BY category_name)

SELECT department_name, t1.category_name, nationality_group, COUNT(DISTINCT t1.customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN pl = 1 THEN t1.customer_id END) AS num_pl_customers,
        (total_customers - num_pl_customers) AS num_non_pl_customers,
        total_category_pl_customers, total_category_non_pl_customers,
        ROUND(num_pl_customers/total_category_pl_customers,4) AS pl_nationality_penetration_of_category,
        ROUND(num_non_pl_customers/total_category_non_pl_customers,4) AS non_pl_nationality_penetration_of_category,
        ROUND(pl_nationality_penetration_of_category/non_pl_nationality_penetration_of_category,4) AS index
FROM sandbox.pj_pl_transactions_info_sept AS t1
JOIN gold.customer_profile AS t2
ON t1.customer_id = t2.account_key
JOIN category_customers AS t3
ON t1.category_name = t3.category_name
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
AND nationality_group NOT IN ("NA", "OTHERS")
AND total_category_pl_customers > 100
GROUP BY department_name, t1.category_name, nationality_group, total_category_pl_customers, total_category_non_pl_customers

-- COMMAND ----------

-- 2023 Transactions

WITH nationality_sales AS (SELECT nationality_group,
                                  ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS total_nationality_pl_sales,
                                  ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS total_nationality_non_pl_sales
                          FROM sandbox.pj_pl_transactions_info_sept AS t1
                          JOIN gold.customer_profile AS t2
                          ON t1.customer_id = t2.account_key
                          WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
                          AND nationality_group NOT IN ("NA", "OTHERS")
                          GROUP BY nationality_group)

SELECT t2.nationality_group, t1.category_name, department_name,
        ROUND(SUM(CASE WHEN pl = 1 THEN amount ELSE 0 END),0) AS pl_sales,
        ROUND(SUM(CASE WHEN pl = 0 THEN amount ELSE 0 END),0) AS non_pl_sales,
        total_nationality_pl_sales, total_nationality_non_pl_sales,
        ROUND(pl_sales/total_nationality_pl_sales,4) AS pl_wallet_share,
        ROUND(non_pl_sales/total_nationality_non_pl_sales,4) AS non_pl_wallet_share,
        ROUND(pl_wallet_share/non_pl_wallet_share,4) AS index
FROM sandbox.pj_pl_transactions_info_sept AS t1
JOIN gold.customer_profile AS t2
ON t1.customer_id = t2.account_key
JOIN nationality_sales AS t3
ON t2.nationality_group = t3.nationality_group
WHERE business_day BETWEEN "2023-01-01" AND "2023-09-30"
AND t2.nationality_group NOT IN ("NA", "OTHERS")
GROUP BY t2.nationality_group, t1.category_name, department_name, total_nationality_pl_sales, total_nationality_non_pl_sales

-- COMMAND ----------

WITH general_cust_counts AS (SELECT nationality_group,
                                    COUNT(DISTINCT t1.customer_id) AS total_customers,
                                    COUNT(DISTINCT CASE WHEN pl = 1 THEN t1.customer_id END) AS total_pl_customers
                            FROM sandbox.pj_pl_transactions_info_feb_to_nov AS t1
                            JOIN gold.customer_profile AS t2
                            ON t1.customer_id = t2.account_key
                            WHERE t2.nationality_group NOT IN ("NA", "OTHERS")
                            GROUP BY nationality_group),

one_timer_flagged AS (SELECT nationality_group, SUM(is_one_timer) AS total_one_timers,
                              SUM(is_pl_one_timer) AS pl_one_timers

                      FROM (SELECT nationality_group, t1.customer_id,
                                    CASE WHEN COUNT(DISTINCT transaction_id) = 1 THEN 1 ELSE 0 END AS is_one_timer,
                                    CASE WHEN COUNT(DISTINCT transaction_id) FILTER(WHERE pl=1) = 1 THEN 1 ELSE 0 END AS is_pl_one_timer
                            FROM sandbox.pj_pl_transactions_info_feb_to_nov AS t1
                            JOIN gold.customer_profile AS t2
                            ON t1.customer_id = t2.account_key
                            WHERE t2.nationality_group NOT IN ("NA", "OTHERS")
                            GROUP BY nationality_group, t1.customer_id)
                      
                      GROUP BY nationality_group)

SELECT t1.nationality_group, t1.total_customers,
        t1.total_pl_customers, t2.total_one_timers, t2.pl_one_timers,
        (t1.total_customers - t2.total_one_timers) AS total_repeat_customers,
        (t1.total_pl_customers - t2.pl_one_timers) AS pl_repeat_customers
FROM general_cust_counts AS t1
JOIN one_timer_flagged AS t2
ON t1.nationality_group = t2.nationality_group

-- COMMAND ----------



-- COMMAND ----------


