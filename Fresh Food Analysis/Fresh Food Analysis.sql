-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Sandbox Table

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_freshfood_data AS (
    WITH gp_perc AS (
        SELECT
            CASE WHEN region = "AUH" THEN "ABU DHABI"
                WHEN region = "ALN" THEN "AL AIN"
                WHEN region = "DXB" THEN "DUBAI"
                WHEN region = "SHJ" THEN "SHARJAH"
                END AS region_name,
            year_month,
            material_id,
            gp_wth_chargeback
        FROM gold.gross_profit
        WHERE country = 'AE'
    ),

    sales_data AS (
        SELECT DISTINCT
            business_day,
            INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
            region_name,
            t1.store_id,
            store_name,
            transaction_id,
            t1.customer_id,
            t2.material_id,
            ean,
            material_name,
            material_group_name,
            category_name,
            department_name,
            amount,
            quantity,
            actual_unit_price,
            loyalty_points
        FROM gold.pos_transactions AS t1
        LEFT JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
        LEFT JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
        WHERE
            business_day BETWEEN "2022-05-01" AND "2024-04-30"
            AND transaction_type IN ("SALE", "SELL_MEDIA")
            AND amount > 0
            AND quantity > 0
            AND department_class_name = "FRESH FOOD"
            AND tayeb_flag = 0
    )

    SELECT
        t1.*,
        COALESCE((amount * gp_wth_chargeback / 100), 0) AS gross_profit
    FROM sales_data AS t1
    LEFT JOIN gp_perc AS t2
        ON t1.region_name = t2.region_name
        AND t1.year_month = t2.year_month
        AND t1.material_id = t2.material_id
)

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_freshfood_data AS (
    SELECT
        t1.*,
        conversion_numerator,
        content,
        content_unit,
        LHRDATE,
        nationality_group,
        CASE WHEN title_desc = "MRS." OR title_desc = "MISS" THEN "Female"
            WHEN title_desc = "MR." THEN "Male"
            ELSE "Unknown"
            END AS gender_modified,
        age
    FROM sandbox.pj_freshfood_data AS t1
    LEFT JOIN gold.material_attributes AS t2 ON t1.ean = t2.ean
    LEFT JOIN gold.customer_profile AS t3 ON t1.customer_id = t3.account_key
)

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_freshfood_data AS (
    WITH customers AS (
        SELECT
            customer_id,
            segment
        FROM analytics.customer_segments
        WHERE key = 'rfm'
        AND channel = 'pos'
        AND country = 'uae'
        AND month_year = 202404
    )

    SELECT
        t1.*,
        segment
    FROM sandbox.pj_freshfood_data AS t1
    LEFT JOIN customers AS t2 ON t1.customer_id = t2.customer_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Sales Contribution - Fresh Food

-- COMMAND ----------

SELECT SUM(amount)
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"

-- COMMAND ----------

SELECT SUM(amount)
FROM gold.pos_transactions AS t1
LEFT JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
WHERE business_day BETWEEN "2023-05-01" AND "2024-04-30"
AND transaction_type IN ("SALE", "SELL_MEDIA")
AND amount > 0
AND quantity > 0
AND tayeb_flag = 0

-- COMMAND ----------

SELECT ROUND(2131036126.2516813 / 9728369631.288485, 3) AS sales_contri

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Top Departments

-- COMMAND ----------

SELECT
    department_name,
    SUM(amount) AS sales,
    SUM(SUM(amount)) OVER () AS total_sales,
    ROUND(sales/total_sales, 3) AS sales_contri
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY sales_contri DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Top Categories in Fruits & Vegetables

-- COMMAND ----------

SELECT
    category_name,
    SUM(amount) AS sales,
    SUM(SUM(amount)) OVER () AS total_sales,
    ROUND(sales/total_sales, 3) AS sales_contri
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND department_name = "FRUITS & VEGETABLES"
GROUP BY category_name
ORDER BY sales_contri DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Top Material Groups in Fruits & Vegetables

-- COMMAND ----------

SELECT
    material_group_name,
    SUM(amount) AS sales,
    SUM(SUM(amount)) OVER () AS total_sales,
    ROUND(sales/total_sales, 3) AS sales_contri
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND department_name = "FRUITS & VEGETABLES"
GROUP BY material_group_name
ORDER BY sales_contri DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Top Categories - Department-wise

-- COMMAND ----------

SELECT
    department_name,
    category_name,
    SUM(amount) AS sales,
    SUM(SUM(amount)) OVER (PARTITION BY department_name) AS total_sales,
    ROUND(SUM(amount) / SUM(SUM(amount)) OVER (PARTITION BY department_name), 3) AS sales_contri,
    ROW_NUMBER() OVER (PARTITION BY department_name ORDER BY SUM(amount) DESC) AS rank
FROM sandbox.pj_freshfood_data
WHERE business_day >= '2023-05-01'
GROUP BY department_name, category_name
HAVING rank <= 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Peak Season

-- COMMAND ----------

SELECT
    year_month,
    ROUND(SUM(amount)) AS sales,
    RANK() OVER (ORDER BY SUM(amount) DESC) AS rank
FROM sandbox.pj_freshfood_data
WHERE business_day < "2023-05-01"
GROUP BY year_month
ORDER BY year_month

-- COMMAND ----------

SELECT
    year_month,
    ROUND(SUM(amount)) AS sales,
    RANK() OVER (ORDER BY ROUND(SUM(amount)) DESC) AS rank
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY year_month
ORDER BY year_month

-- COMMAND ----------

SELECT
    department_name,
    year_month,
    ROUND(SUM(amount)) AS sales,
    SUM(ROUND(SUM(amount))) OVER (PARTITION BY department_name) AS total_sales,
    ROUND(sales/total_sales, 4) AS sales_monthly_perc,
    RANK() OVER (PARTITION BY department_name ORDER BY ROUND(SUM(amount)) DESC) AS rank
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name, year_month
ORDER BY department_name, year_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Seasonality Index

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC WITH cte AS (
-- MAGIC     SELECT
-- MAGIC         MONTH(business_day) AS month,
-- MAGIC         YEAR(business_day) AS year,
-- MAGIC         department_name,
-- MAGIC         ROUND(SUM(amount)) AS sales
-- MAGIC     FROM sandbox.pj_freshfood_data
-- MAGIC     GROUP BY month, year, department_name
-- MAGIC )
-- MAGIC
-- MAGIC SELECT
-- MAGIC     month,
-- MAGIC     department_name,
-- MAGIC     MAX(CASE WHEN (year = 2023 AND month BETWEEN 5 AND 12) OR (year = 2024) THEN sales END) AS cy_sales,
-- MAGIC     MAX(CASE WHEN (year = 2022 AND month BETWEEN 5 AND 12) OR (year = 2023 AND month <= 4) THEN sales END) AS py_sales,
-- MAGIC     (cy_sales + py_sales)/2 AS avg_sales,
-- MAGIC     SUM(MAX(CASE WHEN (year = 2023 AND month BETWEEN 5 AND 12) OR (year = 2024) THEN sales END) + MAX(CASE WHEN (year = 2022 AND month BETWEEN 5 AND 12) OR (year = 2023 AND month <= 4) THEN sales END)) OVER (PARTITION BY department_name) AS total_two_year_sales,
-- MAGIC     ROUND(total_two_year_sales/24, 2) AS avg_two_year_sales,
-- MAGIC     ROUND(avg_sales/avg_two_year_sales, 2) AS seasonality_index
-- MAGIC FROM cte
-- MAGIC GROUP BY month, department_name
-- MAGIC ORDER BY month, department_name
-- MAGIC """
-- MAGIC
-- MAGIC seasonality_index = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC seasonality_index['department_name'] = seasonality_index['department_name'].replace(['FRUITS & VEGETABLES', 'IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)'], ['F&V', 'IHKP'])
-- MAGIC
-- MAGIC seasonality_index['month_order'] = seasonality_index['month'].apply(lambda x: x + 8 if x < 5 else x - 4)
-- MAGIC seasonality_index = seasonality_index.sort_values(by='month_order')
-- MAGIC
-- MAGIC fig = px.line(seasonality_index, x='month_order', y='seasonality_index', color='department_name', markers=True,
-- MAGIC               labels={'month_order': 'Month', 'seasonality_index': 'Seasonality Index', 'department_name': 'Department'},
-- MAGIC               title='Monthly Seasonality Index')
-- MAGIC
-- MAGIC fig.update_layout(
-- MAGIC     xaxis = dict(
-- MAGIC         tickmode = 'array',
-- MAGIC         tickvals = seasonality_index['month_order'].unique(),
-- MAGIC         ticktext = ['May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr']
-- MAGIC     )
-- MAGIC )
-- MAGIC fig.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Store Penetration for Top Performing SKUs

-- COMMAND ----------

WITH top_skus AS (
    SELECT
        material_id,
        SUM(amount) AS sales,
        SUM(SUM(amount)) OVER () AS total_sales,
        ROUND(sales/total_sales, 4) AS sales_contri,
        SUM(gross_profit) AS gp,
        SUM(SUM(gross_profit)) OVER () AS total_gp,
        ROUND(gp/total_gp, 4) AS gp_contri
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY material_id
    ORDER BY sales_contri DESC
    LIMIT 10
),

total_stores AS (
    SELECT COUNT(DISTINCT store_id) AS all_stores
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
),

store_counts AS (
    SELECT
        t2.material_id,
        material_name,
        category_name,
        department_name,
        sales_contri,
        gp_contri,
        COUNT(DISTINCT store_id) AS stores
    FROM sandbox.pj_freshfood_data AS t1
    JOIN top_skus AS t2 ON t1.material_id = t2.material_id
    WHERE business_day >= "2023-05-01"
    GROUP BY t2.material_id, material_name, category_name, department_name, sales_contri, gp_contri
)

SELECT
    department_name,
    category_name,
    material_name,
    sales_contri,
    gp_contri,
    ROUND(stores/all_stores, 3) AS store_pnt
FROM total_stores, store_counts
ORDER BY sales_contri DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Dominating Nationality, Gender, Age

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Nationality

-- COMMAND ----------

WITH total_customers AS (
    SELECT COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND nationality_group NOT IN ("OTHERS", "NA")
)

SELECT
    nationality_group,
    COUNT(DISTINCT customer_id) AS customers,
    ROUND(COUNT(DISTINCT customer_id) / MAX(all_customers), 3) AS customer_perc
FROM total_customers, sandbox.pj_freshfood_data
WHERE
    business_day >= "2023-05-01"
    AND nationality_group NOT IN ("OTHERS", "NA")
GROUP BY nationality_group
ORDER BY customer_perc DESC

-- COMMAND ----------

WITH total_customers AS (
    SELECT
        department_name,
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND nationality_group NOT IN ("OTHERS", "NA")
    GROUP BY department_name
),

nationality_customers AS (
    SELECT
        department_name,
        nationality_group,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND nationality_group NOT IN ("OTHERS", "NA")
    GROUP BY department_name, nationality_group
)

SELECT
    t1.department_name,
    nationality_group,
    ROUND(MAX(customers) / MAX(all_customers), 3) AS customer_perc
FROM total_customers AS t1
JOIN nationality_customers AS t2 ON t1.department_name = t2.department_name
GROUP BY t1.department_name, nationality_group
ORDER BY department_name, customer_perc DESC

-- COMMAND ----------

-- Nationality distribution for each material group

WITH total_customers AS (
    SELECT
        department_name,
        material_group_name,
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND nationality_group NOT IN ("OTHERS", "NA")
    GROUP BY department_name, material_group_name
),

nationality_customers AS (
    SELECT
        department_name,
        material_group_name,
        nationality_group,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND nationality_group NOT IN ("OTHERS", "NA")
    GROUP BY department_name, material_group_name, nationality_group
),

nationality_distribution AS (SELECT
    t1.department_name,
    t1.material_group_name,
    nationality_group,
    ROUND(MAX(customers) / MAX(all_customers), 3) AS customer_perc,
    ROW_NUMBER() OVER (PARTITION BY t1.department_name, t1.material_group_name ORDER BY MAX(customers) / MAX(all_customers) DESC) AS rn
FROM total_customers AS t1
JOIN nationality_customers AS t2
    ON t1.department_name = t2.department_name
    AND t1.material_group_name = t2.material_group_name
GROUP BY t1.department_name, t1.material_group_name, nationality_group
-- ORDER BY t1.department_name, t1.material_group_name, customer_perc DESC
)

SELECT
    department_name,
    material_group_name,
    nationality_group,
    customer_perc
FROM nationality_distribution
WHERE rn = 1
ORDER BY department_name, material_group_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Gender

-- COMMAND ----------

WITH total_customers AS (
    SELECT COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND gender_modified != "Unknown"
)

SELECT
    gender_modified,
    COUNT(DISTINCT customer_id) AS customers,
    ROUND(COUNT(DISTINCT customer_id) / MAX(all_customers), 3) AS customer_perc
FROM total_customers, sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND gender_modified != "Unknown"
GROUP BY gender_modified
ORDER BY customer_perc DESC

-- COMMAND ----------

WITH total_customers AS (
    SELECT
        department_name,
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND gender_modified != "Unknown"
    GROUP BY department_name
),

gender_customers AS (
    SELECT
        department_name,
        gender_modified,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND gender_modified != "Unknown"
    GROUP BY department_name, gender_modified
)

SELECT
    t1.department_name,
    gender_modified,
    ROUND(MAX(customers) / MAX(all_customers), 3) AS customer_perc
FROM total_customers AS t1
JOIN gender_customers AS t2 ON t1.department_name = t2.department_name
GROUP BY t1.department_name, gender_modified
ORDER BY department_name, customer_perc DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Age

-- COMMAND ----------

WITH age AS (
    SELECT DISTINCT
        customer_id,
        age
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND customer_id IS NOT NULL
        AND age BETWEEN 18 AND 80
)

SELECT
    MEDIAN(age)
FROM age

-- COMMAND ----------

WITH age AS (
    SELECT DISTINCT
        department_name,
        customer_id,
        age
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND customer_id IS NOT NULL
        AND age BETWEEN 18 AND 80
)

SELECT
    department_name,
    MEDIAN(age)
FROM age
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Sales Growth

-- COMMAND ----------

SELECT
    SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END) AS cy_sales,
    SUM(CASE WHEN business_day < "2023-05-01" THEN amount END) AS py_sales,
    ROUND((cy_sales - py_sales)/py_sales, 3) AS growth
FROM sandbox.pj_freshfood_data

-- COMMAND ----------

SELECT
    department_name,
    SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END) AS cy_sales,
    SUM(CASE WHEN business_day < "2023-05-01" THEN amount END) AS py_sales,
    ROUND((cy_sales - py_sales)/py_sales, 3) AS growth
FROM sandbox.pj_freshfood_data
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Transactions Growth

-- COMMAND ----------

SELECT
    COUNT(DISTINCT CASE WHEN business_day >= "2023-05-01" THEN transaction_id END) AS cy_trans,
    COUNT(DISTINCT CASE WHEN business_day < "2023-05-01" THEN transaction_id END) AS py_trans,
    ROUND((cy_trans - py_trans)/py_trans, 3) AS growth
FROM sandbox.pj_freshfood_data

-- COMMAND ----------

SELECT
    department_name,
    COUNT(DISTINCT CASE WHEN business_day >= "2023-05-01" THEN transaction_id END) AS cy_trans,
    COUNT(DISTINCT CASE WHEN business_day < "2023-05-01" THEN transaction_id END) AS py_trans,
    ROUND((cy_trans - py_trans)/py_trans, 3) AS growth
FROM sandbox.pj_freshfood_data
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Transactions Contribution

-- COMMAND ----------

SELECT COUNT(DISTINCT transaction_id)
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"

-- COMMAND ----------

SELECT COUNT(DISTINCT transaction_id)
FROM gold.pos_transactions AS t1
LEFT JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
WHERE business_day BETWEEN "2023-05-01" AND "2024-04-30"
AND transaction_type IN ("SALE", "SELL_MEDIA")
AND amount > 0
AND quantity > 0
AND tayeb_flag = 0

-- COMMAND ----------

SELECT ROUND(53636759 / 90082332, 3) AS trans_contri

-- COMMAND ----------

WITH tot_trans AS (
    SELECT
        COUNT(DISTINCT transaction_id) AS total_trans
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
)

SELECT
    department_name,
    COUNT(DISTINCT transaction_id) AS trans_count,
    ROUND(trans_count/MAX(total_trans), 3) AS trans_contri
FROM tot_trans, sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY trans_contri DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Gross Profit Contribution

-- COMMAND ----------

SELECT SUM(gross_profit)
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"

-- COMMAND ----------

WITH gp_perc AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH"
            END AS region_name,
        year_month,
        material_id,
        gp_wth_chargeback
    FROM gold.gross_profit
    WHERE country = 'AE'
),

sales_data AS (
    SELECT
        INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
        region_name,
        product_id,
        SUM(amount) AS sales
    FROM gold.pos_transactions AS t1
    LEFT JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
    WHERE
        business_day BETWEEN "2023-05-01" AND "2024-04-30"
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
        AND tayeb_flag = 0
    GROUP BY year_month, region_name, product_id
),

gp_value AS (
    SELECT
        t1.*,
        (sales * gp_wth_chargeback / 100) AS gross_profit
    FROM sales_data AS t1
    LEFT JOIN gp_perc AS t2
        ON t1.region_name = t2.region_name
        AND t1.year_month = t2.year_month
        AND t1.product_id = t2.material_id
)

SELECT SUM(gross_profit)
FROM gp_value

-- COMMAND ----------

SELECT ROUND(477657513.56353554 / 1764609485.749591, 3) AS gp_contri

-- COMMAND ----------

SELECT
    department_name,
    SUM(gross_profit) AS gp,
    SUM(SUM(gross_profit)) OVER () AS total_gp,
    ROUND(gp/total_gp, 3) AS gp_contri
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY gp_contri DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #ATV

-- COMMAND ----------

SELECT
    ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END) / COUNT(DISTINCT CASE WHEN business_day >= "2023-05-01" THEN transaction_id END), 1) AS cy_ATV,
    ROUND(SUM(CASE WHEN business_day < "2023-05-01" THEN amount END) / COUNT(DISTINCT CASE WHEN business_day < "2023-05-01" THEN transaction_id END), 1) AS py_ATV,
    ROUND((cy_ATV - py_ATV)/py_ATV, 4) AS growth
FROM sandbox.pj_freshfood_data

-- COMMAND ----------

SELECT
    department_name,
    ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END) / COUNT(DISTINCT CASE WHEN business_day >= "2023-05-01" THEN transaction_id END), 1) AS cy_ATV,
    ROUND(SUM(CASE WHEN business_day < "2023-05-01" THEN amount END) / COUNT(DISTINCT CASE WHEN business_day < "2023-05-01" THEN transaction_id END), 1) AS py_ATV,
    ROUND((cy_ATV - py_ATV)/py_ATV, 4) AS growth
FROM sandbox.pj_freshfood_data
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #One Timers

-- COMMAND ----------

-- One Timers Count and % of Total Fresh Food Customers

WITH total_customers AS (
    SELECT
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
),

one_timers AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY customer_id
    HAVING orders = 1
)

SELECT
    COUNT(DISTINCT customer_id) AS OTs,
    ROUND(OTs / MAX(all_customers), 3) AS perc
FROM total_customers, one_timers

-- COMMAND ----------

-- Departments where the One Timers made purchases after which they never transacted in Fresh Food

WITH one_timers AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY customer_id
    HAVING orders = 1
)

SELECT
    department_name,
    COUNT(DISTINCT t2.customer_id) AS one_timers
FROM sandbox.pj_freshfood_data AS t1
JOIN one_timers AS t2 ON t1.customer_id = t2.customer_id
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY one_timers DESC

-- COMMAND ----------

-- RFM Segments where the One Timers belong

WITH one_timers AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY customer_id
    HAVING orders = 1
)

SELECT
    segment,
    COUNT(DISTINCT t2.customer_id) AS one_timers
FROM sandbox.pj_freshfood_data AS t1
JOIN one_timers AS t2 ON t1.customer_id = t2.customer_id
WHERE business_day >= "2023-05-01"
GROUP BY segment
ORDER BY one_timers DESC

-- COMMAND ----------

-- One Timers in Each Department

WITH total_customers AS (
    SELECT
        department_name,
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY department_name
),

one_timers AS (
    SELECT
        department_name,
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY department_name, customer_id
    HAVING orders = 1
)

SELECT
    t1.department_name,
    COUNT(DISTINCT customer_id) AS OTs,
    ROUND(OTs / MAX(all_customers), 3) AS perc
FROM total_customers AS t1
JOIN one_timers AS t2 ON t1.department_name = t2.department_name
GROUP BY t1.department_name
ORDER BY t1.department_name

-- COMMAND ----------

-- One Timers Index in Each Department

WITH mg_total_customers AS (
    SELECT
        department_name,
        material_group_name,
        COUNT(DISTINCT customer_id) AS mg_all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name, material_group_name
),

mg_one_timers AS (
    SELECT
        department_name,
        material_group_name,
        customer_id,
        COUNT(DISTINCT transaction_id) AS mg_orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name, material_group_name, customer_id
    HAVING mg_orders = 1
),

mg_OT_perc AS (
    SELECT
        t1.department_name,
        t1.material_group_name,
        COUNT(DISTINCT customer_id) AS mg_OTs,
        ROUND(mg_OTs / MAX(mg_all_customers), 3) AS mg_perc
    FROM mg_total_customers AS t1
    JOIN mg_one_timers AS t2
        ON t1.department_name = t2.department_name
        AND t1.material_group_name = t2.material_group_name
    GROUP BY t1.department_name, t1.material_group_name
),

dept_total_customers AS (
    SELECT
        department_name,
        COUNT(DISTINCT customer_id) AS dept_all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name
),

dept_one_timers AS (
    SELECT
        department_name,
        customer_id,
        COUNT(DISTINCT transaction_id) AS dept_orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name, customer_id
    HAVING dept_orders = 1
),

dept_OT_perc AS (
    SELECT
        t1.department_name,
        COUNT(DISTINCT customer_id) AS dept_OTs,
        ROUND(dept_OTs / MAX(dept_all_customers), 3) AS dept_perc
    FROM dept_total_customers AS t1
    JOIN dept_one_timers AS t2 ON t1.department_name = t2.department_name
    GROUP BY t1.department_name
    ORDER BY t1.department_name
),

mg_index AS (
    SELECT
        t1.department_name,
        material_group_name,
        mg_perc,
        dept_perc,
        ROUND(mg_perc/dept_perc, 2) AS index,
        ROW_NUMBER() OVER (PARTITION BY t1.department_name ORDER BY ROUND(mg_perc / dept_perc, 2) DESC) AS row_num
    FROM mg_OT_perc AS t1
    JOIN dept_OT_perc AS t2 ON t1.department_name = t2.department_name
)

SELECT
    department_name,
    material_group_name,
    index
FROM mg_index
-- WHERE row_num <= 10
ORDER BY department_name, index DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Repeat Customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Department & MG Level

-- COMMAND ----------

-- Repeat Customers Count and % of Total Fresh Food Customers

WITH total_customers AS (
    SELECT
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
),

repeats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY customer_id
    HAVING orders > 1
)

SELECT
    COUNT(DISTINCT customer_id) AS repeat_customers,
    ROUND(repeat_customers / MAX(all_customers), 3) AS perc
FROM total_customers, repeats

-- COMMAND ----------

-- Departments where the Repeat Customers made purchases

WITH repeats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY customer_id
    HAVING orders > 1
)

SELECT
    department_name,
    COUNT(DISTINCT t2.customer_id) AS repeat_customers
FROM sandbox.pj_freshfood_data AS t1
JOIN repeats AS t2 ON t1.customer_id = t2.customer_id
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY repeat_customers DESC

-- COMMAND ----------

-- RFM Segments where the Repeat Customers belong

WITH repeats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY customer_id
    HAVING orders > 1
)

SELECT
    segment,
    COUNT(DISTINCT t2.customer_id) AS repeat_customers
FROM sandbox.pj_freshfood_data AS t1
JOIN repeats AS t2 ON t1.customer_id = t2.customer_id
WHERE business_day >= "2023-05-01"
GROUP BY segment
ORDER BY repeat_customers DESC

-- COMMAND ----------

-- RFM Segments where the Repeat Customers belong

WITH repeats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY customer_id
    HAVING orders > 2
)

SELECT
    segment,
    COUNT(DISTINCT t2.customer_id) AS repeat_customers
FROM sandbox.pj_freshfood_data AS t1
JOIN repeats AS t2 ON t1.customer_id = t2.customer_id
WHERE business_day >= "2023-05-01"
GROUP BY segment
ORDER BY repeat_customers DESC

-- COMMAND ----------

-- Repeat Customers Index in Each Material Group by Department

WITH mg_total_customers AS (
    SELECT
        department_name,
        material_group_name,
        COUNT(DISTINCT customer_id) AS mg_all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name, material_group_name
),

mg_repeats AS (
    SELECT
        department_name,
        material_group_name,
        customer_id,
        COUNT(DISTINCT transaction_id) AS mg_orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name, material_group_name, customer_id
    HAVING mg_orders > 1
),

mg_repeat_perc AS (
    SELECT
        t1.department_name,
        t1.material_group_name,
        COUNT(DISTINCT customer_id) AS mg_repeat_customers,
        mg_repeat_customers / MAX(mg_all_customers) AS mg_perc
    FROM mg_total_customers AS t1
    JOIN mg_repeats AS t2
        ON t1.department_name = t2.department_name
        AND t1.material_group_name = t2.material_group_name
    GROUP BY t1.department_name, t1.material_group_name
),

dept_total_customers AS (
    SELECT
        department_name,
        COUNT(DISTINCT customer_id) AS dept_all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name
),

dept_repeats AS (
    SELECT
        department_name,
        customer_id,
        COUNT(DISTINCT transaction_id) AS dept_orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY department_name, customer_id
    HAVING dept_orders > 1
),

dept_repeat_perc AS (
    SELECT
        t1.department_name,
        COUNT(DISTINCT customer_id) AS dept_repeat_customers,
        dept_repeat_customers / MAX(dept_all_customers) AS dept_perc
    FROM dept_total_customers AS t1
    JOIN dept_repeats AS t2 ON t1.department_name = t2.department_name
    GROUP BY t1.department_name
    ORDER BY t1.department_name
),

mg_index AS (
    SELECT
        t1.department_name,
        material_group_name,
        mg_perc,
        dept_perc,
        ROUND(mg_perc/dept_perc, 2) AS index,
        ROW_NUMBER() OVER (PARTITION BY t1.department_name ORDER BY ROUND(mg_perc / dept_perc, 2) DESC) AS row_num
    FROM mg_repeat_perc AS t1
    JOIN dept_repeat_perc AS t2 ON t1.department_name = t2.department_name
)

SELECT
    department_name,
    material_group_name,
    index
FROM mg_index
-- WHERE row_num <= 10
ORDER BY department_name, index DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Region Split

-- COMMAND ----------

-- Repeat Customers Index in Each Material Group by Department

WITH mg_total_customers AS (
    SELECT
        region_name,
        department_name,
        material_group_name,
        COUNT(DISTINCT customer_id) AS mg_all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY region_name, department_name, material_group_name
),

mg_repeats AS (
    SELECT
        region_name,
        department_name,
        material_group_name,
        customer_id,
        COUNT(DISTINCT transaction_id) AS mg_orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY region_name, department_name, material_group_name, customer_id
    HAVING mg_orders > 1
),

mg_repeat_perc AS (
    SELECT
        t1.region_name,
        t1.department_name,
        t1.material_group_name,
        COUNT(DISTINCT customer_id) AS mg_repeat_customers,
        mg_repeat_customers / MAX(mg_all_customers) AS mg_perc
    FROM mg_total_customers AS t1
    JOIN mg_repeats AS t2
        ON t1.region_name = t2.region_name
        AND t1.department_name = t2.department_name
        AND t1.material_group_name = t2.material_group_name
    GROUP BY t1.region_name, t1.department_name, t1.material_group_name
),

dept_total_customers AS (
    SELECT
        region_name,
        department_name,
        COUNT(DISTINCT customer_id) AS dept_all_customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY region_name, department_name
),

dept_repeats AS (
    SELECT
        region_name,
        department_name,
        customer_id,
        COUNT(DISTINCT transaction_id) AS dept_orders
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND customer_id IS NOT NULL
    GROUP BY region_name, department_name, customer_id
    HAVING dept_orders > 1
),

dept_repeat_perc AS (
    SELECT
        t1.region_name,
        t1.department_name,
        COUNT(DISTINCT customer_id) AS dept_repeat_customers,
        dept_repeat_customers / MAX(dept_all_customers) AS dept_perc
    FROM dept_total_customers AS t1
    JOIN dept_repeats AS t2
        ON t1.region_name = t2.region_name
        AND t1.department_name = t2.department_name
    GROUP BY t1.region_name, t1.department_name
),

mg_index AS (
    SELECT
        t1.region_name,
        t1.department_name,
        material_group_name,
        mg_perc,
        dept_perc,
        ROUND(mg_perc/dept_perc, 2) AS index,
        ROW_NUMBER() OVER (PARTITION BY t1.region_name, t1.department_name ORDER BY ROUND(mg_perc / dept_perc, 2) DESC) AS row_num
    FROM mg_repeat_perc AS t1
    JOIN dept_repeat_perc AS t2
        ON t1.region_name = t2.region_name
        AND t1.department_name = t2.department_name
)

SELECT
    region_name,
    department_name,
    material_group_name,
    index
FROM mg_index
WHERE row_num <= 10
ORDER BY region_name, department_name, index DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Average Basket Size

-- COMMAND ----------

WITH cte AS (
    SELECT
        business_day,
        transaction_id,
        COUNT(DISTINCT material_id) AS products
    FROM sandbox.pj_freshfood_data
    GROUP BY business_day, transaction_id
)

SELECT
    ROUND(AVG(CASE WHEN business_day >= "2023-05-01" THEN products END), 1) AS cy_avg_basket_size,
    ROUND(AVG(CASE WHEN business_day < "2023-05-01" THEN products END), 1) AS py_avg_basket_size,
    ROUND((cy_avg_basket_size - py_avg_basket_size) / py_avg_basket_size, 4) AS growth
FROM cte

-- COMMAND ----------

WITH cte AS (
      SELECT
          department_name,
          transaction_id,
          COUNT(DISTINCT material_id) AS products
      FROM sandbox.pj_freshfood_data
      WHERE business_day >= "2023-05-01"
      GROUP BY department_name, transaction_id
)

SELECT
    department_name,
    ROUND(AVG(products), 1) AS avg_basket_size
FROM cte
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Highest Selling Day of the Week

-- COMMAND ----------

SELECT
    CASE WHEN DAYOFWEEK(business_day) = 1 THEN "Sun"
        WHEN DAYOFWEEK(business_day) = 2 THEN "Mon"
        WHEN DAYOFWEEK(business_day) = 3 THEN "Tue"
        WHEN DAYOFWEEK(business_day) = 4 THEN "Wed"
        WHEN DAYOFWEEK(business_day) = 5 THEN "Thu"
        WHEN DAYOFWEEK(business_day) = 6 THEN "Fri"
        ELSE "Sat"
        END AS day_of_week,
    ROUND(SUM(amount)) AS sales,
    COUNT(DISTINCT transaction_id) AS transactions,
    ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS sales_rank,
    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT transaction_id) DESC) AS transactions_rank
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY day_of_week

-- COMMAND ----------

SELECT
    department_name,
    CASE WHEN DAYOFWEEK(business_day) = 1 THEN "Sun"
        WHEN DAYOFWEEK(business_day) = 2 THEN "Mon"
        WHEN DAYOFWEEK(business_day) = 3 THEN "Tue"
        WHEN DAYOFWEEK(business_day) = 4 THEN "Wed"
        WHEN DAYOFWEEK(business_day) = 5 THEN "Thu"
        WHEN DAYOFWEEK(business_day) = 6 THEN "Fri"
        ELSE "Sat"
        END AS day_of_week,
    ROUND(SUM(amount)) AS sales,
    COUNT(DISTINCT transaction_id) AS transactions,
    ROW_NUMBER() OVER (PARTITION BY department_name ORDER BY SUM(amount) DESC) AS sales_rank,
    ROW_NUMBER() OVER (PARTITION BY department_name ORDER BY COUNT(DISTINCT transaction_id) DESC) AS transactions_rank
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name, day_of_week

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Frequency - Loyalty & All Customers

-- COMMAND ----------

SELECT
    ROUND(COUNT(DISTINCT transaction_id)/COUNT(DISTINCT MONTH(business_day))) AS all_frequency,
    ROUND(COUNT(DISTINCT CASE WHEN loyalty_points > 0 THEN transaction_id END)/COUNT(DISTINCT MONTH(business_day))) AS loyalty_frequency,
    ROUND(loyalty_frequency/all_frequency, 3) AS perc
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"

-- COMMAND ----------

SELECT
    department_name,
    ROUND(COUNT(DISTINCT transaction_id)/COUNT(DISTINCT MONTH(business_day))) AS all_frequency,
    ROUND(COUNT(DISTINCT CASE WHEN loyalty_points > 0 THEN transaction_id END)/COUNT(DISTINCT MONTH(business_day))) AS loyalty_frequency,
    ROUND(loyalty_frequency/all_frequency, 3) AS perc
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Loyalty Customers

-- COMMAND ----------

SELECT COUNT(DISTINCT customer_id)
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND LHRDATE IS NOT NULL

-- COMMAND ----------

SELECT
    COUNT(DISTINCT CASE WHEN loyalty_points > 0 THEN customer_id END) AS loyalty_customers,
    COUNT(DISTINCT customer_id) AS all_customers,
    ROUND(loyalty_customers/all_customers, 3) AS perc
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"

-- COMMAND ----------

SELECT ROUND(1475364 / 1764296, 4) AS LHP_penetration

-- COMMAND ----------

SELECT
    department_name,
    COUNT(DISTINCT CASE WHEN loyalty_points > 0 THEN customer_id END) AS loyalty_customers,
    COUNT(DISTINCT customer_id) AS all_customers,
    ROUND(loyalty_customers/all_customers, 3) AS perc
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Categories & SKUs

-- COMMAND ----------

SELECT
    COUNT(DISTINCT CASE WHEN business_day >= "2023-05-01" THEN category_name END) AS cy_categories,
    COUNT(DISTINCT CASE WHEN business_day < "2023-05-01" THEN category_name END) AS py_categories,
    ROUND((cy_categories - py_categories)/py_categories, 4) AS category_growth,
    COUNT(DISTINCT CASE WHEN business_day >= "2023-05-01" THEN material_id END) AS cy_materials,
    COUNT(DISTINCT CASE WHEN business_day < "2023-05-01" THEN material_id END) AS py_materials,
    ROUND((cy_materials - py_materials)/py_materials, 4) AS material_growth
FROM sandbox.pj_freshfood_data

-- COMMAND ----------

SELECT
    department_name,
    COUNT(DISTINCT category_name),
    COUNT(DISTINCT material_id)
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Region-wise Sales & Growth

-- COMMAND ----------

SELECT
    region_name,
    ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END)) AS cy_sales,
    SUM(ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END))) OVER () AS total_sales,
    ROUND(cy_sales/total_sales, 3) AS sales_perc,
    ROUND(SUM(CASE WHEN business_day < "2023-05-01" THEN amount END)) AS py_sales,
    ROUND((cy_sales - py_sales)/py_sales, 3) AS growth
FROM sandbox.pj_freshfood_data
GROUP BY region_name
ORDER BY cy_sales DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Department & MG Level RFM Segment Affinity

-- COMMAND ----------

WITH cte AS (
  SELECT
      department_name,
      segment,
      ROUND(SUM(amount)) AS sales
  FROM sandbox.pj_freshfood_data
  WHERE business_day >= "2023-05-01"
  GROUP BY department_name, segment
)

SELECT
    department_name,
    MAX(CASE WHEN segment = "VIP" THEN sales ELSE 0 END) AS VIP_sales,
    MAX(CASE WHEN segment = "Frequentist" THEN sales ELSE 0 END) AS Freq_sales,
    MAX(CASE WHEN segment = "Splurger" THEN sales ELSE 0 END) AS Splur_sales,
    MAX(CASE WHEN segment = "Slipping Loyalist" THEN sales ELSE 0 END) AS SL_sales,
    MAX(CASE WHEN segment = "Moderate" THEN sales ELSE 0 END) AS Mod_sales,
    MAX(CASE WHEN segment = "Lapser" THEN sales ELSE 0 END) AS Lap_sales,
    MAX(CASE WHEN segment = "Newbie" THEN sales ELSE 0 END) AS New_sales
FROM cte
GROUP BY department_name

-- COMMAND ----------

WITH cte AS (
  SELECT
      department_name,
      segment,
      COUNT(DISTINCT transaction_id) AS transactions
  FROM sandbox.pj_freshfood_data
  WHERE business_day >= "2023-05-01"
  GROUP BY department_name, segment
)

SELECT
    department_name,
    MAX(CASE WHEN segment = "VIP" THEN transactions ELSE 0 END) AS VIP_trans,
    MAX(CASE WHEN segment = "Frequentist" THEN transactions ELSE 0 END) AS Freq_trans,
    MAX(CASE WHEN segment = "Splurger" THEN transactions ELSE 0 END) AS Splur_trans,
    MAX(CASE WHEN segment = "Slipping Loyalist" THEN transactions ELSE 0 END) AS SL_trans,
    MAX(CASE WHEN segment = "Moderate" THEN transactions ELSE 0 END) AS Mod_trans,
    MAX(CASE WHEN segment = "Lapser" THEN transactions ELSE 0 END) AS Lap_trans,
    MAX(CASE WHEN segment = "Newbie" THEN transactions ELSE 0 END) AS New_trans
FROM cte
GROUP BY department_name

-- COMMAND ----------

WITH mg_sales_contri AS (
    SELECT
        department_name,
        material_group_name,
        SUM(amount) AS sales,
        SUM(SUM(amount)) OVER() AS total_sales,
        sales/total_sales AS sales_contri,
        SUM(SUM(amount)/SUM(SUM(amount)) OVER()) OVER(ORDER BY SUM(amount)/SUM(SUM(amount)) OVER() DESC) AS cumulative_sales_contri,
        ROW_NUMBER() OVER(ORDER BY SUM(amount)/SUM(SUM(amount)) OVER() DESC) AS row_num
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY department_name, material_group_name
    ORDER BY sales DESC
),

LastMG AS (
    SELECT
        *,
        MAX(CASE WHEN cumulative_sales_contri < 0.95 THEN row_num END) OVER() AS last_below_threshold
    FROM mg_sales_contri
),

top_95_perc_mg AS (
    SELECT
        department_name,
        material_group_name
    FROM LastMG
    WHERE row_num <= last_below_threshold + 1
),

mg_rfm_sales AS (
  SELECT
      t1.department_name,
      t1.material_group_name,
      segment,
      ROUND(SUM(amount)) AS sales
  FROM sandbox.pj_freshfood_data AS t1
  JOIN top_95_perc_mg AS t2
      ON t1.department_name = t2.department_name
      AND t1.material_group_name = t2.material_group_name
  WHERE business_day >= "2023-05-01"
  GROUP BY t1.department_name, t1.material_group_name, segment
)

SELECT
    department_name,
    material_group_name,
    MAX(CASE WHEN segment = "VIP" THEN sales ELSE 0 END) AS VIP_sales,
    MAX(CASE WHEN segment = "Frequentist" THEN sales ELSE 0 END) AS Freq_sales,
    MAX(CASE WHEN segment = "Splurger" THEN sales ELSE 0 END) AS Splur_sales,
    MAX(CASE WHEN segment = "Slipping Loyalist" THEN sales ELSE 0 END) AS SL_sales,
    MAX(CASE WHEN segment = "Moderate" THEN sales ELSE 0 END) AS Mod_sales,
    MAX(CASE WHEN segment = "Lapser" THEN sales ELSE 0 END) AS Lap_sales,
    MAX(CASE WHEN segment = "Newbie" THEN sales ELSE 0 END) AS New_sales
FROM mg_rfm_sales
GROUP BY department_name, material_group_name
ORDER BY material_group_name, department_name

-- COMMAND ----------


WITH mg_sales_contri AS (
    SELECT
        department_name,
        material_group_name,
        SUM(amount) AS sales,
        SUM(SUM(amount)) OVER() AS total_sales,
        sales/total_sales AS sales_contri,
        SUM(SUM(amount)/SUM(SUM(amount)) OVER()) OVER(ORDER BY SUM(amount)/SUM(SUM(amount)) OVER() DESC) AS cumulative_sales_contri,
        ROW_NUMBER() OVER(ORDER BY SUM(amount)/SUM(SUM(amount)) OVER() DESC) AS row_num
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY department_name, material_group_name
    ORDER BY sales DESC
),

LastMG AS (
    SELECT
        *,
        MAX(CASE WHEN cumulative_sales_contri < 0.95 THEN row_num END) OVER() AS last_below_threshold
    FROM mg_sales_contri
),

top_95_perc_mg AS (
    SELECT
        department_name,
        material_group_name
    FROM LastMG
    WHERE row_num <= last_below_threshold + 1
),

mg_rfm_trans AS (
  SELECT
      t1.department_name,
      t1.material_group_name,
      segment,
      COUNT(DISTINCT transaction_id) AS transactions
  FROM sandbox.pj_freshfood_data AS t1
  JOIN top_95_perc_mg AS t2
      ON t1.department_name = t2.department_name
      AND t1.material_group_name = t2.material_group_name
  WHERE business_day >= "2023-05-01"
  GROUP BY t1.department_name, t1.material_group_name, segment
)

SELECT
    department_name,
    material_group_name,
    MAX(CASE WHEN segment = "VIP" THEN transactions ELSE 0 END) AS VIP_trans,
    MAX(CASE WHEN segment = "Frequentist" THEN transactions ELSE 0 END) AS Freq_trans,
    MAX(CASE WHEN segment = "Splurger" THEN transactions ELSE 0 END) AS Splur_trans,
    MAX(CASE WHEN segment = "Slipping Loyalist" THEN transactions ELSE 0 END) AS SL_trans,
    MAX(CASE WHEN segment = "Moderate" THEN transactions ELSE 0 END) AS Mod_trans,
    MAX(CASE WHEN segment = "Lapser" THEN transactions ELSE 0 END) AS Lap_trans,
    MAX(CASE WHEN segment = "Newbie" THEN transactions ELSE 0 END) AS New_trans
FROM mg_rfm_trans
GROUP BY department_name, material_group_name
ORDER BY material_group_name, department_name


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Region & Store-wise Department Affinity

-- COMMAND ----------

WITH cte AS (
  SELECT
      region_name,
      store_name,
      department_name,
      ROUND(SUM(amount)) AS sales
  FROM sandbox.pj_freshfood_data
  WHERE business_day >= "2023-05-01"
  GROUP BY region_name, store_name, department_name
)

SELECT
    region_name,
    store_name,
    MAX(CASE WHEN department_name = "BAKERY" THEN sales ELSE 0 END) AS bakery_sales,
    MAX(CASE WHEN department_name = "DELICATESSEN" THEN sales ELSE 0 END) AS delicatessen_sales,
    MAX(CASE WHEN department_name = "FISH" THEN sales ELSE 0 END) AS fish_sales,
    MAX(CASE WHEN department_name = "FRUITS & VEGETABLES" THEN sales ELSE 0 END) AS fruits_veg_sales,
    MAX(CASE WHEN department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)" THEN sales ELSE 0 END) AS inhouse_prod_sales,
    MAX(CASE WHEN department_name = "MEAT" THEN sales ELSE 0 END) AS meat_sales
FROM cte
GROUP BY region_name, store_name

-- COMMAND ----------

WITH cte AS (
  SELECT
      region_name,
      store_name,
      department_name,
      COUNT(DISTINCT transaction_id) AS transactions
  FROM sandbox.pj_freshfood_data
  WHERE business_day >= "2023-05-01"
  GROUP BY region_name, store_name, department_name
)

SELECT
    region_name,
    store_name,
    MAX(CASE WHEN department_name = "BAKERY" THEN transactions ELSE 0 END) AS bakery_trans,
    MAX(CASE WHEN department_name = "DELICATESSEN" THEN transactions ELSE 0 END) AS delicatessen_trans,
    MAX(CASE WHEN department_name = "FISH" THEN transactions ELSE 0 END) AS fish_trans,
    MAX(CASE WHEN department_name = "FRUITS & VEGETABLES" THEN transactions ELSE 0 END) AS fruits_veg_trans,
    MAX(CASE WHEN department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)" THEN transactions ELSE 0 END) AS inhouse_prod_trans,
    MAX(CASE WHEN department_name = "MEAT" THEN transactions ELSE 0 END) AS meat_trans
FROM cte
GROUP BY region_name, store_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Seasonality Check

-- COMMAND ----------

WITH cte AS (
    SELECT
        material_id,
        material_name,
        category_name,
        MONTH(business_day) AS month,
        SUM(amount) AS sales,
        SUM(SUM(amount)) OVER(PARTITION BY material_id) AS tot_prod_sales,
        ROUND(sales/tot_prod_sales, 3) AS sales_perc
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY material_id, material_name, category_name, month
),

cte2 AS (
    SELECT *
    FROM cte
    WHERE tot_prod_sales > 60000
),

cte3 AS (
    SELECT
        material_id,
        sales_perc
    FROM cte2
    WHERE
        sales_perc >= 0.3
),

cte4 AS (
    SELECT DISTINCT material_id
    FROM cte3
)

SELECT t1.*
FROM cte2 AS t1
JOIN cte4 AS t2 ON t1.material_id = t2.material_id

-- COMMAND ----------

WITH cte AS (
    SELECT
        material_id,
        material_name,
        category_name,
        MONTH(business_day) AS month,
        SUM(amount) AS sales,
        SUM(SUM(amount)) OVER(PARTITION BY material_id) AS tot_prod_sales,
        ROUND(sales/tot_prod_sales, 3) AS sales_perc
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY material_id, material_name, category_name, month
),

cte2 AS (
    SELECT *
    FROM cte
    WHERE tot_prod_sales > 60000
),

cte3 AS (
    SELECT
        material_id,
        sales_perc
    FROM cte2
    WHERE
        sales_perc >= 0.3
),

cte4 AS (
    SELECT DISTINCT material_id
    FROM cte3
),

cte5 AS (
    SELECT t1.*
    FROM cte2 AS t1
    JOIN cte4 AS t2 ON t1.material_id = t2.material_id
)

SELECT
    category_name,
    COUNT(DISTINCT material_id)
FROM cte5
GROUP BY category_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Volume Purchase Trend

-- COMMAND ----------

SELECT
    content_unit,
    COUNT(*)
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND content_unit IS NOT NULL
AND content_unit != "EA"
GROUP BY content_unit

-- COMMAND ----------

WITH cte AS (
    SELECT
        business_day,
        customer_id,
        SUM(quantity * content * conversion_numerator) AS volume
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND content_unit = "KG"
    GROUP BY business_day, customer_id
)

SELECT
    business_day,
    AVG(volume)
FROM cte
GROUP BY business_day
ORDER BY business_day

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Volume Price Changes

-- COMMAND ----------

SELECT
    business_day,
    AVG(actual_unit_price / (conversion_numerator * content)) AS unit_volume_price
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND content_unit = "L"
GROUP BY business_day
ORDER BY business_day

-- COMMAND ----------

SELECT DISTINCT content_unit
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND material_group_name = "ARABIC MEALS"
-- AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
-- AND material_group_name IN ("TORTILLAS PREPACKED", "BOUGHT IN PREPACKED", "CHAPPATHI", "SOFT & GRATED CHEESE", "MALABAR PARATHA")
-- AND material_group_name IN ("IH SALAD & MARINATED", "WHITE CHEESE (BULK)", "BLACK OLIVES", "IH CHEESE", "CHICKEN PREPACKED")

-- COMMAND ----------

SELECT
    business_day,
    AVG(actual_unit_price / (conversion_numerator * content)) AS unit_volume_price
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND content_unit = "KG"
AND material_group_name = "ARABIC MEALS"
GROUP BY business_day
HAVING unit_volume_price IS NOT NULL
ORDER BY business_day

-- COMMAND ----------

SELECT
    business_day,
    actual_unit_price,
    conversion_numerator,
    content
    -- AVG(actual_unit_price / (conversion_numerator * content)) AS unit_volume_price
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND content_unit = "KG"
AND material_group_name = "TORTILLAS PREPACKED"
-- GROUP BY business_day
-- HAVING unit_volume_price IS NOT NULL
ORDER BY business_day

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delicatessen High Growth RCA

-- COMMAND ----------

WITH cte AS (
    SELECT
        year_month,
        segment,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND department_name = "DELICATESSEN"
    GROUP BY year_month, segment
)

SELECT
    year_month,
    MAX(CASE WHEN segment = "VIP" THEN customers END) AS VIPs,
    MAX(CASE WHEN segment = "Frequentist" THEN customers END) AS Frequentists,
    MAX(CASE WHEN segment = "Splurger" THEN customers END) AS Splurgers,
    MAX(CASE WHEN segment = "Slipping Loyalist" THEN customers END) AS Slipping_Loyalists,
    MAX(CASE WHEN segment = "Moderate" THEN customers END) AS Moderates,
    COALESCE(MAX(CASE WHEN segment = "Lapser" THEN customers END), 0) AS Lapsers,
    COALESCE(MAX(CASE WHEN segment = "Newbie" THEN customers END), 0) AS Newbies
FROM cte
GROUP BY year_month
ORDER BY year_month

-- COMMAND ----------

WITH cte AS (
    SELECT
        year_month,
        segment,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    AND department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
    GROUP BY year_month, segment
)

SELECT
    year_month,
    MAX(CASE WHEN segment = "VIP" THEN customers END) AS VIPs,
    MAX(CASE WHEN segment = "Frequentist" THEN customers END) AS Frequentists,
    MAX(CASE WHEN segment = "Splurger" THEN customers END) AS Splurgers,
    MAX(CASE WHEN segment = "Slipping Loyalist" THEN customers END) AS Slipping_Loyalists,
    MAX(CASE WHEN segment = "Moderate" THEN customers END) AS Moderates,
    COALESCE(MAX(CASE WHEN segment = "Lapser" THEN customers END), 0) AS Lapsers,
    COALESCE(MAX(CASE WHEN segment = "Newbie" THEN customers END), 0) AS Newbies
FROM cte
GROUP BY year_month
ORDER BY year_month

-- COMMAND ----------

WITH cte1 AS (
    SELECT
        department_name,
        year_month,
        segment,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY department_name, year_month, segment
),

cte2 AS (
    SELECT
        department_name,
        year_month,
        MAX(CASE WHEN segment = "VIP" THEN customers END) AS VIPs,
        MAX(CASE WHEN segment = "Frequentist" THEN customers END) AS Frequentists,
        MAX(CASE WHEN segment = "Splurger" THEN customers END) AS Splurgers,
        MAX(CASE WHEN segment = "Moderate" THEN customers END) AS Moderates,
        MAX(CASE WHEN segment = "Slipping Loyalist" THEN customers END) AS Slipping_Loyalists
    FROM cte1
    GROUP BY department_name, year_month
    ORDER BY department_name, year_month
),

cte3 AS (
    SELECT
        department_name,
        year_month,
        VIPs,
        Frequentists,
        Splurgers,
        Moderates,
        Slipping_Loyalists,
        COALESCE(LAG(VIPs) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS prev_VIPs,
        COALESCE((VIPs - LAG(VIPs) OVER (PARTITION BY department_name ORDER BY year_month)) / LAG(VIPs) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS mom_growth_VIPs,
        COALESCE(LAG(Frequentists) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS prev_Frequentists,
        COALESCE((Frequentists - LAG(Frequentists) OVER (PARTITION BY department_name ORDER BY year_month)) / LAG(Frequentists) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS mom_growth_Frequentists,
        COALESCE(LAG(Splurgers) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS prev_Splurgers,
        COALESCE((Splurgers - LAG(Splurgers) OVER (PARTITION BY department_name ORDER BY year_month)) / LAG(Splurgers) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS mom_growth_Splurgers,
        COALESCE(LAG(Moderates) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS prev_Moderates,
        COALESCE((Moderates - LAG(Moderates) OVER (PARTITION BY department_name ORDER BY year_month)) / LAG(Moderates) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS mom_growth_Moderates,
        COALESCE(LAG(Slipping_Loyalists) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS prev_Slipping_Loyalists,
        COALESCE((Slipping_Loyalists - LAG(Slipping_Loyalists) OVER (PARTITION BY department_name ORDER BY year_month)) / LAG(Slipping_Loyalists) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS mom_growth_Slipping_Loyalists
    FROM cte2
)

SELECT
    department_name,
    AVG(mom_growth_VIPs) AS avg_mom_growth_VIPs,
    AVG(mom_growth_Frequentists) AS avg_mom_growth_Frequentists,
    AVG(mom_growth_Moderates) AS avg_mom_growth_Moderates,
    AVG(mom_growth_Splurgers) AS avg_mom_growth_Splurgers,
    AVG(mom_growth_Slipping_Loyalists) AS avg_mom_growth_Slipping_Loyalists
FROM cte3
WHERE
    mom_growth_Splurgers > 0
    AND mom_growth_VIPs > 0
    AND mom_growth_Moderates > 0
    AND mom_growth_Slipping_Loyalists > 0
    AND mom_growth_Frequentists > 0
GROUP BY department_name
ORDER BY department_name

-- COMMAND ----------

SELECT
    business_day,
    AVG(actual_unit_price / (conversion_numerator * content)) AS unit_volume_price
FROM sandbox.pj_freshfood_data
WHERE business_day >= "2023-05-01"
AND content_unit = "L"
AND department_name = "DELICATESSEN"
GROUP BY business_day
ORDER BY business_day

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Basket Coverage

-- COMMAND ----------

WITH raw_data AS (
    SELECT
        department_name,
        material_group_name,
        COUNT(*) AS materials,
        SUM(COUNT(*)) OVER () AS total_materials,
        (materials/total_materials) AS mg_perc
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    -- AND segment = "Frequentist"
    GROUP BY department_name, material_group_name
),

mg_counts AS (
    SELECT
        department_name,
        COUNT(DISTINCT material_group_name) AS material_group_count
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY department_name
),

indeces AS (
    SELECT
        t1.department_name,
        material_group_name,
        mg_perc,
        material_group_count,
        ROUND(mg_perc/material_group_count, 4) AS normalized_mg_perc
    FROM raw_data AS t1
    JOIN mg_counts AS t2 ON t1.department_name = t2.department_name
)

SELECT
    department_name,
    material_group_name,
    normalized_mg_perc
FROM indeces
ORDER BY department_name, normalized_mg_perc

-- COMMAND ----------

WITH raw_data AS (
    SELECT
        transaction_id,
        department_name,
        material_group_name,
        COUNT(*) AS materials,
        SUM(COUNT(*)) OVER (PARTITION BY transaction_id) AS tot_trans_materials,
        (materials/tot_trans_materials) AS mg_perc
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY transaction_id, department_name, material_group_name
),

mg_counts AS (
    SELECT
        transaction_id,
        department_name,
        material_group_name,
        COUNT(DISTINCT material_id) AS product_count
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY transaction_id, department_name, material_group_name
),

indeces AS (
    SELECT
        t1.transaction_id,
        t1.department_name,
        t1.material_group_name,
        mg_perc,
        product_count,
        ROUND(mg_perc/product_count, 4) AS normalized_mg_index
    FROM raw_data AS t1
    JOIN mg_counts AS t2
        ON t1.transaction_id = t2.transaction_id
        AND t1.department_name = t2.department_name
        AND t1.material_group_name = t2.material_group_name
)

SELECT
    department_name,
    material_group_name,
    product_count,
    AVG(mg_perc) AS avg_mg_perc,
    AVG(normalized_mg_index) AS avg_normalized_mg_index
FROM indeces
GROUP BY department_name, material_group_name, product_count
ORDER BY department_name, avg_normalized_mg_index

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #IHKP High GP RCA

-- COMMAND ----------

WITH cte AS (
    SELECT
        material_id,
        material_name,
        ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END)) AS sales,
        SUM(ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END))) OVER () AS total_sales,
        ROUND(sales/total_sales, 4) AS sales_contri,
        ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN business_day >= "2023-05-01" THEN amount END) DESC) AS sales_rank,
        ROUND(SUM(CASE WHEN business_day < "2023-05-01" THEN amount END)) AS lfl_sales,
        ROUND((sales - lfl_sales) / lfl_sales, 4) AS sales_growth,
        ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN gross_profit END)) AS gp,
        SUM(ROUND(SUM(CASE WHEN business_day >= "2023-05-01" THEN gross_profit END))) OVER () AS total_gp,
        ROUND(gp/total_gp, 4) AS gp_contri,
        ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN business_day >= "2023-05-01" THEN gross_profit END) DESC) AS gp_rank
    FROM sandbox.pj_freshfood_data
    WHERE department_name = "IN-HOUSE KITCHEN PRODUCTION (HOT/COLD)"
    GROUP BY material_id, material_name
    ORDER BY sales DESC
)

SELECT
    material_name,
    COALESCE(sales, 0) AS sales,
    COALESCE(sales_contri, 0) AS sales_contri,
    sales_rank,
    COALESCE(sales_growth, 0) AS sales_growth,
    COALESCE(gp, 0) AS gp,
    COALESCE(gp_contri, 0) AS gp_contri,
    gp_rank
FROM cte

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Month-over-Month GP Growth

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Department Level

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import plotly.express as px
-- MAGIC import plotly.graph_objects as go
-- MAGIC from plotly.subplots import make_subplots

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC WITH monthly_gp AS (
-- MAGIC     SELECT
-- MAGIC         department_name,
-- MAGIC         year_month,
-- MAGIC         SUM(gross_profit) AS gp
-- MAGIC     FROM sandbox.pj_freshfood_data
-- MAGIC     WHERE business_day >= '2023-05-01'
-- MAGIC     GROUP BY department_name, year_month
-- MAGIC )
-- MAGIC
-- MAGIC SELECT
-- MAGIC     department_name,
-- MAGIC     year_month,
-- MAGIC     gp,
-- MAGIC     COALESCE(LAG(gp) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS prev_gp,
-- MAGIC     COALESCE((gp - LAG(gp) OVER (PARTITION BY department_name ORDER BY year_month)) / LAG(gp) OVER (PARTITION BY department_name ORDER BY year_month), 0) AS mom_growth
-- MAGIC FROM monthly_gp
-- MAGIC ORDER BY department_name, year_month
-- MAGIC """
-- MAGIC
-- MAGIC monthly_gp_growth_dept = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC monthly_gp_growth_dept['year_month'] = monthly_gp_growth_dept['year_month'].astype(str)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC avg_monthly_gp_dept = monthly_gp_growth_dept[monthly_gp_growth_dept['mom_growth'] != 0].groupby('department_name')['mom_growth'].mean().reset_index()
-- MAGIC avg_monthly_gp_dept.rename(columns={'mom_growth': 'avg_monthly_gp_growth'}, inplace=True)
-- MAGIC
-- MAGIC avg_monthly_gp_dept['avg_monthly_gp_growth'] = round(avg_monthly_gp_dept['avg_monthly_gp_growth'], 4)
-- MAGIC
-- MAGIC fig = px.bar(
-- MAGIC     avg_monthly_gp_dept,
-- MAGIC     x='avg_monthly_gp_growth',
-- MAGIC     y='department_name',
-- MAGIC     orientation='h',
-- MAGIC     title='Average Monthly Gross Profit by Department',
-- MAGIC     labels={'avg_monthly_gp_growth': 'Average Monthly GP', 'department_name': 'Department'},
-- MAGIC     text='avg_monthly_gp_growth')
-- MAGIC fig.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Material Group Level

-- COMMAND ----------

SELECT
    department_name,
    material_group_name,
    year_month,
    SUM(gross_profit) AS gp
FROM sandbox.pj_freshfood_data
WHERE business_day >= '2023-05-01'
GROUP BY department_name, material_group_name, year_month

-- COMMAND ----------

-- %py
-- query = """
-- WITH top_95_perc_gp AS (
--     SELECT
--         department_name,
--         material_group_name,
--         SUM(gross_profit) AS gp,
--         SUM(SUM(gross_profit)) OVER (PARTITION BY department_name) AS dept_gp,
--         (gp/dept_gp) AS gp_perc_in_dept
--     FROM sandbox.pj_freshfood_data
--     WHERE business_day >= '2023-05-01'
--     GROUP BY department_name, material_group_name
-- ),

-- ranked_mg AS (
--     SELECT
--         department_name,
--         material_group_name,
--         gp_perc_in_dept,
--         ROW_NUMBER() OVER (PARTITION BY department_name ORDER BY gp_perc_in_dept DESC) AS rn
--     FROM top_95_perc_gp
-- ),

-- mg_cumulative_gp AS (
--     SELECT
--         department_name,
--         material_group_name,
--         gp_perc_in_dept,
--         SUM(gp_perc_in_dept) OVER (PARTITION BY department_name ORDER BY rn) AS cumulative_gp_perc
--     FROM ranked_mg
-- ),

-- monthly_gp AS (
--     SELECT
--         department_name,
--         material_group_name,
--         year_month,
--         SUM(gross_profit) AS gp
--     FROM sandbox.pj_freshfood_data
--     WHERE business_day >= '2023-05-01'
--     GROUP BY department_name, material_group_name, year_month
-- )

-- -- SELECT
-- --     department_name,
-- --     material_group_name,
-- --     year_month,
-- --     gp,
-- --     COALESCE(LAG(gp) OVER (PARTITION BY department_name, material_group_name ORDER BY year_month), 0) AS prev_gp,
-- --     COALESCE((gp - LAG(gp) OVER (PARTITION BY department_name, material_group_name ORDER BY year_month)) / LAG(gp) OVER (PARTITION BY department_name, material_group_name ORDER BY year_month), 0) AS mom_growth
-- -- FROM monthly_gp
-- -- ORDER BY department_name, material_group_name, year_month

-- SELECT * FROM mg_cumulative_gp
-- """

-- monthly_gp_growth_mg = spark.sql(query).toPandas()

-- COMMAND ----------

-- %py
-- monthly_gp_growth_mg['year_month'] = monthly_gp_growth_mg['year_month'].astype(str)

-- COMMAND ----------

-- %py
-- avg_monthly_gp_mg = monthly_gp_growth_mg[monthly_gp_growth_mg['mom_growth'] != 0].groupby(['department_name', 'material_group_name'])['mom_growth'].mean().reset_index()
-- avg_monthly_gp_mg.rename(columns={'mom_growth': 'avg_monthly_gp_growth'}, inplace=True)

-- avg_monthly_gp_mg['avg_monthly_gp_growth'] = round(avg_monthly_gp_mg['avg_monthly_gp_growth'], 4)

-- avg_monthly_gp_mg = avg_monthly_gp_mg.sort_values(by = ['department_name', 'avg_monthly_gp_growth']).reset_index(drop = True)

-- for dept in avg_monthly_gp_mg['department_name'].unique():
--     fig = px.bar(
--         avg_monthly_gp_mg[avg_monthly_gp_mg['department_name'] == dept],
--         x='avg_monthly_gp_growth',
--         y='material_group_name',
--         orientation='h',
--         title=f'Average Monthly Gross Profit by MGs in {dept}',
--         labels={'avg_monthly_gp_growth': 'Average Monthly GP', 'material_group_name': 'Department'})
--     fig.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Bubble Charts

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = """
-- MAGIC WITH a AS (
-- MAGIC     SELECT
-- MAGIC         department_name,
-- MAGIC         material_group_name,
-- MAGIC         ROUND(SUM(CASE WHEN business_day >= '2023-05-01' THEN amount END)) AS cy_sales,
-- MAGIC         ROUND(SUM(CASE WHEN business_day < '2023-05-01' THEN amount END)) AS py_sales,
-- MAGIC         ROUND((cy_sales - py_sales)/py_sales, 4) AS sales_growth,
-- MAGIC         COUNT(DISTINCT CASE WHEN business_day >= '2023-05-01' THEN customer_id END) AS customers,
-- MAGIC         SUM(ROUND(SUM(CASE WHEN business_day >= '2023-05-01' THEN amount END))) OVER (PARTITION BY department_name) AS total_cy_sales,
-- MAGIC         ROUND(cy_sales/total_cy_sales, 4) AS sales_contri
-- MAGIC     FROM sandbox.pj_freshfood_data
-- MAGIC     GROUP BY department_name, material_group_name
-- MAGIC ),
-- MAGIC
-- MAGIC b AS (
-- MAGIC     SELECT
-- MAGIC         COUNT(DISTINCT customer_id) AS total_customers
-- MAGIC     FROM sandbox.pj_freshfood_data
-- MAGIC     WHERE business_day >= '2023-05-01'
-- MAGIC )
-- MAGIC
-- MAGIC SELECT
-- MAGIC     *,
-- MAGIC     ROUND(customers/total_customers, 4) AS customer_penetration
-- MAGIC FROM a, b
-- MAGIC """
-- MAGIC
-- MAGIC ff_depts = spark.sql(query).toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # Removing material groups that haven't had any sales in the last 1 year
-- MAGIC # Any material groups that don't have sales in the LFL year, their null values are replaced with 0
-- MAGIC
-- MAGIC ff_depts = ff_depts.dropna(subset=['cy_sales'])
-- MAGIC ff_depts['py_sales'] = ff_depts['py_sales'].fillna(0)
-- MAGIC ff_depts['sales_growth'] = ff_depts['sales_growth'].fillna(0)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC x_val = 'sales_growth'
-- MAGIC y_val = 'customer_penetration'
-- MAGIC hover_name_val = 'material_group_name'
-- MAGIC size_max_val = 40
-- MAGIC size_val = 'sales_contri'
-- MAGIC
-- MAGIC for dept in ff_depts['department_name'].unique():
-- MAGIC     temp = ff_depts[ff_depts['department_name'] == dept]
-- MAGIC     fig = px.scatter(temp, x = x_val, y = y_val, size=np.abs(temp[size_val]),
-- MAGIC                     hover_name = hover_name_val, log_x = False, log_y = False,
-- MAGIC                     size_max = size_max_val,title = dept,
-- MAGIC                     labels = {'sales_growth': 'Sales Growth',
-- MAGIC                             'customer_penetration': 'Customer Penetration'})
-- MAGIC     fig.add_hline(y=temp[y_val].mean())
-- MAGIC     fig.add_vline(x=temp[x_val].mean())
-- MAGIC     fig.show()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC promote_mg = pd.DataFrame()
-- MAGIC grow_mg = pd.DataFrame()
-- MAGIC for dept in ff_depts['department_name'].unique():
-- MAGIC     temp = ff_depts[ff_depts['department_name'] == dept]
-- MAGIC     y_mean = temp['sales_growth'].mean()
-- MAGIC     x_mean = temp['customer_penetration'].mean()
-- MAGIC
-- MAGIC     temp_promote = temp[(temp['sales_growth'] > y_mean) & (temp['customer_penetration'] < x_mean)][['department_name', 'material_group_name', 'cy_sales', 'sales_contri']]
-- MAGIC     temp_grow = temp[(temp['sales_growth'] < y_mean) & (temp['customer_penetration'] > x_mean)][['department_name', 'material_group_name', 'cy_sales', 'sales_contri']]
-- MAGIC
-- MAGIC     promote_mg = pd.concat([promote_mg, temp_promote])
-- MAGIC     grow_mg = pd.concat([grow_mg, temp_grow])
-- MAGIC
-- MAGIC     promote_mg = promote_mg.sort_values(by = ['department_name', 'sales_contri'], ascending = [True, False]).reset_index(drop=True)
-- MAGIC     grow_mg = grow_mg.sort_values(by = ['department_name', 'sales_contri'], ascending = [True, False]).reset_index(drop=True)
-- MAGIC
-- MAGIC promote_mg.display()
-- MAGIC grow_mg.display()

-- COMMAND ----------

-- Checking Nationality of IHKP Q1

WITH total_customers AS (
    SELECT
        material_group_name,
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND nationality_group NOT IN ("OTHERS", "NA")
        AND material_group_name IN ("INTERNATIONAL MEALS", "FRUIT & SALAD BAR", "INTL BREAKFAST", "DESSERTS", "CHARCOAL GRILLS", "SUSHI", "PASTA STATION", "BOUGHT IN CHILLFOOD")
    GROUP BY material_group_name
),

nationality_customers AS (
    SELECT
        material_group_name,
        nationality_group,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND nationality_group NOT IN ("OTHERS", "NA")
        AND material_group_name IN ("INTERNATIONAL MEALS", "FRUIT & SALAD BAR", "INTL BREAKFAST", "DESSERTS", "CHARCOAL GRILLS", "SUSHI", "PASTA STATION", "BOUGHT IN CHILLFOOD")
    GROUP BY material_group_name, nationality_group
)

SELECT
    t1.material_group_name,
    nationality_group,
    ROUND(MAX(customers) / MAX(all_customers), 3) AS customer_perc
FROM total_customers AS t1
JOIN nationality_customers AS t2 ON t1.material_group_name = t2.material_group_name
GROUP BY t1.material_group_name, nationality_group
ORDER BY material_group_name, customer_perc DESC

-- COMMAND ----------

-- Checking Gender of IHKP Q1

WITH total_customers AS (
    SELECT
        material_group_name,
        COUNT(DISTINCT customer_id) AS all_customers
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND gender_modified != "Unknown"
        AND material_group_name IN ("INTERNATIONAL MEALS", "FRUIT & SALAD BAR", "INTL BREAKFAST", "DESSERTS", "CHARCOAL GRILLS", "SUSHI", "PASTA STATION", "BOUGHT IN CHILLFOOD")
    GROUP BY material_group_name
),

gender_customers AS (
    SELECT
        material_group_name,
        gender_modified,
        COUNT(DISTINCT customer_id) AS customers
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND gender_modified != "Unknown"
        AND material_group_name IN ("INTERNATIONAL MEALS", "FRUIT & SALAD BAR", "INTL BREAKFAST", "DESSERTS", "CHARCOAL GRILLS", "SUSHI", "PASTA STATION", "BOUGHT IN CHILLFOOD")
    GROUP BY material_group_name, gender_modified
)

SELECT
    t1.material_group_name,
    gender_modified,
    ROUND(MAX(customers) / MAX(all_customers), 3) AS customer_perc
FROM total_customers AS t1
JOIN gender_customers AS t2 ON t1.material_group_name = t2.material_group_name
GROUP BY t1.material_group_name, gender_modified
ORDER BY material_group_name, customer_perc DESC

-- COMMAND ----------

-- Checking Median Age of IHKP Q1

WITH age AS (
    SELECT DISTINCT
        material_group_name,
        customer_id,
        age
    FROM sandbox.pj_freshfood_data
    WHERE
        business_day >= "2023-05-01"
        AND customer_id IS NOT NULL
        AND age BETWEEN 18 AND 80
        AND material_group_name IN ("INTERNATIONAL MEALS", "FRUIT & SALAD BAR", "INTL BREAKFAST", "DESSERTS", "CHARCOAL GRILLS", "SUSHI", "PASTA STATION", "BOUGHT IN CHILLFOOD")
)

SELECT
    material_group_name,
    MEDIAN(age)
FROM age
GROUP BY material_group_name
ORDER BY material_group_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Campaign EDA

-- COMMAND ----------

WITH cte AS (
    SELECT
        year_month,
        SUM(amount) AS sales,
        SUM(SUM(amount)) OVER() AS total_sales,
        ROUND(sales/total_sales, 4) AS sales_perc
    FROM sandbox.pj_freshfood_data
    WHERE business_day >= "2023-05-01"
    GROUP BY year_month
    ORDER BY year_month
),

cte2 AS (
    SELECT
        year_month,
        sales_perc,
        COALESCE(LAG(sales_perc) OVER (ORDER BY year_month), 0) AS prev_sales_perc,
        COALESCE((sales_perc - LAG(sales_perc) OVER (ORDER BY year_month)) / LAG(sales_perc) OVER (ORDER BY year_month), 0) AS sales_growth
    FROM cte
)

SELECT
    year_month,
    sales_perc,
    sales_growth
FROM cte2

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


