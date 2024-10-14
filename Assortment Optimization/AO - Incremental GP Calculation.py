# Databricks notebook source
# MAGIC %md
# MAGIC #Water

# COMMAND ----------

# Seasonality Adjusted Sales

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "WATER"
        AND t1.business_day <= "2024-09-27"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        (t1.year = 2023 AND t1.month >= 10)
        OR t1.year = 2024
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_all = spark.sql(query).toPandas()
df_all.display()

# COMMAND ----------

# Seasonality Adjusted Sales (delisted items)

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "WATER"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        ((t1.year = 2023 AND t1.month >= 10)
        OR (t1.year = 2024 AND t1.month <= 5))
        AND CONCAT(t1.region_name, t1.material_id) IN ('ABU DHABI2083789', 'ABU DHABI1403107', 'ABU DHABI1403108', 'ABU DHABI1193068', 'ABU DHABI1168340', 'ABU DHABI1306396', 'ABU DHABI130454', 'ABU DHABI2079852', 'ABU DHABI132825', 'ABU DHABI356145', 'ABU DHABI5531', 'ABU DHABI5539', 'ABU DHABI72593', 'ABU DHABI554815', 'ABU DHABI1333721', 'ABU DHABI45025', 'ABU DHABI356144', 'ABU DHABI958777', 'ABU DHABI969199', 'ABU DHABI1021745', 'ABU DHABI1274721', 'ABU DHABI1306395', 'ABU DHABI1462834', 'ABU DHABI1743443', 'ABU DHABI1745916', 'ABU DHABI1877061', 'ABU DHABI814870', 'AL AIN581855', 'AL AIN1403108', 'AL AIN322860', 'AL AIN595793', 'AL AIN1264055', 'AL AIN1668034', 'AL AIN537673', 'AL AIN2098042', 'AL AIN2068156', 'AL AIN2083561', 'AL AIN1173679', 'AL AIN1315149', 'AL AIN130454', 'AL AIN1193068', 'AL AIN2079852', 'AL AIN1306396', 'AL AIN132825', 'AL AIN649263', 'AL AIN1578070', 'AL AIN1033194', 'AL AIN2083210', 'AL AIN1021752', 'AL AIN338111', 'AL AIN771564', 'AL AIN1943686', 'DUBAI1877061', 'DUBAI1743443', 'DUBAI1306396', 'DUBAI581854', 'DUBAI1306395', 'DUBAI356313', 'DUBAI1674365', 'DUBAI45025', 'DUBAI2088109', 'DUBAI2104671', 'DUBAI1193068', 'DUBAI1536585', 'DUBAI2012059', 'DUBAI2015129', 'DUBAI1824713', 'DUBAI5531', 'DUBAI1021745', 'DUBAI907948', 'DUBAI91929', 'DUBAI788306', 'DUBAI1094974', 'DUBAI1823982', 'DUBAI1882277', 'DUBAI130454', 'DUBAI969199', 'DUBAI958777', 'DUBAI649263', 'DUBAI132825', 'DUBAI1462834', 'DUBAI356145', 'DUBAI1333721', 'DUBAI554815', 'DUBAI5539', 'DUBAI347534', 'DUBAI855264', 'DUBAI43787', 'SHARJAH1877061', 'SHARJAH1743443', 'SHARJAH1306396', 'SHARJAH581854', 'SHARJAH1306395', 'SHARJAH356313', 'SHARJAH1674365', 'SHARJAH45025', 'SHARJAH2088109', 'SHARJAH2104671', 'SHARJAH1193068', 'SHARJAH1536585', 'SHARJAH2012059', 'SHARJAH2015129', 'SHARJAH1824713', 'SHARJAH5531', 'SHARJAH1021745', 'SHARJAH907948', 'SHARJAH91929', 'SHARJAH788306', 'SHARJAH1094974', 'SHARJAH1823982', 'SHARJAH1882277', 'SHARJAH130454', 'SHARJAH969199', 'SHARJAH958777', 'SHARJAH649263', 'SHARJAH132825', 'SHARJAH1462834', 'SHARJAH356145', 'SHARJAH1333721', 'SHARJAH554815', 'SHARJAH5539', 'SHARJAH347534', 'SHARJAH855264', 'SHARJAH43787')
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_delists = spark.sql(query).toPandas()
df_delists.display()

# COMMAND ----------

pre_delist_start = 202310
pre_delist_end = 202312
post_delist_start = 202404
post_delist_end = 202406

pre_sales = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales_adj'].sum())

post_sales = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales_adj'].sum())

post_delists_exp_sales = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['sales_adj'].sum())

pre_gp = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

post_gp = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['gp_abs_adj'].sum())

post_delists_exp_gp = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

incremental_sales = post_sales - post_delists_exp_sales - pre_sales
incremental_gp = post_gp - post_delists_exp_gp - pre_gp

pre_sales_actual = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales'].sum())
post_sales_actual = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales'].sum())

print(f"Pre-delist Actual Sales: {pre_sales_actual}")
print(f"Post delist Actual Sales: {post_sales_actual}\n")

print(f"Pre-delist sales: {pre_sales}")
print(f"Pre-delist profit: {pre_gp}")
print(f"Post delist sales: {post_sales}")
print(f"Post delist profit: {post_gp}")
print(f"Post delist expected sales for delisted items: {post_delists_exp_sales}")
print(f"Post delist expected profit for delisted items: {post_delists_exp_gp}")
print(f"Incremental sales generated for Water: {incremental_sales}")
print(f"Incremental GP generated for Water: {incremental_gp}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Pasta

# COMMAND ----------

# Seasonality Adjusted Sales

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "PASTA & NOODLE"
        AND t2.material_group_name = "PASTA"
        AND t1.business_day <= "2024-09-27"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        (t1.year = 2023 AND t1.month >= 10)
        OR t1.year = 2024
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_all = spark.sql(query).toPandas()
df_all.display()

# COMMAND ----------

# Seasonality Adjusted Sales (delisted items)

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "PASTA & NOODLE"
        AND t2.material_group_name = "PASTA"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        ((t1.year = 2023 AND t1.month >= 10)
        OR (t1.year = 2024 AND t1.month <= 5))
        AND CONCAT(t1.region_name, t1.material_id) IN ('ABU DHABI69487', 'ABU DHABI69488', 'ABU DHABI69489', 'ABU DHABI69490', 'ABU DHABI183770', 'ABU DHABI186318', 'ABU DHABI186319', 'ABU DHABI230673', 'ABU DHABI424746', 'ABU DHABI1026010', 'ABU DHABI1266666', 'ABU DHABI1532512', 'ABU DHABI1573102', 'ABU DHABI1573338', 'ABU DHABI1573340', 'ABU DHABI1573461', 'ABU DHABI1573466', 'ABU DHABI1603087', 'ABU DHABI1767079', 'ABU DHABI1767152', 'ABU DHABI1825589', 'ABU DHABI1849870', 'ABU DHABI1849882', 'ABU DHABI1899659', 'ABU DHABI2076579', 'ABU DHABI2076585', 'ABU DHABI14305', 'ABU DHABI236531', 'ABU DHABI502951', 'ABU DHABI638968', 'ABU DHABI733820', 'ABU DHABI759203', 'ABU DHABI900585', 'ABU DHABI1007506', 'ABU DHABI1156173', 'ABU DHABI1164087', 'ABU DHABI1238886', 'ABU DHABI1488903', 'ABU DHABI1519317', 'ABU DHABI1573463', 'ABU DHABI1603088', 'ABU DHABI1603113', 'ABU DHABI1627748', 'ABU DHABI1737392', 'ABU DHABI1766820', 'ABU DHABI1766990', 'ABU DHABI1767061', 'ABU DHABI1767115', 'ABU DHABI1776939', 'ABU DHABI1794485', 'ABU DHABI1816810', 'ABU DHABI1816842', 'ABU DHABI1893234', 'ABU DHABI2005067', 'ABU DHABI2005068', 'ABU DHABI2005069', 'ABU DHABI2005082', 'ABU DHABI2005083', 'ABU DHABI2036384', 'ABU DHABI2143054', 'ABU DHABI2143031', 'ABU DHABI2143053', 'ABU DHABI2143191', 'ABU DHABI557409', 'ABU DHABI1156485', 'ABU DHABI1290129', 'ABU DHABI1816811', 'ABU DHABI1881928', 'ABU DHABI1057752', 'ABU DHABI1867063', 'ABU DHABI1867062', 'ABU DHABI1519318', 'ABU DHABI1577349', 'ABU DHABI1794482', 'ABU DHABI15653', 'ABU DHABI160516', 'ABU DHABI1026015', 'ABU DHABI1026014', 'ABU DHABI1289946', 'ABU DHABI981036', 'ABU DHABI420538', 'ABU DHABI183771', 'ABU DHABI1849869', 'ABU DHABI1849871', 'ABU DHABI1047147', 'ABU DHABI1047149', 'ABU DHABI180109', 'ABU DHABI136613', 'ABU DHABI1602937', 'ABU DHABI1602938', 'ABU DHABI1603111', 'ABU DHABI653881', 'ABU DHABI653901', 'ABU DHABI653902', 'ABU DHABI653905', 'ABU DHABI653908', 'ABU DHABI653920', 'ABU DHABI770078', 'ABU DHABI878448', 'AL AIN4227', 'AL AIN69487', 'AL AIN180109', 'AL AIN530052', 'AL AIN589091', 'AL AIN711139', 'AL AIN888840', 'AL AIN984463', 'AL AIN994643', 'AL AIN1170457', 'AL AIN1374603', 'AL AIN1374604', 'AL AIN1488903', 'AL AIN1519318', 'AL AIN1573102', 'AL AIN1573110', 'AL AIN1573338', 'AL AIN1573340', 'AL AIN1573463', 'AL AIN1573465', 'AL AIN1573467', 'AL AIN1577347', 'AL AIN1661647', 'AL AIN1700889', 'AL AIN1749710', 'AL AIN1825590', 'AL AIN1825591', 'AL AIN1825662', 'AL AIN1849870', 'AL AIN1849882', 'AL AIN1867062', 'AL AIN1899662', 'AL AIN2033113', 'AL AIN2033114', 'AL AIN2053900', 'AL AIN2070622', 'AL AIN2143030', 'AL AIN2143058', 'AL AIN186319', 'AL AIN236531', 'AL AIN395581', 'AL AIN502182', 'AL AIN502951', 'AL AIN507607', 'AL AIN594767', 'AL AIN638967', 'AL AIN638968', 'AL AIN731418', 'AL AIN733820', 'AL AIN868570', 'AL AIN961457', 'AL AIN1007506', 'AL AIN1047147', 'AL AIN1156485', 'AL AIN1164087', 'AL AIN1266666', 'AL AIN1519317', 'AL AIN1533783', 'AL AIN1573339', 'AL AIN1573461', 'AL AIN1573466', 'AL AIN1577349', 'AL AIN1603088', 'AL AIN1603113', 'AL AIN1647431', 'AL AIN1661642', 'AL AIN1737392', 'AL AIN1776916', 'AL AIN1776939', 'AL AIN1816810', 'AL AIN1816842', 'AL AIN1825589', 'AL AIN1849871', 'AL AIN1893234', 'AL AIN1893353', 'AL AIN1899659', 'AL AIN1899954', 'AL AIN1904860', 'AL AIN1904861', 'AL AIN1916192', 'AL AIN1951025', 'AL AIN2005068', 'AL AIN2005069', 'AL AIN2005082', 'AL AIN2005083', 'AL AIN2025720', 'AL AIN2025721', 'AL AIN2040941', 'AL AIN2091785', 'AL AIN15702', 'AL AIN1731640', 'AL AIN1912936', 'AL AIN1148298', 'AL AIN1848923', 'AL AIN613038', 'AL AIN1057753', 'AL AIN1731639', 'AL AIN902160', 'AL AIN537456', 'AL AIN613045', 'AL AIN2022561', 'AL AIN843300', 'AL AIN1532520', 'AL AIN2143031', 'AL AIN969799', 'AL AIN969820', 'AL AIN2033102', 'AL AIN1532511', 'AL AIN647542', 'AL AIN1776910', 'AL AIN1532531', 'AL AIN570114', 'AL AIN1174597', 'AL AIN449820', 'AL AIN1661653', 'AL AIN820450', 'AL AIN1532514', 'AL AIN15696', 'AL AIN613040', 'AL AIN928866', 'AL AIN1045607', 'AL AIN1941033', 'AL AIN1967781', 'AL AIN1238928', 'AL AIN570192', 'AL AIN618880', 'AL AIN2143053', 'AL AIN1026014', 'AL AIN1532424', 'AL AIN524883', 'AL AIN1532517', 'AL AIN1205352', 'AL AIN1532535', 'AL AIN2022582', 'AL AIN1532513', 'AL AIN618881', 'AL AIN570204', 'AL AIN987623', 'AL AIN1532428', 'AL AIN449712', 'AL AIN1205353', 'AL AIN1532533', 'AL AIN588999', 'AL AIN1532426', 'AL AIN1006690', 'AL AIN1532536', 'AL AIN1840890', 'AL AIN1532534', 'AL AIN2070355', 'AL AIN613039', 'AL AIN1532518', 'AL AIN1045730', 'AL AIN1199166', 'AL AIN1937495', 'AL AIN1984307', 'AL AIN1532512', 'AL AIN1937483', 'AL AIN570199', 'AL AIN613200', 'AL AIN1937491', 'AL AIN14297', 'AL AIN570197', 'AL AIN1532515', 'AL AIN418856', 'AL AIN1532519', 'AL AIN1045602', 'AL AIN1045605', 'AL AIN1156093', 'AL AIN1881928', 'AL AIN449822', 'AL AIN613041', 'AL AIN1937492', 'AL AIN419680', 'AL AIN1867063', 'AL AIN186318', 'AL AIN1881914', 'AL AIN183770', 'AL AIN570195', 'AL AIN1242100', 'AL AIN1164933', 'AL AIN1135571', 'AL AIN2133543', 'AL AIN2156352', 'AL AIN1840888', 'AL AIN1899953', 'AL AIN2122974', 'AL AIN2139315', 'AL AIN1026015', 'AL AIN1573462', 'AL AIN1135763', 'AL AIN1849869', 'AL AIN183771', 'AL AIN984464', 'AL AIN1135761', 'AL AIN1423358', 'AL AIN2143191', 'AL AIN2113799', 'AL AIN2010889', 'AL AIN2134629', 'AL AIN1026010', 'AL AIN2133435', 'AL AIN2143054', 'AL AIN1024727', 'AL AIN1047149', 'AL AIN1024933', 'AL AIN1026016', 'AL AIN1705677', 'AL AIN942705', 'AL AIN1024932', 'AL AIN1374606', 'AL AIN2136420', 'AL AIN1700363', 'AL AIN1461665', 'DUBAI14297', 'DUBAI69488', 'DUBAI69489', 'DUBAI69490', 'DUBAI180109', 'DUBAI183770', 'DUBAI183771', 'DUBAI539142', 'DUBAI570114', 'DUBAI570192', 'DUBAI570204', 'DUBAI594767', 'DUBAI680608', 'DUBAI710815', 'DUBAI731418', 'DUBAI820450', 'DUBAI858068', 'DUBAI888796', 'DUBAI896637', 'DUBAI942705', 'DUBAI1026014', 'DUBAI1045604', 'DUBAI1056983', 'DUBAI1135571', 'DUBAI1156098', 'DUBAI1156173', 'DUBAI1214857', 'DUBAI1238882', 'DUBAI1238886', 'DUBAI1242092', 'DUBAI1266666', 'DUBAI1287067', 'DUBAI1287068', 'DUBAI1374604', 'DUBAI1423358', 'DUBAI1519317', 'DUBAI1532515', 'DUBAI1532516', 'DUBAI1573340', 'DUBAI1602938', 'DUBAI1628869', 'DUBAI1766820', 'DUBAI1766990', 'DUBAI1767061', 'DUBAI1776910', 'DUBAI1825590', 'DUBAI1849882', 'DUBAI1867062', 'DUBAI1868202', 'DUBAI1893353', 'DUBAI1899659', 'DUBAI1899953', 'DUBAI1916192', 'DUBAI1937483', 'DUBAI1937491', 'DUBAI1937495', 'DUBAI2005067', 'DUBAI2005068', 'DUBAI2005082', 'DUBAI2012350', 'DUBAI2012518', 'DUBAI2012519', 'DUBAI2033102', 'DUBAI2070622', 'DUBAI2085536', 'DUBAI2130995', 'DUBAI2143031', 'DUBAI2143053', 'DUBAI15652', 'DUBAI236531', 'DUBAI320427', 'DUBAI502182', 'DUBAI638968', 'DUBAI868570', 'DUBAI961457', 'DUBAI1047147', 'DUBAI1057752', 'DUBAI1057753', 'DUBAI1089365', 'DUBAI1205354', 'DUBAI1229337', 'DUBAI1488903', 'DUBAI1519318', 'DUBAI1533783', 'DUBAI1557009', 'DUBAI1557146', 'DUBAI1603113', 'DUBAI1625642', 'DUBAI1661642', 'DUBAI1714424', 'DUBAI1737392', 'DUBAI1776916', 'DUBAI1776939', 'DUBAI1816810', 'DUBAI1816842', 'DUBAI1893211', 'DUBAI1893234', 'DUBAI1899662', 'DUBAI1904860', 'DUBAI1904861', 'DUBAI1951025', 'DUBAI2033113', 'DUBAI2091784', 'DUBAI2091785', 'DUBAI1130548', 'DUBAI1045730', 'DUBAI1374606', 'DUBAI1374603', 'DUBAI888840', 'DUBAI1195863', 'DUBAI1009187', 'DUBAI984464', 'DUBAI984463', 'DUBAI1135763', 'DUBAI1135572', 'DUBAI502951', 'DUBAI1164087', 'DUBAI647542', 'DUBAI1164933', 'DUBAI2010889', 'DUBAI2009937', 'DUBAI2010888', 'DUBAI1984307', 'DUBAI1007506', 'DUBAI1825591', 'DUBAI1825662', 'DUBAI1825589', 'DUBAI1573465', 'DUBAI1573466', 'DUBAI1573467', 'DUBAI1573462', 'DUBAI1573339', 'DUBAI1573338', 'DUBAI1573102', 'DUBAI1573110', 'DUBAI1573463', 'DUBAI1573461', 'DUBAI925513', 'DUBAI925441', 'DUBAI925443', 'DUBAI677622', 'DUBAI1700889', 'DUBAI609871', 'DUBAI69487', 'DUBAI642851', 'DUBAI2090875', 'DUBAI186318', 'DUBAI186319', 'DUBAI1047149', 'DUBAI1849869', 'DUBAI1849870', 'DUBAI1849871', 'DUBAI653902', 'DUBAI653905', 'DUBAI653908', 'DUBAI653920', 'SHARJAH69488', 'SHARJAH69489', 'SHARJAH93038', 'SHARJAH136613', 'SHARJAH236531', 'SHARJAH320427', 'SHARJAH418856', 'SHARJAH449822', 'SHARJAH570195', 'SHARJAH613038', 'SHARJAH613041', 'SHARJAH642851', 'SHARJAH710814', 'SHARJAH710815', 'SHARJAH731418', 'SHARJAH820450', 'SHARJAH899600', 'SHARJAH918909', 'SHARJAH925441', 'SHARJAH925443', 'SHARJAH1047147', 'SHARJAH1047149', 'SHARJAH1132329', 'SHARJAH1135763', 'SHARJAH1205354', 'SHARJAH1297338', 'SHARJAH1297339', 'SHARJAH1374604', 'SHARJAH1374606', 'SHARJAH1474896', 'SHARJAH1519317', 'SHARJAH1519318', 'SHARJAH1532424', 'SHARJAH1532511', 'SHARJAH1557146', 'SHARJAH1573102', 'SHARJAH1573340', 'SHARJAH1573466', 'SHARJAH1766820', 'SHARJAH1767079', 'SHARJAH1767115', 'SHARJAH1767152', 'SHARJAH1776913', 'SHARJAH1825589', 'SHARJAH1825590', 'SHARJAH1849870', 'SHARJAH1881928', 'SHARJAH1899659', 'SHARJAH1912936', 'SHARJAH1937483', 'SHARJAH2033102', 'SHARJAH2085536', 'SHARJAH2143053', 'SHARJAH2143191', 'SHARJAH15652', 'SHARJAH570197', 'SHARJAH594767', 'SHARJAH613039', 'SHARJAH625147', 'SHARJAH638968', 'SHARJAH677622', 'SHARJAH680608', 'SHARJAH759203', 'SHARJAH902160', 'SHARJAH1007506', 'SHARJAH1045730', 'SHARJAH1057752', 'SHARJAH1089365', 'SHARJAH1135572', 'SHARJAH1199166', 'SHARJAH1205350', 'SHARJAH1238882', 'SHARJAH1266666', 'SHARJAH1290129', 'SHARJAH1374603', 'SHARJAH1573110', 'SHARJAH1573338', 'SHARJAH1577349', 'SHARJAH1577350', 'SHARJAH1603088', 'SHARJAH1603113', 'SHARJAH1625642', 'SHARJAH1661642', 'SHARJAH1661653', 'SHARJAH1699861', 'SHARJAH1717856', 'SHARJAH1766990', 'SHARJAH1776916', 'SHARJAH1776939', 'SHARJAH1816810', 'SHARJAH1816842', 'SHARJAH1899660', 'SHARJAH1899662', 'SHARJAH1899954', 'SHARJAH1904860', 'SHARJAH1904861', 'SHARJAH1951025', 'SHARJAH2033113', 'SHARJAH2059264', 'SHARJAH2070622', 'SHARJAH1700889', 'SHARJAH2090875', 'SHARJAH69487', 'SHARJAH69490', 'SHARJAH180109', 'SHARJAH183770', 'SHARJAH183771', 'SHARJAH186318', 'SHARJAH186319', 'SHARJAH609871', 'SHARJAH925513', 'SHARJAH984463', 'SHARJAH984464', 'SHARJAH1573339', 'SHARJAH1573462', 'SHARJAH1573463', 'SHARJAH1573465', 'SHARJAH1573467', 'SHARJAH1825591', 'SHARJAH1825662', 'SHARJAH1849869', 'SHARJAH1849871', 'SHARJAH1849882', 'SHARJAH1899953', 'SHARJAH1130548', 'SHARJAH2010889', 'SHARJAH888796', 'SHARJAH1135571')
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_delists = spark.sql(query).toPandas()
df_delists.display()

# COMMAND ----------

pre_delist_start = 202402
pre_delist_end = 202404
post_delist_start = 202406
post_delist_end = 202408

pre_sales = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales_adj'].sum())

post_sales = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales_adj'].sum())

post_delists_exp_sales = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['sales_adj'].sum())

pre_gp = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

post_gp = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['gp_abs_adj'].sum())

post_delists_exp_gp = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

pre_sales_actual = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales'].sum())
post_sales_actual = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales'].sum())

print(f"Pre-delist Actual Sales: {pre_sales_actual}")
print(f"Post delist Actual Sales: {post_sales_actual}\n")

incremental_sales = post_sales - post_delists_exp_sales - pre_sales
incremental_gp = post_gp - post_delists_exp_gp - pre_gp
print(f"Pre-delist sales: {pre_sales}")
print(f"Pre-delist profit: {pre_gp}")
print(f"Post delist sales: {post_sales}")
print(f"Post delist profit: {post_gp}")
print(f"Post delist expected sales for delisted items: {post_delists_exp_sales}")
print(f"Post delist expected profit for delisted items: {post_delists_exp_gp}")
print(f"Incremental sales generated for Water: {incremental_sales}")
print(f"Incremental GP generated for Water: {incremental_gp}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Instant Noodle

# COMMAND ----------

# Seasonality Adjusted Sales

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "PASTA & NOODLE"
        AND t2.material_group_name = "INSTANT NOODLE"
        AND t1.business_day <= "2024-09-27"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        (t1.year = 2023 AND t1.month >= 10)
        OR t1.year = 2024
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_all = spark.sql(query).toPandas()
df_all.display()

# COMMAND ----------

# Seasonality Adjusted Sales (delisted items)

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "PASTA & NOODLE"
        AND t2.material_group_name = "INSTANT NOODLE"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        ((t1.year = 2023 AND t1.month >= 10)
        OR (t1.year = 2024 AND t1.month <= 5))
        AND CONCAT(t1.region_name, t1.material_id) IN ('ABU DHABI804087', 'ABU DHABI804088', 'ABU DHABI880897', 'ABU DHABI1503450', 'ABU DHABI1933080', 'ABU DHABI1955740', 'ABU DHABI1959764', 'ABU DHABI1961328', 'ABU DHABI2061133', 'ABU DHABI2061134', 'ABU DHABI2068912', 'ABU DHABI1009547', 'ABU DHABI1520529', 'ABU DHABI1520541', 'ABU DHABI1827214', 'ABU DHABI1827219', 'ABU DHABI2003397', 'ABU DHABI1520524', 'ABU DHABI2097424', 'ABU DHABI2038242', 'ABU DHABI759504', 'ABU DHABI1068151', 'ABU DHABI882288', 'ABU DHABI1068150', 'ABU DHABI1776498', 'ABU DHABI538433', 'ABU DHABI720163', 'ABU DHABI720164', 'ABU DHABI580312', 'ABU DHABI1959812', 'ABU DHABI720161', 'ABU DHABI4371', 'ABU DHABI580478', 'ABU DHABI1785577', 'ABU DHABI1433644', 'ABU DHABI1867363', 'ABU DHABI1845635', 'ABU DHABI409649', 'ABU DHABI1879527', 'ABU DHABI1845634', 'ABU DHABI880899', 'ABU DHABI1433646', 'ABU DHABI1776553', 'ABU DHABI580477', 'ABU DHABI1762388', 'ABU DHABI1651071', 'ABU DHABI2083241', 'ABU DHABI1676528', 'ABU DHABI580479', 'ABU DHABI1997640', 'ABU DHABI1866152', 'ABU DHABI1607644', 'ABU DHABI1320732', 'ABU DHABI1320707', 'ABU DHABI1674811', 'ABU DHABI1843682', 'ABU DHABI2133911', 'ABU DHABI1320703', 'ABU DHABI1785572', 'ABU DHABI1068153', 'ABU DHABI1827216', 'ABU DHABI1827218', 'AL AIN804087', 'AL AIN804088', 'AL AIN1520524', 'AL AIN1827219', 'AL AIN1955740', 'AL AIN1959764', 'AL AIN2061133', 'AL AIN2061134', 'AL AIN397021', 'AL AIN515590', 'AL AIN1520541', 'AL AIN1785572', 'AL AIN1827218', 'AL AIN1845635', 'AL AIN1903985', 'AL AIN1959812', 'AL AIN1068150', 'AL AIN1068153', 'AL AIN2097424', 'AL AIN1520529', 'AL AIN1768802', 'AL AIN543859', 'AL AIN1366760', 'AL AIN1068151', 'AL AIN1515686', 'AL AIN1207367', 'AL AIN4355', 'AL AIN720163', 'AL AIN4348', 'AL AIN1903986', 'AL AIN4359', 'AL AIN1426300', 'AL AIN584974', 'AL AIN580479', 'AL AIN580477', 'AL AIN1252636', 'DUBAI804087', 'DUBAI804088', 'DUBAI880930', 'DUBAI1222380', 'DUBAI1366760', 'DUBAI1520524', 'DUBAI1520528', 'DUBAI1520529', 'DUBAI1520541', 'DUBAI1520543', 'DUBAI1827219', 'DUBAI1955740', 'DUBAI1959764', 'DUBAI1999659', 'DUBAI1999660', 'DUBAI1999775', 'DUBAI1999776', 'DUBAI1999777', 'DUBAI2028671', 'DUBAI2068912', 'DUBAI2083382', 'DUBAI904540', 'DUBAI1252636', 'DUBAI1745845', 'DUBAI1745846', 'DUBAI1745847', 'DUBAI1785572', 'DUBAI1999657', 'DUBAI1999774', 'DUBAI2049088', 'DUBAI2075553', 'DUBAI1578782', 'DUBAI1845635', 'DUBAI1753106', 'DUBAI651681', 'DUBAI651685', 'DUBAI1959812', 'DUBAI1515686', 'DUBAI543754', 'DUBAI1620539', 'DUBAI1702568', 'DUBAI1827216', 'DUBAI1785577', 'DUBAI1903986', 'DUBAI1843682', 'DUBAI1999661', 'DUBAI1827214', 'DUBAI1827218', 'DUBAI1207315', 'DUBAI1394298', 'DUBAI1776553', 'DUBAI1207367', 'DUBAI4364', 'DUBAI882288', 'DUBAI4371', 'DUBAI4355', 'DUBAI1366765', 'DUBAI617005', 'DUBAI1903985', 'DUBAI1674811', 'DUBAI1845634', 'DUBAI482088', 'SHARJAH543706', 'SHARJAH707498', 'SHARJAH804087', 'SHARJAH804088', 'SHARJAH880930', 'SHARJAH1426300', 'SHARJAH1520524', 'SHARJAH1520528', 'SHARJAH1520529', 'SHARJAH1520541', 'SHARJAH1520543', 'SHARJAH1745846', 'SHARJAH1808633', 'SHARJAH1827214', 'SHARJAH1827216', 'SHARJAH1827218', 'SHARJAH1955740', 'SHARJAH1959764', 'SHARJAH1999659', 'SHARJAH1999660', 'SHARJAH1999776', 'SHARJAH1999777', 'SHARJAH2022933', 'SHARJAH2028671', 'SHARJAH2049088', 'SHARJAH2068912', 'SHARJAH1222380', 'SHARJAH1745845', 'SHARJAH1745847', 'SHARJAH1999657', 'SHARJAH1999774', 'SHARJAH1999775', 'SHARJAH2083382', 'SHARJAH17910', 'SHARJAH904540', 'SHARJAH543859', 'SHARJAH1425728', 'SHARJAH543754', 'SHARJAH1657731', 'SHARJAH1651057', 'SHARJAH1651054', 'SHARJAH1785577', 'SHARJAH2038242', 'SHARJAH761665', 'SHARJAH1959812', 'SHARJAH1845634', 'SHARJAH1845635', 'SHARJAH365228', 'SHARJAH1702568', 'SHARJAH1478467', 'SHARJAH2097424', 'SHARJAH1433644', 'SHARJAH880898', 'SHARJAH4366', 'SHARJAH614519', 'SHARJAH1903985', 'SHARJAH1761253', 'SHARJAH1785572', 'SHARJAH1903986', 'SHARJAH720163', 'SHARJAH2171245', 'SHARJAH409649', 'SHARJAH4371', 'SHARJAH1862324', 'SHARJAH4364', 'SHARJAH1207315', 'SHARJAH1666661', 'SHARJAH1478466', 'SHARJAH2171786')
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_delists = spark.sql(query).toPandas()
df_delists.display()

# COMMAND ----------

pre_delist_start = 202402
pre_delist_end = 202404
post_delist_start = 202406
post_delist_end = 202408

pre_sales = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales_adj'].sum())

post_sales = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales_adj'].sum())

post_delists_exp_sales = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['sales_adj'].sum())

pre_gp = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

post_gp = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['gp_abs_adj'].sum())

post_delists_exp_gp = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

post_delists_exp_gp = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

pre_sales_actual = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales'].sum())
post_sales_actual = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales'].sum())

print(f"Pre-delist Actual Sales: {pre_sales_actual}")
print(f"Post delist Actual Sales: {post_sales_actual}\n")

incremental_sales = post_sales - post_delists_exp_sales - pre_sales
incremental_gp = post_gp - post_delists_exp_gp - pre_gp
print(f"Pre-delist sales: {pre_sales}")
print(f"Pre-delist profit: {pre_gp}")
print(f"Post delist sales: {post_sales}")
print(f"Post delist profit: {post_gp}")
print(f"Post delist expected sales for delisted items: {post_delists_exp_sales}")
print(f"Post delist expected profit for delisted items: {post_delists_exp_gp}")
print(f"Incremental sales generated for Water: {incremental_sales}")
print(f"Incremental GP generated for Water: {incremental_gp}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Cup Noodle

# COMMAND ----------

# Seasonality Adjusted Sales

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "PASTA & NOODLE"
        AND t2.material_group_name = "CUP NOODLE"
        AND t1.business_day <= "2024-09-27"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        (t1.year = 2023 AND t1.month >= 10)
        OR t1.year = 2024
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_all = spark.sql(query).toPandas()
df_all.display()

# COMMAND ----------

# Seasonality Adjusted Sales (delisted items)

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "PASTA & NOODLE"
        AND t2.material_group_name = "CUP NOODLE"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        ((t1.year = 2023 AND t1.month >= 10)
        OR (t1.year = 2024 AND t1.month <= 5))
        AND CONCAT(t1.region_name, t1.material_id) IN ('ABU DHABI1336812', 'ABU DHABI1542216', 'ABU DHABI203423', 'ABU DHABI2034628', 'ABU DHABI759424', 'ABU DHABI570812', 'ABU DHABI1649049', 'ABU DHABI1998073', 'ABU DHABI1993080', 'ABU DHABI759426', 'ABU DHABI1745578', 'ABU DHABI4316', 'ABU DHABI1649067', 'ABU DHABI1745565', 'ABU DHABI1953020', 'ABU DHABI2171885', 'ABU DHABI1953038', 'ABU DHABI2171894', 'ABU DHABI1578772', 'ABU DHABI1933079', 'ABU DHABI759423', 'ABU DHABI1290537', 'ABU DHABI4312', 'ABU DHABI1752795', 'ABU DHABI4315', 'ABU DHABI539862', 'ABU DHABI1408194', 'ABU DHABI539861', 'AL AIN570812', 'AL AIN1336812', 'AL AIN1542216', 'AL AIN539860', 'AL AIN203423', 'AL AIN571323', 'AL AIN1408194', 'AL AIN1745578', 'AL AIN759424', 'AL AIN1745565', 'AL AIN1408195', 'AL AIN1649049', 'AL AIN1408188', 'AL AIN4312', 'AL AIN4316', 'DUBAI12587', 'DUBAI12590', 'DUBAI42481', 'DUBAI1433585', 'DUBAI1809474', 'DUBAI2049587', 'DUBAI2049601', 'DUBAI2049603', 'DUBAI2117665', 'DUBAI42480', 'DUBAI570812', 'DUBAI2034628', 'DUBAI874953', 'DUBAI1336812', 'DUBAI759424', 'DUBAI571323', 'DUBAI1651055', 'DUBAI216857', 'DUBAI1649067', 'DUBAI759423', 'DUBAI1998073', 'DUBAI203423', 'SHARJAH1336812', 'SHARJAH1433585', 'SHARJAH1809474', 'SHARJAH2049587', 'SHARJAH2049601', 'SHARJAH2049603', 'SHARJAH1871178', 'SHARJAH203423', 'SHARJAH570813', 'SHARJAH2117665', 'SHARJAH216857', 'SHARJAH2059020', 'SHARJAH2059306', 'SHARJAH759424', 'SHARJAH1578772', 'SHARJAH1651055', 'SHARJAH759423')
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_delists = spark.sql(query).toPandas()
df_delists.display()

# COMMAND ----------

pre_delist_start = 202312
pre_delist_end = 202402
post_delist_start = 202406
post_delist_end = 202408

pre_sales = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales_adj'].sum())

post_sales = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales_adj'].sum())

post_delists_exp_sales = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['sales_adj'].sum())

pre_gp = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

post_gp = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['gp_abs_adj'].sum())

post_delists_exp_gp = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

pre_sales_actual = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales'].sum())
post_sales_actual = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales'].sum())

print(f"Pre-delist Actual Sales: {pre_sales_actual}")
print(f"Post delist Actual Sales: {post_sales_actual}\n")

incremental_sales = post_sales - post_delists_exp_sales - pre_sales
incremental_gp = post_gp - post_delists_exp_gp - pre_gp
print(f"Pre-delist sales: {pre_sales}")
print(f"Pre-delist profit: {pre_gp}")
print(f"Post delist sales: {post_sales}")
print(f"Post delist profit: {post_gp}")
print(f"Post delist expected sales for delisted items: {post_delists_exp_sales}")
print(f"Post delist expected profit for delisted items: {post_delists_exp_gp}")
print(f"Incremental sales generated for Water: {incremental_sales}")
print(f"Incremental GP generated for Water: {incremental_gp}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Coconut Oil

# COMMAND ----------

# Seasonality Adjusted Sales

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "COOKING OILS & GHEE"
        AND t2.material_group_name = "COCONUT OIL"
        AND t1.business_day <= "2024-09-27"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        (t1.year = 2023 AND t1.month >= 10)
        OR t1.year = 2024
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_all = spark.sql(query).toPandas()
df_all.display()

# COMMAND ----------

# Seasonality Adjusted Sales (delisted items)

query = """
WITH sku_sales AS (
    SELECT
        material_id,
        region_name,
        MONTH(business_day) AS month,
        YEAR(business_day) AS year,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t2.category_name = "COOKING OILS & GHEE"
        AND t2.material_group_name = "COCONUT OIL"
        AND t3.tayeb_flag = 0
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4
),

monthly_sales AS (
    SELECT
        month,
        year,
        SUM(sales) AS monthly_sales
    FROM sku_sales
    GROUP BY 1, 2
),

pivoted_sales AS (
    SELECT
        month,
        MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) AS 2022_sales,
        MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) AS 2023_sales,
        MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END) AS 2024_sales,
        ROUND((2022_sales + 2023_sales + 2024_sales) / NULLIF((CASE WHEN 2022_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2023_sales > 0 THEN 1 ELSE 0 END + CASE WHEN 2024_sales > 0 THEN 1 ELSE 0 END), 0)) AS avg_sales,
        SUM(MAX(CASE WHEN year = 2022 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2023 THEN monthly_sales ELSE 0 END) + MAX(CASE WHEN year = 2024 THEN monthly_sales ELSE 0 END)) OVER () AS total_three_year_sales
    FROM monthly_sales
    GROUP BY month
),

total_months AS (
    SELECT
        COUNT(CASE WHEN 2022_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2023_sales > 0 THEN 1 END) + COUNT(CASE WHEN 2024_sales > 0 THEN 1 END) AS months
    FROM pivoted_sales
),

seasonality AS (
    SELECT
        month,
        2022_sales,
        2023_sales,
        2024_sales,
        avg_sales,
        total_three_year_sales,
        ROUND(total_three_year_sales/months) AS avg_three_year_sales,
        ROUND(avg_sales / avg_three_year_sales, 2) AS seasonality_index
    FROM pivoted_sales, total_months
),

gp_data AS (
    SELECT
        CASE WHEN region = "AUH" THEN "ABU DHABI"
            WHEN region = "ALN" THEN "AL AIN"
            WHEN region = "DXB" THEN "DUBAI"
            WHEN region = "SHJ" THEN "SHARJAH" END AS region_name,
        CAST(RIGHT(year_month, 2) AS INT) AS month,
        material_id,
        gp_wth_chargeback
    FROM gold.business.gross_profit
    WHERE
        country = 'AE'
        AND year_month >= 202310
),

region_sales AS (
    SELECT
        t1.material_id,
        t1.region_name,
        t1.month,
        t1.sales,
        t2.seasonality_index,
        ROUND(t1.sales / t2.seasonality_index) AS sales_adj,
        t3.gp_wth_chargeback,
        ROUND(sales_adj * t3.gp_wth_chargeback / 100) AS gp_abs_adj
    FROM sku_sales AS t1
    JOIN seasonality AS t2 ON t1.month = t2.month
    LEFT JOIN gp_data AS t3
        ON t1.region_name = t3.region_name
        AND t1.month = t3.month
        AND t1.material_id = t3.material_id
    WHERE
        ((t1.year = 2023 AND t1.month >= 10)
        OR (t1.year = 2024 AND t1.month <= 5))
        AND CONCAT(t1.region_name, t1.material_id) IN ('ABU DHABI1010199', 'ABU DHABI994082', 'ABU DHABI3205', 'ABU DHABI888604', 'ABU DHABI1358028', 'ABU DHABI828953', 'AL AIN3207', 'AL AIN683133', 'AL AIN823744', 'AL AIN843650', 'AL AIN892002', 'AL AIN388203', 'AL AIN811420', 'AL AIN1604650', 'AL AIN1218548', 'AL AIN3213', 'AL AIN1358028', 'DUBAI3213', 'DUBAI1710303', 'DUBAI3207', 'DUBAI1621475', 'DUBAI1710302', 'DUBAI3210', 'DUBAI1872852', 'DUBAI2111062', 'DUBAI1936540', 'DUBAI338171', 'DUBAI1358028', 'DUBAI840476', 'DUBAI843650', 'DUBAI1281424', 'DUBAI1076057', 'DUBAI648787', 'SHARJAH3207', 'SHARJAH843650', 'SHARJAH1621475', 'SHARJAH1710302', 'SHARJAH1710303', 'SHARJAH3213', 'SHARJAH1872852', 'SHARJAH1358028', 'SHARJAH338171', 'SHARJAH840476', 'SHARJAH648787', 'SHARJAH3210', 'SHARJAH2111062', 'SHARJAH1936540', 'SHARJAH1281424', 'SHARJAH1076057')
)

SELECT
    material_id,
    INT(CASE WHEN month >= 10 THEN CONCAT("2023", month) ELSE CONCAT("2024", LPAD(month, 2, 0)) END) AS year_month,
    seasonality_index,
    SUM(sales) AS sales,
    SUM(sales_adj) AS sales_adj,
    SUM(gp_abs_adj) AS gp_abs_adj
FROM region_sales
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_delists = spark.sql(query).toPandas()
df_delists.display()

# COMMAND ----------

pre_delist_start = 202402
pre_delist_end = 202404
post_delist_start = 202407
post_delist_end = 202409

pre_sales = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales_adj'].sum())

post_sales = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales_adj'].sum())

post_delists_exp_sales = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['sales_adj'].sum())

pre_gp = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

post_gp = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['gp_abs_adj'].sum())

post_delists_exp_gp = int(df_delists[(df_delists['year_month'] >= pre_delist_start) & (df_delists['year_month'] <= pre_delist_end)]['gp_abs_adj'].sum())

pre_sales_actual = int(df_all[(df_all['year_month'] >= pre_delist_start) & (df_all['year_month'] <= pre_delist_end)]['sales'].sum())
post_sales_actual = int(df_all[(df_all['year_month'] >= post_delist_start) & (df_all['year_month'] <= post_delist_end)]['sales'].sum())

print(f"Pre-delist Actual Sales: {pre_sales_actual}")
print(f"Post delist Actual Sales: {post_sales_actual}\n")

incremental_sales = post_sales - post_delists_exp_sales - pre_sales
incremental_gp = post_gp - post_delists_exp_gp - pre_gp
print(f"Pre-delist sales: {pre_sales}")
print(f"Pre-delist profit: {pre_gp}")
print(f"Post delist sales: {post_sales}")
print(f"Post delist profit: {post_gp}")
print(f"Post delist expected sales for delisted items: {post_delists_exp_sales}")
print(f"Post delist expected profit for delisted items: {post_delists_exp_gp}")
print(f"Incremental sales generated for Water: {incremental_sales}")
print(f"Incremental GP generated for Water: {incremental_gp}")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


