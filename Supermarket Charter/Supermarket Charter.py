# Databricks notebook source
# MAGIC %md
# MAGIC #Quarterly

# COMMAND ----------

# MAGIC %md
# MAGIC ##All Group-by Levels

# COMMAND ----------

import pandas as pd

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        region_name,
        department_name,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info, region_name, department_name, mobile
),

main_table AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        region_name,
        department_name,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT category_id) AS Categories,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info, region_name, department_name
),

customer_segments AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        region_name,
        department_name,
        COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year = '202312'
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info, region_name, department_name
),

loyalty AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        region_name,
        department_name,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Quarter_Info, region_name, department_name
)

SELECT
    t1.Quarter_Info,
    t1.region_name,
    t1.department_name,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Categories,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Quarter_Info = t2.Quarter_info
    AND t1.region_name = t2.region_name
    AND t1.department_name = t2.department_name
JOIN customer_segments AS t3
    ON t1.Quarter_Info = t3.Quarter_info
    AND t1.region_name = t3.region_name
    AND t1.department_name = t3.department_name
LEFT JOIN loyalty AS t4
    ON t1.Quarter_Info = t4.Quarter_info
    AND t1.region_name = t4.region_name
    AND t1.department_name = t4.department_name
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3
"""

all_levels_df = spark.sql(query).toPandas()
all_levels_df['Discount'] = all_levels_df['Discount'].fillna(0)
all_levels_df['Loyalty_Sales'] = all_levels_df['Loyalty_Sales'].fillna(0)
all_levels_df['Loyalty_Customers'] = all_levels_df['Loyalty_Customers'].fillna(0)

# COMMAND ----------

def transpose_quarter(df1, region):
    df2 = df1[df1['region_name'] == region].reset_index(drop=True)
    dct = {'Q' + str(i): [] for i in range(1, 5)}

    departments = df2['department_name'].unique()
    for department in departments:
        quarters = df2[df2['department_name'] == department]['Quarter_Info'].values
        sales = df2[df2['department_name'] == department]['Sales'].values
        customers = df2[df2['department_name'] == department]['Customers'].values
        loyalty_sales = df2[df2['department_name'] == department]['Loyalty_Sales'].values
        loyalty_customers = df2[df2['department_name'] == department]['Loyalty_Customers'].values
        VIPs = df2[df2['department_name'] == department]['VIPs'].values
        Freqs = df2[df2['department_name'] == department]['Freqs'].values
        products = df2[df2['department_name'] == department]['Products'].values
        volume = df2[df2['department_name'] == department]['Volume'].values
        categories = df2[df2['department_name'] == department]['Categories'].values
        transactions = df2[df2['department_name'] == department]['Transactions'].values
        discount = df2[df2['department_name'] == department]['Discount'].values
        atv = df2[df2['department_name'] == department]['ATV'].values
        one_timers = df2[df2['department_name'] == department]['One_Timers'].values
        repeats = df2[df2['department_name'] == department]['Repeats'].values

        check = []
        for i, quarter in enumerate(quarters):
            key = 'Q' + str(quarter)

            dct[key].extend([sales[i], customers[i], loyalty_sales[i], loyalty_customers[i], VIPs[i], Freqs[i], products[i], volume[i], categories[i], transactions[i], discount[i], atv[i], one_timers[i], repeats[i]])

            check.append(key)

        keys = list(dct.keys())
        if set(keys).issubset(set(check)) == False:
            difference = set(keys) - set(check)
            if len(difference) == 1:
                key = difference.pop()
                dct[key].extend([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

    converted_df = pd.DataFrame(dct)

    departments = [item for item in departments for _ in range(14)]
    measures = ['Sales', 'Customers', 'Loyalty Sales', 'Loyalty Customers', 'VIPs', 'Freqs', 'Products', 'Volume', 'Categories', 'Transactions', 'Discount', 'ATV', 'One Timers', 'Repeats'] * df2['department_name'].nunique()
    converted_df['department_name'] = departments
    converted_df['Measures'] = measures
    converted_df['Region'] = region

    return converted_df

# COMMAND ----------

regions = all_levels_df['region_name'].unique()

quarterly_df = transpose_quarter(all_levels_df, regions[0])

for i in range(1, len(regions)):
    quarterly_df = pd.concat([quarterly_df, transpose_quarter(all_levels_df, regions[i])], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##UAE Department Level

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        department_name,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info, department_name, mobile
),

main_table AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        department_name,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT category_id) AS Categories,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info, department_name
),

customer_segments AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        department_name,
        COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year = '202312'
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info, department_name
),

loyalty AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        department_name,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Quarter_Info, department_name
)

SELECT
    t1.Quarter_Info,
    "UAE" AS region_name,
    t1.department_name,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Categories,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Quarter_Info = t2.Quarter_info
    AND t1.department_name = t2.department_name
JOIN customer_segments AS t3
    ON t1.Quarter_Info = t3.Quarter_info
    AND t1.department_name = t3.department_name
LEFT JOIN loyalty AS t4
    ON t1.Quarter_Info = t4.Quarter_info
    AND t1.department_name = t4.department_name
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3
"""

uae_department_df = spark.sql(query).toPandas()
uae_department_df['Discount'] = uae_department_df['Discount'].fillna(0)
uae_department_df['Loyalty_Sales'] = uae_department_df['Loyalty_Sales'].fillna(0)
uae_department_df['Loyalty_Customers'] = uae_department_df['Loyalty_Customers'].fillna(0)

# COMMAND ----------

quarterly_df = pd.concat([quarterly_df, transpose_quarter(uae_department_df, 'UAE')], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Region Level Overall

# COMMAND ----------

# MAGIC %py
# MAGIC query = """
# MAGIC WITH one_timers AS (
# MAGIC     SELECT
# MAGIC         QUARTER(business_day) AS Quarter_Info,
# MAGIC         region_name,
# MAGIC         mobile,
# MAGIC         COUNT(DISTINCT transaction_id) AS num_trans
# MAGIC
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) = 2023
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     GROUP BY Quarter_Info, region_name, mobile
# MAGIC ),
# MAGIC
# MAGIC main_table AS (
# MAGIC     SELECT
# MAGIC         QUARTER(business_day) AS Quarter_Info,
# MAGIC         region_name,
# MAGIC         ROUND(SUM(amount),0) AS Sales,
# MAGIC         COUNT(DISTINCT mobile) AS Customers,
# MAGIC         COUNT(DISTINCT product_id) AS Products,
# MAGIC         ROUND(SUM(quantity),0) AS Volume,
# MAGIC         COUNT(DISTINCT category_id) AS Categories,
# MAGIC         COUNT(DISTINCT transaction_id) AS Transactions,
# MAGIC         ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
# MAGIC         ROUND(Sales/Transactions,2) AS ATV
# MAGIC
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) = 2023
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     GROUP BY Quarter_Info, region_name
# MAGIC ),
# MAGIC
# MAGIC customer_segments AS (
# MAGIC     SELECT
# MAGIC         QUARTER(business_day) AS Quarter_Info,
# MAGIC         region_name,
# MAGIC         COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN mobile END) AS VIPs,
# MAGIC         COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN mobile END) AS Freqs
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# MAGIC     WHERE YEAR(business_day) = 2023
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND key = 'rfm'
# MAGIC     AND channel = 'pos'
# MAGIC     AND t4.country = 'uae'
# MAGIC     AND month_year = '202312'
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     GROUP BY Quarter_Info, region_name
# MAGIC ),
# MAGIC
# MAGIC loyalty AS (
# MAGIC     SELECT
# MAGIC         QUARTER(business_day) AS Quarter_Info,
# MAGIC         region_name,
# MAGIC         COUNT(DISTINCT mobile) AS Loyalty_Customers,
# MAGIC         ROUND(SUM(amount),0) AS Loyalty_Sales
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) = 2023
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     AND LHPRDATE IS NOT NULL
# MAGIC     GROUP BY Quarter_Info, region_name
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t1.Quarter_Info,
# MAGIC     t1.region_name,
# MAGIC     "OVERALL" AS department_name,
# MAGIC     Sales,
# MAGIC     Customers,
# MAGIC     Loyalty_Sales,
# MAGIC     Loyalty_Customers,
# MAGIC     VIPs,
# MAGIC     Freqs,
# MAGIC     Products,
# MAGIC     Volume,
# MAGIC     Categories,
# MAGIC     Transactions,
# MAGIC     Discount,
# MAGIC     ATV,
# MAGIC     COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
# MAGIC     (Customers - One_Timers) AS Repeats
# MAGIC FROM one_timers AS t1
# MAGIC JOIN main_table AS t2
# MAGIC     ON t1.Quarter_Info = t2.Quarter_info
# MAGIC     AND t1.region_name = t2.region_name
# MAGIC JOIN customer_segments AS t3
# MAGIC     ON t1.Quarter_Info = t3.Quarter_info
# MAGIC     AND t1.region_name = t3.region_name
# MAGIC LEFT JOIN loyalty AS t4
# MAGIC     ON t1.Quarter_Info = t4.Quarter_info
# MAGIC     AND t1.region_name = t4.region_name
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
# MAGIC ORDER BY 1, 2, 3
# MAGIC """
# MAGIC
# MAGIC region_level_df = spark.sql(query).toPandas()
# MAGIC region_level_df['Discount'] = region_level_df['Discount'].fillna(0)
# MAGIC region_level_df['Loyalty_Sales'] = region_level_df['Loyalty_Sales'].fillna(0)
# MAGIC region_level_df['Loyalty_Customers'] = region_level_df['Loyalty_Customers'].fillna(0)

# COMMAND ----------

regions = region_level_df['region_name'].unique()

for i in range(len(regions)):
    quarterly_df = pd.concat([quarterly_df, transpose_quarter(region_level_df, regions[i])], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##UAE Overall

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info, mobile
),

main_table AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT category_id) AS Categories,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info
),

customer_segments AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year = '202312'
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Quarter_Info
),

loyalty AS (
    SELECT
        QUARTER(business_day) AS Quarter_Info,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) = 2023
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Quarter_Info
)

SELECT
    t1.Quarter_Info,
    "UAE" AS region_name,
    "OVERALL" AS department_name,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Categories,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Quarter_Info = t2.Quarter_info
JOIN customer_segments AS t3
    ON t1.Quarter_Info = t3.Quarter_info
LEFT JOIN loyalty AS t4
    ON t1.Quarter_Info = t4.Quarter_info
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3
"""

uae_overall_df = spark.sql(query).toPandas()
uae_overall_df['Discount'] = uae_overall_df['Discount'].fillna(0)
uae_overall_df['Loyalty_Sales'] = uae_overall_df['Loyalty_Sales'].fillna(0)
uae_overall_df['Loyalty_Customers'] = uae_overall_df['Loyalty_Customers'].fillna(0)

# COMMAND ----------

quarterly_df = pd.concat([quarterly_df, transpose_quarter(uae_overall_df, 'UAE')], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Yearly

# COMMAND ----------

# MAGIC %md
# MAGIC ##All Group-by Levels

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        department_name,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, region_name, department_name, mobile
),

main_table AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        department_name,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT category_id) AS Categories,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, region_name, department_name
),

customer_segments AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        department_name,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'VIP' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'Frequentist' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year IN ('202212', '202312')
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, region_name, department_name
),

loyalty AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        department_name,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Year_Info, region_name, department_name
)

SELECT
    t1.Year_Info,
    t1.region_name,
    t1.department_name,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Categories,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Year_Info = t2.Year_Info
    AND t1.region_name = t2.region_name
    AND t1.department_name = t2.department_name
JOIN customer_segments AS t3
    ON t1.Year_Info = t3.Year_Info
    AND t1.region_name = t3.region_name
    AND t1.department_name = t3.department_name
LEFT JOIN loyalty AS t4
    ON t1.Year_Info = t4.Year_info
    AND t1.region_name = t4.region_name
    AND t1.department_name = t4.department_name
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3
"""

all_levels_df2 = spark.sql(query).toPandas()
all_levels_df2['Discount'] = all_levels_df2['Discount'].fillna(0)
all_levels_df2['Loyalty_Sales'] = all_levels_df2['Loyalty_Sales'].fillna(0)
all_levels_df2['Loyalty_Customers'] = all_levels_df2['Loyalty_Customers'].fillna(0)

# COMMAND ----------

def transpose_year(df1, region):
    df2 = df1[df1['region_name'] == region].reset_index(drop=True)
    dct = {'year_2022': [], 'year_2023': []}

    departments = df2['department_name'].unique()
    for department in departments:
        years = df2[df2['department_name'] == department]['Year_Info'].values
        sales = df2[df2['department_name'] == department]['Sales'].values
        customers = df2[df2['department_name'] == department]['Customers'].values
        loyalty_sales = df2[df2['department_name'] == department]['Loyalty_Sales'].values
        loyalty_customers = df2[df2['department_name'] == department]['Loyalty_Customers'].values
        VIPs = df2[df2['department_name'] == department]['VIPs'].values
        Freqs = df2[df2['department_name'] == department]['Freqs'].values
        products = df2[df2['department_name'] == department]['Products'].values
        volume = df2[df2['department_name'] == department]['Volume'].values
        categories = df2[df2['department_name'] == department]['Categories'].values
        transactions = df2[df2['department_name'] == department]['Transactions'].values
        discount = df2[df2['department_name'] == department]['Discount'].values
        atv = df2[df2['department_name'] == department]['ATV'].values
        one_timers = df2[df2['department_name'] == department]['One_Timers'].values
        repeats = df2[df2['department_name'] == department]['Repeats'].values

        check = []
        for i, year in enumerate(years):
            key = 'year_' + str(year)
            dct[key].extend([sales[i], customers[i], loyalty_sales[i], loyalty_customers[i], VIPs[i], Freqs[i], products[i], volume[i], categories[i], transactions[i], discount[i], atv[i], one_timers[i], repeats[i]])

            check.append(key)

        keys = list(dct.keys())
        if set(keys).issubset(set(check)) == False:
            difference = set(keys) - set(check)
            if len(difference) == 1:
                key = difference.pop()
                dct[key].extend([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

    converted_df = pd.DataFrame(dct)

    departments = [item for item in departments for _ in range(14)]
    measures = ['Sales', 'Customers', 'Loyalty Sales', 'Loyalty Customers', 'VIPs', 'Freqs', 'Products', 'Volume', 'Categories', 'Transactions', 'Discount', 'ATV', 'One Timers', 'Repeats'] * df2['department_name'].nunique()
    converted_df['department_name'] = departments
    converted_df['Measures'] = measures
    converted_df['Region'] = region

    return converted_df

# COMMAND ----------

regions = all_levels_df2['region_name'].unique()

yearly_df = transpose_year(all_levels_df2, regions[0])

for i in range(1, len(regions)):
    yearly_df = pd.concat([yearly_df, transpose_year(all_levels_df2, regions[i])], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##UAE Department Level

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        department_name,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, department_name, mobile
),

main_table AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        department_name,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT category_id) AS Categories,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, department_name
),

customer_segments AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        department_name,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'VIP' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'Frequentist' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year IN ('202212', '202312')
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, department_name
),

loyalty AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        department_name,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Year_Info, department_name
)

SELECT
    t1.Year_Info,
    "UAE" AS region_name,
    t1.department_name,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Categories,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Year_Info = t2.Year_Info
    AND t1.department_name = t2.department_name
JOIN customer_segments AS t3
    ON t1.Year_Info = t3.Year_Info
    AND t1.department_name = t3.department_name
LEFT JOIN loyalty AS t4
    ON t1.Year_Info = t4.Year_info
    AND t1.department_name = t4.department_name
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3
"""

uae_department_df2 = spark.sql(query).toPandas()
uae_department_df2['Discount'] = uae_department_df2['Discount'].fillna(0)
uae_department_df2['Loyalty_Sales'] = uae_department_df2['Loyalty_Sales'].fillna(0)
uae_department_df2['Loyalty_Customers'] = uae_department_df2['Loyalty_Customers'].fillna(0)

# COMMAND ----------

yearly_df = pd.concat([yearly_df, transpose_year(uae_department_df2, 'UAE')], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Region Level Overall

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, region_name, mobile
),

main_table AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT category_id) AS Categories,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, region_name
),

customer_segments AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'VIP' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'Frequentist' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year IN ('202212', '202312')
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, region_name
),

loyalty AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        region_name,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Year_Info, region_name
)

SELECT
    t1.Year_Info,
    t1.region_name,
    "OVERALL" AS department_name,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Categories,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Year_Info = t2.Year_Info
    AND t1.region_name = t2.region_name
JOIN customer_segments AS t3
    ON t1.Year_Info = t3.Year_Info
    AND t1.region_name = t3.region_name
LEFT JOIN loyalty AS t4
    ON t1.Year_Info = t4.Year_info
    AND t1.region_name = t4.region_name
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3
"""

region_level_df2 = spark.sql(query).toPandas()
region_level_df2['Discount'] = region_level_df2['Discount'].fillna(0)
region_level_df2['Loyalty_Sales'] = region_level_df2['Loyalty_Sales'].fillna(0)
region_level_df2['Loyalty_Customers'] = region_level_df2['Loyalty_Customers'].fillna(0)

# COMMAND ----------

regions = region_level_df2['region_name'].unique()

for i in range(len(regions)):
    yearly_df = pd.concat([yearly_df, transpose_year(region_level_df2, regions[i])], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##UAE Overall

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, mobile
),

main_table AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT category_id) AS Categories,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info
),

customer_segments AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'VIP' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'Frequentist' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year IN ('202212', '202312')
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info
),

loyalty AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Year_Info
)

SELECT
    t1.Year_Info,
    "UAE" AS region_name,
    "OVERALL" AS department_name,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Categories,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Year_Info = t2.Year_Info
JOIN customer_segments AS t3
    ON t1.Year_Info = t3.Year_Info
LEFT JOIN loyalty AS t4
    ON t1.Year_Info = t4.Year_Info
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3
"""

uae_overall_df2 = spark.sql(query).toPandas()
uae_overall_df2['Discount'] = uae_overall_df2['Discount'].fillna(0)
uae_overall_df2['Loyalty_Sales'] = uae_overall_df2['Loyalty_Sales'].fillna(0)
uae_overall_df2['Loyalty_Customers'] = uae_overall_df2['Loyalty_Customers'].fillna(0)

# COMMAND ----------

yearly_df = pd.concat([yearly_df, transpose_year(uae_overall_df2, 'UAE')], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Final DF

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Sandbox Table

# COMMAND ----------

final_df = pd.merge(quarterly_df, yearly_df, on=['Region', 'department_name', 'Measures'], how='inner')
final_df = final_df[['Region', 'department_name', 'Measures', 'Q1', 'Q2', 'Q3', 'Q4', 'year_2023', 'year_2022']]

w = spark.createDataFrame(final_df)
w.createOrReplaceTempView('temp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sandbox.pj_supermarket_charter_data;
# MAGIC
# MAGIC CREATE TABLE sandbox.pj_supermarket_charter_data AS (
# MAGIC     SELECT * FROM temp_view
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_view

# COMMAND ----------

# MAGIC %md
# MAGIC #UAE Category Level

# COMMAND ----------

query = """
WITH one_timers AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        category_id,
        mobile,
        COUNT(DISTINCT transaction_id) AS num_trans

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, category_id, mobile
),

main_table AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        category_id,
        ROUND(SUM(amount),0) AS Sales,
        COUNT(DISTINCT mobile) AS Customers,
        COUNT(DISTINCT product_id) AS Products,
        ROUND(SUM(quantity),0) AS Volume,
        COUNT(DISTINCT transaction_id) AS Transactions,
        ROUND(SUM((unit_price - regular_unit_price) * quantity),0) AS Discount,
        ROUND(Sales/Transactions,2) AS ATV

    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, category_id
),

customer_segments AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        category_id,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'VIP' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'VIP' THEN mobile END) AS VIPs,
        COUNT(DISTINCT CASE WHEN YEAR(business_day) = 2022 AND month_year = '202212' AND segment = 'Frequentist' THEN mobile
        WHEN YEAR(business_day) = 2023 AND month_year = '202312' AND segment = 'Frequentist' THEN mobile END) AS Freqs
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND key = 'rfm'
    AND channel = 'pos'
    AND t4.country = 'uae'
    AND month_year IN ('202212', '202312')
    AND ROUND(amount,2) > 0
    AND quantity > 0
    GROUP BY Year_Info, category_id
),

loyalty AS (
    SELECT
        YEAR(business_day) AS Year_Info,
        category_id,
        COUNT(DISTINCT mobile) AS Loyalty_Customers,
        ROUND(SUM(amount),0) AS Loyalty_Sales
    FROM gold.pos_transactions AS t1
    JOIN gold.store_master AS t2 ON t1.store_id = t2.store_id
    JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
    WHERE YEAR(business_day) IN (2022, 2023)
    AND department_class_id IN (1, 2)
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND LHPRDATE IS NOT NULL
    GROUP BY Year_Info, category_id
)

SELECT
    t1.Year_Info,
    "UAE" AS region_name,
    t1.category_id,
    Sales,
    Customers,
    Loyalty_Sales,
    Loyalty_Customers,
    VIPs,
    Freqs,
    Products,
    Volume,
    Transactions,
    Discount,
    ATV,
    COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
    (Customers - One_Timers) AS Repeats
FROM one_timers AS t1
JOIN main_table AS t2
    ON t1.Year_Info = t2.Year_Info
    AND t1.category_id = t2.category_id
JOIN customer_segments AS t3
    ON t1.Year_Info = t3.Year_Info
    AND t1.category_id = t3.category_id
LEFT JOIN loyalty AS t4
    ON t1.Year_Info = t4.Year_Info
    AND t1.category_id = t4.category_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
ORDER BY 1, 2, 3
"""

catg_df = spark.sql(query).toPandas()
catg_df['Discount'] = catg_df['Discount'].fillna(0)
catg_df['Loyalty_Sales'] = catg_df['Loyalty_Sales'].fillna(0)
catg_df['Loyalty_Customers'] = catg_df['Loyalty_Customers'].fillna(0)

# COMMAND ----------

def transpose_catg(df1, region):
    df2 = df1[df1['region_name'] == region].reset_index(drop=True)
    dct = {'year_2022': [], 'year_2023': []}

    categories = df2['category_id'].unique()
    for category in categories:
        years = df2[df2['category_id'] == category]['Year_Info'].values
        sales = df2[df2['category_id'] == category]['Sales'].values
        customers = df2[df2['category_id'] == category]['Customers'].values
        loyalty_sales = df2[df2['category_id'] == category]['Loyalty_Sales'].values
        loyalty_customers = df2[df2['category_id'] == category]['Loyalty_Customers'].values
        VIPs = df2[df2['category_id'] == category]['VIPs'].values
        Freqs = df2[df2['category_id'] == category]['Freqs'].values
        products = df2[df2['category_id'] == category]['Products'].values
        volume = df2[df2['category_id'] == category]['Volume'].values
        transactions = df2[df2['category_id'] == category]['Transactions'].values
        discount = df2[df2['category_id'] == category]['Discount'].values
        atv = df2[df2['category_id'] == category]['ATV'].values
        one_timers = df2[df2['category_id'] == category]['One_Timers'].values
        repeats = df2[df2['category_id'] == category]['Repeats'].values

        check = []
        for i, year in enumerate(years):
            key = 'year_' + str(year)
            dct[key].extend([sales[i], customers[i], loyalty_sales[i], loyalty_customers[i], VIPs[i], Freqs[i], products[i], volume[i], transactions[i], discount[i], atv[i], one_timers[i], repeats[i]])

            check.append(key)

        keys = list(dct.keys())
        if set(keys).issubset(set(check)) == False:
            difference = set(keys) - set(check)
            if len(difference) == 1:
                key = difference.pop()
                dct[key].extend([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

    converted_df = pd.DataFrame(dct)
    categories = [item for item in categories for _ in range(13)]
    measures = ['Sales', 'Customers', 'Loyalty Sales', 'Loyalty Customers', 'VIPs', 'Freqs', 'Products', 'Volume', 'Transactions', 'Discount', 'ATV', 'One Timers', 'Repeats'] * df2['category_id'].nunique()
    converted_df['category_id'] = categories
    converted_df['Measures'] = measures
    converted_df['Region'] = region

    return converted_df

# COMMAND ----------

catg_df2 = transpose_catg(catg_df, 'UAE')

# COMMAND ----------

query = """
SELECT DISTINCT category_id, category_name, department_name
FROM gold.material_master
WHERE department_class_id IN (1, 2)
"""

dept_mapping = spark.sql(query).toPandas()
catg_df2 = pd.merge(catg_df2, dept_mapping, on='category_id', how='left')
catg_df2[['Region', 'department_name', 'category_id', 'category_name', 'Measures', 'year_2023', 'year_2022']].display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Deck Support Numbers

# COMMAND ----------

# MAGIC %md
# MAGIC ##Current State of Business Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         mobile,
# MAGIC         COUNT(DISTINCT transaction_id) AS num_trans,
# MAGIC         DATEDIFF("2023-12-31", MAX(business_day)) AS Recency
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     GROUP BY mobile
# MAGIC ),
# MAGIC
# MAGIC cte2 AS (
# MAGIC     SELECT
# MAGIC         ROUND(SUM(amount),0) AS Sales,
# MAGIC         COUNT(DISTINCT mobile) AS Customers,
# MAGIC         COUNT(DISTINCT transaction_id) AS Transactions,
# MAGIC         ROUND(Sales/Transactions,2) AS ATV
# MAGIC
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC ),
# MAGIC
# MAGIC cte3 AS (
# MAGIC     SELECT
# MAGIC         COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN mobile END) AS VIPs,
# MAGIC         COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN mobile END) AS Freqs
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND key = 'rfm'
# MAGIC     AND channel = 'pos'
# MAGIC     AND t4.country = 'uae'
# MAGIC     AND month_year IN ('202312')
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC ),
# MAGIC
# MAGIC cte4 AS (
# MAGIC     SELECT
# MAGIC         ROUND(SUM(amount),0) AS Loyalty_Sales,
# MAGIC         COUNT(DISTINCT t1.mobile) AS Loyalty_Customers
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND LHPRDATE IS NOT NULL
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     Sales,
# MAGIC     Customers,
# MAGIC     Loyalty_Sales,
# MAGIC     Loyalty_Customers,
# MAGIC     VIPs,
# MAGIC     Freqs,
# MAGIC     Transactions,
# MAGIC     ATV,
# MAGIC     COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
# MAGIC     (Customers - One_Timers) AS Repeats,
# MAGIC     AVG(Recency) AS Avg_Recency,
# MAGIC     SUM(num_trans) / COUNT(DISTINCT mobile) AS Frequency
# MAGIC FROM cte, cte2, cte3, cte4
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT t1.customer_id) AS Loyalty_Customers
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
# MAGIC JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC WHERE YEAR(business_day) IN (2023)
# MAGIC AND LHRDATE IS NOT NULL
# MAGIC AND department_class_id IN (1, 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT t1.customer_id) AS VIP_Customers
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC -- JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# MAGIC WHERE YEAR(business_day) IN (2023)
# MAGIC -- AND department_class_id IN (1, 2)
# MAGIC AND key = 'rfm'
# MAGIC AND channel = 'pos'
# MAGIC AND t4.country = 'uae'
# MAGIC AND month_year IN ('202402')
# MAGIC AND segment = "VIP"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         mobile,
# MAGIC         COUNT(DISTINCT transaction_id) AS num_trans,
# MAGIC         DATEDIFF("2023-12-31", MAX(business_day)) AS Recency
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     GROUP BY mobile
# MAGIC ),
# MAGIC
# MAGIC cte2 AS (
# MAGIC     SELECT
# MAGIC         ROUND(SUM(amount),0) AS Sales,
# MAGIC         COUNT(DISTINCT mobile) AS Customers,
# MAGIC         COUNT(DISTINCT transaction_id) AS Transactions,
# MAGIC         ROUND(Sales/Transactions,2) AS ATV
# MAGIC
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC ),
# MAGIC
# MAGIC cte3 AS (
# MAGIC     SELECT
# MAGIC         COUNT(DISTINCT CASE WHEN segment = 'VIP' THEN mobile END) AS VIPs,
# MAGIC         COUNT(DISTINCT CASE WHEN segment = 'Frequentist' THEN mobile END) AS Freqs
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND key = 'rfm'
# MAGIC     AND channel = 'pos'
# MAGIC     AND t4.country = 'uae'
# MAGIC     AND month_year IN ('202312')
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC ),
# MAGIC
# MAGIC cte4 AS (
# MAGIC     SELECT
# MAGIC         ROUND(SUM(amount),0) AS Loyalty_Sales,
# MAGIC         COUNT(DISTINCT mobile) AS Loyalty_Customers
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) IN (2023)
# MAGIC     AND LHPRDATE IS NOT NULL
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     Sales,
# MAGIC     Customers,
# MAGIC     Loyalty_Sales,
# MAGIC     Loyalty_Customers,
# MAGIC     VIPs,
# MAGIC     Freqs,
# MAGIC     Transactions,
# MAGIC     ATV,
# MAGIC     COUNT(DISTINCT CASE WHEN num_trans = 1 THEN mobile END) AS One_Timers,
# MAGIC     (Customers - One_Timers) AS Repeats,
# MAGIC     AVG(Recency) AS Avg_Recency,
# MAGIC     SUM(num_trans) / COUNT(DISTINCT mobile) AS Frequency
# MAGIC FROM cte, cte2, cte3, cte4
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

# COMMAND ----------

# MAGIC %md
# MAGIC ##High Level Numbers

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Indian Nationals dominate the customer mix with 40% representation in customer base

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     nationality_group,
# MAGIC     COUNT(DISTINCT t1.mobile) AS customers,
# MAGIC     ROUND((COUNT(DISTINCT t1.mobile) / MAX(total_customers)), 4) AS customer_perc
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
# MAGIC JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC JOIN
# MAGIC     (SELECT COUNT(DISTINCT t1.mobile) AS total_customers
# MAGIC      FROM gold.pos_transactions AS t1
# MAGIC      JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
# MAGIC      JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC      WHERE YEAR(business_day) = 2023
# MAGIC      AND department_class_id IN (1, 2)
# MAGIC      AND ROUND(amount,2) > 0
# MAGIC      AND quantity > 0
# MAGIC      AND nationality_group NOT IN ("OTHERS", "NA")
# MAGIC     )
# MAGIC WHERE
# MAGIC     YEAR(business_day) = 2023
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     AND nationality_group NOT IN ("OTHERS", "NA")
# MAGIC GROUP BY nationality_group
# MAGIC ORDER BY customer_perc DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. VIPs generated 36% of all the sales while Frequentists generated 43% in Q4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     segment,
# MAGIC     ROUND(SUM(amount),0) AS sales,
# MAGIC     ROUND((SUM(amount) / MAX(total_sales)), 4) AS sales_perc
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
# MAGIC JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC JOIN
# MAGIC     (SELECT ROUND(SUM(amount),0) AS total_sales
# MAGIC      FROM gold.pos_transactions AS t1
# MAGIC      JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
# MAGIC      JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC      WHERE business_day BETWEEN "2023-10-01" AND "2023-12-31"
# MAGIC      AND department_class_id IN (1, 2)
# MAGIC      AND ROUND(amount,2) > 0
# MAGIC      AND quantity > 0
# MAGIC      AND key = 'rfm'
# MAGIC      AND channel = 'pos'
# MAGIC      AND t2.country = 'uae'
# MAGIC      AND month_year = '202312'
# MAGIC     )
# MAGIC WHERE
# MAGIC     business_day BETWEEN "2023-10-01" AND "2023-12-31"
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     AND key = 'rfm'
# MAGIC     AND channel = 'pos'
# MAGIC     AND t2.country = 'uae'
# MAGIC     AND month_year = '202312'
# MAGIC GROUP BY segment
# MAGIC ORDER BY sales_perc DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 3. On an average, a typical SM basket contains 7 products, while for VIPs it is 22 products.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(products)
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC       transaction_id,
# MAGIC       COUNT(DISTINCT product_id) AS products
# MAGIC   FROM gold.pos_transactions AS t1
# MAGIC   JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC   -- JOIN analytics.customer_segments AS t3 ON t1.customer_id = t3.customer_id
# MAGIC   WHERE YEAR(business_day) = 2023
# MAGIC   AND department_class_id IN (1, 2)
# MAGIC   AND ROUND(amount,2) > 0
# MAGIC   AND quantity > 0
# MAGIC   -- AND key = 'rfm'
# MAGIC   -- AND channel = 'pos'
# MAGIC   -- AND t3.country = 'uae'
# MAGIC   -- AND month_year = '202312'
# MAGIC   -- AND segment = 'VIP'
# MAGIC   GROUP BY transaction_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 4. The average age of an SM shopper is 40 and Frequentist Segment has the highest number of youngest customers

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH avg AS (
# MAGIC     SELECT
# MAGIC         AVG(age) AS avg_age
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     -- JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# MAGIC     WHERE YEAR(business_day) = 2023
# MAGIC     AND t1.mobile IS NOT NULL
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     -- AND key = 'rfm'
# MAGIC     -- AND channel = 'pos'
# MAGIC     -- AND t4.country = 'uae'
# MAGIC     -- AND month_year = '202312'
# MAGIC     -- AND segment = 'Frequentist'
# MAGIC     AND age BETWEEN 18 AND 80
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     segment,
# MAGIC     COUNT(*) AS count_min_age,
# MAGIC     avg_age
# MAGIC FROM avg, gold.pos_transactions AS t1
# MAGIC JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
# MAGIC JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# MAGIC WHERE YEAR(business_day) = 2023
# MAGIC AND t1.mobile IS NOT NULL
# MAGIC AND ROUND(amount,2) > 0
# MAGIC AND quantity > 0
# MAGIC AND department_class_id IN (1, 2)
# MAGIC AND key = 'rfm'
# MAGIC AND channel = 'pos'
# MAGIC AND t4.country = 'uae'
# MAGIC AND month_year = '202312'
# MAGIC AND age = 18
# MAGIC GROUP BY segment, avg_age
# MAGIC ORDER BY count_min_age DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. The M|F mix of SM shoppers is 80%|20% which is same from overall shoppers mix

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     gender,
# MAGIC     COUNT(DISTINCT t1.mobile)
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.customer_profile AS t2 ON t1.customer_id = t2.account_key
# MAGIC JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC WHERE YEAR(business_day) = 2023
# MAGIC AND ROUND(amount,2) > 0
# MAGIC AND quantity > 0
# MAGIC AND department_class_id IN (1, 2)
# MAGIC GROUP BY gender
# MAGIC
# MAGIC -- In supermarket departments
# MAGIC -- UNKNOWN - 2144530
# MAGIC -- MALE - 1263965 - 80%
# MAGIC -- FEMALE - 316353 - 20%
# MAGIC
# MAGIC -- Overall
# MAGIC -- UNKNOWN - 2561803
# MAGIC -- MALE - 1319561 - 80%
# MAGIC -- FEMALE - 326590 - 20%

# COMMAND ----------

# MAGIC %md
# MAGIC 6. The average shopper visits around 20 times a year, while Frequentists and VIPs visit 49 and 27 times respectively

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     segment,
# MAGIC     COUNT(DISTINCT transaction_id)/COUNT(DISTINCT mobile) AS Frequency
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
# MAGIC JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC WHERE YEAR(business_day) = 2023
# MAGIC AND ROUND(amount,2) > 0
# MAGIC AND quantity > 0
# MAGIC AND department_class_id IN (1, 2)
# MAGIC AND key = 'rfm'
# MAGIC AND channel = 'pos'
# MAGIC AND t2.country = 'uae'
# MAGIC AND month_year = '202312'
# MAGIC GROUP BY segment

# COMMAND ----------

# MAGIC %md
# MAGIC ##Store Loyalists

# COMMAND ----------

# MAGIC %md
# MAGIC 72% of top customers (VIP + FREQ) are store loyalists (>70% SM sales from single store)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH total AS (
# MAGIC     SELECT
# MAGIC         mobile,
# MAGIC         ROUND(SUM(amount),0) AS total_sales
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     WHERE YEAR(business_day) = 2023
# MAGIC     AND mobile IS NOT NULL
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND key = 'rfm'
# MAGIC     AND channel = 'pos'
# MAGIC     AND t2.country = 'uae'
# MAGIC     AND month_year = '202312'
# MAGIC     AND UPPER(segment) IN ('VIP', 'FREQUENTIST')
# MAGIC     GROUP BY mobile
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC store_level AS (
# MAGIC     SELECT
# MAGIC         t1.mobile,
# MAGIC         t1.store_id,
# MAGIC         ROUND(SUM(amount),0) AS sales,
# MAGIC         total_sales,
# MAGIC         ROUND(sales/total_sales,4) AS sales_perc
# MAGIC     FROM gold.pos_transactions AS t1
# MAGIC     JOIN analytics.customer_segments AS t2 ON t1.customer_id = t2.customer_id
# MAGIC     JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC     JOIN gold.store_master AS t4 ON t1.store_id = t4.store_id
# MAGIC     JOIN total AS t5 ON t1.mobile = t5.mobile
# MAGIC     WHERE YEAR(business_day) = 2023
# MAGIC     AND t1.mobile IS NOT NULL
# MAGIC     AND ROUND(amount,2) > 0
# MAGIC     AND quantity > 0
# MAGIC     AND department_class_id IN (1, 2)
# MAGIC     AND key = 'rfm'
# MAGIC     AND channel = 'pos'
# MAGIC     AND t2.country = 'uae'
# MAGIC     AND month_year = '202312'
# MAGIC     AND UPPER(segment) IN ('VIP', 'FREQUENTIST')
# MAGIC     GROUP BY t1.mobile, t1.store_id, total_sales
# MAGIC     HAVING sales_perc > 0.7
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     ROUND(COUNT(DISTINCT t1.mobile) / COUNT(DISTINCT t2.mobile),4) AS customer_perc
# MAGIC FROM store_level AS t1
# MAGIC RIGHT JOIN total AS t2
# MAGIC ON t1.mobile = t2.mobile

# COMMAND ----------

# MAGIC %md
# MAGIC ##Key Supermarket Product Insights

# COMMAND ----------

# MAGIC %md
# MAGIC Department-wise product additions

# COMMAND ----------

query = """
WITH new_products AS (
    SELECT department_name, COUNT(DISTINCT product_id) AS new_products
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE YEAR(business_day) = 2023
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND department_class_id IN (1, 2)
    AND product_id NOT IN (
        SELECT DISTINCT product_id
        FROM gold.pos_transactions AS t1
        JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
        WHERE YEAR(business_day) = 2022
        AND ROUND(amount,2) > 0
        AND quantity > 0
        AND department_class_id IN (1, 2)
    )
    GROUP BY department_name
),

removed_products AS (
    SELECT department_name, COUNT(DISTINCT product_id) AS removed_products
    FROM gold.pos_transactions AS t1
    JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
    WHERE YEAR(business_day) = 2022
    AND ROUND(amount,2) > 0
    AND quantity > 0
    AND department_class_id IN (1, 2)
    AND product_id NOT IN (
        SELECT DISTINCT product_id
        FROM gold.pos_transactions AS t1
        JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
        WHERE YEAR(business_day) = 2023
        AND ROUND(amount,2) > 0
        AND quantity > 0
        AND department_class_id IN (1, 2)
    )
    GROUP BY department_name
)

SELECT t1.department_name, new_products, removed_products
FROM new_products AS t1
JOIN removed_products AS t2 ON t1.department_name = t2.department_name
ORDER BY t1.department_name
"""

new_products_df = spark.sql(query).toPandas()

# COMMAND ----------

# 11,046 products added in 2023 (15.7% of all products) with 4898 being in GROCERY FOOD
# 9,102 products removed in 2022 (13.3% of all products) with 3594 being in GROCERY FOOD

# COMMAND ----------

query = """
SELECT
    YEAR(business_day) AS year_info,
    product_id,
    ROUND(SUM(amount),0) AS sales
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
WHERE YEAR(business_day) IN (2022, 2023)
AND ROUND(amount,2) > 0
AND quantity > 0
AND department_class_id IN (1, 2)
GROUP BY year_info, product_id
"""

products_contri_df = spark.sql(query).toPandas()

# COMMAND ----------

products22_df = products_contri_df[products_contri_df['year_info'] == 2022][['product_id', 'sales']].reset_index()
products23_df = products_contri_df[products_contri_df['year_info'] == 2023][['product_id', 'sales']].reset_index()

products23_list = products23_df['product_id'].values
products22_list = products22_df['product_id'].values
new_products_list = [i for i in products23_list if i not in products22_list]

def check_presence(value):
    if value in new_products_list:
        return 1
    else:
        return 0

products23_df['new_flag'] = products23_df['product_id'].apply(lambda x: check_presence(x))

new_products_sales = products23_df[products23_df['new_flag'] == 1]['sales'].sum()
all_products_sales = products23_df['sales'].sum()
new_products_contri = new_products_sales / all_products_sales * 100
new_products_sales, all_products_sales, new_products_contri

# COMMAND ----------

# New products of 2023 contribute to 4.7% of the overall supermarket department class sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     YEAR(business_day) AS year_info,
# MAGIC     material_group_id,
# MAGIC     ROUND(SUM(amount),0) AS sales
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC WHERE YEAR(business_day) IN (2022, 2023)
# MAGIC AND department_id BETWEEN 1 AND 13
# MAGIC AND ROUND(amount,2) > 0
# MAGIC AND quantity > 0
# MAGIC GROUP BY year_info, material_group_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT material_group_id, material_group_name, category_name, department_name
# MAGIC FROM gold.material_master
# MAGIC WHERE department_class_id IN (1, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sandbox Tables - Lapser Prevention Opportunity

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sandbox.pj_sm_data AS (
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     customer_id,
# MAGIC     LHPRDATE,
# MAGIC     transaction_id,
# MAGIC     mocd_flag,
# MAGIC     ROUND(SUM(amount),0) AS sales
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC WHERE business_day BETWEEN "2023-03-01" AND "2024-02-29"
# MAGIC AND department_class_id IN (1, 2)
# MAGIC AND amount > 0
# MAGIC AND quantity > 0
# MAGIC GROUP BY month, customer_id, LHPRDATE, transaction_id, mocd_flag
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Numbers - Lapser Prevention Opportunity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT customer_id) AS ss_total,
# MAGIC     COUNT(DISTINCT CASE WHEN mocd_flag IS NULL THEN customer_id END) AS non_mocd_customers,
# MAGIC     SUM(sales) AS total_sales,
# MAGIC     COUNT(DISTINCT transaction_id) AS transactions,
# MAGIC     COUNT(DISTINCT CASE WHEN mocd_flag = 1 THEN customer_id END) AS mocd_customers,
# MAGIC     SUM(CASE WHEN mocd_flag = 1 THEN sales ELSE 0 END) AS mocd_sales,
# MAGIC     COUNT(DISTINCT CASE WHEN mocd_flag = 1 THEN transaction_id END) AS mocd_transactions
# MAGIC FROM sandbox.pj_sm_data
# MAGIC WHERE month = 3
# MAGIC AND LHPRDATE IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT customer_id) AS new_ss,
# MAGIC     COUNT(DISTINCT CASE WHEN mocd_flag IS NULL THEN customer_id END) AS new_non_mocd_customers,
# MAGIC     COUNT(DISTINCT CASE WHEN mocd_flag = 1 THEN customer_id END) AS new_mocd_customers
# MAGIC FROM sandbox.pj_sm_data
# MAGIC WHERE month = 4
# MAGIC AND customer_id NOT IN (
# MAGIC     SELECT
# MAGIC         DISTINCT customer_id
# MAGIC     FROM sandbox.pj_sm_data
# MAGIC     WHERE month = 3
# MAGIC     -- WHERE month BETWEEN 3 AND 12
# MAGIC     -- WHERE month IN (1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
# MAGIC     AND customer_id IS NOT NULL
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT customer_id) AS ss_lapsers
# MAGIC FROM sandbox.pj_sm_data
# MAGIC WHERE month = 4
# MAGIC AND customer_id NOT IN (
# MAGIC     SELECT
# MAGIC         DISTINCT customer_id
# MAGIC     FROM sandbox.pj_sm_data
# MAGIC     -- WHERE month = 3
# MAGIC     WHERE month BETWEEN 4 AND 9
# MAGIC     -- WHERE month IN (1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
# MAGIC     AND customer_id IS NOT NULL
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##PL Contri in SM

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(CASE WHEN brand = 'LULU PRIVATE LABEL' THEN amount ELSE 0 END),0) AS pl_sales,
# MAGIC     ROUND(SUM(amount),0) AS total_sales,
# MAGIC     (pl_sales/total_sales) AS pl_contri
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC WHERE business_day BETWEEN "2023-01-01" AND "2023-12-31"
# MAGIC AND amount > 0
# MAGIC AND quantity > 0
# MAGIC AND department_class_id IN (1, 2)
# MAGIC -- AND UPPER(department_name) IN ('ELECTRICALS', 'COMPUTERS & GAMING', 'MOBILE PHONES', 'ELECTRONICS', 'CONSUMER ELECTRONICS ACCESSORIES')
# MAGIC -- AND UPPER(department_name) IN ('FASHION JEWELLERY & ACCESSORIES', 'FASHION LIFESTYLE', 'FASHION RETAIL', 'FOOTWEAR', 'LADIES BAGS & ACCESSORIES', 'TEXTILES')
# MAGIC -- AND UPPER(category_name) IN ('TABLEWARE', 'KITCHENWARE')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     department_id,
# MAGIC     COUNT(DISTINCT category_id)
# MAGIC FROM gold.material_master AS t1
# MAGIC JOIN gold.pos_transactions AS t2 ON t1.material_id = t2.product_id
# MAGIC WHERE department_id = 40
# MAGIC AND amount > 0
# MAGIC AND quantity > 0
# MAGIC AND YEAR(business_day) = 2023
# MAGIC GROUP BY department_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT department_id, department_name, COUNT(DISTINCT category_id)
# MAGIC FROM gold.material_master AS t1
# MAGIC JOIN gold.pos_transactions AS t2 ON t1.material_id = t2.product_id
# MAGIC WHERE YEAR(business_day) = 2023
# MAGIC AND amount > 0
# MAGIC AND quantity > 0
# MAGIC AND department_class_name IN ('SUPER MARKET', 'FRESH FOOD')
# MAGIC GROUP BY department_id, department_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##Material Group Level ATV

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     ROUND(SUM(amount),0) AS Sales,
# MAGIC     COUNT(DISTINCT transaction_id) AS Transactions,
# MAGIC     ROUND(Sales/Transactions,2) AS ATV
# MAGIC FROM gold.pos_transactions AS t1
# MAGIC JOIN gold.material_master AS t3 ON t1.product_id = t3.material_id
# MAGIC WHERE YEAR(business_day) IN (2023)
# MAGIC AND department_class_id IN (1, 2)
# MAGIC AND ROUND(amount,2) > 0
# MAGIC AND quantity > 0
# MAGIC GROUP BY material_group_name

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


