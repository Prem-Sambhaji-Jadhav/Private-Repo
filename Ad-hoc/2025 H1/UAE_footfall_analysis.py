# Databricks notebook source
# MAGIC %md
# MAGIC #Read Data

# COMMAND ----------

query = """
WITH main_data AS (
    SELECT
        MONTH(trans.business_day) AS month,
        mm.category_name,
        SUM(trans.amount) AS amount,
        SUM(trans.quantity) AS quantity,
        COUNT(DISTINCT trans.transaction_id) AS transactions,
        COUNT(DISTINCT trans.customer_key) AS customers
    FROM gold.transaction.uae_pos_transactions AS trans
    JOIN gold.material.material_master AS mm ON trans.product_id = mm.material_id
    JOIN gold.customer.vynamic_customer_profile AS cp ON trans.customer_id = cp.maxxing_account_key
    WHERE
        trans.business_day BETWEEn '2024-04-01' AND '2024-07-31'
        AND trans.transaction_type_id NOT IN ("RR", "RT")
        AND trans.amount > 0
        AND trans.quantity > 0
        AND trans.product_id IS NOT NULL
        AND bulk_sale_flag IS NULL
        AND cp.lhrdate IS NOT NULL
        AND cp.loyalty_program_id = 1
        AND cp.LHRDate_utc IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    CASE WHEN month = 7 THEN 'July' ELSE 'Q2' END AS period,
    category_name,
    ROUND(AVG(amount)) AS sales,
    ROUND(AVG(quantity)) AS quantity,
    AVG(amount) / AVG(quantity) AS price,
    ROUND(AVG(transactions)) AS transactions,
    ROUND(AVG(customers)) AS customers
FROM main_data
GROUP BY 1, 2
ORDER BY 1 DESC
"""

df = spark.sql(query).toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(trans.business_day) AS month,
# MAGIC     ROUND(SUM(trans.amount)) AS amount,
# MAGIC     ROUND(SUM(trans.quantity)) AS quantity,
# MAGIC     COUNT(DISTINCT trans.transaction_id) AS transactions,
# MAGIC     COUNT(DISTINCT trans.customer_key) AS customers,
# MAGIC     ROUND(SUM(trans.amount) / SUM(trans.quantity), 2) AS price
# MAGIC FROM gold.transaction.uae_pos_transactions AS trans
# MAGIC JOIN gold.material.material_master AS mm ON trans.product_id = mm.material_id
# MAGIC JOIN gold.customer.vynamic_customer_profile AS cp ON trans.customer_id = cp.maxxing_account_key
# MAGIC WHERE
# MAGIC     trans.business_day BETWEEn '2024-04-01' AND '2024-07-31'
# MAGIC     AND trans.transaction_type_id NOT IN ("RR", "RT")
# MAGIC     AND trans.amount > 0
# MAGIC     AND trans.quantity > 0
# MAGIC     AND trans.product_id IS NOT NULL
# MAGIC     AND bulk_sale_flag IS NULL
# MAGIC     AND cp.lhrdate IS NOT NULL
# MAGIC     AND cp.loyalty_program_id = 1
# MAGIC     AND cp.LHRDate_utc IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

df.groupby('period')[['sales', 'quantity', 'transactions', 'customers']].sum().reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Top Sales Contributing Categories

# COMMAND ----------

top_categories = df.groupby('category_name')[['sales']].sum().reset_index()

top_categories.rename({'sales': 'sales'}, inplace = True)
top_categories = top_categories.sort_values(by = 'sales', ascending = False).reset_index(drop = True)

top_categories['sales_perc'] = top_categories['sales'] / top_categories['sales'].sum()
top_categories['sales_perc_cumsum'] = top_categories['sales_perc'].cumsum()

top_categories_lst = top_categories[top_categories['sales_perc_cumsum'] <= 0.8]['category_name'].tolist()
len(top_categories_lst) # total 298

# COMMAND ----------

df[df['category_name'].isin(top_categories_lst)].groupby('period')[['sales', 'quantity', 'transactions', 'customers']].sum().reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Save Data

# COMMAND ----------

pivot_df = df[df['category_name'].isin(top_categories_lst)].pivot(index='category_name', columns='period', values = 'customers')

# Calculate month-over-month growth
growth_df = pivot_df.pct_change(axis='columns').reset_index().drop(columns = 'July')

growth_df.to_csv('/Workspace/Users/prem@loyalytics.in/Ad-hoc/uae_retention_degrowth_rca.csv', index = False)
