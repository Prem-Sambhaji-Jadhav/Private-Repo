# Databricks notebook source
# MAGIC %md
# MAGIC ##Rice & Oat Cake

# COMMAND ----------

query = """
SELECT
    material_id,
    material_name,
    material_group_name,
    brand,
    ROUND(SUM(amount)) AS sales
FROM gold.pos_transactions AS t1
JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
WHERE
    business_day BETWEEN "2023-07-01" AND "2024-06-28"
    AND material_group_name = "RICE & OAT CAKE"
    AND transaction_type IN ("SALE", "SELL_MEDIA")
    AND amount > 0
    AND quantity > 0
GROUP BY 1, 2, 3, 4
ORDER BY 1
"""

df = spark.sql(query).toPandas()

# COMMAND ----------

df.display()

# COMMAND ----------

volume = [130, 130, 130, 130, 132, 100, 80, 80, 129, 100, 120, 28, 28, 28, 115, 19, 19, 115, 19, 115, 120, 125, 100, 100, 100, 90, 30, 90, 75, 75, 75, 75, 75, 75, 75, 75, 83, 28, 185, 200, 200, 200, 151.2, 129, 95, 85]

type = ['Lightly Salted', 'Unsalted', 'Sesame Seed', 'Lightly Salted', 'Strawberry',
        'Yoghurt', 'Regular', 'Regular', 'Salt & Vinegar', 'Milk Chocolate',
        'Vegetable', 'Caramel Dark Chocolate', 'Caramel White Chocolate',
        'Lemon Dark Chocolate', 'Strawberry Yoghurt', 'Strawberry Yoghurt',
        'White Chocolate', 'Dark Chocolate', 'Dark Chocolate', 'White Chocolate',
        'Multiseed', 'Corn', 'Sweet Potato', 'Carrot', 'Lentil', 'Carob', 'Cinnamon',
        'Caramel', 'Corn', 'Cheddar Cheese', 'Honey Wheat', 'Strawberry', 'Onion',
        'Cinnamon', 'Wholewheat', 'Lentil', 'Chickpea', 'Peanut Cream & Dark Chocolate',
        'Apple Cinnamon', 'Dark Chocolate', 'Milk Chocolate', 'White Chocolate',
        'Japonica', 'Snow', 'Seaweed', 'Cheese Potato']

type_bin = ['Seasoned', 'Seasoned', 'Sesame Seed', 'Seasoned', 'Fruit', 'Yoghurt',
            'Regular', 'Regular', 'Seasoned', 'Chocolate', 'Vegetable', 'Chocolate',
            'Chocolate', 'Chocolate', 'Fruit', 'Fruit', 'Chocolate', 'Chocolate',
            'Chocolate', 'Chocolate', 'Multiseed', 'Fruit', 'Vegetable', 'Vegetable',
            'Legumes', 'Legumes', 'Spice', 'Caramel', 'Fruit', 'Cheddar Cheese', 'Honey', 'Fruit', 'Vegetable', 'Spice', 'Wholewheat', 'Legumes', 'Legumes', 'Chocolate', 'Spice', 'Chocolate', 'Chocolate', 'Chocolate', 'Japonica', 'Snow', 'Seaweed', 'Vegetable']

df['volume'] = volume
df['units'] = 'G'
df['item_count'] = 1
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

import os
folder_path = '/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/rice_&_oat_cake'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# COMMAND ----------

df.to_csv(f'{folder_path}/rice_&_oat_cake_attributes.csv', index = False)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


