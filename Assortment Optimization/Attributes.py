# Databricks notebook source
# MAGIC %md
# MAGIC #Initializations

# COMMAND ----------

table_name = 'dev.sandbox.pj_ao_attributes'

# COMMAND ----------

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests

# COMMAND ----------

def run(start_date, end_date, category, material_group):
    query = f"""
    SELECT
        t2.material_id,
        t2.material_name,
        t2.brand,
        t2.material_group_name,
        t2.category_name,
        ROUND(SUM(t1.amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        t1.business_day BETWEEN "{start_date}" AND "{end_date}"
        AND t2.category_name = "{category}"
        AND t2.material_group_name = "{material_group}"
        AND t3.tayeb_flag = 0
        AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1
    """
    return spark.sql(query).toPandas()

# COMMAND ----------

def web_scrape(df):
    material_ids = df['material_id'].astype(str).tolist()
    dct = {'material_id': material_ids, 'material_name_long': []}

    for i in material_ids:
        url = f'https://www.luluhypermarket.com/en-ae//p/{i}'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers)
        bsObj = BeautifulSoup(response.content, 'html.parser')

        try:
            material_name = bsObj.find('h1', class_='product-name').text.strip()
        except AttributeError:
            material_name = None
        
        dct['material_name_long'].append(material_name)

    df2 = pd.DataFrame(dct)
    df2['material_id'] = df2['material_id'].astype('int64')
    df2 = pd.merge(df2, df[['material_id', 'material_name', 'brand']], on='material_id', how = 'inner')
    df2['material_name_long'] = df2['material_name_long'].fillna(df2['material_name'])
    df2 = df2.drop(columns = 'material_name')
    df2.rename(columns = {'material_name_long': 'material_name'}, inplace = True)

    return df2

# COMMAND ----------

# MAGIC %md
# MAGIC #Rice & Oat Cake

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'BISCUITS & CAKES', 'RICE & OAT CAKE')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [130, 130, 130, 130, 132, 100, 80, 80, 129, 70, 70, 100, 28, 28, 28, 115, 19, 19, 115, 19, 115, 120, 125, 100, 100, 100, 90, 30, 90, 75, 75, 75, 75, 75, 75, 75, 75, 83, 28, 185, 200, 200, 200, 151.2, 129, 95, 86]

type = ['Lightly Salted', 'Unsalted', 'Sesame Seed', 'Lightly Salted', 'Strawberry',
        'Yoghurt', 'Regular', 'Regular', 'Salt & Vinegar', 'Salt & Pepper & Lime', 'Cheddar & Tomato Chutney',
        'Milk Chocolate', 'Caramel Dark Chocolate', 'Caramel White Chocolate',
        'Lemon Dark Chocolate', 'Strawberry Yoghurt', 'Strawberry Yoghurt',
        'White Chocolate', 'Dark Chocolate', 'Dark Chocolate', 'White Chocolate',
        'Multiseed', 'Corn', 'Sweet Potato', 'Carrot', 'Lentil', 'Carob', 'Cinnamon',
        'Caramel', 'Corn', 'Cheddar Cheese', 'Honey Wheat', 'Strawberry', 'Onion',
        'Cinnamon', 'Wholewheat', 'Lentil', 'Chickpea', 'Peanut Cream & Dark Chocolate',
        'Apple Cinnamon', 'Dark Chocolate', 'Milk Chocolate', 'White Chocolate',
        'Japonica', 'Snow', 'Seaweed', 'Cheese Potato']

type_bin = type.copy()

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

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Vinegar

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'SAUCES & PICKLES', 'VINEGAR')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [1, 16, 16, 32, 32, 16, 32, 32, 750, 500, 500, 12, 16, 16, 150, 16, 32, 1, 284, 3.78, 568, 500, 500, 284, 473, 473, 250, 32, 250, 150, 32, 1, 946, 946, 500, 16, 250, 16, 250, 473, 500, 300, 500, 473, 16, 150, 500, 500, 500, 500, 500, 250, 1, 500, 250, 250, 250, 250, 500, 500, 473, 946, 480, 3.78, 1, 310, 500, 400, 445, 450, 500, 3.78, 280, 250, 250, 500, 250, 500, 1000, 5000, 1, 1, 500, 1, 500, 500, 500, 500]

units = ['GAL', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'ML', 'ML', 'ML', 'OZ', 'OZ', 'OZ', 'ML', 'OZ', 'OZ', 'L', 'ML', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'OZ', 'ML', 'ML', 'OZ', 'GAL', 'ML', 'ML', 'ML', 'OZ', 'ML', 'OZ', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'OZ', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'L', 'ML', 'L', 'ML', 'ML', 'ML', 'ML']

type = ['White', 'Apple Cider', 'White', 'Brown', 'White', 'White', 'White', 'Apple Cider', 'Apple', 'Red Grape', 'Balsamic', 'Balsamic', 'Apple', 'Grape', 'Rice', 'White', 'White', 'White', 'Malt', 'White', 'Distilled Malt', 'Balsamic', 'Apple Cider', 'Distilled Malt', 'Natural', 'White', 'Balsamic', 'Grape', 'Balsamic', 'Rice', 'Apple Cider', 'White', 'White', 'Apple Cider', 'Apple Cider', 'White', 'White Balsamic', 'Apple Cider', 'Balsamic', 'White', 'Balsamic', 'Apple', 'Balsamic', 'Natural', 'Apple Cider', 'Brown Rice', 'Balsamic', 'Balsamic', 'Grape', 'Pomegranate', 'Balsamic', 'Balsamic', 'White', 'Coconut', 'Balsamic', 'Balsamic', 'Balsamic', 'Balsamic', 'Apple Cider', 'Apple Cider', 'Apple Cider', 'Apple Cider', 'Coconut', 'White', 'Naturally Fermented', 'Cocosuka', 'Rice', 'Mirin', 'Balsamic', 'Mature', 'Apple Cider', 'White', 'Dates', 'White', 'Tomato', 'Apple Cider', 'Apple Cider', 'White', 'White','White', 'White', 'White', 'White', 'White', 'Apple Cider', 'Synthetic', 'Sushi', 'Synthetic']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([2308582, 2091917]), 'item_count'] = 2
df.loc[df['material_id'].isin([1283071]), 'item_count'] = 3
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Fruits

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'ICE CREAM & DESSERTS', 'FRUITS')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [450, 450, 400, 500, 400, 800, 454, 340, 284, 283, 283, 284, 500, 500, 500, 500, 450, 1, 340, 283, 400, 500, 283, 283, 800, 1, 400, 500, 283, 10, 15, 400, 1, 16, 12, 16, 16, 12, 40, 40, 12, 12, 40, 16, 12, 445.5, 454, 500, 284, 284, 340, 454, 907, 283, 283, 397, 300, 340, 312, 283, 312, 284, 284, 750, 750, 750, 400, 400, 500, 16, 350, 400, 400, 400, 14, 14, 14, 14, 400, 14, 400, 14, 14, 14, 14, 454, 300, 300, 300, 300, 300, 300, 250, 250, 400, 283, 400, 400, 300, 12, 595, 1.36, 1.36, 1.36, 1.36, 907, 907, 1.36, 1.36, 400, 500, 312, 400, 400, 350, 454, 1.36, 454, 400, 400, 312, 350, 6.1, 6.1, 500, 500, 350, 350, 500, 400, 400, 1, 400, 400, 400, 100, 400, 450, 400, 400, 350, 350, 350, 350, 350, 1, 1, 907.2, 500, 1, 80, 80, 2.5, 400, 32, 500, 500, 500, 500, 500, 500, 500, 500, 300, 350, 350, 350, 150, 150, 250, 400, 400, 400, 800, 800, 283, 350, 350, 350, 350, 350, 350, 350, 350, 350, 350, 350, 350]

units = ['G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'KG', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'OZ', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'OZ', 'G', 'KG', 'KG', 'KG', 'KG', 'G', 'G', 'KG', 'KG', 'G', 'ML', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'KG', 'G', 'ML', 'KG', 'G', 'G', 'KG', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

type = ['Strawberry', 'Strawberry', 'Strawberry', 'Summer Fruits', 'Strawberry', 'Strawberry', 'Peach', 'Blueberry', 'Strawberry', 'Strawberry', 'Raspberry', 'Blackberry', 'Black Currant', 'Red Currant', 'Blackberry', 'Strawberry', 'Salad Fruits', 'Mango', 'Dragonfruit', 'Blueberry', 'Strawberry', 'Date', 'Blueberry', 'Mixed Berries', 'Mango', 'Pineapple', 'Strawberry', 'Black Forest Fruits', 'Strawberry', 'Cherry', 'Blueberry', 'Strawberry', 'Cranberry', 'Strawberry', 'Berry Mix', 'Peach', 'Mango', 'Blueberry', 'Strawberry', 'Berry Mix', 'Blackberry', 'Raspberry', 'Blueberry', 'Pineapple', 'Cherry', 'Banana', 'Banana', 'Blueberry', 'Strawberry', 'Berry Mix', 'Mixed Berries', 'Mango', 'Blueberry', 'Berry Mix', 'Berry Mix', 'Corn', 'Raspberry', 'Raspberry', 'Black Jamun', 'Mango', 'Mango', 'Butternut Squash', 'Mango', 'Red Smoothie Mix', 'Yellow Smoothie Mix', 'Green Smoothie Mix', 'Blueberry', 'Blueberry & Strawberry', 'Mango', 'Berry Mix', 'Triple Berry', 'Pineapple', 'Tropical Fruit Mix', 'Strawberry', 'Passion Fruit', 'Blackberry', 'Mango', 'Soursop', 'Strawberry', 'Lulo', 'Date', 'Papaya', 'Pineapple', 'Orange', 'Guava', 'Mixed Fruit', 'Strawberry', 'Raspberry', 'Blueberry', 'Berry Mix', 'Blackberry', 'Mango', 'Red Smoothie Mix', 'Yellow Smoothie Mix', 'Acai Berry', 'Peach', 'Mixed Berries', 'Strawberry', 'Raspberry', 'Raspberry', 'Acai Berry', 'Pineapple', 'Mango', 'Tropical Fruit Mix', 'Peach', 'Fruit Mix', 'Fruit Mix', 'Blueberry', 'Berry Mix', 'Acai Berry', 'Acai Berry', 'Chikoo', 'Tropical Fruit Mix', 'Pineapple', 'Triple Berry', 'Banana', 'Mango', 'Strawberry', 'Mixed Berries', 'Mixed Berries', 'Pomegranate', 'Strawberry', 'Acai Berry', 'Acai Berry', 'Berry Smoothie Mix', 'Black Forest Fruits', 'Blueberry', 'Cherry', 'Mango', 'Caja', 'Pineapple', 'Cupuacu', 'Guava', 'Acerola', 'Soursop', 'Acai Berry', 'Blueberry', 'Strawberry', 'Blueberry & Strawberry', 'Acai Berry', 'Strawberry', 'Blueberry', 'Black Cherry', 'Mixed Berries', 'Strawberry', 'Passion Fruit', 'Strawberry', 'Cherry', 'Acai Berry & Banana', 'Blueberry', 'Mango', 'Custard Apple', 'Strawberry', 'Strawberry', 'Banana', 'Orange Smoothie Mix', 'Pink Smoothie Mix', 'Yellow Smoothie Mix', 'Red Smoothie Mix', 'Blueberry', 'Mixed Berries', 'Mango', 'Pineapple', 'Raspberry', 'Blackberry', 'Blueberry', 'Mixed Berries', 'Raspberry & Milk Chocolate', 'Raspberry & Dark Chocolate', 'Breadfruit', 'Mango', 'Passion Fruit', 'Fruit Mix', 'Mango', 'Custard Apple', 'Blueberry', 'Raspberry', 'Blackberry', 'Blueberry', 'Blueberry', 'Strawberry', 'Cherry', 'Cherry', 'Fruit Mix', 'Pineapple', 'Mango', 'Tropical Fruit Mix', 'Plum']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([779659, 2007234, 2007235, 2007239, 2114249]), 'item_count'] = 2
df.loc[df['material_id'].isin([893214, 1505893]), 'item_count'] = 3
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Spices

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PULSES & SPICES & HERBS', 'SPICES')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [300, 40, 227, 32, 35, 35, 100, 200, 200, 100, 100, 200, 200, 200, 100, 100, 200, 100, 100, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 500, 500, 500, 500, 100, 100, 200, 200, 200, 100, 100, 100, 100, 300, 500, 200, 100, 100, 100, 200, 100, 100, 100, 500, 100, 100, 1, 100, 100, 100, 100, 200, 200, 100, 100, 100, 100, 200, 500, 100, 100, 200, 200, 200, 200, 50, 200, 500, 200, 100, 200, 500, 200, 500, 200, 500, 200, 200, 200, 100, 200, 200, 200, 200, 100, 250, 100, 100, 200, 200, 200, 200, 200, 200, 100, 200, 200, 100, 100, 500, 200, 170, 150, 100, 100, 100, 200, 200, 100, 200, 100, 200, 100, 100, 200, 200, 100, 100, 200, 400, 200, 50, 1, 1, 500, 500, 200, 100, 200, 500, 200, 200, 500, 500, 1, 200, 250, 33, 33, 45, 34, 51, 51, 44, 58, 46, 38, 55, 200, 500, 200, 200, 100, 80, 100, 500, 250, 250, 250, 250, 250, 220, 250, 250, 220, 220, 250, 250, 250, 250, 250, 250, 250, 220, 150, 100, 250, 200, 454, 200, 200, 150, 155, 185, 330, 330, 220, 220, 200, 200, 200, 7, 1, 29, 43, 73, 38, 36, 3, 10, 250, 7, 20, 26, 48, 13, 22, 26, 35, 11, 35, 28, 40, 10, 25, 50, 400, 400, 42, 500, 1, 1, 220, 250, 250, 250, 100, 500, 250, 500, 250, 250, 250, 100, 200, 100, 100, 100, 200, 200, 200, 320, 100, 11, 200, 200, 100, 100, 200, 200, 200, 100, 200, 100, 200, 200, 500, 31, 200, 200, 200, 200, 200, 200, 200, 200, 200, 38, 200, 100, 80, 42, 200, 200, 200, 200, 200, 200, 200, 200, 200, 100, 200, 200, 100, 100, 50, 50, 100, 100, 400, 400, 400, 100, 100, 200, 200, 200, 200, 200, 200, 200, 45, 28, 50, 50, 100, 100, 50, 250, 220, 250, 36, 20, 60, 50, 65, 20, 111, 40, 21, 30, 4, 100, 100, 300, 250, 500, 500, 500, 750, 380, 380, 200, 350, 180, 180, 180, 180, 750, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 1, 500, 150, 180, 100, 100, 35, 400, 200, 400, 200, 400, 200, 400, 200, 100, 100, 100, 57, 160, 145, 380, 750, 900, 150, 135, 190, 155, 200, 135, 200, 165, 200, 200, 200, 200, 200, 200, 200, 100, 100, 250, 100, 200, 100, 200, 350, 750, 200, 200, 170, 80, 100, 30, 150, 150, 140, 160, 160, 8, 500, 200, 45, 40, 300, 100, 100, 100, 100, 100, 100, 95, 200, 500, 650, 11, 39, 37, 50, 100, 95, 40, 28, 70, 39, 70, 40, 38, 39, 58, 42, 50, 200, 500, 485, 485, 100, 100, 100, 180, 250, 250, 330, 200, 500, 200, 100, 100, 100, 100, 200, 100, 200, 200, 100, 1, 1, 1, 100, 85, 80, 200, 500, 200, 200, 100, 100, 100, 125, 22, 30, 36, 100, 100, 20, 20, 100, 100, 200, 100, 100, 50, 100, 100, 100, 100, 200, 200, 250, 400, 100, 485, 485, 200, 200, 200, 40, 40, 30, 30, 15, 40, 50, 485, 400, 400, 100, 100, 100, 100, 100, 200, 200, 250, 200, 250, 250, 250, 250, 150, 150, 250, 250, 250, 200, 350, 200, 125, 225, 275, 80, 475, 475, 250, 475, 425, 475, 630, 105, 160, 150, 135, 140, 160, 135, 160, 170, 150, 150, 150, 170, 150, 150, 130, 160, 130, 150, 160, 150, 130, 227, 200, 250, 220, 200, 200, 230, 220, 200, 200, 250, 100, 200, 170, 160, 160, 40, 170, 320, 225, 225, 240, 240, 180, 100, 200, 450, 250, 100, 400, 400, 400, 90, 90, 1, 100, 100, 150, 100, 200, 200, 150, 227, 400, 400, 250, 250, 250, 45, 45, 45, 45, 300, 250, 320, 200, 200, 150, 70, 24, 220, 195, 220, 215, 180, 200, 200, 200, 200, 200, 425, 425, 3, 35, 45, 45, 40, 50, 20, 40, 40, 180, 200, 75, 75, 75, 100, 100, 50, 100, 100, 100, 100, 200, 200, 200, 200, 200, 100, 150, 150, 150, 100, 250, 100, 250, 100, 250, 250, 100, 200, 1, 400, 600, 250, 100, 115, 145, 130, 165, 120, 130, 200, 320, 100, 100, 50, 50, 400, 100, 200]

units = ["G", "G", "G", "G", "G", "G", "ML", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "ML", "G", "G", "ML", "ML", "ML", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "ML", "G", "G", "G", "G", "G", "G", "ML", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "KG", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G"]

type = ["Tamarind Paste", "Cinnamon Powder", "Tamarind Paste", "Nutmeg Powder", "Chilli Powder", "Paprika Powder", "Garlic Granules", "Aniseeds", "Caraway Seeds", "Cardamom Whole", "Cardamom Whole", "Cardamom Powder", "Chilli Crushed", "Chilli Powder", "Chilli Whole", "Chilli Whole", "Cinnamon Powder", "Cinnamon Whole", "Cloves Whole", "Coriander Powder", "Coriander Whole", "Cumin Powder", "Cumin Powder", "Fenugreek Powder", "Garlic Powder", "Ginger Powder", "Mustard Seeds", "Paprika Powder", "Sesame Seeds", "Sumac", "Turmeric Powder", "Zaatar", "Chilli Powder", "Coriander Powder", "Cumin Powder", "Turmeric Powder", "Paprika Powder", "Cinnamon Powder", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Turmeric Powder", "Ajwain", "Garlic Powder", "Chilli Powder", "Zaatar", "Coriander Powder", "Fennel Seeds", "Pepper Powder", "Ajwain", "Pomegranate", "Arrowroot Powder", "Sesame Seeds", "Chilli Whole", "Chilli Powder", "Sago Seeds", "Mukhwas", "Mukhwas", "Tamarind Whole", "Asafoetida Compounded", "Asafoetida Powder", "Asafoetida Compounded", "Asafoetida Powder", "Garam Masala Whole", "Mustard Powder", "Black Cumin Seeds", "Caraway Seeds", "Sesame Seeds", "Turmeric Whole", "Fennel Powder", "Mash Powder", "Mustard Dal", "Chilli Whole", "Mango Powder", "Ensoon Powder", "Karawia Powder", "Ensoon Seeds", "Camomil", "Lemon Powder", "Tamarind Whole", "Cumin Powder", "Cardamom Whole", "Ginger Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Coriander Powder", "Turmeric Powder", "Turmeric Powder", "Cumin Powder", "Chilli Whole", "Cumin Powder", "Pepper Powder", "Ginger Whole", "Chilli Whole", "Pepper Powder", "Cardamom Whole", "Cloves Whole", "Cloves Whole", "Cinnamon Whole", "Fenugreek Seeds", "Coriander Whole", "Mustard Seeds", "Fennel Seeds", "Sesame Seeds", "Cinnamon Powder", "Ajwain", "Chilli Powder", "Peppercorn", "Nutmeg Whole", "Mukhwas", "Tamarind Whole", "Mukhwas", "Fennel Shahi/Saunf", "Mukhwas", "Lemon Powder", "Mango Powder", "Jaljeera", "Cardamom Powder", "Garlic Powder", "Pepper Powder", "Chilli Crushed", "Cloves Powder", "Turmeric Whole", "Pepper Powder", "Caraway Seeds", "Chilli Crushed", "Ajwain", "Garlic Powder", "Chilli Whole", "Chilli Powder", "Green Chilli", "Chilli Crushed", "Star Seeds", "Coriander Powder", "Tamarind Whole", "Tamarind Whole", "Tamarind Whole", "Mixed Spices", "Tamarind Whole", "Chilli Powder", "Chilli Powder", "Turmeric Powder", "Coriander Powder", "Coriander Powder", "Chilli Powder", "Chilli Powder", "Tamarind Whole", "Turmeric Powder", "Pepper Powder", "Pepper Coarse", "Pepper Powder", "Pepper Powder", "Chicken Seasoning", "Jamaican Jerk Seasoning", "Cajun Seasoning", "Mixed Spices", "Steak Seasoning", "Lamb Seasoning", "Fish Seasoning", "Tamarind Whole", "Tamarind Whole", "Chilli Crushed", "Chilli Powder", "Turmeric Powder", "Cardamom Powder", "Ginger Powder", "Cinnamon Whole", "Sumac", "BBQ Spices", "Cloves Powder", "Ginger Powder", "Cinnamon Whole", "Cardamom Powder", "Garlic Powder", "Chilli Powder", "Cinnamon Powder", "Coriander Powder", "Cumin Powder", "Turmeric Powder", "Nutmeg Powder", "Zaatar", "Coriander Whole", "Cardamom Whole", "Lemon Powder", "Paprika Powder", "Mustard Seeds", "Asafoetida Powder", "Black Cumin Seeds", "Cardamom Whole", "Mustard Powder", "Assorted Spices", "Kokam", "Chilli Powder", "Cumin Powder", "Turmeric Powder", "Coriander Powder", "Cardamom Whole", "Garlic Powder", "Lime Powder", "Meat Spices", "Chicken Spices", "Cajun Spices", "Coriander Whole", "Chives", "Chilli Crushed", "Garlic Italian Seasoning", "Garlic Salt", "Chilli Powder", "Lasagne Mix", "Parsley", "Sage", "Sesame Seeds", "Oregano", "Coriander Seeds", "Chilli Pepper", "Mustard Seeds", "Cinnamon Whole", "Cloves Whole", "Cardamom Whole", "Cumin Powder", "Thyme", "Fenugreek Powder", "Fennel Seeds", "Garlic Grinder", "Basil", "Nutmeg Whole", "Peppercorn", "Chilli Powder", "Turmeric Powder", "Cajun Seasoning", "Turmeric Powder", "Coriander Powder", "Chilli Powder", "Pasta Spices", "Black Cumin Powder", "Onion Powder", "Black Cumin Seeds", "Onion Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Coriander Powder", "Turmeric Powder", "Shawarma Spice", "Mixed Spices", "Pizza Spices", "Mustard Powder", "Zaatar", "Cardamom Powder", "Chilli Powder", "Coriander Powder", "Kudampuli", "Tamarind Paste", "Aniseeds", "Chilli Whole", "Sumac", "Mixed Spices", "Cardamom Whole", "Cinnamon Sticks", "Cumin Powder", "Mustard Seeds", "Fenugreek Seeds", "Coriander Whole", "Fennel Seeds", "Cardamom Whole", "Peppercorn", "Tamarind Whole", "Tamarind Whole", "Pumpkin Pie Spices", "Habbat Al Hamra", "Mixed Spices", "Black Salt Powder", "Nutmeg Powder", "Sesame Seeds", "Fennel Powder", "Fenugreek Powder", "Garlic Powder", "Aniseeds", "Shepherd's Pie Mix", "Paprika Powder", "Cloves Whole", "Cinnamon Whole", "Italian Seasoning", "Chilli Powder", "Chilli Powder", "Mustard Seeds", "Garlic Powder", "Turmeric Powder", "Lemon Dry", "Coriander Whole", "Coriander Powder", "Chilli Crushed", "Chilli Whole", "Cinnamon Powder", "Fenugreek Seeds", "Garam Masala Powder", "Pepper Powder", "Garam Masala Whole", "Cardamom Whole", "Fennel Seeds", "Cumin Seeds", "Coriander Powder", "Chilli Powder", "Turmeric Powder", "Peppercorn", "Pepper Powder", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Coriander Powder", "Chilli Powder", "Chilli Powder", "Turmeric Powder", "Pepper Coarse", "Cinnamon Powder", "Chilli Whole", "Cinnamon Whole", "Cumin Powder", "Ginger Powder", "Cloves Whole", "Mixed Spices", "Lemon Powder", "Falafel Spices", "Chilli Flakes", "Steak Spices", "Kabsa Spices", "Mandi Spices", "Cajun Seasoning", "Peppercorn", "Peppercorn", "Celery Seeds", "Peppercorn", "Allspice Berries", "Mace", "Fenugreek Seeds", "Coriander Whole", "Tamarind Whole", "Mango Powder", "Chilli Powder", "Coriander Powder", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Turmeric Powder", "Fennel Powder", "Fenugreek Powder", "Chilli Powder", "Turmeric Powder", "Chilli Powder", "Coriander Powder", "Chilli Powder", "Arabic Spices", "Margarine Spices", "BBQ Spices", "Natural Dye Spices", "Maternal Post Spices", "Biryani Spice", "Fish Spices", "Curry Powder", "Seafood Spices", "Chicken Spices", "Tamarind Seedless", "Tamarind Seedless", "Chilli Whole", "Assorted Spices", "Chilli Powder", "Mustard Powder", "Peppercorn", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Coriander Powder", "Turmeric Powder", "Turmeric Powder", "Mixed Spices", "Aniseeds", "Asafoetida Compounded", "Lemon & Cracked Pepper Seasoning", "Tomato Rice Powder", "Mixed Spices", "Coriander Powder", "Coriander Powder", "Tamarind Whole", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Cumin Powder", "Pepper Powder", "Ginger Powder", "Pepper Powder", "Mango Powder", "Chilli Crushed", "Peppercorn", "Coriander Whole", "Pepper Powder", "Turmeric Powder", "Cumin Powder", "Garlic Powder", "Peppercorn", "Cinnamon Whole", "Tamarind Paste", "Cinnamon Whole", "Turmeric Powder", "Chilli Whole", "Chilli Powder", "Chilli Crushed", "Turmeric Powder", "Mustard Seeds", "Fenugreek Seeds", "Peppercorn", "Coriander Whole", "Cloves Whole", "Chilli Whole", "Pepper Powder", "Chilli Powder", "Coriander Powder", "Cumin Powder", "Chilli Powder", "Cardamom Whole", "Mixed Spices", "Chilli Powder", "Paprika Smoked", "Paprika Smoked", "Tamarind Paste", "Mustard Seeds", "Mandi Spices", "Shawarma Spice", "Shish Tawook Spices", "Salad Spices", "AlQoosi Spices", "Sumac", "Tamarind Whole", "Tamarind Whole", "Sumac", "Mixed Herbs", "Cinnamon Powder", "Turmeric Powder", "Garlic Granules", "Asafoetida Powder", "Cheddar Cheese Spices", "Peppercorn", "Peppercorns", "Garlic Crushed", "Cumin Powder", "Garlic Flakes", "Pepper Powder", "Chilli Crushed", "Ginger Powder", "Turmeric Powder", "Cinnamon Powder", "Asafoetida Powder", "Black Seeds", "Tamarind Whole", "Tamarind Paste", "Tamarind Sauce", "Coriander Powder", "Chilli Powder", "Turmeric Powder", "Chilli Crushed", "Salad Spices", "Shawarma Spice", "Pepper Powder", "Tamarind Whole", "Tamarind Whole", "Coriander Powder", "Fenugreek Seeds", "Kabab Spice", "Kebbeh Spice", "Shawarma Spice", "Cumin Powder", "Cardamom Whole", "Peppercorn", "Fenugreek Seeds", "Cloves Whole", "Coriander Powder", "Chilli Powder", "Tamarind Whole", "Peppercorn", "Peppercorn", "Paprika Smoked", "Tamarind Whole", "Tamarind Whole", "Kudampuli", "Tamarind Seedless", "Shawarma Spices", "Mixed Spices", "Mixed Spices", "BBQ Spices", "Thyme", "Basil", "Rosemary", "Chicken Seasoning", "Majboos Spices", "Cinnamon Powder", "Nutmeg Powder", "Chicken Spices", "Chicken Spices", "Mustard Seeds", "Cumin Powder", "Ajwain", "Cloves Whole", "Coriander Seeds", "Cumin Powder", "Fennel Seeds", "Sesame Seeds", "BBQ Spices", "Onion Powder", "Paprika Powder", "Chilli Powder", "Asafoetida Powder", "Tamarind Paste", "Tamarind Paste", "Aniseeds", "Cumin Powder", "Sesame Seeds", "Nutmeg Powder", "Cloves Powder", "Cinnamon Powder", "Pepper Powder", "Cinnamon Whole", "Paprika Powder", "Cinnamon Whole", "Tamarind Paste", "Sago Seeds", "Sago Seeds", "Chilli Powder", "Chilli Powder", "Cloves Powder", "Black Cumin Seeds", "Paprika Powder", "Zaatar", "Sumac", "Onion Powder", "Paprika Powder", "Mixed Spices", "Mixed Spices", "Dokka Powder", "BBQ Spices", "Seafood Spices", "Chicken Spices", "Meat Spices", "Potato Spices", "Kebda Spices", "Kabsa Spices", "Fenugreek Seeds", "Fennel Seeds", "Coriander Whole", "Cumin Powder", "Turmeric Powder", "Chilli Whole", "Chilli Powder", "Chilli Powder", "Coriander Whole", "Coriander Powder", "Cumin Powder", "Cumin Powder", "Turmeric Powder", "Chilli Whole", "Chilli Powder", "Mixed Spices", "Mixed Spices", "Shawarma Spices", "Pepper Powder", "Coriander Powder", "Turmeric Powder", "Pepper Powder", "Kabsa Spices", "Stew Spice Mix", "Seafood Spices", "BBQ Spices", "Biryani Spices", "Mixed Spices", "Cardamom Powder", "Chilli Powder", "Ginger Powder", "Cumin Powder", "Curry Powder", "Cloves Powder", "Cinnamon Powder", "Tamarind Paste", "Lemon Powder", "Chicken Seasoning", "Garlic Powder", "Ginger Powder", "Paprika Powder", "Mixed Spices", "Sumac", "Zaatar", "Cardamom Whole", "Shawarma Spice", "Paprika Powder", "Garlic Powder", "Onion Powder", "Paprika Powder", "Paprika Powder", "Chilli Flakes", "Nutmeg Powder", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Cumin Powder", "Chilli Powder", "Garam Masala Whole", "Chilli Powder", "Chilli Paste", "Masala Tikki", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Ginger Powder", "Mango Powder", "Chilli Powder", "Chilli Powder", "Cardamom Whole", "Pepper Powder", "Lemon Dry", "Chilli Powder", "Chilli Powder", "Onion Powder", "Tamarind Paste", "Coriander Powder", "Lime Powder", "Onion Powder", "Chicken Spices", "Prawn Spices", "Truffle Spices", "Truffle Cheese Spices", "Truffle Pesto Spices", "Truffle Porcini Spices", "Tamarind Whole", "Kabsa Spices", "Chilli Powder", "Tamarind Sauce", "Tamarind Sauce", "Turmeric Powder", "Mixed Spices", "Cinnamon Whole", "Chicken BBQ Seasoning", "Pizza Seasoning", "Chips & Potato Seasoning", "Chicken Wings Seasoning", "Roast Veggies Seasoning", "Chilli Powder", "Turmeric Powder", "Coriander Powder", "Turmeric Powder", "Mustard Seeds", "Chicken Broasted Mix", "Chicken Broasted Mix", "Basil", "Chilli Flakes", "Cinnamon Powder", "Garlic Crunchy", "Garlic Powder", "Nutmeg Powder", "Oregano", "Paprika Smoked", "Onion Powder", "Tamarind Seedless", "Fenugreek Seeds", "Chilli Flakes", "BBQ Seasoning", "Chilli Flakes", "BBQ Spices", "Argentinian Grill Spices", "Asafoetida Powder", "Karam Podi", "Karam Podi", "Kandi Podi", "Karam Podi", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Chilli Powder", "Turmeric Powder", "Cinnamon Powder", "Fenugreek Seeds", "Mustard Seeds", "Mustard Seeds", "Cardamom Whole", "Cardamom Whole", "Cardamom Whole", "Cardamom Whole", "Chilli Flakes", "Paprika Powder", "Paprika Smoked", "Chilli Whole", "Lemon Dry", "Mixed Spices", "Biryani Spices", "Mixed Spices", "Cardamom Whole", "Coriander Powder", "Garlic Powder", "Paprika Smoked", "Onion Powder", "Turmeric Powder", "Pepper Powder", "Cardamom Powder", "Coriander Seeds", "Tamarind Paste", "Piccantissimo", "Arrabbiata Spices", "Asafoetida Powder", "Asafoetida Compounded", "Chilli Powder", "Cloves Whole", "Kudampuli"]

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([1576179, 1992982, 2014666, 2269147]), 'item_count'] = 2
df.loc[df['material_id'].isin([1104111, 2135654]), 'item_count'] = 3
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Ice Cream Impulse

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'ICE CREAM & DESSERTS', 'ICE CREAM IMPULSE')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [120, 120, 120, 100, 70, 50, 41.8, 110, 1, 125, 125, 125, 70, 65, 125, 110, 125, 125, 125, 125, 48, 110, 70, 100, 100, 110, 110, 1, 125, 140, 180, 90, 120, 120, 120, 125, 120, 120, 120, 120, 80, 100, 100, 100, 100, 100, 95, 95, 120, 100, 60, 80, 120, 125, 125, 54.6, 125, 110, 120, 120, 170, 70, 70, 70, 100, 100, 100, 105, 110, 125, 90, 110, 100, 100, 145, 95, 100, 120, 65, 150, 90, 120, 65, 105, 39.1, 50, 80, 80, 100, 438, 110, 110, 80, 80, 631, 90, 80, 140, 90, 110, 110, 110, 750, 100, 350, 1, 100, 95, 95, 125, 73.5, 140, 360, 95, 90, 125, 73, 120, 120, 258, 258, 258, 100, 2, 80, 258, 75, 258, 258, 90, 120, 48, 48, 48, 80, 34.5, 95, 140, 65, 110, 65, 150, 150, 150, 270, 258, 750, 60, 110, 110, 120, 140, 250, 464, 292, 260, 130, 100, 110, 70, 70, 70, 70, 58, 62, 125, 90, 125, 270, 270, 270, 270, 356, 110, 110, 135, 258, 258, 258, 120, 120, 120, 60, 90, 58, 58, 58, 460, 750, 500, 260, 480, 60, 443, 443, 443, 443, 118, 80, 81, 1.5, 878, 70, 70, 150, 40.8, 180, 70, 80, 120, 120, 60, 60, 180, 150, 150, 150, 90, 258, 210, 210, 210, 156, 156, 156, 156, 85, 85, 325, 325, 60, 60, 260, 55, 73, 325, 130, 60, 90, 90, 90, 100, 90, 90, 80, 65, 65, 60, 90, 180, 156, 156, 156, 210, 210, 210, 32, 38, 50, 47, 50, 210, 39, 90, 105, 105, 210, 360, 360, 85, 85, 353, 100, 300, 300, 480, 480, 240, 35, 35, 50, 35, 35, 35, 35, 250, 105, 105, 120, 120, 120, 120, 120, 120, 140, 65]

units = ["ML", "ML", "ML", "ML", "ML", "G", "G", "ML", "L", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "ML", "G", "ML", "ML", "ML", "ML", "KG", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "KG", "ML", "ML", "ML", "ML", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "G", "G", "ML", "KG", "ML", "G", "ML", "G", "G", "ML", "ML", "G", "G", "G", "ML", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "ML", "ML", "ML", "G", "G", "G", "G", "G", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "G", "G", "ML", "ML", "ML", "ML", "ML", "G", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "L", "ML", "ML", "ML", "ML", "G", "ML", "G", "ML", "ML", "ML", "G", "G", "ML", "ML", "ML", "ML", "ML", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "ML", "ML", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "ML", "ML", "ML", "ML", "ML", "ML", "ML"]

type = ["Vanilla Tub", "Chocolate Tub", "Strawberry Tub", "Vanilla Almond Stick", "Water Ice", "Vanilla Bar", "Chocolate Bar", "Vanilla & Chocolate Cone", "Vanilla Cookies Tub", "Vanilla Cookies Tub", "Strawberry Tub", "Praline & Cream Tub", "Vanilla Sick", "Pista Kulfi", "Strawberry & Vanilla Cup", "Pista Cone", "Vanilla Cup", "Strawberry Cup", "Mango Cup", "Chocolate Cup", "Peanut Buttter Bar", "Vanilla Cone", "Chocolate Cone", "Vanilla Cone", "Vanilla & Chocolate Cone", "Vanilla Almond Stick", "Vanilla & Chocolate Stick", "Ice Cubes", "Chocolate Tub", "Pista & Cashew Kulfi", "Vanilla Sundae Cup", "Vanilla Sandwich", "Chocolate Cone", "Strawberry Cone", "Praline & Cream Cone", "Chocolate Cone", "Chocolate Cone", "Vanilla Almond Stick", "Vanilla Peanuts Stick", "Vanilla Cashew Stick", "Vanilla Cookies Stick", "Chocolate Tub", "Macadamia Nut Tub", "Vanilla Cup", "Vanilla Cookies Tub", "Strawberry Cheesecake Tub", "Vanilla Almond Stick", "Chocolate Almond Stick", "Butterscotch Cone", "Strawberry Cup", "Strawberry & Chocolate M&M Stick", "Vanilla Macadamia Nut Stick", "Chocolate Cone", "Butterscotch Cashew Tub", "Vanilla Tub", "Vanilla & Chocolate Bar", "Tiramisu Tub", "Vanilla Almond Stick", "Chocolate Brownie Yoghurt", "Chocolate Cookies Dough", "Praline & Cream Cup", "Almond & Pista Kulfi", "Pista Kulfi", "Mango Kulfi", "Vanilla Stick", "Vanilla Almond Stick", "Vanilla White Chocolate Stick", "Orange Lolly", "Vanilla & Chocolate & Hazelnut Stick", "Vanilla & Chocolate Cone", "Milk Chocolate & Peanut Stick", "Chocolate & Hazenut Stick", "Vanilla & Cocoa Cup", "Vanilla & Strawberry Cup", "Vanilla & Chocolate Caramel Sandwich", "Chocolate Stick", "Salted Caramel Tub", "Vanilla & Chocolate Cone", "Pista & Cashew Kulfi", "Chocolate Cupcake", "Vanilla Sandwich", "Vanilla Almond Stick", "Fruit Lolly", "Orange Lolly", "Coconut & Milk Bar", "Vanilla & Caramel Bar", "Chocolate Brownie Stick", "White Chocolate & Almond Stick", "Vanilla Stick", "Fruit Lolly", "Oreo Stick", "Oreo Cone", "Vanilla & Caramel Almond Stick", "Chocolate Almond Stick", "Caramel Strawberry & Honeycomb Stick", "Vanilla Sandwich", "Salted Caramel Stick", "Vanilla Caramel Cone", "Chocolate Stick", "Vanilla Cone", "Vanilla & Chocolate Cone", "Pista Kulfi", "Buco Salad Tub", "Butterscotch Stick", "Fruit Lolly", "Ice Cubes", "Vanilla & Cookie Sandwich", "White Chocolate & Almond Stick", "Vanilla & Caramel Stick", "Vanilla Cone", "Peanut Butter & Chocolate Stick", "Vanilla Oreo Cup", "Chocolate & Blackberry & Black Mulberry Stick", "Mulberry & Blackberry Stick", "Vanilla & Chocolate Cone", "Chocolate Cone", "Watermelon Lolly", "Coconut Cone", "Cotton Candy Cone", "Green Tea Dough", "Strawberry Dough", "Mango Dough", "Vanilla Sandwich", "Ice Cubes", "Peanut Butter Chocolate Stick", "Chocolate Dough", "Mango & Raspberry Stick", "Coffee & Caramel Dough", "Chocolate & Marshmallow Dough", "Chocolate Stick", "Cookies Cone", "Kewda Kulfi", "Elaichi Kulfi", "Kesar Kulfi", "Chocolate Almond Stick", "Peanuts & Caramel Bar", "White Chocolate & Cookies Stick", "Pista Cone", "Vanilla Stick", "Vanilla & Cocoa Stick", "Cocoa Stick", "Cotton Candy Cup", "Mango Cup", "Raspberry Cup", "Vanilla Stick", "Cookies Dough", "Pista & Cashew Tub", "Kulfi", "Vanilla & Chocolate Cone", "Vanilla & Chocolate Cone", "Fruit Cone", "Chocolate Almond Balls", "Bubblegum Lolly", "Fruit Lolly", "Orange Lolly", "Strawberry Cone", "Rose Cone", "Honeycomb & Milk Chocolate Stick", "Milk Chocolate & Caramel Cone", "Mango Lolly", "Raspberry Lolly", "Strawberry Lolly", "Fruit Stick", "Chocolate & Hazelnut Stick", "Chocolate Stick", "Cotton Candy Cup", "Vanilla & Chocolate Tub", "Butterscotch Cup", "Chocolate & Pecan Stick", "Coconut & Mango Stick", "Dark Chocolate & Berry Stick", "Salted Caramel & Macadamia Stick", "Chocolate Bar", "Chocolate Caramel Cone", "Chocolate & Hazelnut Cone", "Oreo Chocolate Sandwich", "Horchata Dough", "Guava Dough", "Vanilla Dough", "Chocolate Cone", "Chocolate Peanut Cone", "Cone", "Mango Stick", "Berry & White Chocolate Stick", "Chocolate Almond Stick", "Vanilla Stick", "Fruit Lolly", "Chocolate Tub", "Fruit Stick", "Lolly", "Fruit Lolly", "Fruit Lolly", "Mango Stick", "Cherry & Grape Popsicle", "Lime & Orange Popsicle", "Strawberry & Lemon Popsicle", "Strawberry & Mango Popsicle", "Fruit Punch & Cotton Candy Popsicle", "Blue Raspberry & Cherry & Lemon Popsicle", "Orange Popsicle", "Orange & Cherry & Grape Popsicle", "Chocolate Popsicle", "Chocolate & Hazelnut Stick", "Coconut Stick", "Chocolate Cup", "White Chocolate Bar & Peanut", "Goat Milk Cup", "Raspberry Cheesecake Cup", "Vanilla & Chocolate Bar", "Strawbery Stick", "Vanilla Stick", "Pastry", "Vanilla Sandwich", "Safron Chocolate Milk Sandwich", "Strawberry Sandwich", "Red Bean Sandwich", "Chocolate Sandwich", "Chocolate Stick", "Coffee Dough", "Ube Dough", "Strawberry Dough", "Passion Fruit Dough", "Fruit Dough", "Green Tea Dough", "Mango Dough", "Strawberry Dough", "Caramel & Popcorn Stick", "Mango & Coconut Stick", "Peanut Butter Stick", "Salted Caramel Stick", "Chocolate & Peanut Bar", "Chocolate Brownie Bar", "Fruit Lolly", "Oreo Sandwich", "Watermelon Popsicle", "Chocolate Stick", "Pista Cone", "Chocolate Stick", "Chocolate & Cookie Stick", "Chocolate & Caramel Cone", "Chocolate & Peanut Butter Stick", "Vanilla & Peanut Butter Cone", "White Chocolate & Hazelnut Cone", "Chocolate Cone", "Chocolate & Caramel Stick", "Fruit Lolly", "Fruit Lolly", "Oreo Stick", "Oreo Stick", "Chocolate & Vanilla Sundae Cup", "Coconut Dough", "Vanilla Dough", "Chocolate Dough", "Pumpkin Spice Dough", "Apple Dough", "Peppermint Dough", "Strawberry Popsicle", "Lolly", "Cherry & Lemon & Cola Stick", "Bubblegum & Popping Candy Coating Lolly", "Raspberry & Lime & Black Currant Lolly", "Cereal & Milk Dough", "Chocolate & Peanut Bar", "Coconut Stick", "Raspberry Popsicle", "Orange Popsicle", "Chocolate Dough", "Strawberry & Blue Raspberry Popsicle", "Fruit Punch & Cotton Candy Popsicle", "Pink Lemonade Stick", "Chocolate Stick", "Chocolate & Almond Stick", "Coconut Stick", "Vanilla Sandwich", "Butterscotch Sandwich", "Almond & Pista & Kesar Kulfi", "Malai Kulfi", "Strawberry Stick", "Vanilla Stick", "Vanilla Barry", "Vanilla & Chocolate & Hazelnut Sandwich", "Chocolate Kuaky", "Vanilla Firky", "Vanilla Vacky", "Vanilla Punky", "Strawberry & Cream Cone", "Strawberry Pushup", "Strawberry Cone", "Mango Tub", "Choco Nuts Tub", "Strawberry Tub", "Vanilla Praline Tub", "Blueberry Tub", "Baklava Tub", "Chocolate & Almond Sandwich", "Blueberry & Vanilla Stick"]

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([994112, 1804310, 1837080, 1846899, 1846900, 1846901, 2001320, 2109920, 2109935, 2124661, 2125648, 2125734, 2128035]), 'item_count'] = 4
df.loc[df['material_id'].isin([1846923, 2176364, 2176366, 2176369]), 'item_count'] = 5
df.loc[df['material_id'].isin([1093646, 1164552, 1446262, 1459533, 2076961, 2139305, 2176365, 2176367, 2181900]), 'item_count'] = 6
df.loc[df['material_id'].isin([2001362]), 'item_count'] = 8
df.loc[df['material_id'].isin([2001319]), 'item_count'] = 10
df.loc[df['material_id'].isin([2302917]), 'item_count'] = 12
df.loc[df['material_id'].isin([2007875]), 'item_count'] = 18
df.loc[df['material_id'].isin([2007872]), 'item_count'] = 32
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Pickles

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'SAUCES & PICKLES', 'PICKLES')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 400, 400, 400, 380, 330, 330, 330, 330, 330, 400, 400, 400, 400, 400, 400, 400, 400, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 350, 300, 32, 1, 1500, 700, 330, 700, 700, 700, 700, 700, 1, 320, 340, 300, 300, 300, 340, 400, 473, 473, 360, 400, 330, 1, 300, 300, 16, 350, 400, 400, 400, 400, 400, 400, 16, 400, 340, 340, 300, 400, 32, 400, 400, 600, 600, 600, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 520, 312, 283, 283, 283, 283, 283, 283, 215, 400, 400, 400, 400, 400, 360, 380, 300, 300, 320, 400, 500, 500, 500, 500, 300, 400, 400, 400, 400, 400, 710, 340, 350, 473, 24, 24, 12, 1, 600, 341, 700, 177, 177, 700, 1, 1, 1, 300, 1, 300, 300, 350, 350, 350, 350, 300, 300, 350, 1, 400, 320, 400, 680, 350, 400, 350, 680, 680, 400, 400, 473, 300, 12, 455, 400, 237, 6.5, 300, 520, 1, 1, 1, 400, 1, 700, 400, 9, 310, 400, 400, 400, 400, 400, 400, 473, 400, 230, 400, 105, 455, 9, 32, 120, 235, 235, 680, 700, 680, 680, 700, 710, 700, 680, 670, 660, 360, 310, 700, 235, 400, 400, 415, 250, 1, 400, 400, 400, 400, 160, 160, 640, 320, 330, 330, 330, 330, 250, 200, 290, 290, 290, 440, 340, 400, 400, 180, 180, 180, 340, 400, 400, 400, 400, 400, 400, 250, 500, 400, 700, 700, 400, 1, 500, 600, 600, 600, 290, 485, 400, 400, 300, 400, 300, 680, 680, 300, 600, 550, 560, 550, 540, 570, 500, 500, 680, 250, 250, 680, 680, 680, 600, 1.25, 750, 1.25, 750, 1.25, 750, 250, 250, 300, 700, 300, 300, 300, 300, 300, 330, 600, 680, 600, 600, 600, 300, 340, 180, 180, 180, 180, 600, 970, 970, 482, 482, 290, 150, 550, 300, 250, 710, 300, 300, 300, 660, 1.36, 9, 9, 9, 9, 350, 205, 210, 350, 980, 400, 400, 400, 400, 300, 300, 300, 300, 300, 300, 300, 660, 660, 660, 660, 300, 660, 660, 650]

units = ["G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "OZ", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "ML", "ML", "G", "G", "G", "KG", "G", "ML", "OZ", "G", "G", "G", "G", "G", "G", "G", "OZ", "G", "G", "G", "G", "G", "OZ", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "G", "G", "ML", "OZ", "OZ", "OZ", "KG", "G", "G", "G", "ML", "ML", "G", "KG", "KG", "KG", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "G", "OZ", "G", "G", "ML", "OZ", "G", "G", "KG", "KG", "KG", "G", "KG", "G", "G", "OZ", "G", "G", "G", "G", "G", "G", "G", "ML", "G", "G", "G", "G", "G", "OZ", "OZ", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "KG", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "L", "G", "L", "G", "L", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "ML", "G", "G", "G", "G", "L", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G", "G"]

type = ["Mango", "Mango", "Tomato", "Gongura", "Garlic", "Mixed Veg", "Ginger", "Green Chilli", "Lime", "Red Chilli", "Mango", "Mango", "Onion", "Mango", "Lemon", "Mixed Veg", "Chilli", "Mango", "Mango", "Mixed Veg", "Lemon", "Garlic", "Garlic", "Mango", "Gooseberry", "Lemon", "Mango", "Mixed Veg", "Bitter Gourd", "Ginger", "Mango", "Hot Mango", "Lime", "Lime Hot", "Mixed Veg", "Mango", "Mango", "Mango", "Mango", "Mango & Chilli", "Lime & Chilli", "Garlic", "Garlic", "Mango", "Carrot & Chilli", "Gherkin", "Cucumber", "Cucumber", "Cucumber", "Mixed Veg", "Mixed Veg", "Shallot", "Garlic", "Chilli", "Mixed Veg", "Mango", "Chilli", "Chilli", "Ginger", "Lime", "Chilli", "Jalapeno Sliced", "Mixed Veg", "Gherkin", "Hamburger Dill Chips", "Lemon", "Fish", "Lime", "Mixed Veg", "Mixed Veg", "Lime & Tomato", "Jalapeno Pepper Sliced", "Lime", "Mango", "Hot Lime", "Mango", "Garlic", "Mixed Veg", "Prawns", "Gherkin", "Lime", "Beetroot", "Beetroot Sliced", "Garlic", "Mango", "Hamburger Dill Chips", "Mango", "Mango", "Mixed Veg", "Cucumber", "Cucumber", "Prawns", "Lemon", "Lime", "Mango", "Mixed Veg", "Mango", "Garlic", "Dates", "Mango", "Lime", "Mango", "Lemon", "Eggplant", "Hot Mango", "Mixed Hot", "Hot Lime", "Hot Lime", "Hot Chilli", "Mango", "Jalapeno", "Mango", "Mango", "Lemon", "Mixed Veg", "Garlic", "Lemon", "Gherkin", "Mango", "Mixed Veg", "Lemon", "Ginger", "Lemon", "Mixed Veg", "Mixed Veg", "Mixed Veg", "Coriander", "Mango & Mixed Veg", "Mango", "Mango", "Lime", "Garlic", "Gherkin", "Beetroot Cut", "Gujarati Choondo", "Bread and Butter", "Gherkin", "Gherkin", "Jalapeno", "Cucumber", "Cucumber", "Mushroom", "Cucumber", "Green Pepper", "Red Pepper", "Tomato", "Mango", "Lemon", "Mixed Veg", "Assorted", "Cucumber", "Mango", "Mixed Veg", "Mango", "Tamarind & Ginger", "Lime", "Garlic", "Chilli", "Mixed Veg", "Lemon", "Cucumber", "Mango & Lime", "Garlic", "Chilli", "Cucumber", "Gherkin", "Garlic", "Cucumber", "Cucumber", "Cucumber", "Tamarind & Ginger", "Bilimbi", "Bread and Butter", "Mango", "Banana Pepper", "Beetroot", "Mango", "Olive", "Artichoke", "Mango", "Lemon", "Cucumber", "Mango", "Lemon", "Fish", "Cucumber", "Grape Leaves", "Mixed Spices", "Hot Cucumber", "Piccalilli", "Lime", "Garlic", "Mango", "Mango", "Fish", "Prawns", "Jalapeno Pepper Sliced", "Red Cabbage", "Artichoke", "Onion", "Pink Peppercorn", "Beetroot Sliced", "Gherkin", "Bread and Butter", "Caper", "Jalapeno Sliced", "Jalapeno Sliced", "Mixed Veg", "Jalapeno", "Gherkin", "Gherkin", "Mixed Veg", "Jalapeno", "Cucumber", "Rosemary & Hot Pepper", "Hot Pepper", "Pepper", "Sauerkraut", "Jalapeno", "Garlic", "Jalapeno Sliced", "Cherry Pepper", "Jalapeno", "Yellow Squash", "Artichoke", "Mixed Veg", "Mango", "Mango & Lime", "Mango & Lime", "Tamarind & Ginger", "Fresh Kimchi", "Cooked Kimchi", "Hot Pepper", "Chilli", "Mixed Veg", "Mango", "Lime", "Garlic", "Mango", "Eggplant", "Cherry Pepper", "Yellow Pepper", "Jalapeno", "Jalapeno Pepper Sliced", "Jalapeno Sliced", "Mango", "Mixed Veg", "Hot Pepper", "Hot Pepper", "Pepper", "Chilli", "Ginger", "Gooseberry", "Lime", "Mixed Veg", "Mango", "Mango", "Dates", "Kashmiri", "Mango", "Cucumber", "Cucumber", "Mango & Lime", "Mixed Veg", "Mixed Veg", "Artichoke", "Turnip", "Green Pepper", "Mushroom", "Garlic", "Lemon", "Mango", "Mango", "Jackfruit Seeds & Mango & Prawns", "Gooseberry", "Jalapeno Sliced", "Gherkin", "Amba Haldar", "Mixed Veg", "Tomato", "Red Pepper", "Hot Pepper", "Eggplant", "Jalapeno Pepper Sliced", "Mixed Veg", "Mango", "Garlic", "Garlic & Chilli", "Lemon & Pepper", "Mixed Veg", "Cucumber", "Cucumber", "Mango", "Mango & Lemon", "Mango", "Mango & Carrot", "Lemon", "Lemon", "Onion", "Tomato", "Mango & Carrot", "Mint", "Mixed Veg", "Black Olive", "Black Olive & Pepper", "Mango & Fenugreek", "Black Olive", "Black Olive & Pepper", "Green Olive & Carrot", "Turnip", "Cucumber", "Cucumber", "Eggplant", "Mixed Veg", "Mushroom", "Yellow Pepper", "Artichoke", "Cherry Pepper", "Cherry Pepper", "Tomato", "Green Pepper", "Gherkins", "Gherkins", "Jalapeno Sliced", "Red Pepper", "Garlic", "Gherkin", "Gherkins", "Gherkins", "Garlic", "Gherkin", "Coriander", "Mint", "Mango", "Cucumber", "Gherkin", "Gherkin", "Gherkins", "Salsa Picante", "Garlic", "Jalapeno Sliced", "Jalapeno Sliced", "Jalapeno", "Paprika Sliced", "Hamburger Dill Chips", "Mixed Veg", "Lemon", "Ginger", "Gooseberry", "Lime", "Mango", "Mango", "Garlic", "Mixed Veg", "Carrot", "Ginger", "Cucumber", "Chilli", "Hot Pepper", "Mixed Veg", "Ginger", "Jalapeno", "Makedon", "Cucumber"]

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([233759, 475959, 536969, 656541, 679330, 826762, 991520, 1869279, 2053746, 2059874, 2080675]), 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Cakes & Gateaux

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'ICE CREAM & DESSERTS', 'CAKES & GATEAUX')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [510, 510, 510, 400, 300, 150, 150, 485, 510, 510, 510, 400, 240, 255, 300, 600, 453, 180, 350, 100, 350, 350, 500, 390, 555, 397, 297, 297, 170, 170, 170, 170, 397, 400, 425, 800, 800, 100, 100, 160, 170, 180, 630, 540, 540, 540, 240, 240, 180, 85, 350, 550, 550]

units = ['G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

type = ['Chocolate Cake', 'Vanilla Cake', 'Chocolate & Vanilla Cake', 'Chocolate & Caramel Cake', 'Jam Cake', 'Munggo Cake', 'Ube Cake', 'Strawberry Cheesecake', 'Chocolate Cake', 'Chocolate & Caramel Cake', 'Vanilla Cake', 'Chocolate & Almond Cake', 'Pancake', 'Pancake', 'Toaster Cake', 'Crepe', 'Butter Pound Cake', 'Chocolate Cake', 'Cheesecake', 'Cheesecake', 'Chocolate Cake', 'Cheesecake', 'Milk Cake', 'Raspberry Cheesecake', 'Chocolate Cake', 'Chocolate Donut', 'Glazed Chocolate Donut', 'Powdered Sugar Donut', 'Cinnamon Donut', 'Glazed Donut', 'Powdered Sugar Donut', 'Glazed Chocolate Donut', 'Glazed Donut', 'Chocolate & Hazelnut Cake', 'Cheesecake', 'Praline Ice Cream Cake', 'Vanilla Ice Cream Cake', 'Strawberry Cheesecake', 'Cherry Cheesecake', 'Cheesecake', 'Cheesecake', 'Chocolate Cake', 'Crepe', 'Chocolate Crepe', 'Apple & Cinnamon Crepe', 'Wild Fruits Crepe', 'Banana Pancake', 'Pancake', 'White Chocolate Cake', 'Cheesecake', 'Blueberry & Red Velvet Cake', 'Chocolate & Black Current Cake', 'Strawberry Cheesecake']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'] == 2285637, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Peanut Butter

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PRESERVATIVES & SPREADS', 'PEANUT BUTTER')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [12, 12, 510, 340, 340, 12, 1, 1, 510, 340, 340, 28, 28, 16.3, 16.3, 510, 227, 751, 454, 340, 16, 462, 462, 28, 510, 462, 462, 462, 12, 18, 510, 280, 280, 280, 280, 18, 15, 510, 340, 12, 340, 12, 425, 462, 425, 12, 12, 18, 16, 16, 16, 440, 16, 16, 12, 18, 462, 28, 454, 454, 12, 454, 454, 454, 453, 340, 184, 400, 400, 510, 340, 340, 340, 1, 1, 16, 16, 1, 340, 16, 16, 16, 16, 340, 250, 16, 454, 454, 170, 397, 170, 340, 340, 320, 320, 340, 453, 453, 453, 453, 453, 453, 453, 453, 510, 510, 225, 310, 310, 225, 310, 225, 310, 453, 453, 453, 453, 510, 454, 1, 16, 16, 340, 340, 400, 400, 440, 340, 325, 325]

units = ['OZ', 'OZ', 'G', 'G', 'G', 'OZ', 'KG', 'KG', 'G', 'G', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'OZ', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'G', 'OZ', 'G', 'OZ', 'G', 'G', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'OZ', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'KG', 'OZ', 'OZ', 'KG', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

type = ['Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Crunchy', 'Smooth', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Smooth', 'Creamy', 'Smooth', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Crunchy', 'Crunchy', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Crunchy', 'Smooth', 'Crunchy', 'Smooth', 'Creamy', 'Creamy', 'Crunchy', 'Crunchy', 'Whipped', 'Creamy', 'Creamy', 'Crunchy', 'Creamy & Roasted Honey', 'Creamy & Honey', 'Creamy', 'Crunchy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy & Honey', 'Blended With Maple Syrup', 'Chocolate', 'White Chocolate', 'Crunchy', 'Smooth', 'Creamy', 'Smooth', 'Powder', 'Crunchy', 'Smooth', 'Creamy', 'Crunchy Dark Roasted', 'Smooth', 'Crunchy', 'Smooth', 'Crunchy', 'Creamy', 'Crunchy', 'Smooth', 'Chocolate & Hazelnut', 'Crunchy', 'Creamy', 'Honey', 'Crunchy', 'Crunchy', 'Creamy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Smooth', 'Smooth', 'Creamy', 'Creamy', 'Crunchy', 'Creamy & Honey', 'Cruchy & Honey', 'Creamy', 'Crunchy Chia & Flaxseed', 'Creamy & Chocolate', 'Creamy & White Chocolate', 'Assorted', 'Crunchy', 'Crunchy', 'Crunchy & Honey', 'Smooth', 'With Orange', 'With Orange', 'With Blackberry', 'With Blackberry', 'Creamy & White Chocolate', 'Crunchy', 'Creamy', 'Creamy', 'Creamy & Honey', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Smooth', 'Crunchy', 'Creamy', 'Crunchy', 'Smooth', 'Crunchy']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([628105, 714491, 808665, 897611, 990750, 993658, 2022935, 2068636, 2159330, 2159331]), 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Sorbets

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'ICE CREAM & DESSERTS', 'SORBETS')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [100, 500, 500, 100, 65, 65, 65, 65, 45, 75, 70, 70, 70, 9, 878, 878, 851, 878, 90, 170, 878, 878, 638, 70, 878, 50, 460, 480, 173, 585, 600, 70, 750, 750, 50, 50, 50, 80, 80, 80, 80, 80, 156, 156, 50, 50, 145, 50, 100, 300, 70, 70, 70, 70, 400, 400, 80, 80, 80, 80, 80, 80, 45, 45, 45, 45, 50, 80]

units = ['ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'OZ', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML']

type = ['Raspberry Stick', 'Raspberry Tub', 'Mango Tub', 'Mango Stick', 'Raspberry & Vanilla Bar', 'Mango & Vanilla Bar', 'Raspberry & Vanilla Bar', 'Mango & Vanilla Bar', 'Mixed Fruit Lolly', 'Orange Juice', 'Melon Bar', 'Banana Bar', 'Mango Bar', 'Berry & Cherry & Raspberry Popsicle', 'Cherry & Grape & Orange Popsicle', 'Fruit Punch & Orange & Pineapple Popsicle', 'Blue Raspberry & Cherry & Lemon Popsicle', 'Blue Raspberry & Cherry & Grape & Green Apple', 'Pink Guaa Stick', 'Acai Blend - Fruit & Granola Tub', 'Blueberry & Raspberry & Strawberry Popsicle', 'Blue Raspberry & Lemonade & Strawberry & Watermelon Popsicle', 'Cherry & Orange & Raspberry & Watermelon Popsicle', 'Mango Stick', 'Cherry & Orange & Grape Popsicle', 'Blueberry & Peach Apricot & Pina Colada Stick', 'Avocado Tub', 'Blueberry & Strawberry & Vanilla Stick', 'Acai Tub', 'Blue Raspberry & Lemon & Lime & Orange & Redberry Popsicle', 'Banana & Blue Raspberry & Strawberry Popsicle', 'Mango & Strawberry & Vanilla Stick', 'Banana & Carrot & Mango & Pineapple Smoothie Blend', 'Avocado & Banana & Cucumber & Melon & Pineapple Smoothie Blend', 'Manog & Passion Fruit Popsicle', 'Orange Popsicle', 'Banana & Strawberry Popsicle', 'Mango & Passion Fruit Popsicle', 'Banana & Strawberry Popsicle', 'Lemon & Mint Popsicle', 'Chocolate & Coconut Popsicle', 'Strawberry Banana & Mango Passion Fruit & Chocolate Coconut & Lemon Mint Popsicle', 'Lychee Mochi Dough', 'Passion Fruit Mochi Dough', 'Cola & Lemon & Raspberry & Tutti Frutti & Watermelon Popsicle', 'Cola & Lemon & Raspberry & Tutti Frutti & Watermelon Popsicle', 'Coconut Pudding', 'Coconut & Green Apple & Lemon & Watermelon Popsicle', 'Coconut & Green Apple & Lemon & Watermelon Popsicle', 'Lemon & Pineapple & Strawberry Lolly', 'Raspberry Lolly', 'Orange Lolly', 'Pineapple Lolly', 'Mango Lolly', 'Acai & Berry Smoothie Blend', 'Acai & Berry Smoothie Blend', 'Mango Lolly', 'Raspberry Lolly', 'Strawberry Lolly', 'Chocolate Lolly', 'Coconut Lolly', 'Lime Lolly', 'Lime & Mango Lolly', 'Raspberry & Strawberry Lolly', 'Chocolate & Coconut Lolly', 'Chocolate & Coffee & Strawberry & Vanilla Lolly', 'Assorted Popsicle', 'Assorted Popsicle']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([779547]), 'item_count'] = 3
df.loc[df['material_id'].isin([2092202, 2261021, 2261093, 2261094, 2261095, 2348824]), 'item_count'] = 4
df.loc[df['material_id'].isin([401298, 401302]), 'item_count'] = 5
df.loc[df['material_id'].isin([756504, 1647437, 2203436, 2346941]), 'item_count'] = 6
df.loc[df['material_id'].isin([2175227, 2175228]), 'item_count'] = 10
df.loc[df['material_id'].isin([1820922]), 'item_count'] = 12
df.loc[df['material_id'].isin([1646914]), 'item_count'] = 18
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Travel Tissue &Wipes

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PAPER GOODS', 'TRAVEL TISSUE &WIPES')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [24, 20, 20, 20, 20, 10, 30, 8, 15, 15, 120, 15, 10, 10, 20, 30, 40, 10, 10, 10, 10, 10, 10, 10, 10, 80, 10, 40, 40, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 100, 100, 15, 25, 25, 10, 10, 20, 40, 20, 40, 5, 5, 10, 25, 10, 40, 15, 10, 25, 10, 40, 40, 10, 10, 10, 15, 9, 9, 9, 30, 10, 25, 20, 100, 100, 10, 72, 72, 72]

units = ['pcs'] * 79

item_count = [10, 1, 1, 1, 4, 6, 1, 1, 1, 1, 2, 1, 1, 1, 1, 3, 2, 1, 1, 5, 1, 5, 5, 10, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 3, 1, 1, 2, 1, 1, 1, 1, 2, 3, 1, 20, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 2, 1, 1, 1]

type = ['Pocket Tissues', 'Travel Wipes', 'Travel Wipes', 'Travel Wipes', 'Pocket Tissues', 'Mini Tissues', 'Travel Wipes', 'Dry Tissues', 'Wet Wipes', 'Wet Wipes', 'Mini Tissues', 'Sanitizing Wipes', 'Travel Wipes', 'Skin Wipes', 'Skin Wipes', 'Travel Wipes', 'Sanitizing Wipes', 'Skin Wipes', 'Skin Wipes', 'Multi Use Wipes', 'Wet Wipes', 'Skin Wipes', 'Skin Wipes', 'Pocket Tissues', 'Pocket Tissues', 'Multi Use Wipes', 'Pocket Tissues', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Skin Wipes', 'Skin Wipes', 'Wet Wipes', 'Sanitizing Wipes', 'Sanitizing Wipes', 'Skin Wipes', 'Skin Wipes', 'Disinfectant Wipes', 'Disinfectant Wipes', 'Disinfectant Wipes', 'Disinfectant Wipes', 'Bath Wipes', 'Pocket Tissues', 'Skin Wipes', 'Pocket Tissues', 'Pocket Tissues', 'Pocket Tissues', 'Cleaning Wipes', 'Sanitizing Wipes', 'Multi Use Wipes', 'Skin Wipes', 'Disifectant Wipes', 'Disifectant Wipes', 'Pocket Tissues', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = item_count
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Canned Corned Beef

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'CANNED MEATS', 'CANNED CORNED BEEF')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [340, 175, 198, 260, 150, 210, 340, 250, 340, 340, 175, 150, 198, 150, 210, 200, 260, 200, 198, 150, 100, 380, 380, 380, 260, 260, 260, 175, 340, 340, 150, 538, 175, 175, 175, 420, 420]

units = ['G'] * 37

type = ['Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Hot & Spicy', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Long Shreds', 'Regular', 'Loaf', 'Regular', 'Ranch Style', 'Garlic & Chilli', 'Caldereta', 'Ranch Style', 'Garlic & Chilli', 'Caldereta', 'Regular', 'Regular', 'Regular', 'Loaf', 'Regular', 'Caldereta', 'Ranch Style', 'Garlic & Chilli', 'Regular', 'Regular']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([384585, 634987, 843342, 971012, 1281757, 1355632, 1998328, 2035423]), 'item_count'] = 2
df.loc[df['material_id'].isin([645573, 970959, 2079767]), 'item_count'] = 3
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Kitchen Rolls

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PAPER GOODS', 'KITCHEN ROLLS')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [90, 140, 100, 350, np.nan, 350, 350, 250, 1500, 216, np.nan, 202, np.nan, 360, 540, 350, 216, np.nan, 350, 360, 350, 265, 204, 500, np.nan, 102, 240, 320, 480, 300, 175, 150, 140, 150, 94, 120, 625, 44, 1500, 280, 350, 150, 300, np.nan, 350, 320, 350, 300, 140, 280, 200, 140, 700, 750, 175, 210, 150, 240, 1000, 320, 200, 350, 500, 1500, 1500, np.nan, 210, 50, 160, 1200, 74, 1650, 450, np.nan, np.nan, np.nan, np.nan, 3, 5, np.nan, 1, np.nan, np.nan, 700, np.nan, 500, np.nan, np.nan, np.nan, 120, np.nan, 450, 246, 240, 450, 480, 120, 240, 360, 720, 480, 3, 240, 588, 360, 600, 120, 400, 1500, 2, 400, 94, np.nan, np.nan, np.nan, np.nan, 480, 360, np.nan, np.nan, np.nan, 300, 1350, 250, 500]

units = ['Sheets'] * 125

type = ['Multi Purpose', 'Regular', 'Regular', 'Large & Extra Absorbent', 'Regular', 'Large & Extra Absorbent', 'Regular', 'Large & Extra Absorbent', 'Large', 'Extra Absorbent', 'Regular', 'Regular', 'Regular', 'Extra Absorbent', 'Regular', 'Large', 'Regular', 'Regular', 'Large', 'Regular', 'Large', 'Large', 'Large', 'Large', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Large', 'Large', 'Large', 'Regular', 'Regular', 'Regular', 'Regular', 'Large', 'Regular', 'Large', 'Regular', 'Large', 'Large', 'Large', 'Regular', 'Large', 'Multi Purpose', 'Large', 'Large & Multi Purpose', 'Regular', 'Regular', 'Regular', 'Regular', 'Large', 'Large', 'Large', 'Multi Purpose', 'Large', 'Regular', 'Large', 'Extra Absorbent', 'Extra Absorbent', 'Large', 'Large', 'Large', 'Large', 'Regular', 'Extra Absorbent', 'Extra Absorbent', 'Multi Purpose', 'Large', 'Regular', 'Large', 'Large', 'Regular', 'Regular', 'Regular', 'Regular', 'Large', 'Large', 'Large', 'Large', 'Large', 'Regular', 'Large', 'Extra Absorbent', 'Large', 'Regular', 'Regular', 'Regular', 'Large', 'Large & Extra Absorbent', 'Regular', 'Regular', 'Regular', 'Large', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Large & Multi Purpose', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Large', 'Large', 'Regular', 'Regular', 'Multi Purpose', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Large', 'Large', 'Large', 'Large', 'Large']

type_bin = type.copy()

df['volume'] = volume
df.loc[df['material_id'].isin([1768237, 1768239]), 'volume'] = 120
df.loc[df['material_id'].isin([943124, 1571235, 2058900]), 'volume'] = 160
df.loc[df['material_id'].isin([481631, 323337]), 'volume'] = 196
df.loc[df['material_id'].isin([1675623]), 'volume'] = 200
df.loc[df['material_id'].isin([1767700]), 'volume'] = 202
df.loc[df['material_id'].isin([2051044, 1768241]), 'volume'] = 240
df.loc[df['material_id'].isin([1705666, 2144523]), 'volume'] = 300
df.loc[df['material_id'].isin([1675230, 2026651]), 'volume'] = 400
df.loc[df['material_id'].isin([1973862]), 'volume'] = 498
df.loc[df['material_id'].isin([1735769]), 'volume'] = 540
df.loc[df['material_id'].isin([657737, 458575]), 'volume'] = 588
df.loc[df['material_id'].isin([1675413, 1675624, 1712393, 2109694, 1138942]), 'volume'] = 600
df.loc[df['material_id'].isin([1744877]), 'volume'] = 640
df.loc[df['material_id'].isin([2108001]), 'volume'] = 1000

df['units'] = units
df.loc[df['material_id'].isin([189971, 344712, 393893, 400631, 436400, 639145, 682380, 802784, 894391, 1019549, 1019651, 1019652, 1086298, 1097173, 1144627, 1166102, 1192593, 1305435, 1355350, 1490343, 1556777, 1557938, 1671102, 2146817, 2162038]), 'units'] = 'Meters'
df.loc[df['material_id'].isin([1678419, 1683562, 1709458, 1886513, 1947520]), 'units'] = 'KG'

df['item_count'] = 1
df.loc[df['material_id'].isin([1290542, 1558168, 2058900, 481631]), 'item_count'] = 2
df.loc[df['material_id'].isin([1300482]), 'item_count'] = 3

df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Jams

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PRESERVATIVES & SPREADS', 'JAMS')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

type = []

len(type)

# COMMAND ----------

volume = [420, 420, 420, 420, 420, 454, 454, 454, 454, 454, 454, 454, 28.3, 28.3, 28.3, 28.3, 420, 400, 400, 400, 400, 400, 400, 320, 320, 320, 320, 320, 400, 400, 420, 450, 400, 450, 450, 400, 500, 500, 400, 400, 400, 400, 400, 400, 28.3, 340, 450, 450, 450, 370, 370, 370, 370, 370, 370, 370, 340, 340, 340, 340, 30, 30, 30, 430, 430, 430, 510, 340, 284, 340, 430, 370, 370, 340, 340, 340, 284, 284, 284, 284, 28, 450, 370, 450, 340, 340, 450, 340, 361, 340, 340, 907, 454, 450, 450, 450, 450, 450, 400, 400, 370, 340, 28, 28, 300, 28, 28, 380, 380, 380, 380, 380, 340, 482, 430, 330, 330, 330, 330, 330, 400, 370, 380, 300, 300, 420, 454, 454, 454, 400, 280, 370, 370, 370, 400, 200, 450, 360, 360, 360, 240, 240, 240, 340, 340, 340, 400, 410, 330, 330, 340, 340, 340, 400, 180, 180, 180, 180, 180, 180, 750, 200, 370, 370, 370, 370, 370, 370, 370, 400, 400, 220, 220, 220, 220, 220]

units = ['G'] * 176

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([60546, 93036, 401310, 1062716, 1062717, 1247406, 1435054, 1550878, 1814399, 1876450, 1876451, 1876452, 1905390, 2040850, 2080144, 2080145, 2080146, 2231718, 2231719]), 'item_count'] = 2
df.loc[df['material_id'].isin([439023, 462899, 504647]), 'item_count'] = 3
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Canned Luncheon Meat

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'CANNED MEATS', 'CANNED LUNCHEON MEAT')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Savoury

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'BISCUITS & CAKES', 'SAVOURY')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Toilet Rolls

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PAPER GOODS', 'TOILET ROLLS')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Cookies

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'BISCUITS & CAKES', 'COOKIES')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Facial Tissues

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PAPER GOODS', 'FACIAL TISSUES')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Canned Sausages

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'CANNED MEATS', 'CANNED SAUSAGES')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Cream Filled Biscuit

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'BISCUITS & CAKES', 'CREAM FILLED BISCUIT')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Mamoul

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'BISCUITS & CAKES', 'MAMOUL')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Honey

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'PRESERVATIVES & SPREADS', 'HONEY')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------

# MAGIC %md
# MAGIC #Chocolate Coated

# COMMAND ----------

df = run('2023-10-01', '2024-09-28', 'BISCUITS & CAKES', 'CHOCOLATE COATED')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = []

units = []

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
# df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin
df['volume'] = df['volume'].astype('float64')

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable(f{table_name})

# COMMAND ----------


