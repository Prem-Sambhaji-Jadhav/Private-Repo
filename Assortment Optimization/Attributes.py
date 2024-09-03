# Databricks notebook source
# MAGIC %md
# MAGIC #Initializations

# COMMAND ----------

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests

# COMMAND ----------

def run(start_date, end_date, category, material_group):
    query = f"""
    SELECT
        material_id,
        material_name,
        brand,
        material_group_name,
        category_name,
        ROUND(SUM(amount)) AS sales
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
    JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
    WHERE
        business_day BETWEEN "{start_date}" AND "{end_date}"
        AND category_name = "{category}"
        AND material_group_name = "{material_group}"
        AND tayeb_flag = 0
        AND transaction_type IN ("SALE", "SELL_MEDIA")
        AND amount > 0
        AND quantity > 0
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

df = run('2023-07-01', '2024-06-28', 'BISCUITS & CAKES', 'RICE & OAT CAKE')
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
df.to_csv(f'{folder_path}/rice_&_oat_cake_attributes.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Vinegar

# COMMAND ----------

df = run('2023-07-01', '2024-06-28', 'SAUCES & PICKLES', 'VINEGAR')
# df.display()

# COMMAND ----------

volume = [1, 16, 16, 32, 32, 16, 32, 32, 750, 500, 500, 12, 500, 32, 16, 150, 16, 32, 1, 284, 3.78, 568, 500, 500, 250, 284, 473, 473, 250, 32, 250, 150, 32, 1, 946, 946, 500, 16, 250, 16, 250, 473, 1, 500, 300, 500, 473, 16, 150, 500, 500, 500, 500, 500, 250, 1, 500, 250, 250, 250, 250, 500, 500, 473, 946, 32, 16, 480, 3.78, 1, 310, 500, 400, 445, 450, 500, 3.78, 280, 250, 250, 500, 250, 500, 1000, 5000, 250, 1, 1, 500, 1, 500, 500, 500]

units = ['GAL', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'ML', 'ML', 'ML', 'OZ', 'ML', 'OZ', 'OZ', 'ML', 'OZ', 'OZ', 'L', 'ML', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'OZ', 'ML', 'ML', 'OZ', 'GAL', 'ML', 'ML', 'ML', 'OZ', 'ML', 'OZ', 'ML', 'ML', 'GAL', 'ML', 'ML', 'ML', 'ML', 'OZ', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'OZ', 'OZ', 'ML', 'L', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'L', 'ML', 'L', 'ML', 'ML', 'ML']

type = ['White', 'Apple Cider', 'White', 'Brown', 'White', 'White', 'White', 'Apple Cider', 'Apple', 'Red Grape', 'Balsamic', 'Balsamic', 'Balsamic', 'Apple Cider', 'Apple Cider', 'Rice', 'White', 'White', 'White', 'Malt', 'White', 'Distilled Malt', 'Balsamic', 'Apple Cider', 'Balsamic', 'Distilled Malt', 'Natural', 'White', 'Balsamic', 'Grape', 'Balsamic', 'Rice', 'Apple Cider', 'White', 'White', 'Apple Cider', 'Apple Cider', 'White', 'White Balsamic', 'Apple Cider', 'Balsamic', 'White', 'White', 'Balsamic', 'Apple', 'Balsamic', 'Natural', 'Apple Cider', 'Brown Rice', 'Balsamic', 'Balsamic', 'Grape', 'Pomegranate', 'Balsamic', 'Balsamic', 'White', 'Coconut', 'Balsamic', 'Balsamic', 'Balsamic', 'Balsamic', 'Apple Cider', 'Apple Cider', 'Apple Cider', 'Apple Cider', 'White', 'White', 'Coconut', 'White', 'Naturally Fermented', 'Cocosuka', 'Rice', 'Mirin', 'Balsamic', 'Mature', 'Apple Cider', 'White', 'Dates', 'White', 'Tomato', 'Apple Cider', 'Apple Cider', 'White', 'White','White', 'Pomegranate', 'White', 'White', 'White', 'White', 'Apple Cider', 'Synthetic', 'Sushi']

type_bin = ['White', 'Apple', 'White', 'Brown', 'White', 'White', 'White', 'Apple', 'Apple', 'Grape', 'Balsamic', 'Balsamic', 'Balsamic', 'Apple', 'Apple', 'Rice', 'White', 'White', 'White', 'Malt', 'White', 'Malt', 'Balsamic', 'Apple', 'Balsamic', 'Malt', 'Natural', 'White', 'Balsamic', 'Grape', 'Balsamic', 'Rice', 'Apple', 'White', 'White', 'Apple', 'Apple', 'White', 'Balsamic', 'Apple', 'Balsamic', 'White', 'White', 'Balsamic', 'Apple', 'Balsamic', 'Natural', 'Apple', 'Rice', 'Balsamic', 'Balsamic', 'Grape', 'Pomegranate', 'Balsamic', 'Balsamic', 'White', 'Coconut', 'Balsamic', 'Balsamic', 'Balsamic', 'Balsamic', 'Apple', 'Apple', 'Apple', 'Apple', 'White', 'White', 'Coconut', 'White', 'Naturally Fermented', 'Coconut', 'Rice', 'Mirin', 'Balsamic', 'Mature', 'Apple', 'White', 'Dates', 'White', 'Tomato', 'Apple', 'Apple', 'White', 'White','White', 'Pomegranate', 'White', 'White', 'White', 'White', 'Apple', 'Synthetic', 'Sushi']

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.at[46, 'item_count'] = 3
df.at[87, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

import os
folder_path = '/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/vinegar'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)
df.to_csv(f'{folder_path}/vinegar_attributes.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Fruits

# COMMAND ----------

df = run('2023-07-01', '2024-06-28', 'ICE CREAM & DESSERTS', 'FRUITS')
df.display()

# COMMAND ----------

volume = [450, 450, 400, 500, 400, 800, 454, 340, 284, 283, 283, 284, 500, 500, 500, 450, 1, 340, 283, 400, 500, 283, 283, 800, 1, 400, 500, 283, 10, 15, 400, 1, 16, 12, 16, 16, 12, 40, 40, 12, 12, 40, 16, 12, 445.5, 283, 454, 500, 284, 284, 340, 454, 907, 283, 283, 397, 300, 340, 312, 283, 312, 284, 284, 750, 750, 750, 400, 400, 16, 350, 400, 400, 400, 14, 14, 14, 14, 400, 14, 400, 14, 14, 14, 14, 454, 300, 300, 300, 300, 300, 300, 250, 250, 400, 283, 400, 400, 300, 12, 595, 1.36, 1.36, 1.36, 1.36, 907, 907, 1.36, 1.36, 1.36, 400, 500, 312, 400, 400, 350, 454, 1.36, 454, 400, 400, 1, 312, 350, 6.1, 6.1, 500, 500, 350, 350, 500, 400, 400, 1, 400, 400, 400, 100, 400, 400, 400, 350, 350, 350, 350, 350, 1, 1, 907.2, 500, 1, 80, 80, 2.5, 400, 32, 500, 500, 500, 500, 500, 500, 500, 500, 300, 350, 350, 350, 400, 400, 400, 800, 800, 283, 350, 350, 350, 350, 350, 350, 350, 350, 350, 350, 350, 350]

units = ['G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'KG', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'OZ', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'OZ', 'G', 'KG', 'KG', 'KG', 'KG', 'G', 'G', 'KG', 'KG', 'KG', 'G', 'ML', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'KG', 'G', 'G', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'KG', 'G', 'ML', 'KG', 'G', 'G', 'KG', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

type = ['Strawberry', 'Strawberry', 'Strawberry', 'Summer Fruits', 'Strawberry', 'Strawberry', 'Peach', 'Blueberry', 'Strawberry', 'Strawberry', 'Raspberry', 'Blackberry', 'Black Currant', 'Blackberry', 'Strawberry', 'Salad Fruits', 'Mango', 'Dragonfruit', 'Blueberry', 'Strawberry', 'Date', 'Blueberry', 'Mixed Berries', 'Mango', 'Pineapple', 'Strawberry', 'Black Forest Fruits', 'Strawberry', 'Cherry', 'Blueberry', 'Strawberry', 'Cranberry', 'Strawberry', 'Berry Mix', 'Peach', 'Mango', 'Blueberry', 'Strawberry', 'Berry Mix', 'Blackberry', 'Raspberry', 'Blueberry', 'Pineapple', 'Cherry', 'Banana', 'Pineapple', 'Banana', 'Blueberry', 'Strawberry', 'Berry Mix', 'Mixed Berries', 'Mango', 'Blueberry', 'Berry Mix', 'Berry Mix', 'Corn', 'Raspberry', 'Raspberry', 'Black Jamun', 'Mango', 'Mango', 'Butternut Squash', 'Mango', 'Red Smoothie Mix', 'Yellow Smoothie Mix', 'Green Smoothie Mix', 'Blueberry', 'Blueberry & Strawberry', 'Berry Mix', 'Triple Berry', 'Pineapple', 'Tropical Fruit Mix', 'Strawberry', 'Passion Fruit', 'Blackberry', 'Mango', 'Soursop', 'Strawberry', 'Lulo', 'Date', 'Papaya', 'Pineapple', 'Orange', 'Guava', 'Mixed Fruit', 'Strawberry', 'Raspberry', 'Blueberry', 'Berry Mix', 'Blackberry', 'Mango', 'Red Smoothie Mix', 'Yellow Smoothie Mix', 'Acai Berry', 'Peach', 'Mixed Berries', 'Strawberry', 'Raspberry', 'Raspberry', 'Acai Berry', 'Pineapple', 'Mango', 'Tropical Fruit Mix', 'Peach', 'Fruit Mix', 'Fruit Mix', 'Blueberry', 'Berry Mix', 'Fruit Mix', 'Acai Berry', 'Acai Berry', 'Chikoo', 'Tropical Fruit Mix', 'Pineapple', 'Triple Berry', 'Banana', 'Mango', 'Strawberry', 'Mixed Berries', 'Mixed Berries', 'Custard Apple', 'Pomegranate', 'Strawberry', 'Acai Berry', 'Acai Berry', 'Berry Smoothie Mix', 'Black Forest Fruits', 'Blueberry', 'Cherry', 'Mango', 'Caja', 'Pineapple', 'Cupuacu', 'Guava', 'Acerola', 'Soursop', 'Acai Berry', 'Blueberry', 'Blueberry & Strawberry', 'Acai Berry', 'Strawberry', 'Blueberry', 'Black Cherry', 'Mixed Berries', 'Strawberry', 'Passion Fruit', 'Strawberry', 'Cherry', 'Acai Berry & Banana', 'Blueberry', 'Mango', 'Custard Apple', 'Strawberry', 'Strawberry', 'Banana', 'Orange Smoothie Mix', 'Pink Smoothie Mix', 'Yellow Smoothie Mix', 'Red Smoothie Mix', 'Blueberry', 'Mixed Berries', 'Mango', 'Pineapple', 'Raspberry', 'Blackberry', 'Blueberry', 'Mixed Berries', 'Mango', 'Passion Fruit', 'Fruit Mix', 'Mango', 'Custard Apple', 'Blueberry', 'Raspberry', 'Blackberry', 'Blueberry', 'Blueberry', 'Strawberry', 'Cherry', 'Cherry', 'Fruit Mix', 'Pineapple', 'Mango', 'Tropical Fruit Mix', 'Plum']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.at[25, 'item_count'] = 2
df.at[30, 'item_count'] = 3
df.at[55, 'item_count'] = 2
df.at[77, 'item_count'] = 3
df.at[137, 'item_count'] = 2
df.at[138, 'item_count'] = 2
df.at[153, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

import os
folder_path = '/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/fruits'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)
df.to_csv(f'{folder_path}/fruits_attributes.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Spices

# COMMAND ----------

df = run('2023-08-01', '2024-07-29', 'PULSES & SPICES & HERBS', 'SPICES')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Akif's Extracted Attributes

# COMMAND ----------

# query = """
# SELECT INT(material_id) AS material_id, material_name, INT(content) AS content, content_unit, product_type AS type
# FROM sandbox.am_spices_attr
# ORDER BY 1
# """

# akif_df = spark.sql(query).toPandas()

# COMMAND ----------

# akif_df.loc[akif_df['material_id'] == 1576944, 'content'] = 200
# akif_df.loc[akif_df['material_id'] == 1576944, 'content_unit'] = 'GM'
# akif_df.loc[akif_df['material_id'] == 11501, 'content'] = 35
# akif_df.loc[akif_df['material_id'] == 11501, 'content_unit'] = 'GM'
# akif_df.loc[akif_df['material_id'] == 2014666, 'content'] = 320
# akif_df.loc[akif_df['material_id'] == 2135654, 'content'] = 200
# akif_df.loc[akif_df['material_id'] == 2269147, 'content'] = 320

# akif_df['content'] = akif_df['content'].astype('int32')

# akif_df['content_unit'] = np.where(akif_df.content_unit == 'GM', 'G', akif_df.content_unit)

# akif_df['type'] = akif_df['type'].str.title()
# akif_df['type'] = np.where(akif_df.type == 'Na', 'NA', akif_df.type)

# COMMAND ----------

# akif_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Types Modifications

# COMMAND ----------

# MAGIC %md
# MAGIC ###Chilli, Coriander, Turmeric

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('CRUSHED') & (akif_df.material_name.str.contains('CHILLI') | akif_df.material_name.str.contains('CHILLY')),
#     akif_df.material_name.str.contains('POWDER') & (akif_df.material_name.str.contains('CHILLI') | akif_df.material_name.str.contains('CHILLY')),
#     akif_df.material_name.str.contains('WHOLE') & (akif_df.material_name.str.contains('CHILLI') | akif_df.material_name.str.contains('CHILLY'))
# ]
# choices = ['Chilli Crushed', 'Chilli Powder', 'Chilli Whole']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# materials = [[26536, 830373, 986108, 1000564, 1601357, 2031091, 2031103, 2031104, 2178302], [987372, 1060200, 1060204, 1060207, 1122443, 1166468, 1166469, 1392503, 1601374, 1805915, 1828883, 1866013, 1918715, 1967488, 1983911, 2135653], [2075039, 2087915, 2087917, 2170687]]
# chillies = ['Chilli Whole', 'Chilli Powder', 'Chilli Flakes']
# for i in range(len(materials)):
#     for j in range(len(materials[i])):
#         akif_df.loc[akif_df['material_id'] == materials[i][j], 'type'] = chillies[i]

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('POWDER') & akif_df.material_name.str.contains('CORIANDER'),
#     akif_df.material_name.str.contains('WHOLE') & akif_df.material_name.str.contains('CORIANDER')
# ]
# choices = ['Coriander Powder', 'Coriander Whole']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# materials = [[470988, 506198, 1060206, 1252352, 1576944, 1601373, 1832577], [554790], [598631, 1687941, 2231611]]
# corianders = ['Coriander Powder', 'Coriander Whole', 'Coriander Seeds']
# for i in range(len(materials)):
#     for j in range(len(materials[i])):
#         akif_df.loc[akif_df['material_id'] == materials[i][j], 'type'] = corianders[i]

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('POWDER') & akif_df.material_name.str.contains('TURMERIC'),
#     akif_df.material_name.str.contains('WHOLE') & akif_df.material_name.str.contains('TURMERIC')
# ]
# choices = ['Turmeric Powder', 'Turmeric Whole']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# materials = [[471002, 987377, 1060203, 1252353, 1332935, 1482032, 1503888, 2031087, 2071303, 2135654], [2031088], [2031089]]
# turmerics = ['Turmeric Powder', 'Turmeric Whole', 'Turmeric Slices']
# for i in range(len(materials)):
#     for j in range(len(materials[i])):
#         akif_df.loc[akif_df['material_id'] == materials[i][j], 'type'] = turmerics[i]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tamarind, Pepper, Cumin

# COMMAND ----------

# materials = [[4431, 9018, 801708, 1312401, 1416283, 1520521, 1727441, 1727452, 1753453, 1832881, 1992982, 2269147], [45024, 55293, 70947, 255383, 255386, 288150, 301710, 354860, 442199, 442210, 896974, 896975, 1032075, 1240575, 1448374, 1448375, 1518749, 1576301, 1576302, 1601930, 1625131, 1625132, 2009391], [1065603, 1065604, 1626719, 2078291], [1520522, 2026473, 2026476]]
# tamarinds = ['Tamarind Paste', 'Tamarind Whole', 'Tamarind Seedless', 'Tamarind Sauce']
# for i in range(len(materials)):
#     for j in range(len(materials[i])):
#         akif_df.loc[akif_df['material_id'] == materials[i][j], 'type'] = tamarinds[i]

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('POWDER') & akif_df.material_name.str.contains('PEPPER'),
#     akif_df.material_name.str.contains('PEPPERCORN'),
#     akif_df.material_name.str.contains('WHOLE') & akif_df.material_name.str.contains('PEPPER')
# ]
# choices = ['Pepper Powder', 'Peppercorn', 'Peppercorn']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# materials = [[26144, 433037, 433047, 433069, 987370, 1252355, 1503884, 1576179, 1739030, 1739172, 1832569, 1971193], [987359, 1493887, 1601359, 1611355, 1611357], [433041], [1739174]]
# peppers = ['Pepper Powder', 'Peppercorn', 'Pepper Coarse', 'Pepper Flakes']
# for i in range(len(materials)):
#     for j in range(len(materials[i])):
#         akif_df.loc[akif_df['material_id'] == materials[i][j], 'type'] = peppers[i]

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('CUMIN') & akif_df.material_name.str.contains('CUMIN'),
#     (akif_df.material_name.str.contains('WHOLE') | akif_df.material_name.str.contains('SEED')) & akif_df.material_name.str.contains('CUMIN')
# ]
# choices = ['Cumin Powder', 'Cumin Seeds']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# akif_df.loc[akif_df['material_id'] == 987342, 'type'] = 'Cumin Seeds'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cardamom, Cinnamon, Spices

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('CARDAMOM') & akif_df.material_name.str.contains('POWDER'),
#     akif_df.material_name.str.contains('CARDAMOM')
# ]
# choices = ['Cardamom Powder', 'Cardamom Whole']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# akif_df.loc[akif_df['material_id'].isin([470968, 1832638]), 'type'] = 'Cardamom Powder'

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('CINNAMON') & akif_df.material_name.str.contains('POWDER'),
#     akif_df.material_name.str.contains('CINNAMON') & (akif_df.material_name.str.contains('STICK') | akif_df.material_name.str.contains('WHOLE'))
# ]
# choices = ['Cinnamon Powder', 'Cinnamon Whole']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# akif_df.loc[akif_df['material_id'].isin([4849, 12139, 470986, 998960, 1482031, 1503889, 1739029, 1832697]), 'type'] = 'Cinnamon Powder'
# akif_df.loc[akif_df['material_id'].isin([1752155, 1739177]), 'type'] = 'Cinnamon Whole'

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([293925, 433162, 846015, 1003637, 1186510, 1218328, 1644899, 1644941, 1811450, 2036525, 2195814]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([470886, 1063456, 1644943, 1694695, 1812569, 2087919, 1832620]), 'type'] = 'BBQ Spices'
# akif_df.loc[akif_df['material_id'].isin([704004]), 'type'] = 'Pasta Spices'
# akif_df.loc[akif_df['material_id'].isin([1104111]), 'type'] = 'Assorted Spices'
# akif_df.loc[akif_df['material_id'].isin([1435658]), 'type'] = 'Mandi Spices'
# akif_df.loc[akif_df['material_id'].isin([1437495]), 'type'] = 'AlQoosi Spices'
# akif_df.loc[akif_df['material_id'].isin([1489732]), 'type'] = 'Cheddar Cheese Spices'
# akif_df.loc[akif_df['material_id'].isin([1561247]), 'type'] = 'Salad Spices'
# akif_df.loc[akif_df['material_id'].isin([1644897, 1832567]), 'type'] = 'Shawarma Spices'
# akif_df.loc[akif_df['material_id'].isin([1660964]), 'type'] = 'Majboos Spices'
# akif_df.loc[akif_df['material_id'].isin([1812570]), 'type'] = 'Seafood Spices'
# akif_df.loc[akif_df['material_id'].isin([1812571, 2008862]), 'type'] = 'Chicken Spices'
# akif_df.loc[akif_df['material_id'].isin([1812642]), 'type'] = 'Meat Spices'
# akif_df.loc[akif_df['material_id'].isin([1812643]), 'type'] = 'Potato Spices'
# akif_df.loc[akif_df['material_id'].isin([1812644]), 'type'] = 'Kebda Spices'
# akif_df.loc[akif_df['material_id'].isin([1812645, 1832615, 2009822]), 'type'] = 'Kabsa Spices'
# akif_df.loc[akif_df['material_id'].isin([1832621, 2199662]), 'type'] = 'Biryani Spices'
# akif_df.loc[akif_df['material_id'].isin([2271795]), 'type'] = 'Arrabbiata Spices'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Garlic, Paprika, Mustard

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([11517, 1482033]), 'type'] = 'Garlic Granules'
# akif_df.loc[akif_df['material_id'].isin([11566, 22721, 103241, 201036, 470969, 522616, 922858, 986089, 1254557, 1835979, 1851335, 2075045, 2231152]), 'type'] = 'Garlic Powder'
# akif_df.loc[akif_df['material_id'].isin([558607]), 'type'] = 'Garlic Italian Seasoning'
# akif_df.loc[akif_df['material_id'].isin([558620]), 'type'] = 'Garlic Salt'
# akif_df.loc[akif_df['material_id'].isin([598741]), 'type'] = 'Garlic Grinder'
# akif_df.loc[akif_df['material_id'].isin([1500931]), 'type'] = 'Garlic Crushed'
# akif_df.loc[akif_df['material_id'].isin([2075044]), 'type'] = 'Garlic Crunchy'
# akif_df.loc[akif_df['material_id'].isin([1503882]), 'type'] = 'Garlic Flakes'

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('PAPRIKA') & akif_df.material_name.str.contains('SMOKE'),
#     akif_df.material_name.str.contains('PAPRIKA') & (akif_df.material_name.str.contains('POWDER') | akif_df.material_name.str.contains('PWDR'))
# ]
# choices = ['Paprika Smoked', 'Paprika Powder']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# akif_df.loc[akif_df['material_id'].isin([11511, 11572, 1748099, 1805920, 1851339]), 'type'] = 'Paprika Powder'

# COMMAND ----------

# conditions = [
#     akif_df.material_name.str.contains('MUSTARD') & akif_df.material_name.str.contains('SEED'),
#     akif_df.material_name.str.contains('MUSTARD') & akif_df.material_name.str.contains('POWDER')
# ]
# choices = ['Mustard Seeds', 'Mustard Powder']
# akif_df['type'] = np.select(conditions, choices, default=akif_df.type)

# akif_df.loc[akif_df['material_id'].isin([1434976, 1684630, 2071322]), 'type'] = 'Mustard Seeds'
# akif_df.loc[akif_df['material_id'].isin([46516]), 'type'] = 'Mustard Dal'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Kashmiri Chilly, Cloves, Fenugreek

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([26581, 581114, 988299, 1035033, 1343270, 1832549, 1854102, 1871670, 1913356, 1983910, 2014666, 2276739]), 'type'] = 'Chilli Powder'
# akif_df.loc[akif_df['material_id'].isin([202619]), 'type'] = 'Chilli Whole'

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([11554, 67359, 67360, 598664, 963335, 1000611, 1343262, 1601371, 1687670]), 'type'] = 'Cloves Whole'
# akif_df.loc[akif_df['material_id'].isin([184325, 470900, 1739028, 1805916, 1832696]), 'type'] = 'Cloves Powder'

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([67362, 895905, 986523, 1029916, 1343146, 1601360, 1828853, 1576946, 2146912, 2082857]), 'type'] = 'Fenugreek Seeds'
# akif_df.loc[akif_df['material_id'].isin([11563, 598692, 922623, 1057870]), 'type'] = 'Fenugreek Powder'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ginger, Fennel, Lemon

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([11567, 58435, 470830, 470902, 557725, 1000569, 1252356, 1503887, 1832641, 1835980, 1946034]), 'type'] = 'Ginger Powder'
# akif_df.loc[akif_df['material_id'].isin([67328]), 'type'] = 'Ginger Whole'
# akif_df.loc[akif_df['material_id'].isin([2031105]), 'type'] = 'Ginger Slices'

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([24790, 67368, 598699, 895907, 987341, 1828854, 1687947]), 'type'] = 'Fennel Seeds'
# akif_df.loc[akif_df['material_id'].isin([46513, 922622, 1057868]), 'type'] = 'Fennel Powder'
# akif_df.loc[akif_df['material_id'].isin([74546]), 'type'] = 'Fennel Shahi/Saunf'

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([53532, 74793, 471421, 1835977, 1003638]), 'type'] = 'Lemon Powder'
# akif_df.loc[akif_df['material_id'].isin([986103, 1980493, 2183619]), 'type'] = 'Lemon Dry'
# akif_df.loc[akif_df['material_id'].isin([1197581]), 'type'] = 'Lemon & Cracked Pepper Seasoning'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Onion Powder, Nutmeg, Sesame

# COMMAND ----------

# # All materials in Onion Powder are already accurately labeled

# akif_df[akif_df['type'].str.contains('Onion Powder')].display()

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([11495, 471053, 922578, 1666007, 1739027, 1853614, 2075046]), 'type'] = 'Nutmeg Powder'
# akif_df.loc[akif_df['material_id'].isin([598845, 67431]), 'type'] = 'Nutmeg Whole'

# akif_df['type'] = np.where(akif_df.type == 'Sesame', 'Sesame Seeds', akif_df.type)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Asafoetida, Sumac, Zaatar

# COMMAND ----------

# akif_df['type'] = np.where(akif_df.type == 'Asafoetida', 'Asafoetida Powder', akif_df.type)
# akif_df.loc[akif_df['material_id'].isin([1194215]), 'type'] = 'Asafoetida Compounded'

# COMMAND ----------

# # All materials in Sumac and Zaatar are already accurately labeled

# akif_df[akif_df['type'].str.contains('Sumac|Zaatar')].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Aniseeds, Ajwan, Mango

# COMMAND ----------

# # All materials in Aniseeds are already accurately labeled

# akif_df[akif_df['type'].str.contains('Aniseeds')].display()

# COMMAND ----------

# akif_df['type'] = np.where(akif_df.type == 'Ajwan', 'Ajwain', akif_df.type)
# akif_df['type'] = np.where(akif_df.type == 'Mango', 'Mango Powder', akif_df.type)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cajun, Karampodi, Mukhwas

# COMMAND ----------

# akif_df['type'] = np.where(akif_df.type == 'Cajun', 'Cajun Seasoning', akif_df.type)
# akif_df.loc[akif_df['material_id'].isin([554030]), 'type'] = 'Cajun Spices'

# akif_df['type'] = np.where(akif_df.type == 'Karampodi', 'Karam Podi', akif_df.type)
# akif_df.loc[akif_df['material_id'].isin([2124678]), 'type'] = 'Kandi Podi'

# COMMAND ----------

# # All materials in Mukhwas are already accurately labeled

# akif_df[akif_df['type'].str.contains('Mukhwas')].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Garam Masala, Sago, Basil

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([46487, 987333, 1879525]), 'type'] = 'Garam Masala Whole'
# akif_df.loc[akif_df['material_id'].isin([986524]), 'type'] = 'Garam Masala Powder'

# akif_df['type'] = np.where(akif_df.type == 'Sago', 'Sago Seeds', akif_df.type)

# COMMAND ----------

# # All materials in Basil are already accurately labeled

# akif_df[akif_df['type'].str.contains('Basil')].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Red Chilly, Kalonji, Hing

# COMMAND ----------

# akif_df['type'] = np.where(akif_df.type == 'Red Chilly', 'Chilli Powder', akif_df.type)

# akif_df.loc[akif_df['material_id'].isin([704009]), 'type'] = 'Black Cumin Powder'
# akif_df.loc[akif_df['material_id'].isin([705607, 1805917]), 'type'] = 'Black Cumin Seeds'

# akif_df.loc[akif_df['material_id'].isin([45143]), 'type'] = 'Asafoetida Compounded'
# akif_df.loc[akif_df['material_id'].isin([45147]), 'type'] = 'Asafoetida Powder'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Kudampuli, Lime, Curry Powder

# COMMAND ----------

# # All materials in Kudampuli and Curry Powder are already accurately labeled

# akif_df[akif_df['type'].str.contains('Kudampuli|Curry Powder')].display()

# COMMAND ----------

# akif_df['type'] = np.where(akif_df.type == 'Lime', 'Lime Powder', akif_df.type)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Oregano, Thyme, Ensoon

# COMMAND ----------

# # All materials in Oregano and Thyme are already accurately labeled

# akif_df[akif_df['type'].str.contains('Oregano|Thyme')].display()

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([46543]), 'type'] = 'Ensoon Powder'
# akif_df.loc[akif_df['material_id'].isin([46545]), 'type'] = 'Ensoon Seeds'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Others

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([1205515]), 'type'] = 'Tomato Rice Powder'
# akif_df.loc[akif_df['material_id'].isin([1063465]), 'type'] = 'Fish Spices'
# akif_df.loc[akif_df['material_id'].isin([2271794]), 'type'] = 'Piccantissimo'
# akif_df.loc[akif_df['material_id'].isin([1913357]), 'type'] = 'Chilli Paste'
# akif_df.loc[akif_df['material_id'].isin([1832617]), 'type'] = 'Stew Spice Mix'
# akif_df.loc[akif_df['material_id'].isin([1003727]), 'type'] = 'Falafel Spices'
# akif_df.loc[akif_df['material_id'].isin([193030]), 'type'] = 'Caraway Seeds'
# akif_df.loc[akif_df['material_id'].isin([207468]), 'type'] = 'Green Chilli'
# akif_df.loc[akif_df['material_id'].isin([236292]), 'type'] = 'Star Seeds'
# akif_df.loc[akif_df['material_id'].isin([987375]), 'type'] = 'Coriander Powder'
# akif_df.loc[akif_df['material_id'].isin([1023078]), 'type'] = 'Celery Seeds'
# akif_df.loc[akif_df['material_id'].isin([1515669]), 'type'] = 'Black Seeds'
# akif_df.loc[akif_df['material_id'].isin([1582239]), 'type'] = 'Kebbeh Spice'
# akif_df.loc[akif_df['material_id'].isin([11544]), 'type'] = 'Caraway Seeds'
# akif_df.loc[akif_df['material_id'].isin([26529]), 'type'] = 'Pomegranate'
# akif_df.loc[akif_df['material_id'].isin([26530]), 'type'] = 'Arrowroot Powder'
# akif_df.loc[akif_df['material_id'].isin([46495]), 'type'] = 'Black Cumin Seeds'
# akif_df.loc[akif_df['material_id'].isin([46496]), 'type'] = 'Caraway Seeds'
# akif_df.loc[akif_df['material_id'].isin([46515]), 'type'] = 'Mash Powder'
# akif_df.loc[akif_df['material_id'].isin([46544]), 'type'] = 'Karawia Powder'
# akif_df.loc[akif_df['material_id'].isin([74547]), 'type'] = 'Mukhwas'
# akif_df.loc[akif_df['material_id'].isin([74809]), 'type'] = 'Jaljeera'
# akif_df.loc[akif_df['material_id'].isin([433132]), 'type'] = 'Chicken Seasoning'
# akif_df.loc[akif_df['material_id'].isin([433136]), 'type'] = 'Jamaican Jerk Seasoning'
# akif_df.loc[akif_df['material_id'].isin([558638]), 'type'] = 'Lasagne Mix'
# akif_df.loc[akif_df['material_id'].isin([598633]), 'type'] = 'Chilli Pepper'
# akif_df.loc[akif_df['material_id'].isin([731086]), 'type'] = 'Pizza Spices'
# akif_df.loc[akif_df['material_id'].isin([897206]), 'type'] = 'Pumpkin Pie Spices'
# akif_df.loc[akif_df['material_id'].isin([922519]), 'type'] = 'Black Salt Powder'
# akif_df.loc[akif_df['material_id'].isin([433223]), 'type'] = 'Lamb Seasoning'
# akif_df.loc[akif_df['material_id'].isin([479393]), 'type'] = 'Black Cumin Seeds'

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([24817]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([45301]), 'type'] = 'Asafoetida Compounded'
# akif_df.loc[akif_df['material_id'].isin([553948]), 'type'] = 'Meat Spices'
# akif_df.loc[akif_df['material_id'].isin([553949]), 'type'] = 'Chicken Spices'
# akif_df.loc[akif_df['material_id'].isin([731082]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([891558]), 'type'] = 'Cinnamon Sticks'
# akif_df.loc[akif_df['material_id'].isin([922511]), 'type'] = 'Habbat Al Hamra'
# akif_df.loc[akif_df['material_id'].isin([922518]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([929028]), 'type'] = "Shepherd's Pie Mix"
# akif_df.loc[akif_df['material_id'].isin([966551]), 'type'] = 'Italian Seasoning'
# akif_df.loc[akif_df['material_id'].isin([998652]), 'type'] = 'Pepper Coarse'
# akif_df.loc[akif_df['material_id'].isin([1007157]), 'type'] = 'Chilli Flakes'
# akif_df.loc[akif_df['material_id'].isin([1019400]), 'type'] = 'Steak Spices'
# akif_df.loc[akif_df['material_id'].isin([1021121]), 'type'] = 'Kabsa Spices'
# akif_df.loc[akif_df['material_id'].isin([1021126]), 'type'] = 'Mandi Spices'
# akif_df.loc[akif_df['material_id'].isin([1063444]), 'type'] = 'Arabic Spices'
# akif_df.loc[akif_df['material_id'].isin([1063455]), 'type'] = 'Margarine Spices'
# akif_df.loc[akif_df['material_id'].isin([1063457]), 'type'] = 'Natural Dye Spices'
# akif_df.loc[akif_df['material_id'].isin([1063458]), 'type'] = 'Maternal Post Spices'
# akif_df.loc[akif_df['material_id'].isin([1063511]), 'type'] = 'Seafood Spices'
# akif_df.loc[akif_df['material_id'].isin([1065359]), 'type'] = 'Chicken Spices'
# akif_df.loc[akif_df['material_id'].isin([1252358]), 'type'] = 'Pepper Powder'
# akif_df.loc[akif_df['material_id'].isin([1252802]), 'type'] = 'Pepper Powder'
# akif_df.loc[akif_df['material_id'].isin([1381828]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([1435671]), 'type'] = 'Shish Tawook Spices'
# akif_df.loc[akif_df['material_id'].isin([1435677]), 'type'] = 'Salad Spices'
# akif_df.loc[akif_df['material_id'].isin([1482020]), 'type'] = 'Mixed Herbs'
# akif_df.loc[akif_df['material_id'].isin([1645611]), 'type'] = 'Chicken Seasoning'
# akif_df.loc[akif_df['material_id'].isin([1682548]), 'type'] = 'Chicken Spices'
# akif_df.loc[akif_df['material_id'].isin([1682553]), 'type'] = 'Chicken Spices'
# akif_df.loc[akif_df['material_id'].isin([1811451]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([1832551]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([1832564]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([1832619]), 'type'] = 'Seafood Spices'
# akif_df.loc[akif_df['material_id'].isin([1832633]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([1835978]), 'type'] = 'Chicken Seasoning'
# akif_df.loc[akif_df['material_id'].isin([1835983]), 'type'] = 'Mixed Spices'
# akif_df.loc[akif_df['material_id'].isin([1852415]), 'type'] = 'Chilli Flakes'
# akif_df.loc[akif_df['material_id'].isin([1913358]), 'type'] = 'Masala Tikki'
# akif_df.loc[akif_df['material_id'].isin([2008863]), 'type'] = 'Prawn Spices'
# akif_df.loc[akif_df['material_id'].isin([2009145]), 'type'] = 'Truffle Spices'
# akif_df.loc[akif_df['material_id'].isin([2009147]), 'type'] = 'Truffle Cheese Spices'
# akif_df.loc[akif_df['material_id'].isin([2009148]), 'type'] = 'Truffle Pesto Spices'
# akif_df.loc[akif_df['material_id'].isin([2009149]), 'type'] = 'Truffle Porcini Spices'
# akif_df.loc[akif_df['material_id'].isin([2070623]), 'type'] = 'Chicken BBQ Seasoning'
# akif_df.loc[akif_df['material_id'].isin([2070624]), 'type'] = 'Pizza Seasoning'
# akif_df.loc[akif_df['material_id'].isin([2070625]), 'type'] = 'Chips & Potato Seasoning'
# akif_df.loc[akif_df['material_id'].isin([2070626]), 'type'] = 'Chicken Wings Seasoning'
# akif_df.loc[akif_df['material_id'].isin([2070627]), 'type'] = 'Roast Veggies Seasoning'
# akif_df.loc[akif_df['material_id'].isin([2071503]), 'type'] = 'Chicken Broasted Mix'
# akif_df.loc[akif_df['material_id'].isin([2071504]), 'type'] = 'Chicken Broasted Mix'
# akif_df.loc[akif_df['material_id'].isin([2075050]), 'type'] = 'Paprika Smoked'
# akif_df.loc[akif_df['material_id'].isin([2087916]), 'type'] = 'BBQ Seasoning'
# akif_df.loc[akif_df['material_id'].isin([2087920]), 'type'] = 'Argentinian Grill Spices'
# akif_df.loc[akif_df['material_id'].isin([2199921]), 'type'] = 'Mixed Spices'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final DF

# COMMAND ----------

# akif_df = akif_df[['material_id', 'content', 'content_unit', 'type']]
# df = pd.merge(df, akif_df, on='material_id', how = 'left')

# COMMAND ----------

# df.loc[df['material_id'].isin([311362, 337777, 466334]), 'content_unit'] = 'G'
# df.loc[df['material_id'].isin([311362, 466334]), 'content'] = 200
# df.loc[df['material_id'].isin([337777]), 'content'] = 500
# df['content'] = df['content'].astype('int32')

# df.loc[df['material_id'].isin([311362, 337777, 466334]), 'type'] = 'Chilli Powder'

# COMMAND ----------

# for i in df['content'].tolist():
#     print(i, end = ', ')

# for i in df['content_unit'].tolist():
#     print("'" + i + "'", end = ', ')

# for i in df['type'].tolist():
#     print('"' + i + '"', end = ', ')

# COMMAND ----------

volume = [300, 40, 227, 32, 35, 35, 100, 200, 200, 100, 100, 200, 200, 200, 100, 100, 200, 100, 100, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 500, 500, 500, 500, 100, 100, 200, 200, 200, 100, 100, 100, 80, 100, 300, 500, 200, 100, 100, 100, 200, 100, 100, 100, 500, 100, 100, 1, 100, 100, 100, 100, 200, 200, 100, 100, 100, 100, 200, 500, 100, 100, 200, 200, 200, 200, 50, 200, 500, 200, 100, 200, 500, 200, 500, 200, 500, 200, 200, 200, 100, 200, 200, 200, 200, 100, 250, 100, 100, 200, 200, 200, 200, 200, 200, 100, 200, 200, 100, 100, 500, 200, 170, 150, 100, 100, 100, 200, 200, 100, 200, 100, 200, 100, 50, 100, 200, 200, 100, 100, 200, 400, 200, 50, 1, 500, 500, 200, 100, 200, 500, 200, 200, 500, 500, 1, 200, 250, 33, 33, 45, 34, 51, 51, 44, 58, 46, 38, 55, 200, 500, 200, 100, 80, 100, 500, 250, 250, 250, 250, 250, 220, 250, 250, 220, 220, 250, 250, 250, 250, 250, 250, 250, 220, 150, 100, 250, 200, 454, 200, 150, 155, 185, 330, 330, 220, 220, 200, 200, 200, 7, 1, 29, 43, 73, 38, 36, 3, 10, 250, 7, 20, 26, 48, 13, 22, 26, 35, 11, 35, 28, 40, 10, 25, 50, 400, 400, 42, 500, 1, 1, 220, 250, 250, 250, 100, 500, 250, 500, 250, 250, 250, 100, 200, 100, 100, 100, 200, 200, 200, 320, 100, 11, 200, 200, 100, 100, 200, 200, 200, 100, 200, 100, 200, 200, 500, 31, 200, 200, 200, 200, 200, 200, 200, 200, 200, 38, 200, 100, 80, 42, 200, 200, 200, 200, 200, 200, 200, 200, 200, 100, 200, 200, 100, 100, 50, 50, 100, 100, 400, 400, 400, 100, 100, 200, 200, 200, 200, 200, 200, 200, 45, 28, 50, 50, 100, 100, 50, 250, 220, 250, 36, 20, 60, 50, 65, 20, 111, 40, 21, 30, 4, 100, 100, 300, 250, 500, 500, 500, 750, 380, 380, 200, 350, 180, 180, 180, 180, 750, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 1, 500, 150, 180, 100, 100, 35, 400, 200, 400, 200, 400, 200, 400, 200, 100, 100, 100, 57, 160, 145, 380, 750, 900, 150, 135, 190, 155, 200, 135, 200, 165, 200, 200, 200, 200, 200, 200, 200, 100, 100, 250, 100, 200, 100, 200, 350, 750, 200, 150, 200, 170, 80, 100, 30, 170, 150, 150, 140, 160, 160, 8, 500, 200, 45, 40, 300, 100, 100, 100, 100, 100, 100, 95, 200, 500, 650, 11, 39, 37, 50, 100, 95, 40, 28, 70, 39, 70, 40, 38, 39, 58, 42, 50, 200, 500, 485, 485, 100, 100, 100, 180, 250, 250, 330, 200, 500, 200, 100, 100, 100, 100, 200, 100, 100, 200, 200, 100, 1, 1, 1, 100, 85, 80, 200, 500, 200, 200, 100, 100, 100, 125, 22, 30, 36, 100, 100, 20, 20, 100, 100, 200, 100, 100, 50, 100, 100, 100, 100, 200, 200, 250, 400, 100, 485, 485, 200, 200, 200, 40, 40, 30, 30, 40, 40, 15, 40, 50, 485, 400, 400, 100, 100, 100, 100, 100, 200, 200, 250, 200, 250, 250, 250, 250, 150, 150, 250, 250, 250, 200, 350, 200, 125, 225, 275, 80, 475, 475, 250, 475, 425, 475, 630, 105, 160, 150, 135, 140, 160, 135, 160, 170, 150, 150, 150, 170, 150, 150, 130, 160, 130, 150, 160, 150, 130, 227, 200, 250, 220, 200, 200, 230, 220, 200, 200, 250, 100, 200, 170, 160, 160, 40, 170, 320, 225, 225, 240, 240, 180, 100, 200, 450, 250, 100, 400, 400, 400, 90, 90, 1, 100, 100, 150, 100, 200, 200, 150, 227, 400, 400, 250, 250, 250, 45, 45, 45, 45, 300, 250, 320, 2, 200, 200, 150, 200, 150, 50, 50, 75, 150, 70, 24, 220, 195, 220, 215, 180, 200, 200, 200, 200, 200, 425, 425, 3, 35, 45, 45, 40, 50, 20, 40, 40, 180, 200, 75, 75, 75, 75, 100, 100, 50, 100, 100, 100, 100, 200, 200, 200, 200, 200, 100, 150, 150, 150, 100, 250, 100, 250, 100, 250, 250, 100, 200, 1, 400, 600, 250, 100, 115, 145, 130, 165, 120, 130, 200, 320, 100, 100, 400]

units = ['G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'KG', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

type = ["Tamarind Paste", "Cinnamon Powder", "Tamarind Paste", "Nutmeg Powder", "Chilli Powder", "Paprika Powder", "Garlic Granules", "Aniseeds", "Caraway Seeds", "Cardamom Whole", "Cardamom Whole", "Cardamom Powder", "Chilli Crushed", "Chilli Powder", "Chilli Whole", "Chilli Whole", "Cinnamon Powder", "Cinnamon Whole", "Cloves Whole", "Coriander Powder", "Coriander Whole", "Cumin Powder", "Cumin Powder", "Fenugreek Powder", "Garlic Powder", "Ginger Powder", "Mustard Seeds", "Paprika Powder", "Sesame Seeds", "Sumac", "Turmeric Powder", "Zaatar", "Chilli Powder", "Coriander Powder", "Cumin Powder", "Turmeric Powder", "Paprika Powder", "Cinnamon Powder", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Turmeric Powder", "Ajwain", "Garlic Powder", "Chilli Crushed", "Chilli Powder", "Zaatar", "Coriander Powder", "Fennel Seeds", "Pepper Powder", "Ajwain", "Pomegranate", "Arrowroot Powder", "Sesame Seeds", "Chilli Whole", "Chilli Powder", "Sago Seeds", "Mukhwas", "Mukhwas", "Tamarind Whole", "Asafoetida Compounded", "Asafoetida Powder", "Asafoetida Compounded", "Asafoetida Powder", "Garam Masala Whole", "Mustard Powder", "Black Cumin Seeds", "Caraway Seeds", "Sesame Seeds", "Turmeric Whole", "Fennel Powder", "Mash Powder", "Mustard Dal", "Chilli Whole", "Mango Powder", "Ensoon Powder", "Karawia Powder", "Ensoon Seeds", "Camomil", "Lemon Powder", "Tamarind Whole", "Cumin Powder", "Cardamom Whole", "Ginger Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Coriander Powder", "Turmeric Powder", "Turmeric Powder", "Cumin Powder", "Chilli Whole", "Cumin Powder", "Pepper Powder", "Ginger Whole", "Chilli Whole", "Pepper Powder", "Cardamom Whole", "Cloves Whole", "Cloves Whole", "Cinnamon Whole", "Fenugreek Seeds", "Coriander Whole", "Mustard Seeds", "Fennel Seeds", "Sesame Seeds", "Cinnamon Powder", "Ajwain", "Chilli Powder", "Peppercorn", "Nutmeg Whole", "Mukhwas", "Tamarind Whole", "Mukhwas", "Fennel Shahi/Saunf", "Mukhwas", "Lemon Powder", "Mango Powder", "Jaljeera", "Cardamom Powder", "Garlic Powder", "Pepper Powder", "Chilli Crushed", "Cloves Powder", "Turmeric Whole", "Pepper Powder", "Kasuri Methi", "Caraway Seeds", "Chilli Crushed", "Ajwain", "Garlic Powder", "Chilli Whole", "Chilli Powder", "Green Chilli", "Chilli Crushed", "Star Seeds", "Tamarind Whole", "Tamarind Whole", "Tamarind Whole", "Mixed Spices", "Tamarind Whole", "Chilli Powder", "Chilli Powder", "Turmeric Powder", "Coriander Powder", "Coriander Powder", "Chilli Powder", "Chilli Powder", "Tamarind Whole", "Turmeric Powder", "Pepper Powder", "Pepper Coarse", "Pepper Powder", "Pepper Powder", "Chicken Seasoning", "Jamaican Jerk Seasoning", "Cajun Seasoning", "Mixed Spices", "Steak Seasoning", "Lamb Seasoning", "Fish Seasoning", "Tamarind Whole", "Tamarind Whole", "Chilli Powder", "Turmeric Powder", "Cardamom Powder", "Ginger Powder", "Cinnamon Whole", "Sumac", "BBQ Spices", "Cloves Powder", "Ginger Powder", "Cinnamon Whole", "Cardamom Powder", "Garlic Powder", "Chilli Powder", "Cinnamon Powder", "Coriander Powder", "Cumin Powder", "Turmeric Powder", "Nutmeg Powder", "Zaatar", "Coriander Whole", "Cardamom Whole", "Lemon Powder", "Paprika Powder", "Mustard Seeds", "Asafoetida Powder", "Black Cumin Seeds", "Cardamom Whole", "Mustard Powder", "Kokam", "Chilli Powder", "Cumin Powder", "Turmeric Powder", "Coriander Powder", "Cardamom Whole", "Garlic Powder", "Lime Powder", "Meat Spices", "Chicken Spices", "Cajun Spices", "Coriander Whole", "Chives", "Chilli Crushed", "Garlic Italian Seasoning", "Garlic Salt", "Chilli Powder", "Lasagne Mix", "Parsley", "Sage", "Sesame Seeds", "Oregano", "Coriander Seeds", "Chilli Pepper", "Mustard Seeds", "Cinnamon Whole", "Cloves Whole", "Cardamom Whole", "Cumin Powder", "Thyme", "Fenugreek Powder", "Fennel Seeds", "Garlic Grinder", "Basil", "Nutmeg Whole", "Peppercorn", "Chilli Powder", "Turmeric Powder", "Cajun Seasoning", "Turmeric Powder", "Coriander Powder", "Chilli Powder", "Pasta Spices", "Black Cumin Powder", "Onion Powder", "Black Cumin Seeds", "Onion Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Coriander Powder", "Turmeric Powder", "Shawarma Spice", "Mixed Spices", "Pizza Spices", "Mustard Powder", "Zaatar", "Cardamom Powder", "Chilli Powder", "Coriander Powder", "Kudampuli", "Tamarind Paste", "Aniseeds", "Chilli Whole", "Sumac", "Mixed Spices", "Cardamom Whole", "Cinnamon Sticks", "Cumin Powder", "Mustard Seeds", "Fenugreek Seeds", "Coriander Whole", "Fennel Seeds", "Cardamom Whole", "Peppercorn", "Tamarind Whole", "Tamarind Whole", "Pumpkin Pie Spices", "Habbat Al Hamra", "Mixed Spices", "Black Salt Powder", "Nutmeg Powder", "Sesame Seeds", "Fennel Powder", "Fenugreek Powder", "Garlic Powder", "Aniseeds", "Shepherd's Pie Mix", "Paprika Powder", "Cloves Whole", "Cinnamon Whole", "Italian Seasoning", "Chilli Powder", "Chilli Powder", "Mustard Seeds", "Garlic Powder", "Turmeric Powder", "Lemon Dry", "Coriander Whole", "Coriander Powder", "Chilli Crushed", "Chilli Whole", "Cinnamon Powder", "Fenugreek Seeds", "Garam Masala Powder", "Pepper Powder", "Garam Masala Whole", "Cardamom Whole", "Fennel Seeds", "Cumin Seeds", "Coriander Powder", "Chilli Powder", "Turmeric Powder", "Peppercorn", "Pepper Powder", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Coriander Powder", "Chilli Powder", "Chilli Powder", "Turmeric Powder", "Pepper Coarse", "Cinnamon Powder", "Chilli Whole", "Cinnamon Whole", "Cumin Powder", "Ginger Powder", "Cloves Whole", "Mixed Spices", "Lemon Powder", "Falafel Spices", "Chilli Flakes", "Steak Spices", "Kabsa Spices", "Mandi Spices", "Cajun Seasoning", "Peppercorn", "Peppercorn", "Celery Seeds", "Peppercorn", "Allspice Berries", "Mace", "Fenugreek Seeds", "Coriander Whole", "Tamarind Whole", "Mango Powder", "Chilli Powder", "Coriander Powder", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Turmeric Powder", "Fennel Powder", "Fenugreek Powder", "Chilli Powder", "Turmeric Powder", "Chilli Powder", "Coriander Powder", "Chilli Powder", "Arabic Spices", "Margarine Spices", "BBQ Spices", "Natural Dye Spices", "Maternal Post Spices", "Biryani Spice", "Fish Spices", "Curry Powder", "Seafood Spices", "Chicken Spices", "Tamarind Seedless", "Tamarind Seedless", "Chilli Whole", "Assorted Spices", "Chilli Powder", "Mustard Powder", "Peppercorn", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Coriander Powder", "Turmeric Powder", "Turmeric Powder", "Mixed Spices", "Aniseeds", "Asafoetida Compounded", "Lemon & Cracked Pepper Seasoning", "Tomato Rice Powder", "Mixed Spices", "Coriander Powder", "Coriander Powder", "Tamarind Whole", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Cumin Powder", "Pepper Powder", "Ginger Powder", "Pepper Powder", "Mango Powder", "Chilli Crushed", "Peppercorn", "Coriander Whole", "Pepper Powder", "Turmeric Powder", "Cumin Powder", "Garlic Powder", "Peppercorn", "Cinnamon Whole", "Tamarind Paste", "Cinnamon Whole", "Turmeric Powder", "Chilli Whole", "Chilli Powder", "Chilli Crushed", "Turmeric Powder", "Mustard Seeds", "Cumin Powder", "Fenugreek Seeds", "Peppercorn", "Coriander Whole", "Cloves Whole", "Chilli Whole", "Turmeric Powder", "Pepper Powder", "Chilli Powder", "Coriander Powder", "Cumin Powder", "Chilli Powder", "Cardamom Whole", "Mixed Spices", "Chilli Powder", "Paprika Smoked", "Paprika Smoked", "Tamarind Paste", "Mustard Seeds", "Mandi Spices", "Shawarma Spice", "Shish Tawook Spices", "Salad Spices", "AlQoosi Spices", "Sumac", "Tamarind Whole", "Tamarind Whole", "Sumac", "Mixed Herbs", "Cinnamon Powder", "Turmeric Powder", "Garlic Granules", "Asafoetida Powder", "Cheddar Cheese Spices", "Peppercorn", "Peppercorns", "Garlic Crushed", "Cumin Powder", "Garlic Flakes", "Pepper Powder", "Chilli Crushed", "Ginger Powder", "Turmeric Powder", "Cinnamon Powder", "Asafoetida Powder", "Black Seeds", "Tamarind Whole", "Tamarind Paste", "Tamarind Sauce", "Coriander Powder", "Chilli Powder", "Turmeric Powder", "Chilli Crushed", "Salad Spices", "Shawarma Spice", "Pepper Powder", "Tamarind Whole", "Tamarind Whole", "Coriander Powder", "Fenugreek Seeds", "Kabab Spice", "Kebbeh Spice", "Shawarma Spice", "Cumin Powder", "Chilli Whole", "Cardamom Whole", "Peppercorn", "Fenugreek Seeds", "Cloves Whole", "Coriander Powder", "Chilli Powder", "Tamarind Whole", "Peppercorn", "Peppercorn", "Paprika Smoked", "Tamarind Whole", "Tamarind Whole", "Kudampuli", "Tamarind Seedless", "Shawarma Spices", "Mixed Spices", "Mixed Spices", "BBQ Spices", "Thyme", "Basil", "Rosemary", "Chicken Seasoning", "Majboos Spices", "Cinnamon Powder", "Nutmeg Powder", "Chicken Spices", "Chicken Spices", "Mustard Seeds", "Cumin Powder", "Ajwain", "Cloves Whole", "Coriander Seeds", "Cumin Powder", "Fennel Seeds", "Sesame Seeds", "BBQ Spices", "Onion Powder", "Paprika Powder", "Chilli Powder", "Asafoetida Powder", "Tamarind Paste", "Tamarind Paste", "Aniseeds", "Cumin Powder", "Sesame Seeds", "Nutmeg Powder", "Cloves Powder", "Cinnamon Powder", "Pepper Powder", "Pepper Powder", "Pepper Flakes", "Cinnamon Whole", "Paprika Powder", "Cinnamon Whole", "Tamarind Paste", "Sago Seeds", "Sago Seeds", "Chilli Powder", "Chilli Powder", "Cloves Powder", "Black Cumin Seeds", "Paprika Powder", "Zaatar", "Sumac", "Onion Powder", "Paprika Powder", "Mixed Spices", "Mixed Spices", "Dokka Powder", "BBQ Spices", "Seafood Spices", "Chicken Spices", "Meat Spices", "Potato Spices", "Kebda Spices", "Kabsa Spices", "Fenugreek Seeds", "Fennel Seeds", "Coriander Whole", "Cumin Powder", "Turmeric Powder", "Chilli Whole", "Chilli Powder", "Chilli Powder", "Coriander Whole", "Coriander Powder", "Cumin Powder", "Cumin Powder", "Turmeric Powder", "Chilli Whole", "Chilli Powder", "Mixed Spices", "Mixed Spices", "Shawarma Spices", "Pepper Powder", "Coriander Powder", "Turmeric Powder", "Pepper Powder", "Kabsa Spices", "Stew Spice Mix", "Seafood Spices", "BBQ Spices", "Biryani Spices", "Mixed Spices", "Cardamom Powder", "Chilli Powder", "Ginger Powder", "Cumin Powder", "Curry Powder", "Cloves Powder", "Cinnamon Powder", "Tamarind Paste", "Lemon Powder", "Chicken Seasoning", "Garlic Powder", "Ginger Powder", "Paprika Powder", "Mixed Spices", "Sumac", "Zaatar", "Cardamom Whole", "Shawarma Spice", "Paprika Powder", "Garlic Powder", "Onion Powder", "Paprika Powder", "Paprika Powder", "Chilli Flakes", "Nutmeg Powder", "Chilli Powder", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Cumin Powder", "Chilli Powder", "Garam Masala Whole", "Chilli Powder", "Chilli Paste", "Masala Tikki", "Chilli Powder", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Ginger Powder", "Mango Powder", "Chilli Powder", "Chilli Powder", "Cardamom Whole", "Pepper Powder", "Lemon Dry", "Chilli Powder", "Chilli Powder", "Onion Powder", "Tamarind Paste", "Coriander Powder", "Lime Powder", "Onion Powder", "Chicken Spices", "Prawn Spices", "Truffle Spices", "Truffle Cheese Spices", "Truffle Pesto Spices", "Truffle Porcini Spices", "Tamarind Whole", "Kabsa Spices", "Chilli Powder", "Chilli Powder", "Tamarind Sauce", "Tamarind Sauce", "Turmeric Powder", "Turmeric Whole", "Turmeric Slices", "Chilli Whole", "Chilli Whole", "Chilli Whole", "Ginger Slices", "Mixed Spices", "Cinnamon Whole", "Chicken BBQ Seasoning", "Pizza Seasoning", "Chips & Potato Seasoning", "Chicken Wings Seasoning", "Roast Veggies Seasoning", "Chilli Powder", "Turmeric Powder", "Coriander Powder", "Turmeric Powder", "Mustard Seeds", "Chicken Broasted Mix", "Chicken Broasted Mix", "Basil", "Chilli Flakes", "Cinnamon Powder", "Garlic Crunchy", "Garlic Powder", "Nutmeg Powder", "Oregano", "Paprika Smoked", "Onion Powder", "Tamarind Seedless", "Fenugreek Seeds", "Chilli Flakes", "BBQ Seasoning", "Chilli Flakes", "Paprika Smoked", "BBQ Spices", "Argentinian Grill Spices", "Asafoetida Powder", "Karam Podi", "Karam Podi", "Kandi Podi", "Karam Podi", "Chilli Powder", "Coriander Powder", "Turmeric Powder", "Chilli Powder", "Turmeric Powder", "Cinnamon Powder", "Fenugreek Seeds", "Mustard Seeds", "Mustard Seeds", "Cardamom Whole", "Cardamom Whole", "Cardamom Whole", "Cardamom Whole", "Chilli Flakes", "Paprika Powder", "Paprika Smoked", "Chilli Whole", "Lemon Dry", "Mixed Spices", "Biryani Spices", "Mixed Spices", "Cardamom Whole", "Coriander Powder", "Garlic Powder", "Paprika Smoked", "Onion Powder", "Turmeric Powder", "Pepper Powder", "Cardamom Powder", "Coriander Seeds", "Tamarind Paste", "Piccantissimo", "Arrabbiata Spices", "Chilli Powder"]

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
dct = {2: [1576179, 1992982, 2014666, 2269147],
       3: [1104111, 2135654]}
for key, value in dct.items():
    for i in value:
        df.loc[df['material_id'] == i, 'item_count'] = key
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("sandbox.pj_assortment_attributes")

# COMMAND ----------

# import os
# folder_path = '/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/spices'
# if not os.path.exists(folder_path):
#     os.makedirs(folder_path)
# df.to_csv(f'{folder_path}/spices_attributes.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Ice Cream Impulse

# COMMAND ----------

df = run('2023-07-01', '2024-06-28', 'ICE CREAM & DESSERTS', 'ICE CREAM IMPULSE')
df.display()

# COMMAND ----------

volume = [120, 120, 120, 100, 70, 50, 41.8, 110, 1.25, 1, 125, 125, 125, 70, 65, 125, 110, 125, 125, 125, 125, 48, 110, 70, 100, 100, 110, 110, 110, 1, 125, 140, 180, 90, 120, 120, 120, 125, 120, 120, 120, 120, 80, 100, 100, 100, 100, 100, 95, 95, 120, 100, 60, 80, 473, 120, 125, 125, 54.6, 125, 110, 120, 120, 170, 70, 70, 70, 100, 100, 100, 105, 110, 125, 90, 110, 100, 100, 145, 95, 100, 120, 65, 150, 90, 120, 65, 105, 39.1, 50, 80, 80, 100, 438, 110, 110, 80, 80, 631, 90, 80, 140, 90, 110, 110, 110, 750, 100, 350, 1, 100, 95, 95, 120, 125, 73.5, 185, 140, 360, 95, 90, 125, 73, 120, 120, 258, 258, 258, 100, 2, 80, 258, 75, 258, 258, 90, 120, 48, 48, 48, 48, 80, 34.5, 95, 100, 140, 65, 110, 65, 150, 150,150, 270, 258, 750, 60, 110, 110, 120, 140, 250, 464, 292, 260, 260, 130, 100, 110, 70, 70, 70, 70, 58, 62, 125, 90, 125, 270, 270, 270, 270, 356, 83, 110, 110, 135, 258, 258, 55, 120, 120, 120, 60, 90, 58, 58, 58, 460, 750, 500, 260, 480, 60, 443, 443, 443, 443, 118, 80, 81, 1.5, 878, 244, 437.7, 70, 70, 150, 40.8, 180, 70, 80, 120, 120, 60, 60, 180, 150, 150, 150, 90, 210, 210, 210, 210, 156, 156, 156, 156, 85, 85, 325, 325, 60, 60, 260, 55, 73, 325, 130, 60, 90, 90, 90, 100, 90, 90, 80, 65, 65, 60, 90, 180, 156, 156, 156, 210, 210, 210, 210, 39, 90, 105, 105, 210, 360, 360, 85, 85, 353, 100, 300, 300, 480, 480]

units = ['ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'L', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'KG', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'KG', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'G', 'ML', 'KG', 'ML', 'G', 'ML', 'G', 'G', 'ML', 'ML', 'G', 'G', 'G', 'G', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'L', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'G', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML']

type = ['Vanilla Tub', 'Chocolate Tub', 'Strawberry Tub', 'Vanilla Almond Stick', 'Water Ice', 'Vanilla Bar', 'Chocolate Bar', 'Vanilla & Chocolate Cone', 'Cassata', 'Vanilla Cookies Tub', 'Vanilla Cookies Tub', 'Strawberry Tub', 'Praline & Cream Tub', 'Vanilla Sick', 'Pista Kulfi', 'Strawberry & Vanilla Cup', 'Pista Cone', 'Vanilla Cup', 'Strawberry Cup', 'Mango Cup', 'Chocolate Cup', 'Peanut Buttter Bar', 'Vanilla Cone', 'Chocolate Cone', 'Vanilla Cone', 'Vanilla & Chocolate Cone', 'Butter Pecan Stick', 'Vanilla Almond Stick', 'Vanilla & Chocolate Stick', 'Ice Cubes', 'Chocolate Tub', 'Pista & Cashew Kulfi', 'Vanilla Sundae Cup', 'Vanilla Sandwich', 'Chocolate Cone', 'Strawberry Cone', 'Praline & Cream Cone', 'Chocolate Cone', 'Chocolate Cone', 'Vanilla Almond Stick', 'Vanilla Peanuts Stick', 'Vanilla Cashew Stick', 'Vanilla Cookies Stick', 'Chocolate Tub', 'Macadamia Nut Tub', 'Vanilla Cup', 'Vanilla Cookies Tub', 'Strawberry Cheesecake Tub', 'Vanilla Almond Stick', 'Chocolate Almond Stick', 'Butterscotch Cone', 'Strawberry Cup', 'Strawberry & Chocolate M&M Stick', 'Vanilla Macadamia Nut Stick', 'Vanilla Tub', 'Chocolate Cone', 'Butterscotch Cashew Tub', 'Vanilla Tub', 'Vanilla & Chocolate Bar', 'Tiramisu Tub', 'Vanilla Almond Stick', 'Chocolate Brownie Yoghurt', 'Chocolate Cookies Dough', 'Praline & Cream Cup', 'Almond & Pista Kulfi', 'Pista Kulfi', 'Mango Kulfi', 'Vanilla Stick', 'Vanilla Almond Stick', 'Vanilla White Chocolate Stick', 'Orange Lolly', 'Vanilla & Chocolate Hazelnut Stick', 'Vanilla & Chocolate Cone', 'Milk Chocolate & Peanut Stick', 'Chocolate & Hazenut Stick', 'Vanilla & Cocoa Cup', 'Vanilla & Strawberry Cup', 'Vanilla & Chocolate Caramel Sandwich', 'Chocolate Stick', 'Salted Caramel Tub', 'Vanilla & Chocolate Cone', 'Pista & Cashew Kulfi', 'Cupcake Ice Cream', 'Vanilla Sandwich', 'Vanilla Almond Stick', 'Fruit Lolly', 'Orange Lolly', 'Coconut & Milk Bar', 'Vanilla & Caramel Bar', 'Chocolate Brownie Stick', 'White Chocolate & Almond Stick', 'Vanilla Stick', 'Fruit Lolly', 'Oreo Stick', 'Oreo Cone', 'Vanilla & Caramel Almond Stick', 'Chocolate Almond Stick', 'Caramel Strawberry & Honeycomb Stick', 'Vanilla Sandwich', 'Salted Caramel Stick', 'Vanilla Caramel Cone', 'Chocolate Stick', 'Vanilla Cone', 'Vanilla & Chocolate Cone', 'Pista Kulfi', 'Buco Salad Tub', 'Butterscotch Stick', 'Fruit Lolly', 'Ice Cubes', 'Vanilla & Cookie Sandwich', 'White Chocolate & Almond Stick', 'Vanilla & Caramel Stick', 'Vanilla & Waffle Pieces Tub', 'Vanilla Cone', 'Peanut Butter & Chocolate Stick', 'Vanilla & White Chocolate Almond Cup', 'Vanilla Oreo Cup', 'Chocolate & Blackberry & Black Mulberry Stick', 'Mulberry & Blackberry Stick', 'Vanilla & Chocolate Cone', 'Chocolate Cone', 'Watermelon Lolly', 'Coconut Cone', 'Cotton Candy Cone', 'Green Tea Dough', 'Strawberry Dough', 'Mango Dough', 'Vanilla Sandwich', 'Ice Cubes', 'Peanut Butter Chocolate Stick', 'Chocolate Dough', 'Mango & Raspberry Stick', 'Coffee & Caramel Dough', 'Chocolate & Marshmallow Dough', 'Chocolate Stick', 'Cookies Cone', 'Kewda Kulfi', 'Mango Kulfi', 'Elaichi Kulfi', 'Kesar Kulfi', 'Chocolate Almond Stick', 'Peanuts & Caramel Bar', 'White Chocolate & Cookies Stick', 'Pista Bar', 'Pista Cone', 'Vanilla Stick', 'Vanilla & Cocoa Stick', 'Cocoa Stick', 'Cotton Candy Cup', 'Mango Cup', 'Raspberry Cup', 'Vanilla Stick', 'Cookies Dough', 'Pista & Cashew Tub', 'Kulfi', 'Vanilla & Chocolate Cone', 'Vanilla & Chocolate Cone', 'Fruit Cone', 'Chocolate Almond Balls', 'Bubblegum Lolly', 'Fruit Lolly', 'Orange Lolly', 'Milk Chocolate & Nut Cone', 'Strawberry Cone', 'Rose Cone', 'Honeycomb & Milk Chocolate Stick', 'Milk Chocolate & Caramel Cone', 'Mango Lolly', 'Raspberry Lolly', 'Strawberry Lolly', 'Fruit Stick', 'Chocolate Hazelnut Stick', 'Chocolate Stick', 'Cotton Candy Cup', 'Vanilla & Chocolate Tub', 'Butterscotch Cup', 'Chocolate & Pecan Stick', 'Coconut & Mango Stick', 'Dark Chocolate & Berry Stick', 'Salted Caramel & Macadamia Stick', 'Chocolate Bar', 'Chocolate Almond Cup', 'Chocolate Caramel Cone', 'Chocolate Hazelnut Cone', 'Oreo Chocolate Sandwich', 'Guava Dough', 'Vanilla Dough', 'Peanut Butter & Chocolate Stick', 'Chocolate Cone', 'Chocolate Peanut Cone', 'Cone', 'Mango Stick', 'Berry & White Chocolate Stick', 'Chocolate Almond Stick', 'Vanilla Stick', 'Fruit Lolly', 'Chocolate Tub', 'Fruit Stick', 'Lolly', 'Fruit Lolly', 'Fruit Lolly', 'Mango Stick', 'Cherry & Grape Popsicle', 'Lime & Orange Popsicle', 'Strawberry & Lemon Popsicle', 'Strawberry & Mango Popsicle', 'Fruit Punch & Cotton Candy Popsicle', 'Blue Raspberry, Cherry & Lemon Popsicle', 'Orange Popsicle', 'Orange, Cherry & Grape Popsicle', 'Chocolate Popsicle', 'Strawberry Popsicle', 'Red White & Boom Popsicle', 'Chocolate & Hazelnut Stick', 'Coconut Stick', 'Chocolate Cup', 'White Chocolate Bar & Peanut', 'Goat Milk Cup', 'Raspberry Cheesecake Cup', 'Vanilla & Chocolate Bar', 'Strawbery Stick', 'Vanilla Stick', 'Pastry', 'Vanilla Sandwich', 'Safron Chocolate Milk Sandwich', 'Strawberry Sandwich', 'Red Bean Sandwich', 'Chocolate Sandwich', 'Chocolate Stick', 'Ube Dough', 'Mixed Berry Dough', 'Passion Fruit Dough', 'Strawberry Dough', 'Fruit Dough', 'Green Tea Dough', 'Mango Dough', 'Strawberry Dough', 'Caramel & Popcorn Stick', 'Mango & Coconut Stick', 'Peanut Butter Stick', 'Salted Caramel Stick', 'Chocolate & Peanut Bar', 'Chocolate Brownie Bar', 'Fruit Lolly', 'Oreo Sandwich', 'Watermelon Popsicle', 'Chocolate Stick', 'Pista Cone', 'Chocolate Stick', 'Chocolate & Cookie Stick', 'Chocolate & Caramel Cone', 'Chocolate & Peanut Butter Stick', 'Vanilla & Peanut Butter Cone', 'White Chocolate & Hazelnut Cone', 'Chocolate Cone', 'Chocolate & Caramel Stick', 'Fruit Lolly', 'Fruit Lolly', 'Oreo Stick', 'Oreo Stick', 'Chocolate & Vanilla Sundae Cup', 'Coconut Dough', 'Vanilla Dough', 'Chocolate Dough', 'Pumpkin Spice Dough', 'Apple Dough', 'Peppermint Dough', 'Cereal & Milk Dough', 'Chocolate & Peanut Bar', 'Coconut Stick', 'Raspberry Popsicle', 'Orange Popsicle', 'Chocolate Dough', 'Strawberry & Blue Raspberry Popsicle', 'Fruit Punch & Cotton Candy Popsicle', 'Pink Lemonade Stick', 'Chocolate Stick', 'Chocolate & Almond Stick', 'Coconut Stick', 'Vanilla Sandwich', 'Butterscotch Sandwich', 'Almond, Pista & Kesar Kulfi', 'Malai Kulfi']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
dct = {4: [85, 162, 163, 165, 167, 168, 169, 187, 199, 239, 240, 243, 244, 245, 246],
       5: [170], 6: [92, 98, 117, 119, 248, 267],
       8: [200], 10: [198], 18: [210], 32: [209]}
for key, value in dct.items():
    for i in value:
        df.at[i, 'item_count'] = key
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

import os
folder_path = '/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/ice_cream_impulse'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)
df.to_csv(f'{folder_path}/ice_cream_impulse_attributes.csv', index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Pickles

# COMMAND ----------

df = run('2023-08-01', '2024-07-29', 'SAUCES & PICKLES', 'PICKLES')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Akif's Extracted Attributes

# COMMAND ----------

# query = """
# SELECT material_id, material_name, content, content_unit, item_count, type
# FROM dev.sandbox.am_pickles_attr
# ORDER BY 1
# """

# akif_df = spark.sql(query).toPandas()

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([2153531, 2154915, 2154916, 2154917]), 'content'] = 9
# akif_df.loc[akif_df['material_id'].isin([2153531, 2154915, 2154916, 2154917]), 'content_unit'] = 'GM'
# akif_df.loc[akif_df['material_id'].isin([397374, 397376]), 'content'] = 400

# akif_df['content_unit'] = np.where(akif_df.content_unit == 'GM', 'G', akif_df.content_unit)

# akif_df['content'] = akif_df['content'].astype('int32')

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([4425, 478641]), 'type'] = 'Mango'
# akif_df.loc[akif_df['material_id'].isin([4460, 152751, 399982, 435622, 810230, 958500]), 'type'] = 'Lemon'
# akif_df.loc[akif_df['material_id'].isin([68844]), 'type'] = 'Shallot'
# akif_df.loc[akif_df['material_id'].isin([233759]), 'type'] = 'Lime & Tomato'
# akif_df.loc[akif_df['material_id'].isin([467362, 467363]), 'type'] = 'Mixed Veg'
# akif_df.loc[akif_df['material_id'].isin([475959]), 'type'] = 'Mango & Mixed Veg'
# akif_df.loc[akif_df['material_id'].isin([656541]), 'type'] = 'Assorted'
# akif_df.loc[akif_df['material_id'].isin([927462]), 'type'] = 'Olive'
# akif_df.loc[akif_df['material_id'].isin([1454900]), 'type'] = 'Yellow Squash'
# akif_df.loc[akif_df['material_id'].isin([2008578]), 'type'] = 'Tomato'
# akif_df.loc[akif_df['material_id'].isin([2083107, 2083108, 2107715, 2107717, 2154915]), 'type'] = 'Gherkins'
# akif_df.loc[akif_df['material_id'].isin([2322816]), 'type'] = 'Chilli'

# COMMAND ----------

# akif_df = akif_df[['material_id', 'material_name', 'type']]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Type Modifications

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mango, Mixed, Cucumber

# COMMAND ----------

# materials = [[1534500, 1947393, 1534531], [2004177], [2015192, 826762], [2016903, 2016910], [2035728], [4380, 4455, 4474, 721864, 4426, 4494, 297796, 383860, 1167125]]
# mangoes = ['Mango & Lime', 'Jackfruit Seeds, Mango & Prawns', 'Mango & Lemon', 'Mango & Carrot', 'Mango & Fenugreek', 'Mango']
# for i in range(len(materials)):
#     for j in range(len(materials[i])):
#         akif_df.loc[akif_df['material_id'] == materials[i][j], 'type'] = mangoes[i]

# COMMAND ----------

# akif_df.loc[akif_df['material_id'] == 423855, 'type'] = 'Mixed Hot'

# akif_df['type'] = np.where(akif_df.type == 'Mixed', 'Mixed Veg', akif_df.type)

# COMMAND ----------

# # All materials in Cucumber are already accurately labeled

# akif_df[akif_df['type'].str.contains('Cucumber')].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lemon, Garlic, Gherkin

# COMMAND ----------

# materials = [[4398, 4479, 72319, 187474, 287632, 383859, 397374, 478643, 726297, 1167122, 1640542, 1891862], [4483], [4498], [826762], [2008828]]
# lemons = ['Lime', 'Lime Hot', 'Lime & Chilli', 'Mango & Lime', 'Lemon & Pepper']
# for i in range(len(materials)):
#     for j in range(len(materials[i])):
#         akif_df.loc[akif_df['material_id'] == materials[i][j], 'type'] = lemons[i]

# COMMAND ----------

# akif_df.loc[akif_df['material_id'] == 2008827, 'type'] = 'Garlic & Chilli'
# akif_df.loc[akif_df['material_id'] == 856111, 'type'] = 'Garlic'

# COMMAND ----------

# # All materials in Gherkin are already accurately labeled

# akif_df[akif_df['type'].str.contains('Gherkin')].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Chilli, Jalapeno, Ginger

# COMMAND ----------

# akif_df.loc[akif_df['material_id'] == 4496, 'type'] = 'Mango & Chilli'

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([1804260, 2006800]), 'type'] = 'Jalapeno Sliced'
# akif_df.loc[akif_df['material_id'].isin([425269, 1367548, 1379938]), 'type'] = 'Jalapeno'

# akif_df['type'] = np.where(akif_df.type == 'Jalapeno Pepper Slice', 'Jalapeno Pepper Sliced', np.where(akif_df.type == 'Jalapeno Slice', 'Jalapeno Sliced', akif_df.type))

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([722194]), 'type'] = 'Tamarind & Ginger'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pepper, Artichoke, Eggplant

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([895500]), 'type'] = 'Banana Pepper'
# akif_df.loc[akif_df['material_id'].isin([1308180]), 'type'] = 'Pink Peppercorn'

# COMMAND ----------

# # All materials in Artichoke and Eggplant are already accurately labeled

# akif_df[akif_df['type'].str.contains('Artichoke|Eggplant')].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Black Olive, Tomato, Mushroom

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([2035402, 2037990]), 'type'] = 'Black Olive & Pepper'

# COMMAND ----------

# # All materials in Tomato and Mushroom are already accurately labeled

# akif_df[akif_df['type'].str.contains('Tomato|Mushroom')].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Bread and Butter, Prawns, Hot Lime

# COMMAND ----------

# # All materials in Bread and Butter, and Prawns are already accurately labeled

# akif_df[akif_df['type'].str.contains('Bread and Butter|Prawns')].display()

# COMMAND ----------

# akif_df['type'] = np.where(akif_df.type == 'Hot Lime', 'Hot Lime', akif_df.type)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gooseberry, Coriander, Onion

# COMMAND ----------

# # All materials in Gooseberry and Coriander are already accurately labeled

# akif_df[akif_df['type'].str.contains('Gooseberry|Coriander')].display()

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([1254891]), 'type'] = 'Onion'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Puliyinchi, Fish, Fresh Kimchi

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([167098]), 'type'] = 'Fish'
# akif_df.loc[akif_df['material_id'].isin([877232, 1576303]), 'type'] = 'Tamarind & Ginger'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mint, Turnip, Cornichon, Carrot

# COMMAND ----------

# # All materials in Mint and Turnip are already accurately labeled

# akif_df[akif_df['type'].str.contains('Mint|Turnip')].display()

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([12111]), 'type'] = 'Carrot & Chilli'
# akif_df.loc[akif_df['material_id'].isin([452265, 856089]), 'type'] = 'Gherkin'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dates, Sliced Beetroot

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([320828, 1320559]), 'type'] = 'Beetroot Sliced'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Others

# COMMAND ----------

# akif_df.loc[akif_df['material_id'].isin([1891751]), 'type'] = 'Gooseberry'
# akif_df.loc[akif_df['material_id'].isin([4468]), 'type'] = 'Bitter Gourd'
# akif_df.loc[akif_df['material_id'].isin([488934]), 'type'] = 'Beetroot Cut'
# akif_df.loc[akif_df['material_id'].isin([2044062]), 'type'] = 'Green Olive & Carrot'
# akif_df.loc[akif_df['material_id'].isin([490749]), 'type'] = 'Gujarati Choondo'
# akif_df.loc[akif_df['material_id'].isin([1852413]), 'type'] = 'Pepper'
# akif_df.loc[akif_df['material_id'].isin([2201577]), 'type'] = 'Paprika Sliced'
# akif_df.loc[akif_df['material_id'].isin([1380136]), 'type'] = 'Rosemary & Hot Pepper'
# akif_df.loc[akif_df['material_id'].isin([1394235]), 'type'] = 'Sauerkraut'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final DF

# COMMAND ----------

# for i in akif_df['content'].tolist():
#     print(i, end = ', ')

# for i in akif_df['item_count'].tolist():
#     print(i, end = ', ')

# for i in akif_df['content_unit'].tolist():
#     print("'" + i + "'", end = ', ')

# for i in akif_df['type'].tolist():
#     print('"' + i + '"', end = ', ')

# COMMAND ----------

volume = [300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 400, 400, 400, 380, 330, 330, 330, 330, 330, 400, 400, 400, 400, 400, 400, 400, 400, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 350, 300, 32, 1, 1500, 700, 330, 700, 700, 700, 700, 700, 1, 320, 340, 300, 300, 300, 340, 473, 473, 360, 400, 330, 1, 300, 300, 16, 350, 400, 400, 400, 400, 400, 400, 16, 340, 340, 300, 400, 32, 400, 400, 600, 600, 600, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 520, 312, 283, 283, 283, 283, 283, 283, 215, 400, 400, 400, 400, 400, 360, 380, 300, 300, 320, 400, 500, 500, 500, 500, 300, 400, 400, 400, 400, 400, 710, 340, 350, 473, 24, 24, 12, 1, 600, 341, 700, 177, 177, 700, 1, 1, 1, 300, 1, 300, 300, 350, 350, 350, 350, 300, 300, 350, 1, 400, 320, 400, 680, 350, 400, 350, 680, 680, 400, 400, 473, 300, 12, 455, 400, 237, 6, 300, 520, 1, 1, 1, 400, 1, 700, 9, 310, 400, 400, 400, 400, 400, 400, 473, 400, 230, 400, 105, 455, 9, 32, 115, 120, 235, 235, 680, 700, 680, 680, 700, 710, 700, 680, 670, 660, 360, 310, 700, 235, 400, 400, 415, 250, 1, 400, 400, 400, 400, 160, 160, 640, 320, 330, 330, 330, 330, 250, 200, 290, 290, 290, 440, 340, 400, 400, 180, 180, 180, 340, 400, 400, 400, 400, 400, 400, 250, 500, 400, 700, 700, 400, 1, 500, 600, 600, 600, 290, 485, 400, 400, 300, 400, 300, 680, 680, 300, 600, 550, 560, 550, 540, 570, 500, 500, 680, 250, 250, 680, 680, 680, 600, 1, 750, 1, 750, 1, 750, 250, 250, 300, 700, 300, 300, 300, 300, 300, 330, 600, 680, 600, 600, 600, 300, 340, 180, 180, 180, 180, 600, 970, 970, 482, 482, 290, 150, 550, 300, 250, 710, 300, 300, 300, 660, 1, 9, 9, 9, 9, 350, 205, 210, 350, 980, 350, 400, 400, 400, 400, 300, 300, 660, 660, 660, 660]

units = ['G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'OZ', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'G', 'G', 'G', 'KG', 'G', 'ML', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'G', 'ML', 'OZ', 'OZ', 'OZ', 'KG', 'G', 'G', 'G', 'ML', 'ML', 'G', 'KG', 'KG', 'KG', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'OZ', 'G', 'G', 'ML', 'OZ', 'G', 'G', 'KG', 'KG', 'KG', 'G', 'KG', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'L', 'G', 'L', 'G', 'L', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'G', 'G', 'G', 'G', 'L', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

item_count = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

type = ["Mango", "Mango", "Tomato", "Gongura", "Garlic", "Mixed Veg", "Ginger", "Green Chilli", "Lime", "Red Chilli", "Mango", "Mango", "Onion", "Mango", "Lemon", "Mixed Veg", "Chilli", "Mango", "Mango", "Mixed Veg", "Lemon", "Garlic", "Garlic", "Mango", "Gooseberry", "Lemon", "Mango", "Mixed Veg", "Bitter Gourd", "Ginger", "Mango", "Hot Mango", "Lime", "Lime Hot", "Mixed Veg", "Mango", "Mango", "Mango", "Mango", "Mango & Chilli", "Lime & Chilli", "Garlic", "Garlic", "Mango", "Carrot & Chilli", "Gherkin", "Cucumber", "Cucumber", "Cucumber", "Mixed Veg", "Mixed Veg", "Shallot", "Garlic", "Chilli", "Mixed Veg", "Mango", "Chilli", "Chilli", "Ginger", "Lime", "Chilli", "Jalapeno Sliced", "Gherkin", "Gherkin", "Lemon", "Fish", "Lime", "Mixed Veg", "Mixed Veg", "Lime & Tomato", "Jalapeno Pepper Sliced", "Lime", "Mango", "Hot Lime", "Mango", "Garlic", "Mixed Veg", "Prawns", "Gherkin", "Beetroot", "Beetroot Sliced", "Garlic", "Mango", "Gherkin", "Mango", "Mango", "Mixed Veg", "Cucumber", "Cucumber", "Prawns", "Lemon", "Lime", "Mango", "Mixed Veg", "Mango", "Garlic", "Dates", "Mango", "Lime", "Mango", "Lemon", "Eggplant", "Hot Mango", "Mixed Hot", "Hot Lime", "Hot Lime", "Hot Chilli", "Mango", "Jalapeno", "Mango", "Mango", "Lemon", "Mixed Veg", "Garlic", "Lemon", "Gherkin", "Mango", "Mixed Veg", "Lemon", "Ginger", "Lemon", "Mixed Veg", "Mixed Veg", "Mixed Veg", "Coriander", "Mango & Mixed Veg", "Mango", "Mango", "Lime", "Garlic", "Gherkin", "Beetroot Cut", "Gujarati Choondo", "Bread and Butter", "Gherkin", "Gherkin", "Jalapeno", "Cucumber", "Cucumber", "Mushroom", "Cucumber", "Green Pepper", "Red Pepper", "Tomato", "Mango", "Lemon", "Mixed Veg", "Assorted", "Cucumber", "Mango", "Mixed Veg", "Mango", "Tamarind & Ginger", "Lime", "Garlic", "Chilli", "Mixed Veg", "Lemon", "Cucumber", "Mango & Lime", "Garlic", "Chilli", "Cucumber", "Gherkin", "Garlic", "Cucumber", "Cucumber", "Cucumber", "Tamarind & Ginger", "Bilimbi", "Bread and Butter", "Mango", "Banana Pepper", "Beetroot", "Mango", "Olive", "Artichoke", "Mango", "Lemon", "Cucumber", "Mango", "Lemon", "Fish", "Cucumber", "Grape Leaves", "Hot Cucumber", "Piccalilli", "Lime", "Garlic", "Mango", "Mango", "Fish", "Prawns", "Jalapeno Pepper Sliced", "Red Cabbage", "Artichoke", "Onion", "Pink Peppercorn", "Beetroot Sliced", "Gherkin", "Bread and Butter", "Green Pepper", "Caper", "Jalapeno Sliced", "Jalapeno Sliced", "Mixed Veg", "Jalapeno", "Gherkin", "Gherkin", "Mixed Veg", "Jalapeno", "Cucumber", "Rosemary & Hot Pepper", "Hot Pepper", "Pepper", "Sauerkraut", "Jalapeno", "Garlic", "Jalapeno Sliced", "Cherry Pepper", "Jalapeno", "Yellow Squash", "Artichoke", "Mixed Veg", "Mango", "Mango & Lime", "Mango & Lime", "Tamarind & Ginger", "Fresh Kimchi", "Cooked Kimchi", "Hot Pepper", "Chilli", "Mixed Veg", "Mango", "Lime", "Garlic", "Mango", "Eggplant", "Cherry Pepper", "Yellow Pepper", "Jalapeno", "Jalapeno Pepper Sliced", "Jalapeno Sliced", "Mango", "Mixed Veg", "Hot Pepper", "Hot Pepper", "Pepper", "Chilli", "Ginger", "Gooseberry", "Lime", "Mixed Veg", "Mango", "Mango", "Dates", "Kashmiri", "Mango", "Cucumber", "Cucumber", "Mango & Lime", "Mixed Veg", "Mixed Veg", "Artichoke", "Turnip", "Green Pepper", "Mushroom", "Garlic", "Lemon", "Mango", "Mango", "Jackfruit Seeds, Mango & Prawns", "Gooseberry", "Jalapeno Sliced", "Gherkin", "Amba Haldar", "Mixed Veg", "Tomato", "Red Pepper", "Hot Pepper", "Eggplant", "Jalapeno Pepper Sliced", "Mixed Veg", "Mango", "Garlic", "Garlic & Chilli", "Lemon & Pepper", "Mixed Veg", "Cucumber", "Cucumber", "Mango", "Mango & Lemon", "Mango", "Mango & Carrot", "Lemon", "Lemon", "Onion", "Tomato", "Mango & Carrot", "Mint", "Mixed Veg", "Black Olive", "Black Olive & Pepper", "Mango & Fenugreek", "Black Olive", "Black Olive & Pepper", "Green Olive & Carrot", "Turnip", "Cucumber", "Cucumber", "Eggplant", "Mixed Veg", "Mushroom", "Yellow Pepper", "Artichoke", "Cherry Pepper", "Cherry Pepper", "Tomato", "Green Pepper", "Gherkins", "Gherkins", "Jalapeno Sliced", "Red Pepper", "Garlic", "Gherkin", "Gherkins", "Gherkins", "Garlic", "Gherkin", "Coriander", "Mint", "Mango", "Cucumber", "Gherkin", "Gherkin", "Gherkins", "Salsa Picante", "Garlic", "Jalapeno Sliced", "Jalapeno Sliced", "Jalapeno", "Paprika Sliced", "Gherkin", "Gherkin", "Mixed Veg", "Lemon", "Ginger", "Gooseberry", "Carrot", "Ginger", "Cucumber", "Chilli", "Hot Pepper", "Mixed Veg"]

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = item_count
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Cakes & Gateaux

# COMMAND ----------

df = run('2023-08-01', '2024-07-29', 'ICE CREAM & DESSERTS', 'CAKES & GATEAUX')
df.display()

# COMMAND ----------

volume = [510, 510, 510, 400, 300, 150, 150, 485, 510, 510, 510, 400, 240, 255, 300, 600, 453, 180, 1.02, 350, 100, 350, 350, 500, 390, 555, 397, 297, 297, 170, 170, 170, 170, 397, 400, 425, 800, 800, 100, 100, 160, 170, 180, 630, 540, 540, 540, 240, 240, 180, 85, 350, 550, 550]

units = ['G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'ML', 'ML', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

type = ['Chocolate Cake', 'Vanilla Cake', 'Chocolate & Vanilla Cake', 'Chocolate & Caramel Cake', 'Jam Cake', 'Munggo Cake', 'Ube Cake', 'Strawberry Cheesecake', 'Chocolate Cake', 'Chocolate & Caramel Cake', 'Vanilla Cake', 'Chocolate & Almond Cake', 'Pancake', 'Pancake', 'Toaster Cake', 'Crepe', 'Butter Pound Cake', 'Chocolate Cake', 'Chocolate Cheesecake', 'Cheesecake', 'Cheesecake', 'Chocolate Cake', 'Cheesecake', 'Milk Cake', 'Raspberry Cheesecake', 'Chocolate Cake', 'Chocolate Donut', 'Glazed Chocolate Donut', 'Powdered Sugar Donut', 'Cinnamon Donut', 'Glazed Donut', 'Powdered Sugar Donut', 'Glazed Chocolate Donut', 'Glazed Donut', 'Chocolate & Hazelnut Cake', 'Cheesecake', 'Praline Ice Cream Cake', 'Vanilla Ice Cream Cake', 'Strawberry Cheesecake', 'Cherry Cheesecake', 'Cheesecake', 'Cheesecake', 'Chocolate Cake', 'Crepe', 'Chocolate Crepe', 'Apple & Cinnamon Crepe', 'Wild Fruits Crepe', 'Banana Pancake', 'Pancake', 'White Chocolate Cake', 'Cheesecake', 'Blueberry & Red Velvet Cake', 'Chocolate & Black Current Cake', 'Strawberry Cheesecake']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'] == 2285637, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Peanut Butter

# COMMAND ----------

df = run('2023-08-01', '2024-07-29', 'PRESERVATIVES & SPREADS', 'PEANUT BUTTER')
df.display()

# COMMAND ----------

volume = [12, 12, 510, 340, 340, 12, 1, 1, 510, 340, 340, 28, 28, 16.3, 16.3, 510, 227, 751, 454, 340, 16, 462, 462, 28, 510, 462, 462, 462, 12, 18, 510, 280, 280, 280, 280, 18, 15, 510, 340, 12, 340, 12, 425, 462, 425, 12, 12, 18, 16, 16, 16, 440, 16, 16, 12, 18, 462, 28, 454, 453, 454, 12, 454, 454, 454, 453, 340, 184, 400, 400, 510, 340, 340, 340, 1, 1, 16, 16, 1, 340, 16, 16, 16, 16, 340, 250, 16, 454, 454, 170, 397, 170, 340, 340, 340, 320, 320, 340, 453, 453, 453, 453, 453, 453, 453, 453, 510, 510, 225, 310, 310, 225, 310, 225, 310, 453, 453, 453, 453, 510, 454, 1, 16, 16, 340, 340, 400, 400, 440, 340, 325, 325]

units = ['OZ', 'OZ', 'G', 'G', 'G', 'OZ', 'KG', 'KG', 'G', 'G', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'OZ', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'OZ', 'OZ', 'G', 'G', 'OZ', 'G', 'OZ', 'G', 'G', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'OZ', 'G', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'KG', 'OZ', 'OZ', 'KG', 'G', 'OZ', 'OZ', 'OZ', 'OZ', 'G', 'G', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'KG', 'OZ', 'OZ', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'G']

type = ['Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Crunchy', 'Smooth', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Smooth', 'Creamy', 'Smooth', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Crunchy', 'Crunchy', 'Creamy', 'Creamy', 'Creamy', 'Creamy', 'Crunchy', 'Smooth', 'Crunchy', 'Smooth', 'Creamy', 'Creamy', 'Crunchy', 'Crunchy', 'Whipped', 'Creamy', 'Creamy', 'Crunchy', 'Creamy & Roasted Honey', 'Creamy & Honey', 'Creamy', 'Crunchy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy & Honey', 'Creamy & Coconut', 'Blended With Maple Syrup', 'Chocolate', 'White Chocolate', 'Crunchy', 'Smooth', 'Creamy', 'Smooth', 'Powder', 'Crunchy', 'Smooth', 'Creamy', 'Crunchy Dark Roasted', 'Smooth', 'Crunchy', 'Smooth', 'Crunchy', 'Creamy', 'Crunchy', 'Smooth', 'Chocolate & Hazelnut', 'Crunchy', 'Creamy', 'Honey', 'Crunchy', 'Crunchy', 'Creamy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Smooth', 'Smooth', 'Creamy', 'Creamy', 'Crunchy', 'Creamy & Honey', 'Cruchy & Honey', 'Creamy', 'Crunchy Chia & Flaxseed', 'Creamy & Chocolate', 'Creamy & White Chocolate', '', 'Crunchy', 'Crunchy', 'Crunchy & Honey', 'Smooth', 'With Orange', 'With Orange', 'With Blackberry', 'With Blackberry', 'Creamy & White Chocolate', 'Crunchy', 'Creamy', 'Creamy', 'Creamy & Honey', 'Creamy', 'Creamy', 'Crunchy', 'Creamy', 'Creamy', 'Crunchy', 'Smooth', 'Crunchy', 'Creamy', 'Crunchy', 'Smooth', 'Crunchy']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
lst = [628105, 714491, 808665, 897611, 990750, 993658, 2022935, 2068636, 2159330, 2159331]
for i in lst:
    df.loc[df['material_id'] == i, 'item_count'] = 2
df['type'] = type
df['type_bin'] = type_bin

# COMMAND ----------

temp = df.groupby('type')['sales'].sum().reset_index()
temp = temp.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
temp['type_sales_perc'] = round(temp.sales.cumsum() / temp.sales.sum() * 100, 2)
temp.display()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Sorbets

# COMMAND ----------

df = run('2023-08-01', '2024-07-29', 'ICE CREAM & DESSERTS', 'SORBETS')
df.display()

# COMMAND ----------

volume = [100, 500, 500, 100, 65, 65, 65, 65, 45, 75, 70, 70, 70, 9, 878, 878, 851, 878, 90, 170, 878, 878, 638, 70, 878, 50, 100, 460, 480, 173, 585, 600, 480, 600, 70, 750, 750, 750, 50, 50, 50, 80, 80, 80, 80, 80, 156, 156, 50, 50, 145, 50, 100, 300, 70, 70, 70, 70, 400, 400, 80, 80, 80, 80, 80, 80, 45, 45, 45, 45]

units = ['ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'OZ', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'G', 'G', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML', 'ML']

type = ['Raspberry Stick', 'Raspberry Tub', 'Mango Tub', 'Mango Stick', 'Raspberry & Vanilla Bar', 'Mango & Vanilla Bar', 'Raspberry & Vanilla Bar', 'Mango & Vanilla Bar', 'Mixed Fruit Lolly', 'Orange Juice', 'Melon Bar', 'Banana Bar', 'Mango Bar', 'Berry & Cherry & Raspberry Popsicle', 'Cherry & Grape & Orange Popsicle', 'Fruit Punch & Orange & Pineapple Popsicle', 'Blue Raspberry & Cherry & Lemon Popsicle', 'Blue Raspberry & Cherry & Grape & Green Apple', 'Pink Guaa Stick', 'Acai Blend - Fruit & Granola Tub', 'Blueberry & Raspberry & Strawberry Popsicle', 'Blue Raspberry & Lemonade & Strawberry & Watermelon Popsicle', 'Cherry & Orange & Raspberry & Watermelon Popsicle', 'Mango Stick', 'Cherry & Orange & Grape Popsicle', 'Blueberry & Peach Apricot & Pina Colada Stick', 'Mago Tub', 'Avocado Tub', 'Blueberry & Strawberry & Vanilla Stick', 'Acai Tub', 'Blue Raspberry & Lemon & Lime & Orange & Redberry Popsicle', 'Banana & Blue Raspberry & Strawberry Popsicle', 'Mango & Strawberry & Vanilla Stick', 'Blue Raspberry & Lime & Strawberry Popsicle', 'Purple Yam Stick', 'Banana & Beetroot & Cherry & Strawberry Smoothie Blend', 'Banana & Carrot & Mango & Pineapple Smoothie Blend', 'Avocado & Banana & Cucumber & Melon & Pineapple Smoothie Blend', 'Manog & Passion Fruit Popsicle', 'Orange Popsicle', 'Banana & Strawberry Popsicle', 'Mango & Passion Fruit Popsicle', 'Banana & Strawberry Popsicle', 'Lemon & Mint Popsicle', 'Chocolate & Coconut Popsicle', 'Strawberry Banana & Mango Passion Fruit & Chocolate Coconut & Lemon Mint Popsicle', 'Lychee Mochi Dough', 'Passion Fruit Mochi Dough', 'Cola & Lemon & Raspberry & Tutti Frutti & Watermelon Popsicle', 'Cola & Lemon & Raspberry & Tutti Frutti & Watermelon Popsicle', 'Coconut Pudding', 'Coconut & Green Apple & Lemon & Watermelon Popsicle', 'Coconut & Green Apple & Lemon & Watermelon Popsicle', 'Lemon & Pineapple & Strawberry Lolly', 'Raspberry Lolly', 'Orange Lolly', 'Pineapple Lolly', 'Mango Lolly', 'Acai & Berry Smoothie Blend', 'Acai & Berry Smoothie Blend', 'Mango Lolly', 'Raspberry Lolly', 'Strawberry Lolly', 'Chocolate Lolly', 'Coconut Lolly', 'Lime Lolly', 'Lime & Mango Lolly', 'Raspberry & Strawberry Lolly', 'Chocolate & Coconut Lolly', 'Chocolate & Coffee & Strawberry & Vanilla Lolly']

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df['item_count'] = 1
df.loc[df['material_id'].isin([779547]), 'item_count'] = 3
df.loc[df['material_id'].isin([2092202, 2261021, 2261093, 2261094, 2261095]), 'item_count'] = 4
df.loc[df['material_id'].isin([401298, 401302]), 'item_count'] = 5
df.loc[df['material_id'].isin([756504, 1647437, 2203436]), 'item_count'] = 6
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
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Travel Tissue &Wipes

# COMMAND ----------

df = run('2023-08-01', '2024-07-29', 'PAPER GOODS', 'TRAVEL TISSUE &WIPES')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [24, 20, 20, 20, 20, 10, 30, 8, 15, 15, 120, 15, 10, 10, 20, 30, 40, 10, 10, 10, 10, 10, 10, 10, 10, 80, 10, 40, 40, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 100, 100, 15, 25, 25, 10, 10, 20, 40, 20, 40, 5, 5, 10, 25, 10, 40, 15, 10, 25, 10, 40, 40, 10, 10, 10, 15, 9, 9, 9, 10, 10, 30, 10, 25, 20, 100, 100, 10, 72, 72, 72]

units = ['pcs'] * 81

item_count = [10, 1, 1, 1, 4, 6, 1, 1, 1, 1, 2, 1, 1, 1, 1, 3, 2, 1, 1, 5, 1, 5, 5, 10, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 3, 1, 1, 2, 1, 1, 1, 1, 2, 3, 1, 20, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 2, 1, 1, 1]

type = ['Pocket Tissues', 'Travel Wipes', 'Travel Wipes', 'Travel Wipes', 'Pocket Tissues', 'Mini Tissues', 'Travel Wipes', 'Dry Tissues', 'Wet Wipes', 'Wet Wipes', 'Mini Tissues', 'Sanitizing Wipes', 'Travel Wipes', 'Skin Wipes', 'Skin Wipes', 'Travel Wipes', 'Sanitizing Wipes', 'Skin Wipes', 'Skin Wipes', 'Multi Use Wipes', 'Wet Wipes', 'Skin Wipes', 'Skin Wipes', 'Pocket Tissues', 'Pocket Tissues', 'Multi Use Wipes', 'Pocket Tissues', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Skin Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Baby Wipes', 'Skin Wipes', 'Skin Wipes', 'Wet Wipes', 'Sanitizing Wipes', 'Sanitizing Wipes', 'Skin Wipes', 'Skin Wipes', 'Disinfectant Wipes', 'Disinfectant Wipes', 'Disinfectant Wipes', 'Disinfectant Wipes', 'Bath Wipes', 'Pocket Tissues', 'Skin Wipes', 'Pocket Tissues', 'Pocket Tissues', 'Pocket Tissues', 'Cleaning Wipes', 'Sanitizing Wipes', 'Sanitizing Wipes', 'Pocket Tissues', 'Multi Use Wipes', 'Skin Wipes', 'Disifectant Wipes', 'Disifectant Wipes', 'Pocket Tissues', 'Wet Wipes', 'Wet Wipes', 'Wet Wipes']

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
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Canned Corned Beef

# COMMAND ----------

df = run('2023-08-01', '2024-07-29', 'CANNED MEATS', 'CANNED CORNED BEEF')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [340, 175, 198, 260, 150, 210, 340, 250, 340, 340, 175, 150, 198, 150, 210, 200, 260, 200, 198, 150, 100, 380, 380, 380, 260, 260, 260, 175, 340, 340, 150, 538, 175, 175, 175]

units = ['G'] * 35

type = ['Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Hot & Spicy', 'Regular', 'Regular', 'Regular', 'Regular', 'Regular', 'Long Shreds', 'Regular', 'Loaf', 'Regular', 'Ranch Style', 'Garlic & Chilli', 'Caldereta', 'Ranch Style', 'Garlic & Chilli', 'Caldereta', 'Regular', 'Regular', 'Regular', 'Loaf', 'Regular', 'Caldereta', 'Ranch Style', 'Garlic & Chilli']

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
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Kitchen Rolls

# COMMAND ----------

df = run('2023-09-01', '2024-08-29', 'PAPER GOODS', 'KITCHEN ROLLS')
df2 = web_scrape(df)
df2.display()

# COMMAND ----------

volume = [90, 140, 100, 350, np.nan, 350, 350, 250, 1500, 216, np.nan, 202, np.nan, 360, 540, ]

units = ['pcs'] * 125

type = []

type_bin = type.copy()

df['volume'] = volume
df['units'] = units
df.loc[df['material_id'].isin([189971, 344712, 393893, 400631, 436400]), 'units'] = 'meters'
df.loc[df['material_id'].isin([]), 'units'] = np.nan
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
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Jams

# COMMAND ----------

df = run('2023-09-01', '2024-08-29', 'PRESERVATIVES & SPREADS', 'JAMS')
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
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Canned Luncheon Meat

# COMMAND ----------

df = run('2023-09-01', '2024-08-29', 'CANNED MEATS', 'CANNED LUNCHEON MEAT')
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
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC #Savoury

# COMMAND ----------

df = run('2023-09-01', '2024-08-29', 'BISCUITS & CAKES', 'SAVOURY')
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
spark_df.write.option("overwriteSchema", "true").mode("append").saveAsTable("dev.sandbox.pj_assortment_attributes")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


