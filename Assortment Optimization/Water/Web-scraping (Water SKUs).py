# Databricks notebook source
# MAGIC %md
# MAGIC ##Import Statements

# COMMAND ----------

from typing_extensions import Text
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests
import csv
import time
import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ##Extract data from the webpage

# COMMAND ----------

dct = {'Brand': [],
       'Name': [],
       'MRP': [],
       'Selling_Price': [],
       'Product_ID': [],
       'Size': [],
       'Type': []}

# COMMAND ----------

brands = ['Al Ain', 'Carrefour', 'Oasis', 'Mai Dubai', 'Masafi',
          'evian', 'Perrier', 'Arwa', 'Fiji', 'Pure Life',
          'Volvic', 'Acqua Panna', 'Voss', 'Aquafina', 'Sannine',
          'Alpin', 'Gasteiner', 'San Pellegrino', 'Blu', 'Glaceau']

for i in brands:
    if len(i.split()) > 1:
        brand = i.replace(' ', '%20')
    else:
        brand = i
    
    url = 'https://www.carrefouruae.com/mafuae/en/c/F1570000?currentPage=0&filter=brand_name%3A%27' + brand + '%27&nextPageOffset=0&pageSize=60&sortBy=relevance'
    doc = urlopen(url)

    bsObj = BeautifulSoup(doc)
    script = bsObj.find('script', id = '__NEXT_DATA__').text.strip()
    data = json.loads(script)

    for i in data['props']['initialState']['search']['products']:
        dct['Brand'].append(i['brand'])
        dct['Name'].append(i['name'])
        dct['MRP'].append(i['originalPrice'])
        dct['Selling_Price'].append(i['applicablePrice'])
        dct['Product_ID'].append(i['productId'])
        dct['Size'].append(i['size'])

        parts = i['url'].split('/')
        dct['Type'].append(parts[3])

# COMMAND ----------

df = pd.DataFrame(dct)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Cleaning

# COMMAND ----------

# Product ID 1920196 has a blank size which is not being considered as null by pandas. Replace this with its actual size
df['Size'] = df['Size'].replace('', '500ml')

# Product ID 1222254 has an erroneous size - 200mlx24&#39; - Replace this with its actual size
df['Size'] = df['Size'].replace('200mlx24&#39;', '200mlx24')

# COMMAND ----------

# Remove '-' sign from the water type strings and capitalize the words

def format_water_type(water_type):
    words = water_type.split('-')
    capitalized_words = [word.capitalize() for word in words]
    formatted_string = ' '.join(capitalized_words)
    return formatted_string

df['Type'] = df['Type'].apply(lambda x: format_water_type(x))

# COMMAND ----------

# Remove 'Water' from some of the water types to match with our attriubtes perfectly

df['Type'] = df['Type'].replace('Still Water', 'Still')
df['Type'] = df['Type'].replace('Sparkling Water', 'Sparkling')
df['Type'] = df['Type'].replace('Alkaline Water', 'Alkaline')
df['Type'] = df['Type'].replace('Flavoured Sparking Water', 'Flavoured Sparkling Water')

# COMMAND ----------

# Split the 'Size' column into Volume, Units, and Item Count

df[['Volume', 'Item_Count']] = df['Size'].str.split('x', expand=True)
df['Item_Count'] = df['Item_Count'].fillna(1)
df[['Volume', 'Units']] = df['Volume'].str.extract('([\d.]+)([a-zA-Z]+)')
df['Units'] = df['Units'].replace('ml', 'ML')

# COMMAND ----------

# Save the dataframe into a temporary view to download the CSV file

final_df = df[['Product_ID', 'Name', 'Brand', 'Type', 'MRP', 'Selling_Price', 'Volume', 'Item_Count', 'Units']]
final_df = spark.createDataFrame(final_df)
final_df.createOrReplaceTempView('carrefour_water')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM carrefour_water

# COMMAND ----------



# COMMAND ----------


