# Databricks notebook source
# MAGIC %md
# MAGIC #Import Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #User Inputs

# COMMAND ----------

cy_start_date = '2023-12-01'
cy_end_date = str(date.today() - timedelta(days = 1))

# rfm_month_year = 202405

categories = ['WATER']
categories_recommendation_dates = ['2024-02-12']
catg_analysis_period_start_dates = ['2023-01-01']

material_groups = ['PASTA', 'INSTANT NOODLE', 'CUP NOODLE', 'COCONUT OIL', 'OLIVE OIL']
material_groups_recommendation_date = ['2024-04-26', '2024-04-26', '2024-04-26', '2024-05-24', '2024-06-14']
mg_analysis_period_start_dates = ['2023-03-01', '2023-03-01', '2023-03-01', '2023-05-01', '2023-06-01']

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales & Volume

# COMMAND ----------

py_start_date = (datetime.strptime(cy_start_date, "%Y-%m-%d") - timedelta(days=364)).strftime("%Y-%m-%d")
py_end_date = str(date.today() - timedelta(days=365))
# 365 days because of leap year in CY. Otherwise, 364 days

categories_sorted = sorted(categories)
material_groups_sorted = sorted(material_groups)

categories_sql = ', '.join([f"'{item}'" for item in categories])
material_groups_sql = ', '.join([f"'{item}'" for item in material_groups])

# COMMAND ----------

query = f"""
SELECT
    business_day,
    region_name,
    t1.store_id,
    store_name,
    department_name,
    category_name,
    material_group_name,
    material_id,
    material_name,
    brand,
    ROUND(SUM(amount), 2) AS sales,
    ROUND(SUM(quantity), 2) AS volume
FROM gold.transaction.uae_pos_transactions AS t1
JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
WHERE
    (business_day BETWEEN '{py_start_date}' AND '{py_end_date}'
    OR business_day BETWEEN '{cy_start_date}' AND '{cy_end_date}')

    AND (category_name IN ({categories_sql})
    OR material_group_name IN ({material_groups_sql}))
    
    AND tayeb_flag = 0
    AND transaction_type IN ('SALE', 'SELL_MEDIA')
    AND amount > 0
    AND quantity > 0
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
"""

region_view_df = spark.sql(query).toPandas()

# COMMAND ----------

df = region_view_df.copy()
df['year_month'] = df['business_day'].str[:7].str.replace('-', '')
df['year_month'] = df['year_month'].astype(int)
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Mapping DataFrame

# COMMAND ----------

query = f"""
SELECT
    department_name,
    category_name,
    material_group_name,
    brand,
    material_id,
    material_name
FROM gold.material.material_master
WHERE
    category_name IN ({categories_sql})
    OR material_group_name IN ({material_groups_sql})
GROUP BY 1, 2, 3, 4, 5, 6
"""

mapping_df = spark.sql(query).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Manual Reco & Delisted Dates

# COMMAND ----------

# MAGIC %md
# MAGIC ###Water

# COMMAND ----------

region_names = []
for n in range(4):
    lst1 = [68, 50, 86, 86]
    lst2 = ["ABU DHABI", "AL AIN", "DUBAI", "SHARJAH"]
    for i in range(lst1[n]):
        region_names.append(lst2[n])

material_ids = [2083789, 1403107, 1403108, 1193068, 1168340, 1306396, 130454, 2079852, 132825, 356145, 5531, 5539, 72593, 554815, 1333721, 45025, 356144, 958777, 969199, 1021745, 1274721, 1306395, 1462834, 1743443, 1745916, 1877061, 814870, 581855, 985962, 1021752, 322860, 985961, 2098042, 595793, 1033193, 338111, 1264055, 581854, 1498464, 771564, 1979151, 1668034, 2083210, 1943686, 1631281, 537673, 2079853, 1911254, 2068156, 1744013, 2011466, 1033194, 649263, 2083561, 1173679, 1974669, 1051919, 1021747, 649261, 1054016, 761404, 898817, 1970225, 1582950, 356313, 1970228, 649262, 1582948, 581855, 1403108, 322860, 595793, 1264055, 1668034, 537673, 2098042, 2068156, 2083561, 1173679, 1315149, 130454, 1193068, 2079852, 1306396, 132825, 649263, 1578070, 1033194, 2083210, 1021752, 338111, 771564, 1943686, 1744013, 1911254, 1033193, 1403107, 1021747, 985962, 958777, 1274721, 1093037, 1877061, 649261, 1631281, 2198272, 1979151, 1498464, 761404, 45025, 1979149, 1582950, 985961, 1306395, 743976, 969199, 649262, 1021745, 1877061, 1743443, 1306396, 581854, 1306395, 356313, 1674365, 45025, 2088109, 2104671, 1193068, 1536585, 2012059, 2015129, 1824713, 5531, 1021745, 907948, 91929, 788306, 1094974, 1823982, 1882277, 130454, 969199, 958777, 649263, 132825, 1462834, 356145, 1333721, 554815, 5539, 347534, 855264, 43787, 1631281, 1021752, 338111, 1033194, 2098072, 2098074, 1798971, 1264055, 2083210, 1173679, 2098042, 985961, 1578070, 1498464, 2068156, 1403107, 771564, 1744013, 1979151, 1021747, 1911254, 1943686, 985962, 2098075, 581855, 2079852, 1033193, 537673, 1668034, 1403108, 1582950, 595793, 322860, 2079853, 675069, 1582948, 1838750, 649261, 2083561, 1168340, 1799062, 1798970, 2098051, 2083789, 1851740, 1093050, 1300924, 808218, 1679132, 898817, 1877061, 1743443, 1306396, 581854, 1306395, 356313, 1674365, 45025, 2088109, 2104671, 1193068, 1536585, 2012059, 2015129, 1824713, 5531, 1021745, 907948, 91929, 788306, 1094974, 1823982, 1882277, 130454, 969199, 958777, 649263, 132825, 1462834, 356145, 1333721, 554815, 5539, 347534, 855264, 43787, 1744013, 1911254, 985961, 2083561, 1403108, 338111, 1021752, 2098042, 1578070, 322860, 2083210, 1033194, 1498464, 581855, 1021747, 1851740, 2098075, 1979151, 1264055, 1093039, 1668034, 1168340, 985962, 1173679, 1033193, 1403107, 2079852, 858604, 1631281, 1943686, 2068156, 1582950, 595793, 771564, 2083789, 1838750, 2098072, 1274721, 1557143, 1054016, 537673, 2079853, 675069, 1093038, 72593, 1582948, 1093050, 2011466, 2098074, 1348689]

delisted_dates = ["07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "08/03/2024", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA"]

confirmed_by = ["Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA"]

water_df = pd.DataFrame({'region_name': region_names,
                         'material_id': material_ids,
                         'delisted_date': delisted_dates,
                         'confirmed_by': confirmed_by})
water_df = water_df[water_df['delisted_date'] != 'NA'].sort_values(by = 'material_id', ascending = True).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pasta

# COMMAND ----------

region_names = []
for n in range(4):
    lst1 = [134, 216, 156, 128]
    lst2 = ["ABU DHABI", "AL AIN", "DUBAI", "SHARJAH"]
    for i in range(lst1[n]):
        region_names.append(lst2[n])

material_ids = [69487, 69488, 69489, 69490, 93038, 183770, 186318, 186319, 230673, 420565, 424740, 424742, 424743, 424746, 594767, 613041, 711139, 961457, 1026010, 1026016, 1199166, 1266666, 1532424, 1532512, 1532517, 1532531, 1532533, 1532536, 1573102, 1573338, 1573340, 1573461, 1573466, 1603087, 1628869, 1767079, 1767152, 1825589, 1849870, 1849882, 1893353, 1899659, 2070622, 2076579, 2076585, 14305, 236531, 502182, 502951, 507607, 638968, 733820, 759203, 868570, 900585, 1007506, 1156173, 1164087, 1229337, 1238886, 1488903, 1519317, 1533783, 1557146, 1573463, 1603088, 1603113, 1627748, 1700889, 1737392, 1766820, 1766990, 1767061, 1767115, 1776939, 1794485, 1816810, 1816842, 1893211, 1893234, 1899662, 1899954, 1904860, 2005067, 2005068, 2005069, 2005082, 2005083, 2025720, 2025721, 2036384, 2091784, 2091785, 1205354, 2143054, 2143031, 2143053, 2143191, 557409, 1156485, 1290129, 1816811, 1881928, 1057752, 1867063, 1867062, 1519318, 1577349, 1794482, 15653, 160516, 1026015, 1026014, 1289946, 981036, 420538, 183771, 1849869, 1849871, 1047147, 1047149, 180109, 136613, 1602937, 1602938, 1603111, 653881, 653901, 653902, 653905, 653908, 653920, 770078, 878448, 4227, 69487, 180109, 530052, 589091, 711139, 888840, 984463, 994643, 1170457, 1297339, 1374603, 1374604, 1488903, 1519318, 1573102, 1573110, 1573338, 1573340, 1573463, 1573465, 1573467, 1577347, 1661647, 1700889, 1749710, 1825590, 1825591, 1825662, 1849870, 1849882, 1867062, 1899662, 2033113, 2033114, 2053900, 2070622, 2143030, 2143058, 186319, 236531, 395581, 502182, 502951, 507607, 594767, 638967, 638968, 731418, 733820, 868570, 961457, 1007506, 1047147, 1156485, 1164087, 1266666, 1474896, 1519317, 1533783, 1573339, 1573461, 1573466, 1577349, 1603088, 1603113, 1647431, 1661642, 1737392, 1776916, 1776939, 1816810, 1816842, 1825589, 1849871, 1893234, 1893353, 1899659, 1899954, 1904860, 1904861, 1916192, 1951025, 2005068, 2005069, 2005082, 2005083, 2025720, 2025721, 2040941, 2091784, 2091785, 15702, 1731640, 1912936, 1148298, 1848923, 613038, 1057753, 1731639, 902160, 537456, 613045, 2022561, 843300, 1532520, 2143031, 969799, 969820, 2033102, 1532511, 647542, 1776910, 1532531, 570114, 1174597, 449820, 1661653, 820450, 1532514, 15696, 613040, 928866, 1045607, 1941033, 1967781, 1238928, 570192, 618880, 2143053, 1026014, 1532424, 524883, 1532517, 1205352, 1532535, 2022582, 1532513, 618881, 570204, 987623, 1532428, 449712, 1205353, 1532533, 588999, 1532426, 1006690, 1532536, 1840890, 1532534, 2070355, 613039, 1532518, 1045730, 1199166, 1937495, 1984307, 1532512, 1937483, 570199, 613200, 1937491, 14297, 570197, 1532515, 418856, 1532519, 1045602, 1045605, 1156093, 1881928, 449822, 613041, 1937492, 419680, 1867063, 186318, 1881914, 183770, 570195, 1242100, 1164933, 1135571, 2133543, 2156352, 1840888, 1899953, 2122974, 2139315, 1026015, 1573462, 1135763, 1849869, 183771, 984464, 1135761, 1423358, 2143191, 2113799, 2010889, 2134629, 1026010, 2133435, 2143054, 1024727, 1047149, 1024933, 1026016, 1705677, 942705, 1024932, 1374606, 2136420, 1700363, 1461665, 14297, 69488, 69489, 69490, 180109, 183770, 183771, 539142, 570114, 570192, 570204, 594767, 680608, 710815, 731418, 820450, 858068, 888796, 896637, 942705, 1026014, 1045604, 1056983, 1135571, 1156098, 1156173, 1214857, 1238882, 1238886, 1242092, 1266666, 1287067, 1287068, 1374604, 1423358, 1519317, 1532515, 1532516, 1573340, 1602938, 1628869, 1766820, 1766990, 1767061, 1776910, 1825590, 1849882, 1867062, 1868202, 1893353, 1899659, 1899953, 1916192, 1937483, 1937491, 1937495, 2005067, 2005068, 2005082, 2012350, 2012518, 2012519, 2033102, 2070622, 2085536, 2130995, 2143031, 2143053, 15652, 236531, 320427, 502182, 638968, 868570, 961457, 1047147, 1057752, 1057753, 1089365, 1205354, 1229337, 1488903, 1519318, 1533783, 1557009, 1557146, 1603113, 1625642, 1661642, 1714424, 1737392, 1776916, 1776939, 1816810, 1816842, 1893211, 1893234, 1899662, 1904860, 1904861, 1951025, 2033113, 2091784, 2091785, 1130548, 1045730, 1374606, 1374603, 888840, 1195863, 1009187, 984464, 984463, 1135763, 1135572, 502951, 1164087, 647542, 1164933, 2010889, 2009937, 2010888, 1984307, 1007506, 1825591, 1825662, 1825589, 1573465, 1573466, 1573467, 1573462, 1573339, 1573338, 1573102, 1573110, 1573463, 1573461, 925513, 925441, 925443, 677622, 1700889, 609871, 69487, 642851, 2090875, 186318, 186319, 1047149, 1849869, 1849870, 1849871, 653902, 653905, 653908, 653920, 69488, 69489, 93038, 136613, 236531, 320427, 418856, 449822, 570195, 613038, 613041, 642851, 710814, 710815, 731418, 820450, 899600, 918909, 925441, 925443, 1047147, 1047149, 1132329, 1135763, 1205354, 1297338, 1297339, 1374604, 1374606, 1474896, 1519317, 1519318, 1532424, 1532511, 1557146, 1573102, 1573340, 1573466, 1766820, 1767079, 1767115, 1767152, 1776913, 1825589, 1825590, 1849870, 1881928, 1899659, 1912936, 1937483, 2033102, 2085536, 2143053, 2143191, 15652, 570197, 594767, 613039, 625147, 638968, 677622, 680608, 759203, 902160, 1007506, 1045730, 1057752, 1089365, 1135572, 1199166, 1205350, 1238882, 1266666, 1290129, 1374603, 1573110, 1573338, 1577349, 1577350, 1603088, 1603113, 1625642, 1661642, 1661653, 1699861, 1717856, 1766990, 1776916, 1776939, 1816810, 1816842, 1899660, 1899662, 1899954, 1904860, 1904861, 1951025, 2033113, 2059264, 2070622, 1700889, 2090875, 69487, 69490, 180109, 183770, 183771, 186318, 186319, 609871, 925513, 984463, 984464, 1573339, 1573462, 1573463, 1573465, 1573467, 1825591, 1825662, 1849869, 1849871, 1849882, 1899953, 1130548, 2010889, 888796, 1135571]

delisted_dates = ["07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "NA", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "NA", "NA", "NA", "NA", "07/03/2024", "NA", "NA", "NA", "NA", "07/03/2024", "NA", "NA", "07/03/2024", "NA", "10/07/2024", "NA", "NA", "NA", "NA", "10/07/2024", "10/07/2024", "10/07/2024", "10/07/2024", "10/07/2024", "07/03/2024", "NA", "07/03/2024", "07/03/2024", "10/07/2024", "07/03/2024", "07/03/2024", "NA", "10/07/2024", "NA", "10/07/2024", "10/07/2024", "07/03/2024", "07/03/2024", "NA", "10/07/2024", "NA", "07/03/2024", "07/03/2024", "07/03/2024", "NA", "07/03/2024", "10/07/2024", "10/07/2024", "10/07/2024", "NA", "07/03/2024", "10/07/2024", "07/03/2024", "NA", "NA", "10/07/2024", "07/03/2024", "07/03/2024", "07/03/2024", "NA", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "10/07/2024", "07/03/2024", "07/03/2024", "07/03/2024", "NA", "10/07/2024", "NA", "NA", "NA", "10/07/2024", "10/07/2024", "10/07/2024", "10/07/2024", "10/07/2024", "NA", "NA", "07/03/2024", "NA", "NA", "NA", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "07/03/2024", "18/05/2024", "12/03/2024", "18/05/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "NA", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "18/05/2024", "12/03/2024", "12/03/2024", "12/03/2024", "18/05/2024", "12/03/2024", "18/05/2024", "12/03/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "12/03/2024", "12/03/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "NA", "12/03/2024", "18/05/2024", "12/03/2024", "12/03/2024", "12/03/2024", "18/05/2024", "18/05/2024", "18/05/2024", "12/03/2024", "12/03/2024", "18/05/2024", "18/05/2024", "12/03/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "12/03/2024", "12/03/2024", "18/05/2024", "18/05/2024", "18/05/2024", "12/03/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "12/03/2024", "NA", "18/05/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "12/03/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "13/03/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024", "13/03/2024"]

confirmed_by = ["Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "NA", "NA", "NA", "Mubin Andipat Mohammed Ali", "NA", "NA", "NA", "NA", "Mubin Andipat Mohammed Ali", "NA", "NA", "Mubin Andipat Mohammed Ali", "NA", "Mohamed Sherin Poovamparambil", "NA", "NA", "NA", "NA", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mohamed Sherin Poovamparambil", "NA", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mohamed Sherin Poovamparambil", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "NA", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "NA", "NA", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mohamed Sherin Poovamparambil", "NA", "NA", "NA", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "NA", "NA", "Mubin Andipat Mohammed Ali", "NA", "NA", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri"]

pasta_df = pd.DataFrame({'region_name': region_names,
                         'material_id': material_ids,
                         'delisted_date': delisted_dates,
                         'confirmed_by': confirmed_by})
pasta_df = pasta_df[pasta_df['delisted_date'] != 'NA'].sort_values(by = 'material_id', ascending = True).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Instant Noodle

# COMMAND ----------

region_names = []
for n in range(4):
    lst1 = [78, 40, 62, 69]
    lst2 = ["ABU DHABI", "AL AIN", "DUBAI", "SHARJAH"]
    for i in range(lst1[n]):
        region_names.append(lst2[n])

material_ids = [804087, 804088, 880897, 1366760, 1503450, 1787230, 1933080, 1955740, 1959764, 1961328, 2061133, 2061134, 2068912, 1009547, 1520529, 1520541, 1084619, 407356, 651687, 651685, 651680, 651681, 1827214, 1827219, 2003397, 1861262, 1520524, 2097424, 2038242, 759504, 1068151, 882288, 969797, 1068150, 1776498, 538433, 720163, 720164, 580312, 4373, 4366, 4364, 1959812, 720161, 4371, 4355, 4348, 2152602, 580478, 1785577, 1433644, 1867363, 1845635, 409649, 1879527, 1845634, 880899, 1433646, 1776553, 580477, 1762388, 1651071, 2083241, 1676528, 580479, 1997640, 1866152, 1607644, 1320732, 1320707, 1674811, 1843682, 2133911, 1320703, 1785572, 1068153, 1827216, 1827218, 804087, 804088, 1520524, 1827219, 1955740, 1959764, 2061133, 2061134, 397021, 515590, 1520541, 1785572, 1827218, 1845635, 1903985, 1959812, 1068150, 1068153, 2097424, 1520529, 1607644, 1768802, 543859, 1366760, 1068151, 1515686, 1207367, 4355, 720163, 1776495, 4348, 882288, 1903986, 4359, 1426300, 1862324, 584974, 580479, 580477, 1252636, 804087, 804088, 880930, 1222380, 1366760, 1520524, 1520528, 1520529, 1520541, 1520543, 1827219, 1955740, 1959764, 1999659, 1999660, 1999775, 1999776, 1999777, 2028671, 2068912, 2083382, 904540, 1252636, 1745845, 1745846, 1745847, 1785572, 1999657, 1999774, 2049088, 2075553, 1578782, 1845635, 1753106, 651681, 651685, 1959812, 1515686, 543754, 1620539, 1702568, 1827216, 1785577, 1903986, 1843682, 1999661, 1827214, 1827218, 1207315, 1394298, 1776553, 1207367, 4364, 882288, 4371, 4355, 1366765, 617005, 1903985, 1674811, 1845634, 482088, 543706, 707498, 804087, 804088, 880930, 1426300, 1520524, 1520528, 1520529, 1520541, 1520543, 1745846, 1808633, 1827214, 1827216, 1827218, 1955740, 1959764, 1999659, 1999660, 1999776, 1999777, 2022933, 2028671, 2049088, 2068912, 1222380, 1745845, 1745847, 1999657, 1999774, 1999775, 2083382, 17910, 904540, 543859, 1425728, 543754, 1657731, 1651057, 1651054, 1785577, 2038242, 761665, 1959812, 1845634, 1845635, 365228, 1702568, 1478467, 2097424, 1433644, 880898, 4366, 614519, 1903985, 1761253, 1785572, 1903986, 720163, 2171245, 409649, 4371, 1862324, 4364, 1207315, 1666661, 1478466, 2171786]

delisted_dates = ["20/05/2024", "20/05/2024", "20/05/2024", "NA", "20/05/2024", "NA", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "10/07/2024", "10/07/2024", "NA", "NA", "NA", "NA", "NA", "NA", "10/07/2024", "10/07/2024", "10/07/2024", "NA", "10/07/2024", "20/05/2024", "20/05/2024", "10/07/2024", "10/07/2024", "20/05/2024", "NA", "10/07/2024", "20/05/2024", "10/07/2024", "20/05/2024", "20/05/2024", "20/05/2024", "NA", "NA", "NA", "20/05/2024", "20/05/2024", "20/05/2024", "NA", "NA", "NA", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "10/07/2024", "10/07/2024", "10/07/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "NA", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "NA", "18/05/2024", "NA", "18/05/2024", "18/05/2024", "18/05/2024", "NA", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "13/03/2024", "13/03/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024"]

confirmed_by = ["Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mubin Andipat Mohammed Ali", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "NA", "NA", "NA", "NA", "NA", "NA", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "NA", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "NA", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "NA", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "NA", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri"]

instant_noodle_df = pd.DataFrame({'region_name': region_names,
                         'material_id': material_ids,
                         'delisted_date': delisted_dates,
                         'confirmed_by': confirmed_by})
instant_noodle_df = instant_noodle_df[instant_noodle_df['delisted_date'] != 'NA'].sort_values(by = 'material_id', ascending = True).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cup Noodle

# COMMAND ----------

region_names = []
for n in range(4):
    lst1 = [30, 15, 22, 17]
    lst2 = ["ABU DHABI", "AL AIN", "DUBAI", "SHARJAH"]
    for i in range(lst1[n]):
        region_names.append(lst2[n])

material_ids = [1336812, 1542216, 203423, 2034628, 759424, 570812, 539860, 1408188, 1649049, 1998073, 1993080, 759426, 1745578, 4316, 1649067, 1745565, 1953020, 2171885, 1953038, 2171894, 1578772, 1933079, 759423, 1290537, 4312, 1752795, 4315, 539862, 1408194, 539861, 570812, 1336812, 1542216, 539860, 203423, 571323, 1408194, 1745578, 759424, 1745565, 1408195, 1649049, 1408188, 4312, 4316, 12587, 12590, 42481, 1433585, 1809474, 2049587, 2049601, 2049603, 2117665, 42480, 570812, 2034628, 874953, 1336812, 759424, 571323, 1651055, 216857, 1649067, 759423, 1998073, 203423, 1336812, 1433585, 1809474, 2049587, 2049601, 2049603, 1871178, 203423, 570813, 2117665, 216857, 2059020, 2059306, 759424, 1578772, 1651055, 759423]

delisted_dates = ["10/07/2024", "10/07/2024", "20/05/2024", "20/05/2024", "20/05/2024", "10/07/2024", "NA", "NA", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "10/07/2024", "10/07/2024", "10/07/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "18/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024", "20/05/2024"]

confirmed_by = ["Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "NA", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Mohamed Sherin Poovamparambil", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri"]

cup_noodle_df = pd.DataFrame({'region_name': region_names,
                         'material_id': material_ids,
                         'delisted_date': delisted_dates,
                         'confirmed_by': confirmed_by})
cup_noodle_df = cup_noodle_df[cup_noodle_df['delisted_date'] != 'NA'].sort_values(by = 'material_id', ascending = True).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Coconut Oil

# COMMAND ----------

region_names = []
for n in range(4):
    lst1 = [15, 14, 16, 22]
    lst2 = ["ABU DHABI", "AL AIN", "DUBAI", "SHARJAH"]
    for i in range(lst1[n]):
        region_names.append(lst2[n])

material_ids = [3207, 636410, 811420, 823744, 1621475, 3215, 843650, 3213, 1010199, 994082, 3205, 1274728, 888604, 1358028, 828953, 3207, 683133, 823744, 843650, 892002, 388203, 811420, 1604650, 1218548, 3213, 1358028, 994082, 946203, 946202, 3213, 1710303, 3207, 1621475, 1710302, 3210, 1872852, 2111062, 1936540, 338171, 1358028, 840476, 843650, 1281424, 1076057, 648787, 3207, 843650, 1621475, 1710302, 1710303, 1710304, 3213, 1872852, 1817170, 892002, 1872853, 1358028, 888604, 338171, 840476, 648787, 1055881, 3210, 2111062, 1936540, 1281424, 1076057]

delisted_dates = ["NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "27/05/2024", "27/05/2024", "27/05/2024", "NA", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "NA", "NA", "NA", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "NA", "27/05/2024", "27/05/2024", "NA", "NA", "NA", "27/05/2024", "NA", "27/05/2024", "27/05/2024", "27/05/2024", "NA", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024", "27/05/2024"]

confirmed_by = ["NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "NA", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Mubin Andipat Mohammed Ali", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "NA", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri"]

coconut_oil_df = pd.DataFrame({'region_name': region_names,
                         'material_id': material_ids,
                         'delisted_date': delisted_dates,
                         'confirmed_by': confirmed_by})
coconut_oil_df = coconut_oil_df[coconut_oil_df['delisted_date'] != 'NA'].sort_values(by = 'material_id', ascending = True).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Olive Oil

# COMMAND ----------

region_names = []
for n in range(4):
    lst1 = [52, 64, 44, 49]
    lst2 = ["ABU DHABI", "AL AIN", "DUBAI", "SHARJAH"]
    for i in range(lst1[n]):
        region_names.append(lst2[n])

material_ids = [201914, 456171, 595388, 639819, 639832, 645240, 683103, 971035, 1470390, 1546942, 1913069, 1954277, 2040992, 144938, 144939, 194757, 471895, 494028, 510571, 584334, 596389, 599445, 633205, 892014, 893091, 919597, 1175344, 1210381, 1463021, 2073765, 2175891, 514806, 1734238, 346178, 202413, 712764, 641995, 346177, 514808, 185019, 1470164, 1021175, 789698, 885538, 382583, 1025008, 514807, 408106, 967049, 2989, 368848, 1550723, 144938, 144939, 201914, 442697, 471895, 494028, 510571, 595388, 599445, 639818, 639832, 683103, 893091, 971035, 1470390, 1546942, 1954277, 2061295, 185017, 456171, 514806, 596389, 614320, 641995, 967049, 1021175, 1025008, 1734238, 2073537, 2073765, 892014, 194757, 1175344, 408106, 633205, 970425, 454589, 19432, 368849, 581266, 564388, 1703527, 971051, 1836920, 964020, 382583, 201919, 985978, 639819, 1071320, 1342411, 772026, 506048, 776021, 1564388, 1667239, 1780411, 919597, 865380, 1079956, 1446180, 1564389, 537376, 1806885, 19432, 144938, 577507, 595388, 599445, 639818, 639819, 645240, 683103, 702709, 901274, 1470390, 1550723, 1933032, 1954277, 2014256, 144939, 202413, 514806, 514807, 1210381, 1463021, 2061295, 2073537, 2204390, 1734238, 346179, 2189234, 581268, 346178, 1175344, 633205, 1470164, 1021175, 202415, 2185427, 641995, 789698, 510571, 581266, 1811188, 1741245, 885538, 919597, 19435, 144939, 456171, 510571, 514806, 514807, 577507, 584334, 599445, 639818, 639819, 645240, 683103, 892014, 919597, 1463021, 1470390, 1546942, 1550723, 1703525, 1811188, 1933132, 1954277, 2014256, 2015148, 2022934, 19432, 346178, 595388, 1210381, 2073537, 2189234, 633205, 346179, 471895, 202413, 1175344, 2061295, 789698, 346177, 581268, 641995, 1470164, 505578, 1844367, 581266, 885538, 187671, 201914]

delisted_dates = ["NA", "26/06/2024", "NA", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "NA", "NA", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "26/06/2024", "NA", "NA", "26/06/2024", "26/06/2024", "26/06/2024", "NA", "NA", "26/06/2024", "26/06/2024", "NA", "26/06/2024", "NA", "26/06/2024", "NA", "NA", "NA", "NA", "26/06/2024", "NA", "26/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "NA", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "NA", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "NA", "NA", "NA", "NA", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "NA", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "NA", "NA", "29/06/2024", "NA", "NA", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "NA", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "29/06/2024", "NA", "29/06/2024"]

confirmed_by = ["NA", "Afsal Rahmathali", "NA", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "NA", "NA", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "NA", "NA", "Afsal Rahmathali", "Afsal Rahmathali", "Afsal Rahmathali", "NA", "NA", "Afsal Rahmathali", "Afsal Rahmathali", "NA", "Afsal Rahmathali", "NA", "Afsal Rahmathali", "NA", "NA", "NA", "NA", "Afsal Rahmathali", "NA", "Afsal Rahmathali", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "NA", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Rafeek Panikkaveettil Hameed", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "NA", "NA", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "NA", "NA", "Nishad Kodakkancheri", "NA", "NA", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "Nishad Kodakkancheri", "NA", "Nishad Kodakkancheri"]

olive_oil_df = pd.DataFrame({'region_name': region_names,
                         'material_id': material_ids,
                         'delisted_date': delisted_dates,
                         'confirmed_by': confirmed_by})
olive_oil_df = olive_oil_df[olive_oil_df['delisted_date'] != 'NA'].sort_values(by = 'material_id', ascending = True).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reco Dates & Delisted Dates

# COMMAND ----------

temp_catg = pd.DataFrame({
    'category_name': categories,
    'recommendation_date': categories_recommendation_dates,
    'analysis_period_start_dates': catg_analysis_period_start_dates
})

temp_matg = pd.DataFrame({
    'material_group_name': material_groups,
    'recommendation_date': material_groups_recommendation_date,
    'analysis_period_start_dates': mg_analysis_period_start_dates
})

df = pd.merge(df, temp_catg, on='category_name', how='left')
df = pd.merge(df, temp_matg, on='material_group_name', how='left', suffixes=('', '2'))

df['recommendation_date'] = df['recommendation_date'].fillna(df['recommendation_date2'])
df['analysis_period_start_dates'] = df['analysis_period_start_dates'].fillna(df['analysis_period_start_dates2'])
df = df.drop(columns = ['recommendation_date2', 'analysis_period_start_dates2'])

# COMMAND ----------

all_mg_catg = categories_sorted + material_groups_sorted
delisted_uae = pd.DataFrame()

for mg_catg in all_mg_catg:
    mg_catg = mg_catg.lower()
    mg_catg = mg_catg.replace(' ', '_')

    # delisted_auh = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_auh.csv")
    # delisted_aln = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_aln.csv")
    # delisted_dxb = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_dxb.csv")
    # delisted_shj = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/{mg_catg}_rationalized_shj.csv")

    if mg_catg == 'water':
        delisted_auh = water_df[water_df['region_name'] == 'ABU DHABI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_aln = water_df[water_df['region_name'] == 'AL AIN'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_dxb = water_df[water_df['region_name'] == 'DUBAI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_shj = water_df[water_df['region_name'] == 'SHARJAH'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
    elif mg_catg == 'pasta':
        delisted_auh = pasta_df[pasta_df['region_name'] == 'ABU DHABI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_aln = pasta_df[pasta_df['region_name'] == 'AL AIN'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_dxb = pasta_df[pasta_df['region_name'] == 'DUBAI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_shj = pasta_df[pasta_df['region_name'] == 'SHARJAH'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
    elif mg_catg == 'instant_noodle':
        delisted_auh = instant_noodle_df[instant_noodle_df['region_name'] == 'ABU DHABI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_aln = instant_noodle_df[instant_noodle_df['region_name'] == 'AL AIN'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_dxb = instant_noodle_df[instant_noodle_df['region_name'] == 'DUBAI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_shj = instant_noodle_df[instant_noodle_df['region_name'] == 'SHARJAH'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
    elif mg_catg == 'cup_noodle':
        delisted_auh = cup_noodle_df[cup_noodle_df['region_name'] == 'ABU DHABI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_aln = cup_noodle_df[cup_noodle_df['region_name'] == 'AL AIN'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_dxb = cup_noodle_df[cup_noodle_df['region_name'] == 'DUBAI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_shj = cup_noodle_df[cup_noodle_df['region_name'] == 'SHARJAH'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
    elif mg_catg == 'coconut_oil':
        delisted_auh = coconut_oil_df[coconut_oil_df['region_name'] == 'ABU DHABI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_aln = coconut_oil_df[coconut_oil_df['region_name'] == 'AL AIN'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_dxb = coconut_oil_df[coconut_oil_df['region_name'] == 'DUBAI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_shj = coconut_oil_df[coconut_oil_df['region_name'] == 'SHARJAH'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
    elif mg_catg == 'olive_oil':
        delisted_auh = olive_oil_df[olive_oil_df['region_name'] == 'ABU DHABI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_aln = olive_oil_df[olive_oil_df['region_name'] == 'AL AIN'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_dxb = olive_oil_df[olive_oil_df['region_name'] == 'DUBAI'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)
        delisted_shj = olive_oil_df[olive_oil_df['region_name'] == 'SHARJAH'].sort_values(by = 'material_id', ascending = True).reset_index(drop = True)

    # delisted_auh['region_name'] = 'ABU DHABI'
    # delisted_aln['region_name'] = 'AL AIN'
    # delisted_dxb['region_name'] = 'DUBAI'
    # delisted_shj['region_name'] = 'SHARJAH'

    delisted_uae = pd.concat([delisted_uae, delisted_auh, delisted_aln, delisted_dxb, delisted_shj], ignore_index=True)

# delisted_uae = delisted_uae[['region_name', 'material_id', 'delisted_date', 'confirmed_by']]
delisted_uae = delisted_uae.dropna(subset=['delisted_date']).reset_index(drop = True)

delisted_uae['delisted_date'] = pd.to_datetime(delisted_uae['delisted_date'], format='%d/%m/%Y')
delisted_uae['delisted_date'] = delisted_uae['delisted_date'].dt.strftime('%Y-%m-%d')

df = pd.merge(df, delisted_uae, on=['region_name', 'material_id'], how='left')
df['period_type'] = np.where(df.business_day >= df.delisted_date, "Post delist", "Pre-delist")

df['delisted_date'] = df['delisted_date'].fillna("NA")
df['confirmed_by'] = df['confirmed_by'].fillna("NA")

# COMMAND ----------

delisted_mg_catg_df = pd.merge(delisted_uae, mapping_df, on='material_id', how='left')

delisted_mg_catg_df['material_group_name'] = np.where(delisted_mg_catg_df.category_name == 'WATER', 'OVERALL', delisted_mg_catg_df.material_group_name)

delisted_mg_catg_df = delisted_mg_catg_df[['region_name', 'category_name', 'material_group_name', 'delisted_date']].drop_duplicates().reset_index(drop = True)

delisted_mg_catg_df.rename(columns = {'delisted_date':'category_delisted_date'}, inplace=True)

delisted_mg_catg_df = delisted_mg_catg_df.groupby(['region_name', 'category_name', 'material_group_name'])['category_delisted_date'].max().reset_index()

# COMMAND ----------

temp = delisted_mg_catg_df[delisted_mg_catg_df['category_name'].isin(categories)][['region_name', 'category_name', 'category_delisted_date']]
df = pd.merge(df, temp, on=['region_name', 'category_name'], how='left')

temp = delisted_mg_catg_df[delisted_mg_catg_df['material_group_name'].isin(material_groups)][['region_name', 'material_group_name', 'category_delisted_date']]
df = pd.merge(df, temp, on=['region_name', 'material_group_name'], how='left', suffixes=('','2'))

df['category_delisted_date'] = df['category_delisted_date'].fillna(df['category_delisted_date2'])
df = df.drop(columns = ['category_delisted_date2'])

df['category_period_type'] = np.where(df.business_day >= df.category_delisted_date, "Post delist", "Pre-delist")

# COMMAND ----------

df['business_day'] = pd.to_datetime(df['business_day'])
df['category_delisted_date'] = pd.to_datetime(df['category_delisted_date'])

category_delisted_date_lfl = df.category_delisted_date - pd.DateOffset(days=364)
df['category_period_type_lfl'] = np.where(
    (df.business_day >= df.category_delisted_date) | ((df.business_day >= category_delisted_date_lfl) & (df.business_day <= py_end_date)),
    'Post delist',
    'Pre-delist'
)

df['business_day'] = df['business_day'].dt.strftime('%Y-%m-%d')
df['category_delisted_date'] = df['category_delisted_date'].dt.strftime('%Y-%m-%d')

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##GP

# COMMAND ----------

year_month_start = int(py_start_date[:7].replace('-', ''))
year_month_end = int(cy_end_date[:7].replace('-', ''))

query = f"""
SELECT
    region AS region_name,
    year_month,
    material_id,
    gp_wth_chargeback
FROM gold.business.gross_profit
WHERE
    country = 'AE'
    AND year_month BETWEEN {year_month_start} AND {year_month_end}
"""

gp = spark.sql(query).toPandas()
gp['region_name'] = gp['region_name'].replace(['AUH', 'ALN', 'DXB', 'SHJ'], ['ABU DHABI', 'AL AIN', 'DUBAI', 'SHARJAH'])

# COMMAND ----------

df = pd.merge(df, gp, on=['region_name', 'year_month', 'material_id'], how='left')

df['gp_wth_chargeback'] = df['gp_wth_chargeback'].fillna(0)
df['gross_profit'] = df['gp_wth_chargeback'] * df['sales']/100

# COMMAND ----------

# MAGIC %md
# MAGIC ##New SKUs Tagging

# COMMAND ----------

def find_new_sku_flag(row):
    region = row['region_name']
    material_id = row['material_id']
    category_period_type = row['category_period_type']
    
    if category_period_type == 'Post delist' and material_id in post_period_dict[region] - pre_period_dict[region]:
        return 1
    else:
        return 0

# COMMAND ----------

new_skus = df[['region_name', 'category_period_type', 'material_id']].drop_duplicates().reset_index(drop=True)

pre_period_dict = {}
post_period_dict = {}

for region in new_skus['region_name'].unique():
    pre_period_dict[region] = set(df[(df['region_name'] == region) & (df['category_period_type'] == 'Pre-delist')]['material_id'].unique())
    post_period_dict[region] = set(df[(df['region_name'] == region) & (df['category_period_type'] == 'Post delist')]['material_id'].unique())

new_skus['new_sku_flag'] = new_skus.apply(lambda row: find_new_sku_flag(row), axis=1)
new_skus = new_skus[['region_name', 'material_id', 'new_sku_flag']].drop_duplicates().reset_index(drop =True)
df = pd.merge(df, new_skus, on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Recommendations Table

# COMMAND ----------

# reco_buckets_uae = pd.DataFrame()

# for mg_catg in all_mg_catg:
#     mg_catg = mg_catg.lower()
#     mg_catg = mg_catg.replace(' ', '_')

#     reco_buckets_auh = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_auh.csv")
#     reco_buckets_aln = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_aln.csv")
#     reco_buckets_dxb = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_dxb.csv")
#     reco_buckets_shj = pd.read_csv(f"/dbfs/FileStore/shared_uploads/prem@loyalytics.in/assortment_optimization/{mg_catg}/ao_gp_{mg_catg}_shj.csv")

#     reco_buckets_auh['region_name'] = 'ABU DHABI'
#     reco_buckets_aln['region_name'] = 'AL AIN'
#     reco_buckets_dxb['region_name'] = 'DUBAI'
#     reco_buckets_shj['region_name'] = 'SHARJAH'

#     reco_buckets_uae = pd.concat([reco_buckets_uae, reco_buckets_auh, reco_buckets_aln, reco_buckets_dxb, reco_buckets_shj], ignore_index=True)

# reco_buckets_uae = reco_buckets_uae[['region_name', 'material_id', 'new_buckets']]

# reco_buckets_uae.rename(columns = {'new_buckets':'recommendation'}, inplace=True)
# reco_buckets_uae['recommendation'] = reco_buckets_uae['recommendation'].replace('Maintain','Support_with_more_distribution')
# reco_buckets_uae['recommendation'] = reco_buckets_uae['recommendation'].replace('Grow','Maintain_or_grow_by_promo')

# COMMAND ----------

# reco_buckets_uae = pd.merge(reco_buckets_uae, delisted_uae, on = ['region_name', 'material_id'], how = 'outer')

# reco_buckets_uae[['recommendation', 'delisted_date', 'confirmed_by']] = reco_buckets_uae[['recommendation', 'delisted_date', 'confirmed_by']].fillna("NA")

# reco_buckets_uae = pd.merge(reco_buckets_uae, mapping_df, on='material_id', how='left')

# COMMAND ----------

# spark_df = spark.createDataFrame(reco_buckets_uae)
# spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.pj_ao_dashboard_reco")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.sandbox.pj_ao_dashboard_reco

# COMMAND ----------

# MAGIC %md
# MAGIC ##Main Table

# COMMAND ----------

reco_buckets_uae = spark.sql("SELECT * FROM dev.sandbox.pj_ao_dashboard_reco").toPandas()

# COMMAND ----------

df = pd.merge(df, reco_buckets_uae[['region_name', 'material_id', 'recommendation']], on = ['region_name', 'material_id'], how = 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC ###EDA Temp (Start)

# COMMAND ----------

var_mg = ['PASTA', 'INSTANT NOODLE', 'CUP NOODLE', 'COCONUT OIL']
for i in var_mg:
    temp = reco_buckets_uae[reco_buckets_uae['material_group_name'] == i]

    temp_auh = temp[temp['region_name'] == 'ABU DHABI'].reset_index(drop = True)
    temp_aln = temp[temp['region_name'] == 'AL AIN'].reset_index(drop = True)
    temp_dxb = temp[temp['region_name'] == 'DUBAI'].reset_index(drop = True)
    temp_shj = temp[temp['region_name'] == 'SHARJAH'].reset_index(drop = True)

    delist_from_reco_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['recommendation'] == 'Delist']['material_id'].nunique() * 100)
    delist_from_reco_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['recommendation'] == 'Delist']['material_id'].nunique() * 100)
    delist_from_reco_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['recommendation'] == 'Delist']['material_id'].nunique() * 100)
    delist_from_reco_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['recommendation'] == 'Delist']['material_id'].nunique() * 100)

    reco_from_delist_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['delisted_date'] != 'NA']['material_id'].nunique() * 100)
    reco_from_delist_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['delisted_date'] != 'NA']['material_id'].nunique() * 100)
    reco_from_delist_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['delisted_date'] != 'NA']['material_id'].nunique() * 100)
    reco_from_delist_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['delisted_date'] != 'NA']['material_id'].nunique() * 100)

    print(f"----------{i}----------")
    print(f"% Delisted From Reco\nAbu Dhabi: {delist_from_reco_perc_auh}%\nAl Ain: {delist_from_reco_perc_aln}%\nDubai: {delist_from_reco_perc_dxb}%\nSharjah: {delist_from_reco_perc_shj}%")
    print(f"\nReco From Delisted\nAbu Dhabi: {reco_from_delist_perc_auh}%\nAl Ain: {reco_from_delist_perc_aln}%\nDubai: {reco_from_delist_perc_dxb}%\nSharjah: {reco_from_delist_perc_shj}%\n")

# COMMAND ----------

temp = reco_buckets_uae[reco_buckets_uae['material_group_name'].isin(var_mg)]

temp_auh = temp[temp['region_name'] == 'ABU DHABI'].reset_index(drop = True)
temp_aln = temp[temp['region_name'] == 'AL AIN'].reset_index(drop = True)
temp_dxb = temp[temp['region_name'] == 'DUBAI'].reset_index(drop = True)
temp_shj = temp[temp['region_name'] == 'SHARJAH'].reset_index(drop = True)

delist_from_reco_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['recommendation'] == 'Delist']['material_id'].nunique() * 100)
delist_from_reco_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['recommendation'] == 'Delist']['material_id'].nunique() * 100)
delist_from_reco_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['recommendation'] == 'Delist']['material_id'].nunique() * 100)
delist_from_reco_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['recommendation'] == 'Delist']['material_id'].nunique() * 100)

reco_from_delist_perc_auh = round(temp_auh[(temp_auh['delisted_date'] != 'NA') & (temp_auh['recommendation'] == 'Delist')]['material_id'].nunique() / temp_auh[temp_auh['delisted_date'] != 'NA']['material_id'].nunique() * 100)
reco_from_delist_perc_aln = round(temp_aln[(temp_aln['delisted_date'] != 'NA') & (temp_aln['recommendation'] == 'Delist')]['material_id'].nunique() / temp_aln[temp_aln['delisted_date'] != 'NA']['material_id'].nunique() * 100)
reco_from_delist_perc_dxb = round(temp_dxb[(temp_dxb['delisted_date'] != 'NA') & (temp_dxb['recommendation'] == 'Delist')]['material_id'].nunique() / temp_dxb[temp_dxb['delisted_date'] != 'NA']['material_id'].nunique() * 100)
reco_from_delist_perc_shj = round(temp_shj[(temp_shj['delisted_date'] != 'NA') & (temp_shj['recommendation'] == 'Delist')]['material_id'].nunique() / temp_shj[temp_shj['delisted_date'] != 'NA']['material_id'].nunique() * 100)

print("----------OVERALL----------")
print(f"% Delisted From Reco\nAbu Dhabi: {delist_from_reco_perc_auh}%\nAl Ain: {delist_from_reco_perc_aln}%\nDubai: {delist_from_reco_perc_dxb}%\nSharjah: {delist_from_reco_perc_shj}%")
print(f"\nReco From Delisted\nAbu Dhabi: {reco_from_delist_perc_auh}%\nAl Ain: {reco_from_delist_perc_aln}%\nDubai: {reco_from_delist_perc_dxb}%\nSharjah: {reco_from_delist_perc_shj}%\n")

# COMMAND ----------

dates_dct = {'WATER': {'pre_start': '2023-10-01', 'pre_end': '2023-12-30', 'post_start': '2024-04-01',
                       'post_end': '2024-06-06', 'pre_months': 3, 'post_months': 2.2},
             'PASTA': {'pre_start': '2024-02-19', 'pre_end': '2024-05-17', 'post_start': '2024-06-08',
                       'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 2},
             'INSTANT NOODLE': {'pre_start': '2024-02-01', 'pre_end': '2024-04-28', 'post_start': '2024-06-01',
                       'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 2.266},
             'CUP NOODLE': {'pre_start': '2023-12-01', 'pre_end': '2024-02-27', 'post_start': '2024-06-01',
                       'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 2.266},
             'COCONUT OIL': {'pre_start': '2024-02-01', 'pre_end': '2024-04-28', 'post_start': '2024-06-15',
                       'post_end': '2024-08-08', 'pre_months': 3, 'post_months': 1.766}}

for i in var_mg:
    pre_start = dates_dct[i]['pre_start']
    pre_end = dates_dct[i]['pre_end']
    post_start = dates_dct[i]['post_start']
    post_end = dates_dct[i]['post_end']
    pre_months = dates_dct[i]['pre_months']
    post_months = dates_dct[i]['post_months']

    temp = df[df['material_group_name'] == i]

    temp_pre_uae = temp[(temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
    temp_pre_auh = temp[(temp['region_name'] == 'ABU DHABI') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
    temp_pre_aln = temp[(temp['region_name'] == 'AL AIN') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
    temp_pre_dxb = temp[(temp['region_name'] == 'DUBAI') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)
    temp_pre_shj = temp[(temp['region_name'] == 'SHARJAH') & (temp['business_day'] >= pre_start) & (temp['business_day'] <= pre_end)].reset_index(drop = True)

    temp_post_uae = temp[(temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
    temp_post_auh = temp[(temp['region_name'] == 'ABU DHABI') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
    temp_post_aln = temp[(temp['region_name'] == 'AL AIN') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
    temp_post_dxb = temp[(temp['region_name'] == 'DUBAI') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)
    temp_post_shj = temp[(temp['region_name'] == 'SHARJAH') & (temp['business_day'] >= post_start) & (temp['business_day'] <= post_end)].reset_index(drop = True)

    temp_pre_uae_reco = temp_pre_uae[temp_pre_uae['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_pre_auh_reco = temp_pre_auh[temp_pre_auh['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_pre_aln_reco = temp_pre_aln[temp_pre_aln['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_pre_dxb_reco = temp_pre_dxb[temp_pre_dxb['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_pre_shj_reco = temp_pre_shj[temp_pre_shj['recommendation'] != 'Delist'].reset_index(drop = True)

    temp_post_uae_reco = temp_post_uae[temp_post_uae['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_post_auh_reco = temp_post_auh[temp_post_auh['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_post_aln_reco = temp_post_aln[temp_post_aln['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_post_dxb_reco = temp_post_dxb[temp_post_dxb['recommendation'] != 'Delist'].reset_index(drop = True)
    temp_post_shj_reco = temp_post_shj[temp_post_shj['recommendation'] != 'Delist'].reset_index(drop = True)

    ########## Average Monthly Sales Growth

    avg_monthly_sales_pre_uae = temp_pre_uae['sales'].sum() / pre_months
    avg_monthly_sales_pre_auh = temp_pre_auh['sales'].sum() / pre_months
    avg_monthly_sales_pre_aln = temp_pre_aln['sales'].sum() / pre_months
    avg_monthly_sales_pre_dxb = temp_pre_dxb['sales'].sum() / pre_months
    avg_monthly_sales_pre_shj = temp_pre_shj['sales'].sum() / pre_months

    avg_monthly_sales_post_uae = temp_post_uae['sales'].sum() / post_months
    avg_monthly_sales_post_auh = temp_post_auh['sales'].sum() / post_months
    avg_monthly_sales_post_aln = temp_post_aln['sales'].sum() / post_months
    avg_monthly_sales_post_dxb = temp_post_dxb['sales'].sum() / post_months
    avg_monthly_sales_post_shj = temp_post_shj['sales'].sum() / post_months

    avg_month_sales_growth_uae = round((avg_monthly_sales_post_uae - avg_monthly_sales_pre_uae) / avg_monthly_sales_pre_uae*100, 2)
    avg_month_sales_growth_auh = round((avg_monthly_sales_post_auh - avg_monthly_sales_pre_auh) / avg_monthly_sales_pre_auh*100, 2)
    avg_month_sales_growth_aln = round((avg_monthly_sales_post_aln - avg_monthly_sales_pre_aln) / avg_monthly_sales_pre_aln*100, 2)
    avg_month_sales_growth_dxb = round((avg_monthly_sales_post_dxb - avg_monthly_sales_pre_dxb) / avg_monthly_sales_pre_dxb*100, 2)
    avg_month_sales_growth_shj = round((avg_monthly_sales_post_shj - avg_monthly_sales_pre_shj) / avg_monthly_sales_pre_shj*100, 2)

    ########## Average Monthly GP Growth

    avg_monthly_gp_pre_uae = temp_pre_uae['gross_profit'].sum() / pre_months
    avg_monthly_gp_pre_auh = temp_pre_auh['gross_profit'].sum() / pre_months
    avg_monthly_gp_pre_aln = temp_pre_aln['gross_profit'].sum() / pre_months
    avg_monthly_gp_pre_dxb = temp_pre_dxb['gross_profit'].sum() / pre_months
    avg_monthly_gp_pre_shj = temp_pre_shj['gross_profit'].sum() / pre_months

    avg_monthly_gp_post_uae = temp_post_uae['gross_profit'].sum() / post_months
    avg_monthly_gp_post_auh = temp_post_auh['gross_profit'].sum() / post_months
    avg_monthly_gp_post_aln = temp_post_aln['gross_profit'].sum() / post_months
    avg_monthly_gp_post_dxb = temp_post_dxb['gross_profit'].sum() / post_months
    avg_monthly_gp_post_shj = temp_post_shj['gross_profit'].sum() / post_months

    avg_month_gp_growth_uae = round((avg_monthly_gp_post_uae - avg_monthly_gp_pre_uae) / avg_monthly_gp_pre_uae*100, 2)
    avg_month_gp_growth_auh = round((avg_monthly_gp_post_auh - avg_monthly_gp_pre_auh) / avg_monthly_gp_pre_auh*100, 2)
    avg_month_gp_growth_aln = round((avg_monthly_gp_post_aln - avg_monthly_gp_pre_aln) / avg_monthly_gp_pre_aln*100, 2)
    avg_month_gp_growth_dxb = round((avg_monthly_gp_post_dxb - avg_monthly_gp_pre_dxb) / avg_monthly_gp_pre_dxb*100, 2)
    avg_month_gp_growth_shj = round((avg_monthly_gp_post_shj - avg_monthly_gp_pre_shj) / avg_monthly_gp_pre_shj*100, 2)

    ########## GP Margin Delta (Actual Delisted)

    gp_margin_pre_actual_uae = temp_pre_uae['gross_profit'].sum() / temp_pre_uae['sales'].sum()
    gp_margin_pre_actual_auh = temp_pre_auh['gross_profit'].sum() / temp_pre_auh['sales'].sum()
    gp_margin_pre_actual_aln = temp_pre_aln['gross_profit'].sum() / temp_pre_aln['sales'].sum()
    gp_margin_pre_actual_dxb = temp_pre_dxb['gross_profit'].sum() / temp_pre_dxb['sales'].sum()
    gp_margin_pre_actual_shj = temp_pre_shj['gross_profit'].sum() / temp_pre_shj['sales'].sum()

    gp_margin_post_actual_uae = temp_post_uae['gross_profit'].sum() / temp_post_uae['sales'].sum()
    gp_margin_post_actual_auh = temp_post_auh['gross_profit'].sum() / temp_post_auh['sales'].sum()
    gp_margin_post_actual_aln = temp_post_aln['gross_profit'].sum() / temp_post_aln['sales'].sum()
    gp_margin_post_actual_dxb = temp_post_dxb['gross_profit'].sum() / temp_post_dxb['sales'].sum()
    gp_margin_post_actual_shj = temp_post_shj['gross_profit'].sum() / temp_post_shj['sales'].sum()

    gp_margin_delta_actual_uae = round((gp_margin_post_actual_uae - gp_margin_pre_actual_uae)*100, 2)
    gp_margin_delta_actual_auh = round((gp_margin_post_actual_auh - gp_margin_pre_actual_auh)*100, 2)
    gp_margin_delta_actual_aln = round((gp_margin_post_actual_aln - gp_margin_pre_actual_aln)*100, 2)
    gp_margin_delta_actual_dxb = round((gp_margin_post_actual_dxb - gp_margin_pre_actual_dxb)*100, 2)
    gp_margin_delta_actual_shj = round((gp_margin_post_actual_shj - gp_margin_pre_actual_shj)*100, 2)

    ########## GP Margin Delta (Delist Reco)

    gp_margin_pre_reco_uae = temp_pre_uae_reco['gross_profit'].sum() / temp_pre_uae_reco['sales'].sum()
    gp_margin_pre_reco_auh = temp_pre_auh_reco['gross_profit'].sum() / temp_pre_auh_reco['sales'].sum()
    gp_margin_pre_reco_aln = temp_pre_aln_reco['gross_profit'].sum() / temp_pre_aln_reco['sales'].sum()
    gp_margin_pre_reco_dxb = temp_pre_dxb_reco['gross_profit'].sum() / temp_pre_dxb_reco['sales'].sum()
    gp_margin_pre_reco_shj = temp_pre_shj_reco['gross_profit'].sum() / temp_pre_shj_reco['sales'].sum()

    gp_margin_post_reco_uae = temp_post_uae_reco['gross_profit'].sum() / temp_post_uae_reco['sales'].sum()
    gp_margin_post_reco_auh = temp_post_auh_reco['gross_profit'].sum() / temp_post_auh_reco['sales'].sum()
    gp_margin_post_reco_aln = temp_post_aln_reco['gross_profit'].sum() / temp_post_aln_reco['sales'].sum()
    gp_margin_post_reco_dxb = temp_post_dxb_reco['gross_profit'].sum() / temp_post_dxb_reco['sales'].sum()
    gp_margin_post_reco_shj = temp_post_shj_reco['gross_profit'].sum() / temp_post_shj_reco['sales'].sum()

    gp_margin_delta_reco_uae = round((gp_margin_post_reco_uae - gp_margin_pre_reco_uae)*100, 2)
    gp_margin_delta_reco_auh = round((gp_margin_post_reco_auh - gp_margin_pre_reco_auh)*100, 2)
    gp_margin_delta_reco_aln = round((gp_margin_post_reco_aln - gp_margin_pre_reco_aln)*100, 2)
    gp_margin_delta_reco_dxb = round((gp_margin_post_reco_dxb - gp_margin_pre_reco_dxb)*100, 2)
    gp_margin_delta_reco_shj = round((gp_margin_post_reco_shj - gp_margin_pre_reco_shj)*100, 2)

    print(f"----------{i}----------")
    print(f"Pre-delist Sales\nUAE: {temp_pre_uae['sales'].sum().round()}\nAbu Dhabi: {temp_pre_auh['sales'].sum().round()}\nAl Ain: {temp_pre_aln['sales'].sum().round()}\nDubai: {temp_pre_dxb['sales'].sum().round()}\nSharjah: {temp_pre_shj['sales'].sum().round()}")
    print(f"\nPost Delist Sales\nUAE: {temp_post_uae['sales'].sum().round()}\nAbu Dhabi: {temp_post_auh['sales'].sum().round()}\nAl Ain: {temp_post_aln['sales'].sum().round()}\nDubai: {temp_post_dxb['sales'].sum().round()}\nSharjah: {temp_post_shj['sales'].sum().round()}")
    print(f"\nAverage Monthly Sales Growth\nUAE: {avg_month_sales_growth_uae}%\nAbu Dhabi: {avg_month_sales_growth_auh}%\nAl Ain: {avg_month_sales_growth_aln}%\nDubai: {avg_month_sales_growth_dxb}%\nSharjah: {avg_month_sales_growth_shj}%")
    print(f"\nAverage Monthly GP Growth\nUAE: {avg_month_gp_growth_uae}%\nAbu Dhabi: {avg_month_gp_growth_auh}%\nAl Ain: {avg_month_gp_growth_aln}%\nDubai: {avg_month_gp_growth_dxb}%\nSharjah: {avg_month_gp_growth_shj}%")
    print(f"\nGP Margin Delta (Actual Delistings)\nUAE: {gp_margin_delta_actual_uae}%\nAbu Dhabi: {gp_margin_delta_actual_auh}%\nAl Ain: {gp_margin_delta_actual_aln}%\nDubai: {gp_margin_delta_actual_dxb}%\nSharjah: {gp_margin_delta_actual_shj}%\n")
    print(f"\nGP Margin Delta (Delist Reco)\nUAE: {gp_margin_delta_reco_uae}%\nAbu Dhabi: {gp_margin_delta_reco_auh}%\nAl Ain: {gp_margin_delta_reco_aln}%\nDubai: {gp_margin_delta_reco_dxb}%\nSharjah: {gp_margin_delta_reco_shj}%\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ###RCA

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
# MAGIC         category_name,
# MAGIC         material_group_name,
# MAGIC         material_id,
# MAGIC         SUM(amount) AS sales
# MAGIC     FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC     WHERE
# MAGIC         business_day BETWEEN "2024-01-01" AND "2024-08-08"
# MAGIC         AND category_name = "PASTA & NOODLE"
# MAGIC         AND tayeb_flag = 0
# MAGIC         AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC         AND amount > 0
# MAGIC         AND quantity > 0
# MAGIC     GROUP BY 1, 2, 3, 4, 5
# MAGIC ),
# MAGIC
# MAGIC gp AS (
# MAGIC     SELECT
# MAGIC         CASE WHEN region = 'AUH' THEN 'ABU DHABI'
# MAGIC             WHEN region = 'ALN' THEN 'AL AIN'
# MAGIC             WHEN region = 'DXB' THEN 'DUBAI'
# MAGIC             ELSE 'SHARJAH' END AS region_name,
# MAGIC         year_month,
# MAGIC         material_id,
# MAGIC         gp_wth_chargeback
# MAGIC     FROM gold.business.gross_profit
# MAGIC     WHERE year_month BETWEEN 202401 AND 202408
# MAGIC     AND country = 'AE'
# MAGIC ),
# MAGIC
# MAGIC combined AS (
# MAGIC     SELECT
# MAGIC         t1.region_name,
# MAGIC         t1.year_month,
# MAGIC         category_name,
# MAGIC         material_group_name,
# MAGIC         t1.material_id,
# MAGIC         sales,
# MAGIC         COALESCE(sales*gp_wth_chargeback/100, 0) AS gp
# MAGIC     FROM sales AS t1
# MAGIC     LEFT JOIN gp AS t2
# MAGIC         ON t1.region_name = t2.region_name
# MAGIC         AND t1.year_month = t2.year_month
# MAGIC         AND t1.material_id = t2.material_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     category_name,
# MAGIC     material_group_name,
# MAGIC     ROUND(SUM(gp) / SUM(sales) * 100, 2) AS gp_margin,
# MAGIC     ROUND(SUM(sales) / SUM(SUM(sales)) OVER () * 100, 2) AS sales_contri,
# MAGIC     ROUND(SUM(gp) / SUM(SUM(gp)) OVER () * 100, 2) AS gp_contri
# MAGIC FROM combined
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 4 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     ROUND(SUM(amount)) AS post_sales
# MAGIC FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC WHERE
# MAGIC     business_day BETWEEN "2024-06-08" AND "2024-08-08"
# MAGIC     AND material_group_name = "PASTA"
# MAGIC     AND tayeb_flag = 0
# MAGIC     AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC     AND amount > 0
# MAGIC     AND quantity > 0
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     ROUND(SUM(amount)) AS post_sales
# MAGIC FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC WHERE
# MAGIC     business_day BETWEEN "2024-06-01" AND "2024-08-08"
# MAGIC     AND material_group_name IN ("INSTANT NOODLE", "CUP NOODLE")
# MAGIC     AND tayeb_flag = 0
# MAGIC     AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC     AND amount > 0
# MAGIC     AND quantity > 0
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     ROUND(SUM(amount)) AS post_sales
# MAGIC FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC WHERE
# MAGIC     business_day BETWEEN "2024-06-15" AND "2024-08-08"
# MAGIC     AND material_group_name = "COCONUT OIL"
# MAGIC     AND tayeb_flag = 0
# MAGIC     AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC     AND amount > 0
# MAGIC     AND quantity > 0
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     region_name,
# MAGIC     CASE WHEN business_day <= "2023-01-01" THEN "Year 1" ELSE "Year 2" END AS year,
# MAGIC     ROUND(SUM(CASE WHEN MONTH(business_day) <= 5 THEN amount END)) AS pre_sales,
# MAGIC     ROUND(SUM(CASE WHEN MONTH(business_day) >= 6 THEN amount END)) AS post_sales,
# MAGIC     ROUND((post_sales - pre_sales) / pre_sales * 100, 2) AS growth
# MAGIC FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC WHERE
# MAGIC     (business_day BETWEEN "2022-02-19" AND "2022-05-17"
# MAGIC         OR business_day BETWEEN "2022-06-08" AND "2022-08-08"
# MAGIC         OR business_day BETWEEN "2023-02-19" AND "2023-05-17"
# MAGIC         OR business_day BETWEEN "2023-06-08" AND "2023-08-08")
# MAGIC     AND material_group_name = "PASTA"
# MAGIC     AND tayeb_flag = 0
# MAGIC     AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC     AND amount > 0
# MAGIC     AND quantity > 0
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     region_name,
# MAGIC     CASE WHEN business_day <= "2023-01-01" THEN "Year 1" ELSE "Year 2" END AS year,
# MAGIC     ROUND(SUM(CASE WHEN MONTH(business_day) <= 5 THEN amount END)) AS pre_sales,
# MAGIC     ROUND(SUM(CASE WHEN MONTH(business_day) >= 6 THEN amount END)) AS post_sales,
# MAGIC     ROUND((post_sales - pre_sales) / pre_sales * 100, 2) AS growth
# MAGIC FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC WHERE
# MAGIC     (business_day BETWEEN "2022-01-01" AND "2022-04-28"
# MAGIC         OR business_day BETWEEN "2022-06-15" AND "2022-08-08"
# MAGIC         OR business_day BETWEEN "2023-02-01" AND "2023-04-28"
# MAGIC         OR business_day BETWEEN "2023-06-15" AND "2023-08-08")
# MAGIC     AND material_group_name = "COCONUT OIL"
# MAGIC     AND tayeb_flag = 0
# MAGIC     AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC     AND amount > 0
# MAGIC     AND quantity > 0
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales as (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         material_group_name,
# MAGIC         SUM(CASE WHEN business_day <= "2024-05-17" THEN amount END) AS pre_sales,
# MAGIC         SUM(CASE WHEN business_day >= "2024-06-08" THEN amount END) AS post_sales
# MAGIC     FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC     WHERE
# MAGIC         business_day BETWEEN "2024-02-19" AND "2024-08-08"
# MAGIC         AND material_group_name = "PASTA"
# MAGIC         AND tayeb_flag = 0
# MAGIC         AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC         AND amount > 0
# MAGIC         AND quantity > 0
# MAGIC     GROUP BY 1, 2, 3, 4
# MAGIC ),
# MAGIC
# MAGIC promo_table AS (
# MAGIC     SELECT
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         CONCAT(SUBSTRING(business_date, 1, 4), '-',
# MAGIC             SUBSTRING(business_date, 5, 2), '-',
# MAGIC             SUBSTRING(business_date, 7, 2)) AS formatted_date
# MAGIC     FROM gold.marketing.uae_pos_sales_campaign
# MAGIC     WHERE
# MAGIC         void_flag IS NULL
# MAGIC         AND campaign_id IS NOT NULL
# MAGIC         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
# MAGIC         AND (pm_reason_code != "HAPPINESS VOUCHER")
# MAGIC         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
# MAGIC         AND business_date BETWEEN "20240201" AND "20240808"
# MAGIC     GROUP BY transaction_id, product_id, business_date
# MAGIC ),
# MAGIC
# MAGIC final AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         material_group_name,
# MAGIC         ROUND(SUM(pre_sales)) AS total_pre_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
# MAGIC         ROUND(pre_promo_sales/total_pre_sales*100, 2) AS pre_promo_sales_perc,
# MAGIC         ROUND(SUM(post_sales)) AS total_post_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales,
# MAGIC         ROUND(post_promo_sales/total_post_sales*100, 2) AS post_promo_sales_perc
# MAGIC     FROM sales AS t1
# MAGIC     LEFT JOIN promo_table AS t2
# MAGIC         ON t1.transaction_id = t2.transaction_id
# MAGIC         AND t1.product_id = t2.product_id
# MAGIC     GROUP BY 1, 2
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     region_name,
# MAGIC     pre_promo_sales_perc,
# MAGIC     post_promo_sales_perc,
# MAGIC     ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta
# MAGIC FROM final

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales as (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         material_group_name,
# MAGIC         SUM(CASE WHEN business_day <= "2024-05-17" THEN amount END) AS pre_sales,
# MAGIC         SUM(CASE WHEN business_day >= "2024-06-08" THEN amount END) AS post_sales
# MAGIC     FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC     WHERE
# MAGIC         (business_day BETWEEN "2024-02-19" AND "2024-05-17"
# MAGIC         OR business_day BETWEEN "2024-06-08" AND "2024-08-08")
# MAGIC         AND material_group_name = "PASTA"
# MAGIC         AND tayeb_flag = 0
# MAGIC         AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC         AND amount > 0
# MAGIC         AND quantity > 0
# MAGIC     GROUP BY 1, 2, 3, 4, 5
# MAGIC ),
# MAGIC
# MAGIC promo_table AS (
# MAGIC     SELECT
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         CONCAT(SUBSTRING(business_date, 1, 4), '-',
# MAGIC             SUBSTRING(business_date, 5, 2), '-',
# MAGIC             SUBSTRING(business_date, 7, 2)) AS formatted_date
# MAGIC     FROM gold.marketing.uae_pos_sales_campaign
# MAGIC     WHERE
# MAGIC         void_flag IS NULL
# MAGIC         AND campaign_id IS NOT NULL
# MAGIC         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
# MAGIC         AND (pm_reason_code != "HAPPINESS VOUCHER")
# MAGIC         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
# MAGIC         AND business_date BETWEEN "20240201" AND "20240808"
# MAGIC     GROUP BY transaction_id, product_id, business_date
# MAGIC ),
# MAGIC
# MAGIC combined AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         year_month,
# MAGIC         material_group_name,
# MAGIC         t1.product_id,
# MAGIC         ROUND(SUM(pre_sales)) AS total_pre_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
# MAGIC         ROUND(SUM(post_sales)) AS total_post_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales
# MAGIC     FROM sales AS t1
# MAGIC     LEFT JOIN promo_table AS t2
# MAGIC         ON t1.transaction_id = t2.transaction_id
# MAGIC         AND t1.product_id = t2.product_id
# MAGIC     GROUP BY 1, 2, 3, 4
# MAGIC     ORDER BY 5 DESC
# MAGIC ),
# MAGIC
# MAGIC final AS (
# MAGIC     SELECT
# MAGIC         material_group_name,
# MAGIC         region_name,
# MAGIC         year_month,
# MAGIC         product_id,
# MAGIC         total_pre_sales,
# MAGIC         pre_promo_sales,
# MAGIC         total_post_sales,
# MAGIC         post_promo_sales
# MAGIC     FROM combined
# MAGIC ),
# MAGIC
# MAGIC gp_data AS (
# MAGIC     SELECT
# MAGIC         CASE WHEN region = "AUH" THEN "ABU DHABI"
# MAGIC             WHEN region = "ALN" THEN "AL AIN"
# MAGIC             WHEN region = "DXB" THEN "DUBAI"
# MAGIC             ELSE "SHARJAH" END AS region_name,
# MAGIC         year_month,
# MAGIC         material_id,
# MAGIC         gp_wth_chargeback
# MAGIC     FROM gold.business.gross_profit
# MAGIC     WHERE
# MAGIC         country = 'AE'
# MAGIC         AND year_month BETWEEN 202402 AND 202405
# MAGIC ),
# MAGIC
# MAGIC final_2 AS (
# MAGIC     SELECT
# MAGIC         material_group_name,
# MAGIC         t1.year_month,
# MAGIC         t1.region_name,
# MAGIC         product_id,
# MAGIC         total_pre_sales,
# MAGIC         pre_promo_sales,
# MAGIC         total_post_sales,
# MAGIC         post_promo_sales,
# MAGIC         ROUND(total_pre_sales * gp_wth_chargeback / 100, 2) AS gp
# MAGIC     FROM final AS t1
# MAGIC     LEFT JOIN gp_data AS t2
# MAGIC         ON t1.region_name = t2.region_name
# MAGIC         AND t1.year_month = t2.year_month
# MAGIC         AND t1.product_id = t2.material_id
# MAGIC ),
# MAGIC
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         material_group_name,
# MAGIC         region_name,
# MAGIC         product_id,
# MAGIC         SUM(total_pre_sales) AS total_pre_sales_,
# MAGIC         ROUND(SUM(pre_promo_sales)/total_pre_sales_*100, 2) AS pre_promo_sales_perc,
# MAGIC         ROUND(SUM(post_promo_sales)/SUM(total_post_sales)*100, 2) AS post_promo_sales_perc,
# MAGIC         ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta,
# MAGIC         ROUND(total_pre_sales_ / SUM(gp) * 100, 2) AS gp_margin,
# MAGIC         ROW_NUMBER() OVER(PARTITION BY region_name ORDER BY SUM(total_pre_sales) DESC) AS rk
# MAGIC     FROM final_2
# MAGIC     GROUP BY 1, 2, 3
# MAGIC     ORDER BY region_name, total_pre_sales_ DESC
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     region_name,
# MAGIC     product_id,
# MAGIC     total_pre_sales_,
# MAGIC     pre_promo_sales_perc,
# MAGIC     post_promo_sales_perc,
# MAGIC     delta,
# MAGIC     gp_margin,
# MAGIC     rk
# MAGIC FROM ranked
# MAGIC WHERE rk <= 20

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales as (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         material_group_name,
# MAGIC         SUM(CASE WHEN business_day <= "2024-04-28" THEN amount END) AS pre_sales,
# MAGIC         SUM(CASE WHEN business_day >= "2024-06-01" THEN amount END) AS post_sales
# MAGIC     FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC     WHERE
# MAGIC         business_day BETWEEN "2024-02-01" AND "2024-08-08"
# MAGIC         AND material_group_name = "INSTANT NOODLE"
# MAGIC         AND tayeb_flag = 0
# MAGIC         AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC         AND amount > 0
# MAGIC         AND quantity > 0
# MAGIC     GROUP BY 1, 2, 3, 4
# MAGIC ),
# MAGIC
# MAGIC promo_table AS (
# MAGIC     SELECT
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         CONCAT(SUBSTRING(business_date, 1, 4), '-',
# MAGIC             SUBSTRING(business_date, 5, 2), '-',
# MAGIC             SUBSTRING(business_date, 7, 2)) AS formatted_date
# MAGIC     FROM gold.marketing.uae_pos_sales_campaign
# MAGIC     WHERE
# MAGIC         void_flag IS NULL
# MAGIC         AND campaign_id IS NOT NULL
# MAGIC         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
# MAGIC         AND (pm_reason_code != "HAPPINESS VOUCHER")
# MAGIC         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
# MAGIC         AND business_date BETWEEN "20240201" AND "20240808"
# MAGIC     GROUP BY transaction_id, product_id, business_date
# MAGIC ),
# MAGIC
# MAGIC final AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         material_group_name,
# MAGIC         ROUND(SUM(pre_sales)) AS total_pre_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
# MAGIC         ROUND(pre_promo_sales/total_pre_sales*100, 2) AS pre_promo_sales_perc,
# MAGIC         ROUND(SUM(post_sales)) AS total_post_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales,
# MAGIC         ROUND(post_promo_sales/total_post_sales*100, 2) AS post_promo_sales_perc
# MAGIC     FROM sales AS t1
# MAGIC     LEFT JOIN promo_table AS t2
# MAGIC         ON t1.transaction_id = t2.transaction_id
# MAGIC         AND t1.product_id = t2.product_id
# MAGIC     GROUP BY 1, 2
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     region_name,
# MAGIC     pre_promo_sales_perc,
# MAGIC     post_promo_sales_perc,
# MAGIC     ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta
# MAGIC FROM final

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sales as (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         INT(CONCAT(YEAR(business_day), LPAD(MONTH(business_day), 2, '0'))) AS year_month,
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         material_group_name,
# MAGIC         SUM(CASE WHEN business_day <= "2024-04-28" THEN amount END) AS pre_sales,
# MAGIC         SUM(CASE WHEN business_day >= "2024-06-01" THEN amount END) AS post_sales
# MAGIC     FROM gold.transaction.uae_pos_transactions AS t1
# MAGIC     JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
# MAGIC     JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
# MAGIC     WHERE
# MAGIC         (business_day BETWEEN "2024-02-01" AND "2024-04-28"
# MAGIC         OR business_day BETWEEN "2024-06-01" AND "2024-08-08")
# MAGIC         AND material_group_name = "INSTANT NOODLE"
# MAGIC         AND tayeb_flag = 0
# MAGIC         AND transaction_type IN ("SALE", "SELL_MEDIA")
# MAGIC         AND amount > 0
# MAGIC         AND quantity > 0
# MAGIC     GROUP BY 1, 2, 3, 4, 5
# MAGIC ),
# MAGIC
# MAGIC promo_table AS (
# MAGIC     SELECT
# MAGIC         transaction_id,
# MAGIC         product_id,
# MAGIC         CONCAT(SUBSTRING(business_date, 1, 4), '-',
# MAGIC             SUBSTRING(business_date, 5, 2), '-',
# MAGIC             SUBSTRING(business_date, 7, 2)) AS formatted_date
# MAGIC     FROM gold.marketing.uae_pos_sales_campaign
# MAGIC     WHERE
# MAGIC         void_flag IS NULL
# MAGIC         AND campaign_id IS NOT NULL
# MAGIC         AND (pm_campaign_group NOT IN ('HAPPINESS BURN', 'HAPPINESS EARNED') OR pm_campaign_group IS NULL)
# MAGIC         AND (pm_reason_code != "HAPPINESS VOUCHER")
# MAGIC         AND (pm_discount_media_type != 'Special Offer0' OR pm_discount_media_type IS NULL)
# MAGIC         AND business_date BETWEEN "20240201" AND "20240808"
# MAGIC     GROUP BY transaction_id, product_id, business_date
# MAGIC ),
# MAGIC
# MAGIC combined AS (
# MAGIC     SELECT
# MAGIC         region_name,
# MAGIC         year_month,
# MAGIC         material_group_name,
# MAGIC         t1.product_id,
# MAGIC         ROUND(SUM(pre_sales)) AS total_pre_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN pre_sales ELSE 0 END)) AS pre_promo_sales,
# MAGIC         ROUND(SUM(post_sales)) AS total_post_sales,
# MAGIC         ROUND(SUM(CASE WHEN formatted_date IS NOT NULL THEN post_sales ELSE 0 END)) AS post_promo_sales
# MAGIC     FROM sales AS t1
# MAGIC     LEFT JOIN promo_table AS t2
# MAGIC         ON t1.transaction_id = t2.transaction_id
# MAGIC         AND t1.product_id = t2.product_id
# MAGIC     GROUP BY 1, 2, 3, 4
# MAGIC     ORDER BY 5 DESC
# MAGIC ),
# MAGIC
# MAGIC final AS (
# MAGIC     SELECT
# MAGIC         material_group_name,
# MAGIC         region_name,
# MAGIC         year_month,
# MAGIC         product_id,
# MAGIC         total_pre_sales,
# MAGIC         pre_promo_sales,
# MAGIC         total_post_sales,
# MAGIC         post_promo_sales
# MAGIC     FROM combined
# MAGIC ),
# MAGIC
# MAGIC gp_data AS (
# MAGIC     SELECT
# MAGIC         CASE WHEN region = "AUH" THEN "ABU DHABI"
# MAGIC             WHEN region = "ALN" THEN "AL AIN"
# MAGIC             WHEN region = "DXB" THEN "DUBAI"
# MAGIC             ELSE "SHARJAH" END AS region_name,
# MAGIC         year_month,
# MAGIC         material_id,
# MAGIC         gp_wth_chargeback
# MAGIC     FROM gold.business.gross_profit
# MAGIC     WHERE
# MAGIC         country = 'AE'
# MAGIC         AND year_month BETWEEN 202402 AND 202404
# MAGIC ),
# MAGIC
# MAGIC final_2 AS (
# MAGIC     SELECT
# MAGIC         material_group_name,
# MAGIC         t1.year_month,
# MAGIC         t1.region_name,
# MAGIC         product_id,
# MAGIC         total_pre_sales,
# MAGIC         pre_promo_sales,
# MAGIC         total_post_sales,
# MAGIC         post_promo_sales,
# MAGIC         ROUND(total_pre_sales * gp_wth_chargeback / 100, 2) AS gp
# MAGIC     FROM final AS t1
# MAGIC     LEFT JOIN gp_data AS t2
# MAGIC         ON t1.region_name = t2.region_name
# MAGIC         AND t1.year_month = t2.year_month
# MAGIC         AND t1.product_id = t2.material_id
# MAGIC ),
# MAGIC
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         material_group_name,
# MAGIC         region_name,
# MAGIC         product_id,
# MAGIC         SUM(total_pre_sales) AS total_pre_sales_,
# MAGIC         ROUND(SUM(pre_promo_sales)/total_pre_sales_*100, 2) AS pre_promo_sales_perc,
# MAGIC         ROUND(SUM(post_promo_sales)/SUM(total_post_sales)*100, 2) AS post_promo_sales_perc,
# MAGIC         ROUND(post_promo_sales_perc - pre_promo_sales_perc, 2) AS delta,
# MAGIC         ROUND(total_pre_sales_ / SUM(gp) * 100, 2) AS gp_margin,
# MAGIC         ROW_NUMBER() OVER(PARTITION BY region_name ORDER BY SUM(total_pre_sales) DESC) AS rk
# MAGIC     FROM final_2
# MAGIC     GROUP BY 1, 2, 3
# MAGIC     ORDER BY region_name, total_pre_sales_ DESC
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     material_group_name,
# MAGIC     region_name,
# MAGIC     product_id,
# MAGIC     total_pre_sales_,
# MAGIC     pre_promo_sales_perc,
# MAGIC     post_promo_sales_perc,
# MAGIC     delta,
# MAGIC     gp_margin,
# MAGIC     rk
# MAGIC FROM ranked
# MAGIC WHERE rk <= 20

# COMMAND ----------

# MAGIC %md
# MAGIC ###EDA Temp (End)

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.pj_assortment_dashboard_region_view")

# COMMAND ----------

# %sql
# SELECT * FROM dev.sandbox.pj_assortment_dashboard_region_view LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calendar Table

# COMMAND ----------

query = f"""
SELECT business_day AS cy_date
FROM dev.sandbox.pj_assortment_dashboard_region_view
WHERE business_day >= '{cy_start_date}'
GROUP BY 1
ORDER BY 1
"""

cal_df = spark.sql(query).toPandas()

cal_df['cy_date'] = pd.to_datetime(cal_df['cy_date']).dt.date
cal_df['py_date'] = cal_df.cy_date - pd.DateOffset(days=364)
cal_df['py_date'] = cal_df['py_date'].dt.date
cal_df = cal_df[cal_df['cy_date'] != pd.to_datetime('2024-02-29').date()].reset_index(drop = True)
cal_df['dayno'] = range(1, len(cal_df) + 1)

spark_df = spark.createDataFrame(cal_df)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.tbl_assortment_lfl_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Current & Previous Year Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.sandbox.pj_assortment_dashboard_cy_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM dev.sandbox.pj_assortment_dashboard_region_view AS t1
# MAGIC     JOIN dev.sandbox.tbl_assortment_lfl_calendar AS t2 ON t1.business_day = t2.cy_date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.sandbox.pj_assortment_dashboard_py_region_view AS (
# MAGIC     SELECT
# MAGIC         t1.*,
# MAGIC         t2.dayno
# MAGIC     FROM dev.sandbox.pj_assortment_dashboard_region_view AS t1
# MAGIC     JOIN dev.sandbox.tbl_assortment_lfl_calendar AS t2 ON t1.business_day = t2.py_date
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Customer Penetration Table

# COMMAND ----------

# query = f"""
# SELECT DISTINCT
#     business_day,
#     region_name,
#     t1.store_id,
#     store_name,
#     department_name,
#     category_name,
#     material_group_name,
#     t1.customer_id,
#     segment
# FROM gold.pos_transactions AS t1
# JOIN gold.material_master AS t2 ON t1.product_id = t2.material_id
# JOIN gold.store_master AS t3 ON t1.store_id = t3.store_id
# JOIN analytics.customer_segments AS t4 ON t1.customer_id = t4.customer_id
# WHERE
#     (business_day BETWEEN '{py_start_date}' AND '{py_end_date}'
#     OR business_day BETWEEN '{cy_start_date}' AND '{cy_end_date}')

#     AND (category_name IN ({categories_sql})
#     OR material_group_name IN ({material_groups_sql}))

#     AND transaction_type IN ('SALE', 'SELL_MEDIA')
#     AND key = 'rfm'
#     AND channel = 'pos'
#     AND t4.country = 'uae'
#     AND month_year = {rfm_month_year}
# """

# cust_df = spark.sql(query).toPandas()

# COMMAND ----------

# spark_df = spark.createDataFrame(cust_df)
# spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("sandbox.pj_assortment_dashboard_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transactions Table

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_cy AS (
#     WITH pj_assortment_dashboard_cy_trans AS (
#         SELECT
#             a.business_day AS trans_date,
#             cast(a.transaction_id AS STRING) AS transaction_id,
#             a.ean,
#             a.product_id AS material_id,
#             material_name,
#             material_group_name,
#             category_name,
#             department_name,
#             brand,
#             store_name,
#             region_name,
#             a.store_id AS store_id,
#             SUM(a.amount) AS amount,
#             SUM(a.quantity) AS quantity 
#         FROM gold.pos_transactions a
#         LEFT JOIN gold.material_master e ON a.product_id = e.material_id
#         LEFT JOIN gold.store_master f ON a.store_id = f.store_id
#         WHERE
#             a.business_day BETWEEN '2023-12-01' AND DATE_SUB(CURRENT_DATE(), 1)
#             AND f.tayeb_flag = 0
#             AND amount > 0
#             AND quantity > 0
#             AND a.transaction_type IN ('SALE','SELL_MEDIA')
#             AND a.product_id IS NOT NULL
#             AND category_name = "WATER"
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
#     ),

#     pj_assortment_dashboard_cy_ovr AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             'overall' AS material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_cy_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_assortment_dashboard_cy_dept AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_cy_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_assortment_dashboard_cy AS (
#         SELECT *
#         FROM pj_assortment_dashboard_cy_ovr

#         UNION

#         SELECT *
#         FROM pj_assortment_dashboard_cy_dept
#     )

#     SELECT
#         a.*,
#         b.cy_date,
#         b.dayno AS r_day
#     FROM pj_assortment_dashboard_cy a
#     JOIN dashboard.tbl_water_assortment_lfl_calendar b ON a.trans_date = b.cy_date
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_py AS (
#     WITH pj_assortment_dashboard_py_trans AS (
#         SELECT
#             a.business_day AS trans_date,
#             cast(a.transaction_id AS STRING) AS transaction_id,
#             a.ean,
#             a.product_id AS material_id,
#             material_name,
#             material_group_name,
#             category_name,
#             department_name,
#             brand,
#             store_name,
#             region_name,
#             a.store_id AS store_id,
#             SUM(a.amount) AS amount,
#             SUM(a.quantity) AS quantity 
#         FROM gold.pos_transactions a
#         LEFT JOIN gold.material_master e ON a.product_id = e.material_id
#         LEFT JOIN gold.store_master f ON a.store_id = f.store_id
#         WHERE
#             a.business_day BETWEEN '2022-12-01' AND DATE_SUB(CURRENT_DATE(), 365)
#             AND f.tayeb_flag = 0
#             AND amount > 0
#             AND quantity > 0
#             AND a.transaction_type IN ('SALE','SELL_MEDIA')
#             AND a.product_id IS NOT NULL
#             AND category_name = "WATER"
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
#     ),

#     pj_assortment_dashboard_py_ovr AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             'overall' AS material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_py_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),


#     pj_assortment_dashboard_py_dept AS (
#         SELECT
#             trans_date,
#             department_name,
#             category_name,
#             material_group_name,
#             store_id,
#             store_name,
#             region_name,
#             SUM(amount) AS amount,
#             SUM(quantity) AS quantity,
#             COUNT(DISTINCT transaction_id) AS trans
#         FROM pj_assortment_dashboard_py_trans
#         GROUP BY 1, 2, 3, 4, 5, 6, 7
#     ),

#     pj_assortment_dashboard_py AS (
#         SELECT *
#         FROM pj_assortment_dashboard_py_ovr

#         UNION

#         SELECT *
#         FROM pj_assortment_dashboard_py_dept
#     )

#     SELECT
#         a.*,
#         b.py_date,
#         b.dayno AS r_day
#     FROM pj_assortment_dashboard_py a
#     JOIN dashboard.tbl_water_assortment_lfl_calendar b ON a.trans_date = b.py_date
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE sandbox.pj_assortment_dashboard_lfl_data AS (
#     SELECT
#         NVL(a.r_day, b.r_day) AS r_day,
#         a.trans_date AS cy_date,
#         b.trans_date AS py_date,
#         NVL(a.department_name, b.department_name) AS department_name,
#         NVL(a.category_name, b.category_name) AS category_name,
#         NVL(a.material_group_name, b.material_group_name) AS material_group_name,
#         NVL(a.store_id, b.store_id) AS store_id,
#         NVL(a.store_name, b.store_name) AS store_name,
#         NVL(a.region_name, b.region_name) AS region_name,
#         NVL(a.amount, 0) AS cy_amount,
#         NVL(a.quantity, 0) AS cy_quantity,
#         NVL(a.trans, 0) AS cy_trans,
#         NVL(b.amount, 0) AS py_amount,
#         NVL(b.quantity, 0) AS py_quantity,
#         NVL(b.trans, 0) AS py_trans
#     FROM sandbox.pj_assortment_dashboard_cy AS a
#     FULL JOIN sandbox.pj_assortment_dashboard_py AS b
#         ON a.r_day = b.r_day
#         AND a.store_id = b.store_id
#         AND a.department_name = b.department_name
#         AND a.category_name = b.category_name
#         AND a.material_group_name = b.material_group_name
#         AND a.region_name = b.region_name
# )

# COMMAND ----------

# MAGIC %md
# MAGIC #EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ##KPI Cards - Top

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delist Recommended SKUs
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM sandbox.pj_assortment_dashboard_reco
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rationalized SKUs
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE delisted_date != "NA"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rationalized SKUs From Recommendation
# MAGIC SELECT COUNT(DISTINCT material_id)
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE delisted_date != "NA"
# MAGIC AND category_name = "WATER"
# MAGIC AND recommendation = "Delist"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sales of Delisted SKUs
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC     SUM(sales) AS total_sales,
# MAGIC     ROUND(delist_sales/total_sales,4) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE category_period_type = "Pre-delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 Sales of Delisted SKUs in Pre-delist Period
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-07"
# MAGIC     AND region_name = "ABU DHABI"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-08"
# MAGIC     AND region_name = "AL AIN"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS delist_sales,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-13"
# MAGIC     AND region_name IN ("DUBAI", "SHARJAH")
# MAGIC     AND category_name = "WATER"
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(delist_sales) AS tot_delist_sales,
# MAGIC     SUM(total_sales) AS tot_sales,
# MAGIC     ROUND(tot_delist_sales/tot_sales, 4) AS perc
# MAGIC FROM cte

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GP of Delisted SKUs
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC     SUM(gross_profit) AS total_gp,
# MAGIC     ROUND(delist_gp/total_gp,4) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE category_period_type = "Pre-delist"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 GP of Delisted SKUs in Pre-delist Period
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC         SUM(gross_profit) AS total_gp
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-07"
# MAGIC     AND region_name = "ABU DHABI"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC         SUM(gross_profit) AS total_gp
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-08"
# MAGIC     AND region_name = "AL AIN"
# MAGIC     AND category_name = "WATER"
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS delist_gp,
# MAGIC         SUM(gross_profit) AS total_gp
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE category_period_type = "Pre-delist"
# MAGIC     AND business_day >= "2023-12-13"
# MAGIC     AND region_name IN ("DUBAI", "SHARJAH")
# MAGIC     AND category_name = "WATER"
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(delist_gp) AS tot_delist_gp,
# MAGIC     SUM(total_gp) AS tot_gp,
# MAGIC     ROUND(tot_delist_gp/tot_gp, 4) AS perc
# MAGIC FROM cte

# COMMAND ----------

# MAGIC %md
# MAGIC ##Region-wise Delist Recommended vs Rationalized

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise SKU Count of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     COUNT(DISTINCT material_id) AS recommended,
# MAGIC     COUNT(DISTINCT CASE WHEN delisted_date != "NA" THEN material_id END) AS rationalized,
# MAGIC     ROUND(rationalized/recommended,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND material_group_name = "COCONUT OIL"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Sales of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(sales) AS sales_reco,
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN sales END) AS sales_reco_rationalized,
# MAGIC     ROUND(sales_reco_rationalized/sales_reco,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Volume of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(volume) AS volume_reco,
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN volume END) AS volume_reco_rationalized,
# MAGIC     ROUND(volume_reco_rationalized/volume_reco,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC -- FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise GP of Delist Recommended, and Rationalized Within Them
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(gross_profit) AS gp_reco,
# MAGIC     SUM(CASE WHEN delisted_date != "NA" THEN gross_profit END) AS gp_reco_rationalized,
# MAGIC     ROUND(gp_reco_rationalized/gp_reco,2) AS perc
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC -- FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##Region-wise Pre-delist vs LFL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Pre-delist vs LFL Sales
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN sales END) AS pre_delist_sales,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN sales END) AS lfl_sales,
# MAGIC     ROUND((pre_delist_sales - lfl_sales)/lfl_sales,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Post delist vs LFL Sales
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Post delist" THEN sales END) AS post_delist_sales,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Post delist" THEN sales END) AS lfl_sales,
# MAGIC     ROUND((post_delist_sales - lfl_sales)/lfl_sales,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Pre-delist vs LFL Volume
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN volume END) AS pre_delist_volume,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN volume END) AS lfl_volume,
# MAGIC     ROUND((pre_delist_volume - lfl_volume)/lfl_volume,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Post delist vs LFL Volume
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Post delist" THEN volume END) AS post_delist_volume,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Post delist" THEN volume END) AS lfl_volume,
# MAGIC     ROUND((post_delist_volume - lfl_volume)/lfl_volume,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Pre-delist vs LFL GP
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN gross_profit END) AS pre_delist_gp,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Pre-delist" THEN gross_profit END) AS lfl_gp,
# MAGIC     ROUND((pre_delist_gp - lfl_gp)/lfl_gp,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region-wise Post delist vs LFL GP
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     SUM(CASE WHEN business_day >= "2023-12-01" AND category_period_type_lfl = "Post delist" THEN gross_profit END) AS post_delist_gp,
# MAGIC     SUM(CASE WHEN business_day < "2023-12-01" AND category_period_type_lfl = "Post delist" THEN gross_profit END) AS lfl_gp,
# MAGIC     ROUND((post_delist_gp - lfl_gp)/lfl_gp,3) AS growth
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##KPI Cards - Middle

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(gross_profit)
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE new_sku_flag = 1
# MAGIC AND region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC         category_period_type,
# MAGIC         MONTH(business_day) AS month,
# MAGIC         SUM(sales) AS total_sales
# MAGIC     FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC     WHERE region_name = "ABU DHABI"
# MAGIC     AND category_name = "WATER"
# MAGIC     GROUP BY category_period_type, month
# MAGIC     ORDER BY 1, 2
# MAGIC )
# MAGIC
# MAGIC SELECT category_period_type, AVG(total_sales)
# MAGIC FROM cte
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trend Chart

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(volume)) AS periodly_volume
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(business_day) AS month,
# MAGIC     ROUND(SUM(volume)) AS periodly_volume
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     WEEKOFYEAR(business_day) AS week,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales,
# MAGIC     ROUND((periodly_gp/periodly_sales)*100,2) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales,
# MAGIC     ROUND((periodly_gp/periodly_sales)*100,2) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_day,
# MAGIC     ROUND(SUM(gross_profit)) AS periodly_gp,
# MAGIC     ROUND(SUM(sales)) AS periodly_sales,
# MAGIC     ROUND((periodly_gp/periodly_sales)*100,2) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Post delist"
# MAGIC AND business_day < "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY business_day
# MAGIC ORDER BY business_day

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top 10 Brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 Brands by Sales - Pre-delist vs Post Delist
# MAGIC SELECT
# MAGIC     brand,
# MAGIC     SUM(CASE WHEN category_period_type = "Post delist" THEN sales END) AS post_delist_sales,
# MAGIC     SUM(CASE WHEN category_period_type = "Pre-delist" THEN sales END) AS pre_delist_sales
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY brand
# MAGIC ORDER BY pre_delist_sales DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 Brands by Volume - Pre-delist vs Post Delist
# MAGIC SELECT
# MAGIC     brand,
# MAGIC     SUM(CASE WHEN category_period_type = "Post delist" THEN volume END) AS post_delist_volume,
# MAGIC     SUM(CASE WHEN category_period_type = "Pre-delist" THEN volume END) AS pre_delist_volume
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY brand
# MAGIC ORDER BY pre_delist_volume DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 Brands by GP - Pre-delist vs Post Delist
# MAGIC SELECT
# MAGIC     brand,
# MAGIC     SUM(CASE WHEN category_period_type = "Post delist" THEN gross_profit END) AS post_delist_gp,
# MAGIC     SUM(CASE WHEN category_period_type = "Pre-delist" THEN gross_profit END) AS pre_delist_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY brand
# MAGIC ORDER BY pre_delist_gp DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top 10 SKUs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     material_id,
# MAGIC     material_name,
# MAGIC     SUM(sales) AS total_sales,
# MAGIC     SUM(gross_profit) AS total_gp,
# MAGIC     (total_gp/total_sales) AS gp_margin
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE region_name = "ABU DHABI"
# MAGIC AND category_period_type_lfl = "Pre-delist"
# MAGIC AND business_day >= "2023-12-01"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY material_id, material_name
# MAGIC ORDER BY gp_margin DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delist View

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     material_id,
# MAGIC     SUM(sales) AS tot_sales,
# MAGIC     SUM(volume) AS tot_volume,
# MAGIC     SUM(gross_profit) AS tot_gp
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE material_id IN (5539, 91929)
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reco View

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(sales)
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE business_day BETWEEN "2023-12-01" AND "2024-01-30"
# MAGIC AND material_id = 123693

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delist Reco Adoption

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     material_id,
# MAGIC     material_name,
# MAGIC     brand,
# MAGIC     CASE WHEN delisted_date = "NA" THEN "NA" ELSE "Delisted" END AS delist_status
# MAGIC FROM sandbox.pj_assortment_dashboard_region_view
# MAGIC WHERE recommendation = "Delist"
# MAGIC AND category_name = "WATER"
# MAGIC GROUP BY ALL
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region_name,
# MAGIC     COUNT(DISTINCT material_id) AS delisted
# MAGIC FROM sandbox.pj_assortment_dashboard_reco
# MAGIC WHERE material_group_name = "COCONUT OIL"
# MAGIC AND recommendation = "Delist"
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##Daily Rate of Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     category_period_type,
# MAGIC     ROUND(SUM(sales) / COUNT(DISTINCT business_day), 2) AS daily_ros
# MAGIC FROM sandbox.pj_assortment_dashboard_cy_region_view
# MAGIC WHERE category_name = "WATER"
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND((297441.74 - 221098.56) / 221098.56, 4)

# COMMAND ----------

# MAGIC %md
# MAGIC #Misc

# COMMAND ----------

# %sql
# with qa_Data 
# as 
# (select 
#         DISTINCT transaction_id, 
#         business_day, 
#         transaction_begin_datetime, 
#         store_id, 
#         billed_amount,
#         gift_sale, 
#         mobile, 
#         loyalty_account_id,
#         t1.redemption_loyalty_id,
#         t1.returned_loyalty_id,
#         loyalty_points, 
#         loyalty_points_returned,
#         redeemed_points,
#         redeemed_amount, 
#         --voucher_sale, 
#         transaction_type_id
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration))
# select
# sum(billed_amount) 
# from
# qa_Data

# COMMAND ----------

# %sql
# select 
#         sum(t1.amount)
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)

# COMMAND ----------

# %sql
# with qa_bill_amt_data
# as (
# select 
#         DISTINCT transaction_id, 
#         business_day,
#         billed_amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
# ),
# qa_billamt_final as (
#   select
#   business_day,
#   sum(billed_amount) as bill_amount
#   from   
#   qa_bill_amt_data
#   group by
#   1
# ),
# qa_amt_final as (
#   select 
#         t1.business_day,
#         sum(t1.amount) as amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day between "2024-02-20" and "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
#          group by 1
# )
# select
# a.business_day,
# a.bill_amount,
# b.amount
# from   
# qa_billamt_final a 
# join
# qa_amt_final b 
# on a.business_day = b.business_day



# COMMAND ----------

# %sql
# with qa_bill_amt_data
# as (
# select 
#         DISTINCT transaction_id, 
#         t1.store_id,
#         business_day,
#         billed_amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day = "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
# ),
# qa_billamt_final as (
#   select
#   business_day,
#   store_id,
#   sum(billed_amount) as bill_amount
#   from   
#   qa_bill_amt_data
#   group by
#   1,2
# ),
# qa_amt_final as (
#   select 
#         t1.business_day,
#         t1.store_id.
#         sum(t1.amount) as amount
#         from gold.qatar_pos_transactions t1
#         where t1.business_day = "2024-03-27"
#          and t1.store_id in (SELECT DISTINCT storeid from dashboard.v_qatar_lhp_registration)
#          group by 1,2
# )
# select
# a.store_id,
# a.bill_amount,
# b.amount
# from   
# qa_billamt_final a 
# join
# qa_amt_final b 
# on a.business_day = b.business_day


