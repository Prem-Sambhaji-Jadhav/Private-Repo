# Databricks notebook source
# MAGIC %md
# MAGIC #Sandbox Table

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_ao_v2 AS (
#     WITH sales_data AS (
#         SELECT
#             t1.business_day,
#             INT(CONCAT(YEAR(t1.business_day), LPAD(MONTH(t1.business_day), 2, '0'))) AS year_month,
#             t3.region_name,
#             t1.transaction_id,
#             t2.material_id,
#             t2.material_name,
#             t2.brand,
#             t1.ean,
#             INT(t4.conversion_numerator) AS conversion_numerator,
#             t1.unit_price,
#             t1.regular_unit_price,
#             t1.quantity AS quantity,
#             t1.regular_unit_price * t1.quantity AS amount,
#             ROUND(t1.unit_price - t1.regular_unit_price, 2) AS discount,
#             ROUND(discount/t1.unit_price) AS discount_perc,
#             CASE WHEN discount > 0 THEN 1 ELSE 0 END AS discount_flag,
#             1 AS purchase_flag
#         FROM gold.transaction.uae_pos_transactions AS t1
#         LEFT JOIN gold.material.material_master AS t2 ON t1.product_id = t2.material_id
#         LEFT JOIN gold.store.store_master AS t3 ON t1.store_id = t3.store_id
#         LEFT JOIN gold.material.material_attributes AS t4 ON t1.ean = t4.ean
#         WHERE
#             t1.business_day BETWEEN "2023-07-01" AND "2024-06-30"
#             AND t2.category_name = "PASTA & NOODLE"
#             AND t2.material_group_name = "PASTA"
#             AND t1.store_id = 2370 -- AL WAHDA, AUH
#             AND t3.tayeb_flag = 0
#             AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
#             AND t1.amount > 0
#             AND t1.quantity > 0
#         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
#         ORDER BY 1, 4, 5
#     ),

#     gp_data AS (
#         SELECT
#             CASE WHEN region = "AUH" THEN "ABU DHABI"
#                 WHEN region = "ALN" THEN "AL AIN"
#                 WHEN region = "DXB" THEN "DUBAI"
#                 ELSE "SHARJAH" END AS region_name,
#             year_month,
#             material_id,
#             gp_wth_chargeback
#         FROM gold.business.gross_profit
#         WHERE country = 'AE'
#         AND year_month BETWEEN 202307 AND 202406
#     )

#     SELECT
#         t1.*,
#         ROUND(COALESCE(t1.amount * t2.gp_wth_chargeback / 100, 0), 2) AS abs_gp
#     FROM sales_data AS t1
#     LEFT JOIN gp_data AS t2
#         ON t1.region_name = t2.region_name
#         AND t1.year_month = t2.year_month
#         AND t1.material_id = t2.material_id
# )

# COMMAND ----------

# MAGIC %md
# MAGIC #Attributes Data

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

materials = [4073, 4076, 4080, 4087, 4089, 4227, 10126, 10127, 10128, 14057, 14292, 14296, 14297, 14305, 15653, 15657, 15658, 15661, 15688, 15689, 15690, 15691, 15692, 15693, 15696, 15701, 15702, 19076, 42476, 62067, 62069, 62070, 62080, 62092, 62093, 69487, 69488, 69489, 69490, 88478, 88483, 88491, 93038, 93039, 132364, 132368, 134352, 134353, 136613, 137342, 138887, 138893, 138902, 138906, 179399, 179400, 179401, 180109, 183770, 183771, 184401, 186318, 186319, 190311, 205413, 205414, 230673, 236531, 288964, 288965, 296562, 296799, 310208, 314751, 314968, 318328, 318336, 318337, 320427, 346774, 375319, 395581, 418855, 418856, 419680, 420538, 420565, 424739, 424740, 424742, 424743, 424746, 424769, 424770, 424772, 433297, 444411, 444412, 444413, 449676, 449712, 449820, 449822, 449912, 454057, 497883, 500069, 502951, 510216, 510217, 524883, 530052, 537456, 539142, 539539, 544673, 566410, 570114, 570192, 570195, 570197, 570199, 570200, 570204, 586119, 586183, 588998, 588999, 589091, 594767, 605090, 609871, 613038, 613039, 613040, 613041, 613045, 613200, 618880, 618881, 638967, 638968, 642851, 647542, 677622, 680608, 710813, 710814, 710815, 711139, 731418, 733820, 751430, 759203, 765495, 787787, 820450, 822080, 843300, 858068, 888796, 888840, 896637, 898147, 898148, 899600, 900584, 900585, 902160, 918909, 925441, 925443, 925513, 928866, 942705, 961457, 969799, 969820, 971188, 981036, 984463, 984464, 987623, 987626, 994643, 1006690, 1007506, 1009187, 1024727, 1024932, 1024933, 1026010, 1026014, 1026015, 1026016, 1026769, 1026813, 1045602, 1045604, 1045605, 1045607, 1045730, 1047147, 1047149, 1056983, 1057753, 1079068, 1089365, 1130548, 1132329, 1135571, 1135572, 1135760, 1135761, 1135763, 1148298, 1148829, 1156093, 1156098, 1156173, 1164087, 1164933, 1170457, 1174597, 1195863, 1199166, 1205350, 1205352, 1205353, 1205354, 1214857, 1214961, 1238882, 1238886, 1238928, 1242092, 1242100, 1253689, 1266666, 1287066, 1287067, 1287068, 1289946, 1297338, 1297339, 1374603, 1374604, 1374606, 1413125, 1423358, 1423624, 1423627, 1452018, 1452566, 1461665, 1474896, 1488903, 1493807, 1519317, 1519318, 1524601, 1532424, 1532426, 1532428, 1532511, 1532512, 1532513, 1532514, 1532515, 1532516, 1532517, 1532518, 1532519, 1532520, 1532531, 1532533, 1532534, 1532535, 1532536, 1533783, 1552699, 1552700, 1557146, 1573102, 1573110, 1573338, 1573339, 1573340, 1573461, 1573462, 1573463, 1573465, 1573466, 1573467, 1577347, 1577349, 1602938, 1603087, 1603088, 1603113, 1618679, 1625642, 1627748, 1628869, 1629782, 1629783, 1647431, 1661642, 1661647, 1661649, 1661653, 1699861, 1700363, 1700889, 1705677, 1706988, 1717856, 1721331, 1721333, 1721334, 1731638, 1731639, 1731640, 1737392, 1749710, 1766820, 1766990, 1767061, 1767079, 1767115, 1767152, 1776910, 1776911, 1776912, 1776913, 1776914, 1776939, 1816810, 1816811, 1816842, 1825589, 1825590, 1825591, 1825662, 1840888, 1840890, 1848923, 1849869, 1849870, 1849871, 1849882, 1861241, 1865605, 1867062, 1867063, 1868202, 1877892, 1881914, 1881928, 1893234, 1893353, 1894085, 1899659, 1899662, 1899953, 1912924, 1912933, 1912936, 1916192, 1937483, 1937485, 1937491, 1937492, 1937495, 1941033, 1951025, 1967780, 1967781, 1984307, 2005067, 2005068, 2005069, 2005082, 2005083, 2009937, 2010888, 2010889, 2012350, 2012518, 2012519, 2012522, 2022560, 2022561, 2022582, 2033102, 2033113, 2033114, 2036384, 2040941, 2053900, 2059264, 2070355, 2070356, 2070357, 2070358, 2070359, 2070360, 2070622, 2076579, 2076582, 2076585, 2085536, 2090875, 2091784, 2113799, 2122973, 2122974, 2126736, 2130995, 2133435, 2133543, 2134629, 2136420, 2139315, 2143030, 2143031, 2143053, 2143054, 2143058, 2143191, 2156352, 2158951, 2159026, 2190421, 2220093, 2220094, 2220095, 2225436, 2229324, 2229325, 2229326, 2229327, 2229328, 2247249, 2247392, 2252239, 2252241, 2252284, 2252285, 2269267, 2269269, 2269270, 2269271, 2269384, 2269386, 2269387, 2271520, 2296700, 2302708]

item_counts = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 3, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 6, 1, 1, 1, 1, 2, 1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 2, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 2, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 1, 3, 3, 1, 1, 1, 3, 1, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 1, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 4, 4, 4, 1, 1, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 2, 1, 1, 1, 1, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 1, 4, 4, 4, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

types = ['Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Spaghetti', 'Macaroni', 'Macaroni', 'Spaghetti', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Spaghetti', 'Macaroni', 'Macaroni', 'Macaroni', 'Rigatoni', 'Spaghetti', 'Spaghetti', 'Spaghetti', 'Farfalle', 'Penne Rigate', 'Fusili', 'Penne Rigate', 'Lasagna', 'Canneloni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Spaghetti', 'Not Available', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Spaghetti', 'Penne Rigate', 'Spaghetti', 'Macaroni', 'Macaroni', 'Spaghetti', 'Macaroni', 'Penne Rigate', 'Fusili', 'Fusili', 'Tagliatelle', 'Lasagna', 'Farfalle', 'Penne Rigate', 'Penne Rigate', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Spaghetti', 'Spaghetti', 'Lasagna', 'Lasagna', 'Penne Rigate', 'Fusili', 'Lasagna', 'Macaroni', 'Spaghetti', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Angel Hair', 'Spaghetti', 'Macaroni', 'Vermicelli', 'Tagliatelle', 'Lasagna', 'Penne Rigate', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Serpentini', 'Chifferini', 'Linguine', 'Angel Hair', 'Fusili', 'Spaghetti', 'Penne Rigate', 'Fettuccine', 'Macaroni', 'Tortiglioni', 'Lasagna', 'Mac&Cheese', 'Spaghetti', 'Fettuccine', 'Mac&Cheese', 'Farfalle', 'Spaghetti', 'Spaghetti', 'Mac&Cheese', 'Penne Rigate', 'Macaroni', 'Fusili', 'Spaghetti', 'Linguine', 'Spaghetti', 'Not Available', 'Fettuccine', 'Fettuccine', 'Penne Rigate', 'Rigatoni', 'Penne Rigate', 'Not Available', 'Spaghetti', 'Spaghetti', 'Penne Rigate', 'Macaroni', 'Penne Rigate', 'Linguine', 'Fusili', 'Farfalle', 'Rigatoni', 'Spaghetti', 'Penne Rigate', 'Fusili', 'Fettuccine', 'Lasagna', 'Not Available', 'Mac&Cheese', 'Vermicelli', 'Spaghetti', 'Penne Rigate', 'Fusili', 'Spaghetti', 'Not Available', 'Canneloni', 'Not Available', 'Macaroni', 'Macaroni', 'Lasagna', 'Fusili', 'Not Available', 'Spaghetti', 'Spaghetti', 'Lasagna', 'Not Available', 'Fusili', 'Not Available', 'Macaroni', 'Macaroni', 'Rotini', 'Macaroni', 'Macaroni', 'Spaghetti', 'Spaghetti', 'Not Available', 'Rotini', 'Not Available', 'Lasagna', 'Bunny Shaped', 'Spaghetti', 'Spaghetti', 'Fettuccine', 'Spaghetti', 'Spaghetti', 'Fettuccine', 'Not Available', 'Penne Rigate', 'Spaghetti', 'Mac&Cheese', 'Not Available', 'Not Available', 'Fusili', 'Linguine', 'Not Available', 'Tagliatelle', 'Fusili', 'Penne Rigate', 'Not Available', 'Spaghetti', 'Penne Rigate', 'Penne Rigate', 'Angel Hair', 'Rigatoni', 'Not Available', 'Tortiglioni', 'Not Available', 'Spaghetti', 'Chifferini', 'Macaroni', 'Penne Rigate', 'Fusili', 'Mac&Cheese', 'Not Available', 'Linguine', 'Not Available', 'Not Available', 'Not Available', 'Not Available', 'Not Available', 'Mac&Cheese', 'Chifferini', 'Not Available', 'Penne Rigate', 'Not Available', 'Macaroni', 'Mac&Cheese', 'Spaghetti', 'Spaghetti', 'Alphabet Shaped', 'Not Available', 'Spaghetti', 'Fusili', 'Penne Rigate', 'Spaghetti', 'Farfalle', 'Mac&Cheese', 'Penne Rigate', 'Fusili', 'Spaghetti', 'Lasagna', 'Not Available', 'Penne Rigate', 'Spaghetti', 'Penne Rigate', 'Not Available', 'Spaghetti', 'Macaroni', 'Macaroni', 'Macaroni', 'Penne Rigate', 'Penne Rigate', 'Lasagna', 'Macaroni', 'Mac&Cheese', 'Macaroni', 'Macaroni', 'Penne Rigate', 'Macaroni', 'Fusili', 'Spaghetti', 'Not Available', 'Fusili', 'Spaghetti', 'Tortiglioni', 'Macaroni', 'Spaghetti', 'Spaghetti', 'Fusili', 'Penne Rigate', 'Not Available', 'Farfalle', 'Spaghetti', 'Not Available', 'Tortiglioni', 'Fusili', 'Penne Rigate', 'Not Available', 'Farfalle', 'Spaghetti', 'Tortiglioni', 'Fusili', 'Penne Rigate', 'Farfalle', 'Fusili', 'Not Available', 'Penne Rigate', 'Spaghetti', 'Tagliatelle', 'Tortiglioni', 'Fusili', 'Spaghetti', 'Fusili', 'Tortiglioni', 'Penne Rigate', 'Penne Rigate', 'Not Available', 'Fusili', 'Tagliatelle', 'Tagliatelle', 'Lasagna', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Macaroni', 'Not Available', 'Farfalle', 'Tagliatelle', 'Macaroni', 'Macaroni', 'Not Available', 'Not Available', 'Lasagna', 'Macaroni', 'Spaghetti', 'Penne Rigate', 'Instant', 'Gnocchi', 'Instant', 'Rigatoni', 'Spaghetti', 'Instant', 'Instant', 'Instant', 'Not Available', 'Not Available', 'Not Available', 'Fusili', 'Rigatoni', 'Not Available', 'Fusili', 'Penne Rigate', 'Fusili', 'Penne Rigate', 'Spaghetti', 'Penne Rigate', 'Fusili', 'Fusili', 'Not Available', 'Spaghetti', 'Not Available', 'Instant', 'Fusili', 'Penne Rigate', 'Angel Hair', 'Linguine', 'Linguine', 'Not Available', 'Not Available', 'Spaghetti', 'Not Available', 'Spaghetti', 'Linguine', 'Not Available', 'Penne Rigate', 'Mac&Cheese', 'Mac&Cheese', 'Farfalle', 'Tagliatelle', 'Chifferini', 'Mac&Cheese', 'Lasagna', 'Spaghetti', 'Fusili', 'Macaroni', 'Lasagna', 'Spaghetti', 'Fusili', 'Not Available', 'Macaroni', 'Macaroni', 'Fusili', 'Not Available', 'Instant', 'Instant', 'Instant', 'Instant', 'Instant', 'Tagliatelle', 'Macaroni', 'Macaroni', 'Spaghetti', 'Not Available', 'Fettuccine', 'Penne Rigate', 'Spaghetti', 'Spaghetti', 'Fusili', 'Mac&Cheese', 'Mac&Cheese', 'Mac&Cheese', 'Not Available', 'Not Available', 'Not Available', 'Penne Rigate', 'Mac&Cheese', 'Mac&Cheese', 'Mac&Cheese', 'Instant', 'Instant', 'Instant', 'Spaghetti', 'Penne Rigate', 'Lasagna', 'Spaghetti', 'Fusili', 'Spaghetti', 'Fusili', 'Penne Rigate', 'Macaroni', 'Spaghetti', 'Fettuccine', 'Not Available', 'Not Available', 'Not Available', 'Fettuccine', 'Not Available', 'Lasagna', 'Not Available', 'Spaghetti', 'Tagliatelle', 'Penne Rigate', 'Penne Rigate', 'Penne Rigate', 'Not Available', 'Rotini', 'Spaghetti', 'Rotini', 'Fusili', 'Penne Rigate', 'Fusili', 'Penne Rigate', 'Spaghetti', 'Macaroni', 'Lasagna', 'Mac&Cheese', 'Mac&Cheese', 'Spaghetti', 'Fettuccine', 'Spaghetti', 'Angel Hair', 'Macaroni', 'Words Shaped', 'Number Shaped', 'Minion Shaped', 'Spongebob Shaped', 'Cars Shaped', 'Spaghetti', 'Macaroni', 'Macaroni', 'Macaroni', 'Penne Rigate', 'Spaghetti', 'Cocciolini', 'Ditalini', 'Vermicelli', 'Fusili', 'Spaghetti', 'Penne Rigate', 'Macaroni', 'Not Available', 'Mac&Cheese', 'Fusili']

volume_in_grams = [400, 400, 400, 400, 400, 500, 400, 400, 400, 450, 450, 450, 450, 400, 450, 450, 500, 450, 500, 500, 500, 500, 500, 500, 500, 500, 250, 400, 450, 400, 400, 400, 400, 400, 400, 500, 500, 500, 500, 400, 400, 400, 900, 800, 450, 500, 500, 500, 450, 400, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 900, 500, 500, 400, 400, 400, 500, 500, 400, 400, 500, 300, 500, 400, 400, 400, 400, 400, 500, 400, 400, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 400, 500, 500, 500, 500, 500, 500, 500, 500, 400, 500, 500, 206, 400, 500, 170, 500, 450, 400, 156, 500, 400, 500, 500, 500, 500, 500, 500, 250, 400, 400, 400, 400, 400, 500, 500, 400, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 170, 454, 454, 500, 500, 500, 400, 250, 500, 500, 500, 500, 400, 250, 400, 900, 250, 340, 340, 340, 300, 400, 454, 400, 400, 500, 500, 454, 454, 454, 450, 170, 250, 200, 200, 800, 400, 121, 125, 500, 500, 156, 220, 250, 340, 250, 250, 250, 400, 400, 400, 400, 400, 400, 500, 500, 500, 500, 500, 500, 500, 450, 500, 400, 213, 235, 500, 116, 124, 121, 124, 127, 73, 500, 385, 270, 250, 337, 156, 500, 500, 453, 250, 500, 500, 500, 500, 454, 156, 400, 400, 400, 500, 500, 400, 500, 400, 400, 400, 500, 400, 400, 250, 250, 250, 450, 170, 400, 400, 400, 400, 500, 500, 250, 400, 500, 500, 400, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 250, 400, 400, 400, 250, 250, 250, 250, 250, 250, 250, 400, 250, 250, 250, 400, 400, 500, 500, 500, 500, 400, 150, 500, 250, 400, 400, 99, 250, 250, 500, 500, 400, 155, 300, 105, 500, 400, 67, 67, 67, 400, 400, 400, 500, 340, 250, 250, 250, 500, 500, 500, 500, 500, 500, 500, 500, 250, 70, 70, 70, 250, 250, 250, 250, 400, 400, 400, 500, 500, 500, 500, 164, 160, 400, 400, 500, 170, 250, 250, 250, 400, 500, 450, 450, 450, 340, 340, 340, 250, 70, 70, 70, 70, 70, 450, 170, 400, 400, 300, 300, 300, 300, 300, 300, 58, 156, 170, 250, 250, 250, 250, 264, 240, 256, 67, 67, 67, 500, 400, 454, 500, 500, 500, 500, 500, 500, 500, 500, 240, 240, 240, 500, 500, 500, 300, 250, 250, 170, 400, 454, 340, 454, 226, 340, 500, 500, 500, 500, 500, 500, 454, 66, 170, 400, 200, 200, 200, 400, 500, 500, 500, 500, 500, 400, 500, 400, 400, 400, 400, 500, 500, 500, 400, 500, 400, 500, 500, 275, 500]

attr = pd.DataFrame({'material_id': materials,
                     'item_count': item_counts,
                     'type': types,
                     'volume_in_grams': volume_in_grams})

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Prep

# COMMAND ----------

query = """
SELECT *
FROM dev.sandbox.pj_ao_v2
WHERE year_month = 202406
"""

df = spark.sql(query).toPandas()
df = df.drop(columns = 'year_month')
df = pd.merge(df, attr, on = 'material_id', how = 'left')

# COMMAND ----------

# Make a column for number of packs in a material
df['item_count'] = df['item_count'].astype(str)
df['Packs'] = np.where(df.item_count == '1', df.item_count + ' Pack', df.item_count + ' Packs')
df['item_count'] = df['item_count'].astype('int32')

# Calculate unit weight price for each product
df['conversion_numerator'] = df['conversion_numerator'].fillna(1)
df['unit_wgt_price'] = df['amount'] / (df['conversion_numerator'] * df['volume_in_grams'] * df['quantity'] * df['item_count'])

# Convert volume_in_grams to string type and concatenate the units to it
df['volume_in_grams'] = df['volume_in_grams'].astype(str)
df['volume_in_grams'] = df['volume_in_grams'] + 'G'

# Concatenate all the attributes into one column
df['attribute_combination'] = df['type'] + ", " + df['volume_in_grams'] + ", " + df['Packs'] + ", " + df['brand']

# COMMAND ----------

df['material_id'].nunique(), df.shape

# COMMAND ----------

df.head().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Attribute Shares Check

# COMMAND ----------

# Calculating cumulative sum of sales contribution for the unique type of products available in the category
attr_share_df = df.groupby('type')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'type_sales'})
attr_share_df = attr_share_df.sort_values(by = 'type_sales', ascending = False).reset_index(drop = True)
attr_share_df['type_sales'] = round(attr_share_df['type_sales'], 0)
attr_share_df['type_sales_perc'] = round(attr_share_df.type_sales.cumsum() / attr_share_df.type_sales.sum() * 100, 2)

df = pd.merge(df, attr_share_df, on='type', how='inner')

# Calculating cumulative sum of sales contribution for the unique product volumes available in the category
attr_share_df = df.groupby('volume_in_grams')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'volume_sales'})
attr_share_df = attr_share_df.sort_values(by = 'volume_sales', ascending = False).reset_index(drop = True)
attr_share_df['volume_sales'] = round(attr_share_df['volume_sales'], 0)
attr_share_df['volume_sales_perc'] = round(attr_share_df.volume_sales.cumsum() / attr_share_df.volume_sales.sum() * 100, 2)
df = pd.merge(df, attr_share_df, on='volume_in_grams', how='inner')

# Calculating cumulative sum of sales contribution for the unique pack types available in the category
attr_share_df = df.groupby('Packs')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'Pack_sales'})
attr_share_df = attr_share_df.sort_values(by = 'Pack_sales', ascending = False).reset_index(drop = True)
attr_share_df['Pack_sales'] = round(attr_share_df['Pack_sales'], 0)
attr_share_df['Pack_sales_perc'] = round(attr_share_df.Pack_sales.cumsum() / attr_share_df.Pack_sales.sum() * 100, 2)
df = pd.merge(df, attr_share_df, on='Packs', how='inner')

# Calculating cumulative sum of sales contribution for the unique attribute combinations available in the category
attr_share_df = df.groupby('attribute_combination')['amount'].sum().reset_index()
attr_share_df = attr_share_df.rename(columns={'amount': 'attr_combo_sales'})
attr_share_df = attr_share_df.sort_values(by = 'attr_combo_sales', ascending = False).reset_index(drop = True)
attr_share_df['attr_combo_sales'] = round(attr_share_df['attr_combo_sales'], 0)
attr_share_df['attr_combo_sales_perc'] = round(attr_share_df.attr_combo_sales.cumsum() / attr_share_df.attr_combo_sales.sum() * 100, 2)
df = pd.merge(df, attr_share_df, on='attribute_combination', how='inner')

# Storing only the relevant columns from above into a dataframe
attr_share_df = df[['type', 'type_sales', 'type_sales_perc', 'volume_in_grams', 'volume_sales', 'volume_sales_perc', 'Packs', 'Pack_sales', 'Pack_sales_perc', 'attribute_combination', 'attr_combo_sales', 'attr_combo_sales_perc']].drop_duplicates().sort_values(by='attr_combo_sales_perc', ascending = True).reset_index(drop=True)

# COMMAND ----------

# Save the attribute shares data to a sandbox
spark_df = spark.createDataFrame(attr_share_df)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.pj_ao_v2_attribute_sales")

# COMMAND ----------

# attr_share_df.iloc[:, :3].drop_duplicates().sort_values(by = 'type_sales', ascending = False).display()

# attr_share_df.iloc[:, 3:6].drop_duplicates().sort_values(by = 'volume_sales', ascending = False).display()

# attr_share_df.iloc[:, 6:9].drop_duplicates().sort_values(by = 'Pack_sales', ascending = False).display()

attr_share_df.iloc[:, 9:].drop_duplicates().sort_values(by = 'attr_combo_sales', ascending = False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Compression

# COMMAND ----------

# Attributes that fall in the bottom 20% of sales are combined together and labeled as "Others" 
top_attrs_count = len(attr_share_df[attr_share_df['attr_combo_sales_perc'] < 80]) + 1
top_attrs = attr_share_df[:top_attrs_count]['attribute_combination'].tolist()
df['attribute_combination'] = np.where(df.attribute_combination.isin(top_attrs), df.attribute_combination, 'Others')

# Aggregating the data on a transaction and attribute level
df2 = df.groupby(['business_day', 'transaction_id', 'attribute_combination']).agg(
    {'unit_wgt_price': 'mean',
     'amount': 'sum',
     'quantity': 'sum',
     'discount': 'sum',
     'abs_gp': 'sum',
     'purchase_flag': 'mean',
     'discount_flag': 'mean'}).reset_index()

# Correcting the datatypes of boolean columns
df2['discount_flag'] = round(df2['discount_flag']).astype('int32')
df2['purchase_flag'] = df2['purchase_flag'].astype('int32')

# Rounding off values for float type columns
df2['unit_wgt_price'] = round(df2['unit_wgt_price'], 4)
df2['amount'] = round(df2['amount'], 2)
df2['discount'] = round(df2['discount'], 2)

# Re-calculating discount percentage column
df2['discount_perc'] = round(df2['discount']/(df2['amount'] + df2['discount']), 4)

# Renaming columns to aggregated level names
df2 = df2.rename(columns={'unit_wgt_price': 'avg_unit_wgt_price', 'amount': 'sales', 'discount_flag': 'dominant_discount_flag'})

# COMMAND ----------

df2.head().display()

# COMMAND ----------

# Records reduced from the data compression
len(df) - len(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #Non-Purchase Incidences Data Creation

# COMMAND ----------

# Creating all instances of purchases and non-purchases

temp1 = df2[['transaction_id']].drop_duplicates().reset_index(drop = True)
temp2 = df2[['attribute_combination']].drop_duplicates().reset_index(drop = True)
temp3 = pd.merge(temp1, temp2, how='cross')

df3 = pd.merge(df2, temp3, on=['transaction_id', 'attribute_combination'], how='outer')

# COMMAND ----------

df3.info()

# COMMAND ----------

# Impute null values for non-purchases incidences
df3['business_day'] = df3.groupby('transaction_id')['business_day'].transform(lambda x: x.ffill().bfill())
df3['purchase_flag'] = df3['purchase_flag'].fillna(0)
df3['sales'] = df3['sales'].fillna(0)
df3['quantity'] = df3['quantity'].fillna(0)
df3['abs_gp'] = df3['abs_gp'].fillna(0)
df3['discount'] = df3['discount'].fillna(0)

# COMMAND ----------

# Impute null values for non-purchase incidences
# Price and Discount are values independent of purchase incidences. So, we impute them differently
columns_to_impute = ['avg_unit_wgt_price', 'discount_perc', 'dominant_discount_flag']
df3['business_day'] = pd.to_datetime(df3['business_day'])

# If a purchase occured for the matching attribute on the same day, then use its values for imputation
for col in columns_to_impute:
    df_same_day = df3.groupby(['business_day', 'attribute_combination'])[col].transform(lambda x: x.fillna(method='ffill').fillna(method='bfill'))
    df3[col] = df3[col].fillna(df_same_day)

# If a purchase did not occur for the matching attribute on the same day, then look for the next closest date where a purchase did occur and use its values for imputation
def impute_closest_date(row, df3, col):
    if pd.notna(row[col]):
        return row[col]
    else:
        same_material_df = df3[(df3['attribute_combination'] == row['attribute_combination']) & (df3['business_day'] != row['business_day']) & pd.notna(df3[col])]
        if not same_material_df.empty:
            same_material_df['date_diff'] = abs(same_material_df['business_day'] - row['business_day'])
            closest_date_value = same_material_df.loc[same_material_df['date_diff'].idxmin(), col]
            return closest_date_value
        else:
            return np.nan

for col in columns_to_impute:
    df3[col] = df3.apply(lambda row: impute_closest_date(row, df3, col), axis=1)

# COMMAND ----------

# Convert the data type of business_day to string type
df3['business_day'] = df3['business_day'].dt.strftime('%Y-%m-%d')

# COMMAND ----------

df3.info()

# COMMAND ----------

# Model Dataset Preparation

# Split the attribute combination column back to their original individual attribute columns
df3[['type', 'volume', 'packs', 'brand']] = df3['attribute_combination'].str.split(',', expand=True)

# For the attribute combination "Others", the split will result in null values. Impute them with "Others"
df3['volume'] = df3['volume'].fillna('Others')
df3['packs'] = df3['packs'].fillna('Others')
df3['brand'] = df3['brand'].fillna('Others')

# Taking only relevant columns for model dataset
df3 = df3[['type', 'volume', 'packs', 'brand', 'avg_unit_wgt_price', 'discount_perc', 'dominant_discount_flag', 'purchase_flag']]

# COMMAND ----------

# Save the model dataset to a sandbox
spark_df = spark.createDataFrame(df3)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.pj_ao_v2_mds")

# COMMAND ----------

# MAGIC %md
# MAGIC #Fitting Models

# COMMAND ----------

# MAGIC %md
# MAGIC ##MDS Prep

# COMMAND ----------

from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.preprocessing import MinMaxScaler
from sklearn.utils import resample
import pandas as pd
import numpy as np

# Read model dataset
data_df = spark.sql("SELECT * FROM dev.sandbox.pj_ao_v2_mds").toPandas()

# Normalize numeric columns
# scaler = MinMaxScaler()
# columns_to_scale = ['avg_unit_wgt_price', 'discount_perc']
# data_df[columns_to_scale] = scaler.fit_transform(data_df[columns_to_scale])

# Drop price and discount columns
data_df = data_df.drop(['avg_unit_wgt_price', 'discount_perc', 'dominant_discount_flag'], axis = 1)

# Perform one-hot encoding for categorical columns
df_encoded = pd.get_dummies(data_df, columns=['type', 'volume', 'packs', 'brand'], drop_first = True)

# Remove any blank spaces in column names
df_encoded.columns = [col.replace('_ ', '_') for col in df_encoded.columns]

# COMMAND ----------

# Undersample the majority class

# Separate majority (class 0) and minority (class 1) classes
df_majority = df_encoded[df_encoded.purchase_flag == 0]
df_minority = df_encoded[df_encoded.purchase_flag == 1]

# Undersample the majority class (class 0) to have the same number of instances as the minority class
df_majority_undersampled = resample(df_majority, 
                                    replace = False,    # Do not replace samples
                                    n_samples = len(df_minority),  # Make the size equal to the minority class
                                    random_state = 42)  # Ensure reproducibility

# Combine the undersampled majority class with the minority class
df_balanced = pd.concat([df_majority_undersampled, df_minority])

# Shuffle the resulting dataframe
df_balanced = df_balanced.sample(frac = 1, random_state = 42).reset_index(drop=True)

# Check the new class distribution
print(df_balanced['purchase_flag'].value_counts())

# COMMAND ----------

# X = np.array(df_balanced, dtype=float)
# vif_data = pd.DataFrame()
# vif_data["feature"] = df_balanced.columns
# vif_data["VIF"] = [variance_inflation_factor(X, i) for i in range(X.shape[1])]
# vif_data.sort_values(by = 'VIF', ascending = False).reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Baseline Models

# COMMAND ----------

# pip install eli5

# COMMAND ----------

import sys
import os
stderr = sys.stderr
sys.stderr = open(os.devnull, 'w')

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import randint, uniform
import optuna

from sklearn.model_selection import RandomizedSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from catboost import CatBoostClassifier

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(df_balanced.drop(columns = 'purchase_flag'), df_balanced['purchase_flag'], test_size=0.3, random_state=42)

models = {
    "Logistic Regression": LogisticRegression(max_iter=10000),
    "Decision Tree": DecisionTreeClassifier(random_state = 42),
    "Random Forest": RandomForestClassifier(random_state = 42),
    "Gradient Boosting": GradientBoostingClassifier(random_state = 42),
    # "Support Vector Classifier": SVC(random_state = 42),
    "K-Nearest Neighbors": KNeighborsClassifier(),
    "Naive Bayes": GaussianNB(),
    "XGBoost": XGBClassifier(random_state = 42),
    "LightGBM": LGBMClassifier(verbosity=-1, silent=True, random_state = 42),
    "CatBoost": CatBoostClassifier(verbose=0, random_state = 42),
    "AdaBoost": AdaBoostClassifier(random_state = 42)
}

for model_name, model in models.items():
    model.fit(X_train, y_train)
    train_score = accuracy_score(y_train, model.predict(X_train))
    test_score = accuracy_score(y_test, model.predict(X_test))
    print(f"{model_name} - Train Score: {train_score:.3f}, Test Score: {test_score:.3f}")

for model_name, model in models.items():
    y_pred = model.predict(X_test)
    conf_matrix = confusion_matrix(y_test, y_pred)
    # class_report = classification_report(y_test, y_pred)
    print(f"\n{model_name}:")
    print(f"Confusion Matrix:\n{conf_matrix}")
    # print(f"\nClassification Report:\n{class_report}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Model Testing

# COMMAND ----------

# MAGIC %md
# MAGIC ###Logistic Regression Model (FINAL)

# COMMAND ----------

param_dist = {'penalty': ['l1', 'l2', 'elasticnet', 'none'],
              'C': np.logspace(-4, 4, 20),
              'solver': ['liblinear', 'saga']}

model = LogisticRegression(max_iter = 10000)

random_search = RandomizedSearchCV(
    estimator = model,
    param_distributions = param_dist,
    n_iter = 100,
    cv = 5,
    scoring = 'accuracy',
    random_state = 42
)

random_search.fit(X_train, y_train)

# print("Best Parameters:", random_search.best_params_, "\n")

train_score = np.round(random_search.score(X_train, y_train), 3)
test_score = np.round(random_search.score(X_test, y_test), 3)
print(f'Train Score: {train_score}\nTest Score: {test_score}')

best_model = random_search.best_estimator_
conf_matrix = confusion_matrix(y_test, best_model.predict(X_test))
print(f"Confusion Matrix:\n{conf_matrix}")

coefficients = best_model.coef_.flatten()
importance_df = pd.DataFrame({'Feature': X_train.columns, 'Importance': coefficients})
importance_df = importance_df.sort_values(by='Importance', ascending=False)
importance_df.display()

# COMMAND ----------

# Get the beta coefficients and create a DataFrame
beta_coefficients = best_model.coef_[0]
coef_df = pd.DataFrame({
    'Feature': X_train.columns,
    'Coefficient': beta_coefficients
})

# Sort coefficients by absolute value for easier interpretation
coef_df['Abs_Coefficient'] = abs(coef_df['Coefficient'])
coef_df = coef_df.sort_values('Abs_Coefficient', ascending=False).drop('Abs_Coefficient', axis=1)

# Print the beta coefficients
print("Beta Coefficients:")
print(coef_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Beta Values

# COMMAND ----------

# Create a dataframe with the dummification for each Material ID

all_materials = df[['material_id', 'material_name', 'attribute_combination']].drop_duplicates().reset_index(drop = True)

all_materials[['type', 'volume', 'packs', 'brand']] = all_materials['attribute_combination'].str.split(',', expand=True)
all_materials.drop(columns = 'attribute_combination', inplace = True)

# For the attribute combination "Others", the split will result in null values. Impute them with "Others"
all_materials['volume'] = all_materials['volume'].fillna('Others')
all_materials['packs'] = all_materials['packs'].fillna('Others')
all_materials['brand'] = all_materials['brand'].fillna('Others')

all_materials_encoded = pd.get_dummies(all_materials, columns=['type', 'volume', 'packs', 'brand'], drop_first = True)

# Remove any blank spaces in column names
all_materials_encoded.columns = [col.replace('_ ', '_') for col in all_materials_encoded.columns]

# COMMAND ----------

# Multiply beta values with each respective attribute

columns = all_materials_encoded.columns.tolist()
columns_to_remove = ['material_id', 'material_name']

columns = [col for col in columns if col not in columns_to_remove]

all_materials_beta = all_materials_encoded.copy()
for col in columns:
    all_materials_beta[col] = all_materials_beta[col] * coef_df[coef_df['Feature'] == col]['Coefficient'].iloc[0]

all_materials_beta['beta_x_field'] = all_materials_beta[columns].sum(axis=1)

# COMMAND ----------

all_materials_beta.display()

# COMMAND ----------

# Calculate average price, total quantity sold, and the final probability value
query = """
SELECT
    material_id,
    ROUND(AVG(regular_unit_price), 2) AS avg_price,
    ROUND(SUM(quantity)) AS total_units
FROM dev.sandbox.pj_ao_v2
GROUP BY material_id
"""
price_units_df = spark.sql(query).toPandas()

all_materials_prob = all_materials_beta[['material_id', 'material_name', 'beta_x_field']].copy()
all_materials_prob = all_materials_prob.merge(price_units_df, on = 'material_id', how = 'left')
all_materials_prob['total_units'] = all_materials_prob['total_units'].astype(int)
all_materials_prob = all_materials_prob.sort_values(by = 'material_id').reset_index(drop = True)

all_materials_prob['probability'] = round(1 / (1 + np.exp(-all_materials_prob['beta_x_field'])) * 100, 2)

# COMMAND ----------

all_materials_prob.display()

# COMMAND ----------

spark_df = spark.createDataFrame(all_materials_prob)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dev.sandbox.pj_ao_v2_lr_beta_values")

# COMMAND ----------

all_materials_encoded.to_csv("/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_v2_lr_encoded.csv", index = False)
coef_df.to_csv("/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_v2_lr_coef.csv", index = False)
all_materials_prob.to_csv("/Workspace/Users/prem@loyalytics.in/Assortment Optimization/ao_v2_lr_beta_x_fields.csv", index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Random Forest Classifier

# COMMAND ----------

# param_dist = {
#     'n_estimators': [100, 200, 300, 400, 500],
#     'max_depth': randint(1, 30),
#     'min_samples_split': randint(1, 10),
#     'min_samples_leaf': randint(1, 10),
#     'max_features': ['sqrt', 'log2']
# }

# rf = RandomForestClassifier(random_state = 42)
# random_search = RandomizedSearchCV(rf, param_distributions=param_dist, n_iter=100, cv=5, random_state=42, n_jobs=-1)
# random_search.fit(X_train, y_train)

# train_score = np.round(random_search.score(X_train, y_train), 3)
# test_score = np.round(random_search.score(X_test, y_test), 3)
# print(f'Train Score: {train_score}\nTest Score: {test_score}')

# best_clf = random_search.best_estimator_
# importances = best_clf.feature_importances_
# feature_names = X_train.columns
# importance_df = pd.DataFrame({
#     'Feature': feature_names,
#     'Importance': importances
# }).sort_values(by='Importance', ascending=False).reset_index(drop = True)
# importance_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Support Vector Classifier

# COMMAND ----------

# from eli5.sklearn import PermutationImportance
# import eli5

# svc = SVC(random_state = 42)

# param_dist = {'C': uniform(0.1, 100),
#               'gamma': uniform(0.001, 1),
#               'kernel': ['linear', 'rbf', 'poly', 'sigmoid']}

# random_search = RandomizedSearchCV(svc, param_dist, n_iter=50, cv=5, n_jobs=-1, verbose=0, random_state=42)
# random_search.fit(X_train, y_train)

# # print("Best Parameters:", random_search.best_params_, "\n")

# train_score = np.round(random_search.score(X_train, y_train), 3)
# test_score = np.round(random_search.score(X_test, y_test), 3)
# print(f'Train Score: {train_score}\nTest Score: {test_score}')

# perm = PermutationImportance(random_search.best_estimator_, random_state=42).fit(X_test, y_test)
# eli5.show_weights(perm, feature_names=X_test.columns.tolist())

# COMMAND ----------

# from eli5.sklearn import PermutationImportance
# import eli5

# svc = SVC(kernel = 'linear')

# param_dist = {'C': uniform(0.1, 100),
#               'gamma': uniform(0.001, 1)}

# random_search = RandomizedSearchCV(svc, param_dist, n_iter=50, cv=5, n_jobs=-1, verbose=0, random_state=42)
# random_search.fit(X_train, y_train)

# # print("Best Parameters:", random_search.best_params_, "\n")

# train_score = np.round(random_search.score(X_train, y_train), 3)
# test_score = np.round(random_search.score(X_test, y_test), 3)
# print(f'Train Score: {train_score}\nTest Score: {test_score}')

# best_model = random_search.best_estimator_
# coefficients = best_model.coef_.flatten()
# importance_df = pd.DataFrame({'Feature': X_train.columns, 'Importance': coefficients})
# importance_df = importance_df.sort_values(by='Importance', ascending=False)
# importance_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###XGBoost

# COMMAND ----------

# def objective(trial):
#     params = {
#         # "verbosity": 0,
#         "objective": "binary:logistic",
#         "eval_metric": "logloss",
#         "tree_method": "auto",
#         "booster": trial.suggest_categorical("booster", ["gbtree"]),
#         "lambda": trial.suggest_float("lambda", 1e-8, 1.0, log=True),
#         "alpha": trial.suggest_float("alpha", 1e-8, 1.0, log=True),
#         "subsample": trial.suggest_float("subsample", 0.5, 1.0),
#         "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
#         "learning_rate": trial.suggest_float("learning_rate", 1e-8, 1.0, log=True),
#         "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
#         "max_depth": trial.suggest_int("max_depth", 1, 9),
#         "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
#     }

#     model = XGBClassifier(random_state = 42, **params)
#     model.fit(X_train, y_train)
    
#     preds = model.predict(X_test)
#     accuracy = accuracy_score(y_test, preds)
    
#     return accuracy

# study = optuna.create_study(direction="maximize")
# study.optimize(objective, n_trials=50)
# # print("Best hyperparameters:", study.best_params)

# best_trial = study.best_trial
# best_params = best_trial.params
# final_model = XGBClassifier(**best_params)
# final_model.fit(X_train, y_train)
# final_train_preds = final_model.predict(X_train)
# final_test_preds = final_model.predict(X_test)
# final_train_accuracy = accuracy_score(y_train, final_train_preds)
# final_test_accuracy = accuracy_score(y_test, final_test_preds)
# print(f"Final Train Accuracy: {final_train_accuracy:.3f}")
# print(f"Final Test Accuracy: {final_test_accuracy:.3f}")

# feature_importances = final_model.feature_importances_
# importance_df = pd.DataFrame({'Feature': X_train.columns,
#                               'Importance': feature_importances})
# importance_df = importance_df.sort_values(by='Importance', ascending=False).reset_index(drop=True).display()

# COMMAND ----------

# param_grid = {
#     'n_estimators': [100, 200, 300, 400, 500],
#     'max_depth': randint(1, 10),
#     'learning_rate': uniform(0.01, 0.2),
#     'subsample': uniform(0.1, 1),
#     'colsample_bytree': uniform(0.1, 1),
#     'gamma': uniform(0, 1)
# }

# xgb = XGBClassifier(random_state = 42)
# random_search = RandomizedSearchCV(estimator=xgb, param_distributions=param_grid,
#                                    n_iter=100, cv=5, verbose=1, n_jobs=-1, random_state=42)
# random_search.fit(X_train, y_train)

# train_score = np.round(random_search.score(X_train, y_train), 3)
# test_score = np.round(random_search.score(X_test, y_test), 3)
# print(f'Train Score: {train_score}\nTest Score: {test_score}')

# best_clf = random_search.best_estimator_
# importances = best_clf.feature_importances_
# feature_names = X_train.columns
# importance_df = pd.DataFrame({'Feature': feature_names,
#                               'Importance': importances}).sort_values(by='Importance',
#                                ascending=False).reset_index(drop = True)
# importance_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###CatBoost

# COMMAND ----------

# param_dist = {
#     'iterations': [100, 200, 300],
#     'learning_rate': uniform(0.01, 0.2),
#     'depth': randint(2, 10),
#     'l2_leaf_reg': randint(1, 10)
# }

# model = CatBoostClassifier(silent=True, random_state = 42)
# random_search = RandomizedSearchCV(model, param_distributions=param_dist, 
#                                    n_iter=10, cv=3, random_state=42, n_jobs=-1)
# random_search.fit(X_train, y_train)

# train_score = np.round(random_search.score(X_train, y_train), 3)
# test_score = np.round(random_search.score(X_test, y_test), 3)
# print(f'Train Score: {train_score}\nTest Score: {test_score}')

# best_clf = random_search.best_estimator_
# importances = best_clf.feature_importances_
# feature_names = X_train.columns
# importance_df = pd.DataFrame({
#     'Feature': feature_names,
#     'Importance': importances
# }).sort_values(by='Importance', ascending=False).reset_index(drop = True)
# importance_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###K-Nearest Neighbors

# COMMAND ----------

# param_grid = {
#     'n_neighbors': np.arange(1, 20),
#     'weights': ['uniform', 'distance'],
#     'p': [1, 2]  # p=1 is Manhattan distance, p=2 is Euclidean distance
# }

# knn = KNeighborsClassifier()
# random_search = RandomizedSearchCV(knn, param_distributions=param_grid,
#                                    n_iter=1, cv=5, random_state=42, n_jobs=-1)
# random_search.fit(X_train, y_train)

# train_score = np.round(random_search.score(X_train, y_train), 3)
# test_score = np.round(random_search.score(X_test, y_test), 3)
# print(f'Train Score: {train_score}\nTest Score: {test_score}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Decision Tree Classifier

# COMMAND ----------

# param_dist = {
#     'criterion': ['gini', 'entropy'],
#     'splitter': ['best', 'random'],
#     'max_depth': randint(1, 40),
#     'min_samples_split': randint(2, 10),
#     'min_samples_leaf': randint(1, 10),
#     'max_features': [None, 'sqrt', 'log2']
# }

# model = DecisionTreeClassifier(random_state=42)

# random_search = RandomizedSearchCV(
#     estimator=model,
#     param_distributions=param_dist,
#     n_iter=100,
#     cv=5,
#     scoring='accuracy',
#     random_state=42
# )

# random_search.fit(X_train, y_train)

# # print("Best Parameters:", random_search.best_params_)

# train_score = np.round(random_search.score(X_train, y_train), 3)
# test_score = np.round(random_search.score(X_test, y_test), 3)
# print(f'Train Score: {train_score}\nTest Score: {test_score}')

# best_clf = random_search.best_estimator_
# importances = best_clf.feature_importances_
# feature_names = X_train.columns
# importance_df = pd.DataFrame({
#     'Feature': feature_names,
#     'Importance': importances
# }).sort_values(by='Importance', ascending=False).reset_index(drop = True)
# importance_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


