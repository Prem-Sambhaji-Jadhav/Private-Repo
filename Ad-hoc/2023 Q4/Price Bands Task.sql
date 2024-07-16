-- Databricks notebook source
-- ---- DATAFRAME CONVERSION
-- to psdf:
-- 	from pyspark
-- 	df.pandas_api()
-- 	from pandas
-- 	ps.from_pandas(pdf)
-- to pandas:
-- 	from pyspark
-- 	df.toPandas()
-- 	from psdf
-- 	psdf.to_pandas()
-- to pyspark:
-- 	from pandas
-- 	spark.createDataFrame(pdf)
-- 	from psdf
-- 	psdf.to_spark()

-- COMMAND ----------

-- select *
-- from sandbox.pg_electronics_trans_raw_oct

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Read the data
-- MAGIC
-- MAGIC df_price = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/piyush@loyalytics.in/tcg_category_price_slabs.csv")
-- MAGIC df_price = df_price.na.drop(how='all', subset=df_price.columns)
-- MAGIC #df_price.createOrReplaceTempView('category_price_slabs_view')
-- MAGIC pdf_price = df_price.toPandas()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Fix the two indeces where price slabs are in the wrong place, and one price slab has a wrong lower limit
-- MAGIC
-- MAGIC pdf_price_new = pdf_price.copy()
-- MAGIC pdf_price_new.loc[[106,107], ['price_slab_1', 'price_slab_2']] = pdf_price.loc[[106,107], ['price_slab_2', 'price_slab_1']].values
-- MAGIC pdf_price_new.loc[[106, 107], 'price_slab_3'] = pdf_price_new.loc[[106, 107], 'price_slab_3'].str.replace('200 -', '250 -')
-- MAGIC
-- MAGIC #Fix the record where price_slab_5 does not end with '&Above'
-- MAGIC pdf_price_new['price_slab_5'][114] = '200&Above'
-- MAGIC
-- MAGIC #Fix the formatting such that its consistent: "200-250" and not "200 - 250"
-- MAGIC slab_columns = ['price_slab_1', 'price_slab_2', 'price_slab_3', 'price_slab_4', 'price_slab_5']
-- MAGIC for col in slab_columns:
-- MAGIC     pdf_price_new[col] = pdf_price_new[col].str.replace(' ', '')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Fix the upper limits of each price slab so that price slabs are mutually exclusive
-- MAGIC
-- MAGIC for i in range(len(slab_columns) - 1):
-- MAGIC     for index, row in pdf_price_new.iterrows():
-- MAGIC         if i == len(slab_columns)-2:
-- MAGIC             condition2 = row[slab_columns[i+1]].split('&')[0][-1]
-- MAGIC         else:
-- MAGIC             condition2 = row[slab_columns[i+1]].split('-')[0][-1]
-- MAGIC         if row[slab_columns[i]].split('-')[-1][-1] == condition2:
-- MAGIC             pdf_price_new.at[index, slab_columns[i]] = f"{row[slab_columns[i]].split('-')[0]}-{int(row[slab_columns[i]].split('-')[-1]) - 1}"

-- COMMAND ----------

-- MAGIC %py
-- MAGIC def convert_values(value):
-- MAGIC     lower, upper = map(int, value.split('-'))
-- MAGIC     return f'{upper}&Below'
-- MAGIC
-- MAGIC # Apply the above function to the price slab column and update the values
-- MAGIC pdf_price_new['price_slab_1'] = pdf_price_new['price_slab_1'].apply(convert_values)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df_price_new = spark.createDataFrame(pdf_price_new)
-- MAGIC df_price_new.createOrReplaceTempView('category_price_slabs_view')

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.pj_tcg_price_slabs_view AS (
    SELECT *
    FROM category_price_slabs_view
)

-- COMMAND ----------

-- %python
-- query = """select *
-- from category_price_slabs_view
-- where appliances_tag = 'MDA'"""

-- #spark.sql(query).display()
-- df = spark.sql(query)
-- df.display()

-- COMMAND ----------

SELECT
    t1.material_group_id,
    product_id,
    ROUND(SUM(amount)/SUM(quantity)) AS item_price,
    appliances_tag,
    CASE WHEN item_price < SUBSTRING_INDEX(price_slab_2, '-', 1) THEN "Budget"
        WHEN item_price < SUBSTRING_INDEX(price_slab_3, '-', 1) THEN "Affordable"
        WHEN item_price < SUBSTRING_INDEX(price_slab_4, '-', 1) THEN "Mid-range"
        WHEN item_price < SUBSTRING_INDEX(price_slab_5, '&', 1) THEN "Premium"
        ELSE "Luxury" END AS price_band_type
FROM sandbox.pg_electronics_trans_raw_oct AS t1
JOIN category_price_slabs_view AS t2 ON t1.material_group_id = t2.material_group_id
WHERE appliances_tag IS NOT NULL
GROUP BY
    t1.material_group_id,
    product_id,
    appliances_tag,
    price_slab_2,
    price_slab_3,
    price_slab_4,
    price_slab_5

-- COMMAND ----------

select t2.material_group_name, price_slab_1 as budget, price_slab_2 as affordable,
price_slab_3 as mid_range, price_slab_4 as premium, price_slab_5 as luxury,
count(distinct t2.material_id) as num_materials_source, count(distinct t3.product_id) as num_materials_sandbox
from category_price_slabs_view t1
join gold.material_master t2
on t1.material_group_id = t2.material_group_id
join sandbox.pg_electronics_trans_raw_oct t3
on t3.material_group_id = t2.material_group_id
where appliances_tag is not null
group by t2.material_group_name, budget, affordable, mid_range, premium, luxury

-- COMMAND ----------

with cte1 as (select case when price_slab_1 = "499&Below" then "A"
            when price_slab_1 = "99&Below" then "B"
            when price_slab_1 = "49&Below" then "C"
            when price_slab_1 = "999&Below" then "D"
            when price_slab_1 = "199&Below" then "E"
            when price_slab_1 = "899&Below" then "F"
            when price_slab_1 = "1799&Below" then "G"
            when price_slab_1 = "1399&Below" then "H"
            when price_slab_1 = "299&Below" then "I"
            when price_slab_1 = "249&Below" then "J"
            when price_slab_1 = "274&Below" then "K"
            when price_slab_1 = "129&Below" then "L"
            when price_slab_1 = "799&Below" then "M"
            end as material_group_type,
price_slab_1 as budget, price_slab_2 as affordable,
price_slab_3 as mid_range, price_slab_4 as premium, price_slab_5 as luxury,
count(distinct t2.material_group_id) as num_material_groups
from category_price_slabs_view t1
join gold.material_master t2
on t1.material_group_id = t2.material_group_id
join sandbox.pg_electronics_trans_raw_oct t3
on t3.material_group_id = t2.material_group_id
where appliances_tag is not null
group by t2.material_group_name, budget, affordable, mid_range, premium, luxury
order by material_group_type)

with cte2 as (select material_group_type, MAX(budget), MAX(affordable), MAX(mid_range), MAX(premium), MAX(luxury), SUM(num_material_groups)
from cte1
group by material_group_type)



-- COMMAND ----------

select MAX(appliances_tag), t1.material_group_id, MAX(material_group_name),
case when price_slab_1 = "499&Below" then "A"
      when price_slab_1 = "99&Below" then "B"
      when price_slab_1 = "49&Below" then "C"
      when price_slab_1 = "999&Below" then "D"
      when price_slab_1 = "199&Below" then "E"
      when price_slab_1 = "899&Below" then "F"
      when price_slab_1 = "1799&Below" then "G"
      when price_slab_1 = "1399&Below" then "H"
      when price_slab_1 = "299&Below" then "I"
      when price_slab_1 = "249&Below" then "J"
      when price_slab_1 = "274&Below" then "K"
      when price_slab_1 = "129&Below" then "L"
      when price_slab_1 = "799&Below" then "M"
      end as material_group_type
from gold.material_master t1
join category_price_slabs_view t2
on t1.material_group_id = t2.material_group_id
where appliances_tag is not null
group by t1.material_group_id, price_slab_1

-- COMMAND ----------

select
t2.appliances_tag,
t2.material_group_id,
t1.material_group_name,
case when t1.transaction_date between '2022-01-02' and '2022-11-01' then 'period_22'
     when t1.transaction_date between '2023-01-01' and '2023-10-31' then 'period_23'
     end as period_type,
sum(t1.amount) as total_sales,
-- count(distinct product_id) as num_products
count(distinct t1.transaction_id) as num_transactions,
count(distinct t1.customer_id) as num_customers,
sum(t1.amount)/count(distinct t1.customer_id) as RPC

from sandbox.pg_electronics_trans_raw_oct t1
join (select distinct material_group_id, appliances_tag
      from category_price_slabs_view) t2
on t1.material_group_id = t2.material_group_id
where t2.material_group_id != 46001012 and t2.appliances_tag is not null

group by t2.appliances_tag, t2.material_group_id, t1.material_group_name, period_type
having period_type is not null
order by t2.material_group_id

-- COMMAND ----------

with price_bands as (select t1.material_group_id as material_group_id, product_id,
  round(sum(amount)/sum(quantity),0) as item_price, appliances_tag,
  case when item_price < SUBSTRING_INDEX(price_slab_2, '-', 1) then "Budget"
    when item_price < SUBSTRING_INDEX(price_slab_3, '-', 1) then "Affordable"
    when item_price < SUBSTRING_INDEX(price_slab_4, '-', 1) then "Mid-range"
    when item_price < SUBSTRING_INDEX(price_slab_5, '&', 1) then "Premium"
    else "Luxury" end as price_band_type
from sandbox.pg_electronics_trans_raw_oct t1
join category_price_slabs_view t2
on t1.material_group_id = t2.material_group_id
where appliances_tag is not null
group by t1.material_group_id, product_id, appliances_tag, price_slab_2, price_slab_3, price_slab_4, price_slab_5),

overall_info as (select t1.material_group_id as material_group_id,
t1.material_group_name as material_group_name, t1.transaction_date as transaction_date,
t1.product_id as product_id, t2.item_price as item_price, t1.amount as amount,
t1.transaction_id as transaction_id, t1.customer_id as customer_id,
t2.appliances_tag as appliances_tag, t2.price_band_type as price_band_type
from sandbox.pg_electronics_trans_raw_oct t1
join price_bands t2
on t1.product_id = t2.product_id),

sales_info_overall (select
t2.appliances_tag,
t2.material_group_id,
t1.material_group_name,
case when t1.transaction_date between '2022-01-02' and '2022-11-01' then 'period_22'
     when t1.transaction_date between '2023-01-01' and '2023-10-31' then 'period_23'
     end as period_type,
sum(t1.amount) as total_sales,
-- count(distinct product_id) as num_products
count(distinct t1.transaction_id) as num_transactions,
count(distinct t1.customer_id) as num_customers,
sum(t1.amount)/count(distinct t1.customer_id) as RPC,
t1.price_band_type

from overall_info t1
join (select distinct material_group_id, appliances_tag
      from category_price_slabs_view) t2
on t1.material_group_id = t2.material_group_id
where t2.material_group_id != 46001012 and t2.appliances_tag is not null

group by t2.appliances_tag, t2.material_group_id, t1.material_group_name, period_type, price_band_type
having period_type is not null
order by t2.material_group_id)

SELECT appliances_tag,
    material_group_name,
    round(MAX(CASE WHEN period_type = 'period_22' THEN total_sales END),0) AS period_22,
    round(MAX(CASE WHEN period_type = 'period_23' THEN total_sales END),0) AS period_23,
    price_band_type,
    CONCAT(ROUND(((period_23 - period_22) / period_22 * 100), 2), '%') AS sales_change
FROM sales_info_overall
GROUP BY appliances_tag, material_group_name, price_band_type
order by material_group_name

-- COMMAND ----------


