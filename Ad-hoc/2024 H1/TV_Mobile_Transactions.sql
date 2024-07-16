-- Databricks notebook source
with trans_data_raw as (
select transaction_id, transaction_date, customer_id,
product_id, quantity, amount,
category_name, material_group_name, department_id,
department_name, department_class_name,
store_id
from(
    select t1.*, t2.*
    from gold.pos_transactions t1
    join gold.material_master t2
    on t1.product_id = t2.material_id
    where year(t1.business_day) = 2023
    and round(t1.amount,2) > 0
    and t1.quantity > 0)
),

mobile_purchase_trans as (
  select transaction_id, collect_set(upper(category_name)) as category_set,
  size(collect_set(upper(category_name))) as category_size,
  case when array_contains(collect_set(upper(category_name)), 'MOBILE PHONE') then 1 else 0 end as mobile_presence_flag
  from trans_data_raw
  group by transaction_id
)

SELECT category_name, COUNT(DISTINCT transaction_id) as num_transactions
FROM (
    SELECT transaction_id, EXPLODE(category_set) as category_name
    FROM mobile_purchase_trans
    WHERE mobile_presence_flag = 1
)
GROUP BY category_name
ORDER BY num_transactions desc

-- COMMAND ----------

with trans_data_raw as (
select transaction_id, transaction_date, customer_id,
product_id, quantity, amount,
category_name, material_group_name, department_id,
department_name, department_class_name,
store_id
from(
    select t1.*, t2.*
    from gold.pos_transactions t1
    join gold.material_master t2
    on t1.product_id = t2.material_id
    where year(t1.business_day) = 2023
    and round(t1.amount,2) > 0
    and t1.quantity > 0)
),

mobile_purchase_trans as (
  select transaction_id, collect_set(upper(category_name)) as category_set,
  size(collect_set(upper(category_name))) as category_size,
  case when array_contains(collect_set(upper(category_name)), 'MOBILE PHONE') then 1 else 0 end as mobile_presence_flag
  from trans_data_raw
  group by transaction_id
)

select count(*) as total_mobile_trans,
count(case when category_size = 1 then transaction_id end) as only_mobile_trans,
ROUND(only_mobile_trans/total_mobile_trans,4) AS only_mobile_trans_perc
from mobile_purchase_trans
where mobile_presence_flag = 1

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

with trans_data_raw as (
select transaction_id, transaction_date, customer_id,
product_id, quantity, amount,
category_name, material_group_name, department_id,
department_name, department_class_name,
store_id
from(
    select t1.*, t2.*
    from gold.pos_transactions t1
    join gold.material_master t2
    on t1.product_id = t2.material_id
    where year(t1.business_day) = 2023
    and round(t1.amount,2) > 0
    and t1.quantity > 0)
),

tv_purchase_trans as (
  select transaction_id, collect_set(upper(category_name)) as category_set,
  size(collect_set(upper(category_name))) as category_size,
  case when array_contains(collect_set(upper(category_name)), 'TV') then 1 else 0 end as tv_presence_flag
  from trans_data_raw
  group by transaction_id
)

select count(*) as total_tv_trans,
count(case when category_size = 1 then transaction_id end) as only_tv_trans,
ROUND(only_tv_trans/total_tv_trans,4) AS only_tv_trans_percentage
from tv_purchase_trans
where tv_presence_flag = 1

-- COMMAND ----------

with trans_data_raw as (
select transaction_id, transaction_date, customer_id,
product_id, quantity, amount,
category_name, material_group_name, department_id,
department_name, department_class_name,
store_id
from(
    select t1.*, t2.*
    from gold.pos_transactions t1
    join gold.material_master t2
    on t1.product_id = t2.material_id
    where year(t1.business_day) = 2023
    and round(t1.amount,2) > 0
    and t1.quantity > 0)
),

tv_purchase_trans as (
  select transaction_id, collect_set(upper(category_name)) as category_set,
  size(collect_set(upper(category_name))) as category_size,
  case when array_contains(collect_set(upper(category_name)), 'TV') then 1 else 0 end as tv_presence_flag
  from trans_data_raw
  group by transaction_id
)

SELECT category_name, COUNT(DISTINCT transaction_id) as num_transactions
FROM (
    SELECT transaction_id, EXPLODE(category_set) as category_name
    FROM tv_purchase_trans
    WHERE tv_presence_flag = 1
)
GROUP BY category_name
ORDER BY num_transactions desc

-- COMMAND ----------



-- COMMAND ----------


