-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Private Label Overall Info

-- COMMAND ----------

select t1.transaction_id, t1.transaction_date, t1.business_day, t1.billed_amount, t1.product_id,
        t1.amount, t1.quantity, t1.unit_price, t1.regular_unit_price, t1.actual_unit_price,
        t1.customer_id, t2.material_name, t2.material_group_id, t2.material_group_name,
        t2.category_id, t2.category_name, t2.department_id, t2.department_name, t2.brand,
        t4.store_id, t4.store_name, t4.region_name, t4.broad_segment, t4.sub_segment,
        case when t3.material_id is null then 0 else 1 end as pl
from gold.pos_transactions t1
join gold.material_master t2
on t1.product_id = t2.material_id
left join private_labels t3
on t1.product_id = t3.material_id
join gold.store_master t4
on t1.store_id = t4.store_id
where t1.business_day between "2022-01-01" and "2023-09-30"
and t1.amount > 0
and t1.quantity > 0
and department_name not in ('018', 'EX.EKPHK', 'ADVANCE / EXCHANGE DEPOSITS', 'FRUIT & VEGETABLES',
                            'HARD GOODS', 'PERFUMES COSMETICS & JEWELLERY', 'MALL LEASE', 'GIFT CARD',
                            'CHARITY', 'RESTAURANT', 'FOUNDATION PROGRAM', '016', 'THE KITCHEN', 'HUDA SHIPPING')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Customer Profile Data

-- COMMAND ----------

select account_key as customer_id, lower(email) as email, mobile, LHRDATE
from gold.customer_profile
where MONTH(LHRDATE) = 4
and email is not null
and mobile is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Customer Segments Data

-- COMMAND ----------

select customer_id, recency,round(inter_purchase_time,2) as inter_purchase_time,
  round(average_order_value,2) as average_order_value,
  total_orders, round(total_spend,2) as total_spend, segment
from analytics.customer_segments
where key = 'rfm'
and channel = 'pos'
and country = 'uae'
and month_year = 202401

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Transactional Data - Brief Overview

-- COMMAND ----------

select transaction_id, transaction_date, customer_id,
product_id, quantity, amount,
category_name, material_group_name, department_id,
department_name, department_class_name,
store_id, store_name, region_name
from(
    select t1.*, t2.*, t3.store_name, t3.region_name
    from gold.pos_transactions t1
    join gold.material_master t2
    on t1.product_id = t2.material_id
    join gold.store_master t3
    on t1.store_id = t3.store_id
    where t1.transaction_date = "2023-04-30"
    and round(t1.amount,2) > 0
    and t1.quantity > 0
    and t1.mobile is not null)
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Department Level Transaction Data

-- COMMAND ----------

select transaction_id, transaction_date,
trunc(transaction_date, 'month') as date_month,
customer_id, product_id, quantity, amount,
category_name, category_id, material_group_name, department_id,
department_name, department_class_name, material_group_id,
store_id, store_name, region_name, brand
from(
    select t1.*, t2.*, t3.store_name, t3.region_name
    from gold.pos_transactions t1
    join gold.material_master t2
    on t1.product_id = t2.material_id
    join gold.store_master t3
    on t1.store_id = t3.store_id
    where t1.business_day between '2022-01-01' and '2023-10-31'
    and round(amount,2) > 0
    and quantity > 0
    and transaction_type in ('SALE','SELL_MEDIA')
    and department_name in ('ELECTRICALS',
                            'COMPUTERS & GAMING',
                            'MOBILE PHONES',
                            'ELECTRONICS',
                            'CONSUMER ELECTRONICS ACCESSORIES'
                            )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Validation on Billed Amount

-- COMMAND ----------

select transaction_id,
        round(avg(billed_amount), 0) avg_billed,
        round(sum(amount), 0) sum_amount
from gold.pos_transactions
where year(business_day) = 2023
group by transaction_id
HAVING avg_billed != sum_amount

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Department level total transactions and sales

-- COMMAND ----------

select t2.department_name,
        count(distinct t1.transaction_id) as total_transactions,
        round(sum(t1.amount),2) as total_sales
from gold.pos_transactions t1
join gold.material_master t2
on t1.product_id = t2.material_id
where year(t1.transaction_date) = 2023
group by t2.department_name

-- COMMAND ----------


