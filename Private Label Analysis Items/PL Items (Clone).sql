-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## PL Materials

-- COMMAND ----------

select * from sandbox.pj_private_label_material_id_raw

-- COMMAND ----------

-- SELECT
--   date,
--   sales,
--   SUM(sales) OVER (ORDER BY sales desc ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW) AS running_sum,
--   SUM(sales) OVER (ORDER BY sales desc) AS running_sum_2 ---wrong
-- FROM
--   sandbox.pg_test_temp_1
-- ORDER BY sales desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## quarterly sales

-- COMMAND ----------


with to_remove_mg as (
  select material_group_name, count(distinct category_name) as cnt
  from gold.material_master
  group by material_group_name
  having cnt > 1
)

SELECT
concat(year(transaction_date), " ", "Q",quarter(transaction_date)) as quarter_info,
department_name,
category_name,
material_group_name,
COUNT(DISTINCT product_id) AS num_materials,
COUNT(DISTINCT CASE WHEN pl = 1 THEN product_id END) AS num_pl_materials,
SUM(amount) as sales,
SUM(case when pl=1 then amount end) as pl_sales
FROM sandbox.pj_pl_transactions_info_sept
where material_group_name not in (select material_group_name from to_remove_mg)
group by quarter_info, department_name, category_name, material_group_name
order by quarter_info

-- COMMAND ----------


with to_remove_mg as (
  select material_group_name, count(distinct category_name) as cnt
  from gold.material_master
  group by material_group_name
  having cnt > 1
)

SELECT
concat(year(transaction_date), " ", "Q",quarter(transaction_date)) as quarter_info,
department_name,
category_name,
material_group_name,
case when pl=1 then 'PRIVATE LABEL' else brand end as brand_type,
COUNT(DISTINCT product_id) AS num_materials,
SUM(amount) as sales
FROM sandbox.pj_pl_transactions_info_sept
where material_group_name not in (select material_group_name from to_remove_mg)
group by quarter_info, department_name, category_name, material_group_name, brand_type
having brand_type is not null
order by quarter_info

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # price analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## price (regular selling price) table
-- MAGIC **Table desc:** takes raw transaction data (from gold.pos_transactions) and gives product_id level regular_price and sales info with an additional flag if the given product is a lulu private lable item

-- COMMAND ----------

-- drop table if exists sandbox.pg_product_prices_pl_raw;

-- create table sandbox.pg_product_prices_pl_raw (
-- WITH product_prices as (
--   SELECT
--     t1.product_id,
--     t1.regular_unit_price,
--     t1.total_sales,
--     t2.material_name,
--     t2.material_group_name,
--     t2.category_name,
--     t2.department_name
--   FROM
--     (
--       SELECT
--         product_id,
--         ROUND(avg(regular_unit_price), 2) as regular_unit_price,
--         ROUND(sum(amount), 2) as total_sales
--       FROM
--         sandbox.pj_pl_transactions_info_sept
--       GROUP BY
--         product_id
--     ) t1
--     JOIN gold.material_master t2 ON t1.product_id = t2.material_id
-- )


-- select
--   t1.*,
--   case
--     when t2.material_id is null then 0
--     else 1
--   end as pl_flag
-- from
--   (
--     SELECT
--       department_name,
--       category_name,
--       product_id,
--       material_name,
--       regular_unit_price,
--       total_sales,
--       round(total_sales /(sum(total_sales) over(PARTITION BY department_name, category_name)),8) as category_sales_perc
--     FROM
--       product_prices
--   ) t1
--   left join sandbox.pj_private_label_material_id_raw t2 on t1.product_id = t2.material_id

-- )

-- COMMAND ----------

select *
from sandbox.pg_product_prices_pl_raw
where category_name = 'FRESH JUICE & DESSERTS' limit 10

-- COMMAND ----------

select * from sandbox.pg_product_prices_pl_raw where department_name = 'DELICATESSEN' and category_name = 'COOKED MEAT'

-- COMMAND ----------

select *
from(
    select
    department_name,
    category_name,
    count(product_id) as num_products,
    count(case when pl_flag = 1 then product_id end) as pl_products,
    sum(total_sales) as total_sales,
    sum(total_sales)/(sum(sum(total_sales)) over(partition by department_name)) as dept_sales_perc,
    sum(case when pl_flag=1 then total_sales end) as pl_sales,
    sum(case when pl_flag=1 then total_sales end)/sum(total_sales) as pl_sales_perc,
    sum(regular_unit_price*category_sales_perc) as avg_category_price_wt,
    sum(case when pl_flag=0 then regular_unit_price*category_sales_perc end) as avg_category_price_wt_pl_exc,
    avg(case when pl_flag=1 then regular_unit_price end) as avg_price_pl_items,
    sum(regular_unit_price*category_sales_perc)-avg(case when pl_flag=1 then regular_unit_price end) as price_diff
    from sandbox.pg_product_prices_pl_raw
    group by department_name, category_name
    order by department_name, category_name
)
where pl_products > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Key Takeaways

-- COMMAND ----------

select case when num_trans >=4 then '>=4' else num_trans end as customer_type,
count(customer_id) as num_customers
from(
    select
      customer_id,
      count(distinct product_id) as num_materials,
      count(distinct category_id) as num_categories,
      count(distinct material_group_id) as num_mg,
      count(distinct transaction_id) as num_trans
    from
      sandbox.pj_pl_transactions_info_sept
    where pl=1 and customer_id is not null
    and year(business_day) = 2023
    group by customer_id)
group by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## customer metrics

-- COMMAND ----------

-- segment pl penetration (sales and PL activation)

with rfm_segment_info as (
      select customer_id, segment
      from analytics.customer_segments
      where key = 'rfm'
      and channel = 'pos'
      and country = 'uae'
      and month_year = 202309)

select t2.segment,
sum(case when t1.pl=1 then t1.amount end) as pl_sales,
sum(case when t1.pl=0 then t1.amount end) as non_pl_sales,
sum(t1.amount) as total_sales,
sum(case when t1.pl=1 then t1.amount end)/sum(t1.amount) as pl_sales_perc,
count(distinct case when t1.pl=1 then t1.customer_id end) as pl_customers,
count(distinct t1.customer_id) as total_customers,
count(distinct case when t1.pl=1 then t1.customer_id end)/count(distinct t1.customer_id) as pl_cust_perc

from sandbox.pj_pl_transactions_info_sept t1
join rfm_segment_info t2
on t1.customer_id = t2.customer_id
where year(t1.business_day) = 2023

group by t2.segment

-- COMMAND ----------

with rfm_segment_info as (
      select customer_id, segment
      from analytics.customer_segments
      where key = 'rfm'
      and channel = 'pos'
      and country = 'uae'
      and month_year = 202309),

      nationality_info as (
      select account_key as customer_id, nationality_group as nationality
      from gold.customer_profile
      ),

      segment_pl_one_timers as (
      select segment, sum(is_pl_one_timer) as pl_one_timers
      from(
          select t2.segment, t2.customer_id,
          case when count(distinct case when pl=1 then transaction_id end) = 1 then 1 else 0 end as is_pl_one_timer
          from sandbox.pj_pl_transactions_info_sept t1
          join rfm_segment_info t2
          on t1.customer_id = t2.customer_id
          where year(t1.business_day) = 2023
          group by t2.segment, t2.customer_id)
      group by segment
      )



select a.*, b.pl_one_timers, b.pl_one_timers/a.pl_customers as pl_one_timer_perc
from(
    select t2.segment,
    sum(case when t1.pl=1 then t1.amount end) as pl_sales,
    sum(case when t1.pl=0 then t1.amount end) as non_pl_sales,
    sum(t1.amount) as total_sales,
    sum(case when t1.pl=1 then t1.amount end)/sum(t1.amount) as pl_sales_perc,
    count(distinct case when t1.pl=1 then t1.customer_id end) as pl_customers,
    count(distinct t1.customer_id) as total_customers,
    count(distinct case when t1.pl=1 then t1.customer_id end)/count(distinct t1.customer_id) as pl_cust_perc

    from sandbox.pj_pl_transactions_info_sept t1
    join rfm_segment_info t2
    on t1.customer_id = t2.customer_id
    where year(t1.business_day) = 2023

    group by t2.segment) a

JOIN segment_pl_one_timers b
on a.segment = b.segment

-- COMMAND ----------

-- rfm preferred categories


with rfm_segment_info as (
      select customer_id, segment
      from analytics.customer_segments
      where key = 'rfm'
      and channel = 'pos'
      and country = 'uae'
      and month_year = 202309),

      nationality_info as (
      select account_key as customer_id, nationality_group as nationality
      from gold.customer_profile
      ),

      segment_sales_info as (
      select t2.nationality,
      t1.category_name,
      sum(case when t1.pl=1 then t1.amount end) as pl_sales,
      sum(case when t1.pl=0 then t1.amount end) as non_pl_sales,
      sum(t1.amount) as total_sales,
      sum(sum(case when t1.pl=1 then t1.amount end)) over(partition by t2.nationality) as segment_total_pl_sales,
      sum(sum(case when t1.pl=0 then t1.amount end)) over(partition by t2.nationality) as segment_total_non_pl_sales,
      sum(sum(t1.amount)) over(partition by t2.nationality) as segment_total_sales



      from sandbox.pj_pl_transactions_info_sept t1
      join nationality_info t2
      on t1.customer_id = t2.customer_id
      where year(t1.business_day) = 2023
      -- and t2.nationality in ('ARABS','INDIAN','EMIRATI','FILIPINO')
      group by t2.nationality, t1.category_name
      )


-- select *, (pl_sales/segment_total_pl_sales)/(non_pl_sales/segment_total_non_pl_sales) as sales_index
-- from segment_sales_info
-- where pl_sales is not null
-- and total_sales >= 1000


select distinct nationality, segment_total_pl_sales, segment_total_non_pl_sales
from segment_sales_info
