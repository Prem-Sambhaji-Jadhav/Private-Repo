-- Databricks notebook source
--VALIDATION ON BILLED AMOUNT

select count(*), sum(match_flag)
from (
  select transaction_id, round(avg(billed_amount), 0) avg_billed, round(sum(amount), 0) sum_amount,
  case when round(sum(amount),0) = round(avg(billed_amount),0) then 1 else 0 end as match_flag
  from gold.pos_transactions
  where year(business_day) = 2023
  group by transaction_id
  )

-- COMMAND ----------

--total transactions in the last year, total sales in the last year, quarter over quarter sales change, segregated by department

select t2.department_name, 
count(distinct t1.transaction_id) as total_transactions,
round(sum(t1.amount),2) as total_sales
from gold.pos_transactions t1
join gold.material_master t2
on t1.product_id = t2.material_id
where year(t1.transaction_date) = 2023
group by t2.department_name

-- COMMAND ----------

--RFM customer segment distribution (oct 2023), how the distribution has changed from last 2 months

select oct.segment, (oct.count - aug.count) as distribution_change
from (
  select segment, count(*) as count
  from analytics.customer_segments
  where key = 'rfm'
  and month_year = 202310
  group by segment
) as oct
join (
  select segment, count(*) as count
  from analytics.customer_segments
  where key = 'rfm'
  and month_year = 202308
  group by segment
) as aug
on oct.segment = aug.segment

-- COMMAND ----------


