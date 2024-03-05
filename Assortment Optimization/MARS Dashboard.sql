-- Databricks notebook source
create or replace table sandbox.pj_mars_trans_cust_store_prod_raw as
  with base as (
      select transaction_id, business_day,
          year(business_day) as year,
          month(business_day) as month,
          -- quarter(business_day) as quarter,
          customer_id, 
          product_id as material_id, unit_measure_code,
          transaction_type, quantity, billed_amount,
          amount, store_id, mocd_flag
      from gold.pos_transactions
      where business_day between '2023-02-01' and '2024-01-30'
      and customer_id is not null
      and product_id is not null),

  mat_master as (
      select material_id, material_name, material_group_id, material_group_name, 
          category_id, category_name, department_name, brand,
          case
              when (brand = "MARS UNCLE BEN S" AND category_name = "BABY FOODS")
                    OR (brand = "MARS TWIX" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS IMP" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS MALTESERS" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS UK" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS GALAXY" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS SNICKERS" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS BOUNTY" AND category_name = "BEVERAGES - HOT")
                    OR (brand = "MARS GALAXY" AND category_name = "BISCUITS & CAKES")
                    OR (brand = "MARS M&M S" AND category_name = "BISCUITS & CAKES")
                    OR (brand = "MARS UK" AND category_name = "BISCUITS & CAKES")
                    OR (brand = "MARS" AND category_name = "BISCUITS & CAKES")
                    OR (brand = "MARS TWIX" AND category_name = "BISCUITS & CAKES")
                    OR (brand = "MARS BOUNTY" AND category_name = "BISCUITS & CAKES")
                    OR (brand = "MARS IMP" AND category_name = "BISCUITS & CAKES")
                    OR (brand = "MARS IMP" AND category_name = "BOUGHT IN BREAD & CAKES")
                    OR (brand = "MARS" AND category_name = "BOUGHT IN BREAD & CAKES")
                    OR (brand = "MARS IMP" AND category_name = "BREAKFAST CEREALS")
                    OR (brand = "MARS UNCLE BEN S" AND category_name = "CANNED FRUITS & DESSERTS")
                    OR (brand = "MARS GALAXY" AND category_name = "CHEESE & MARGARINES")
                    OR (brand = "MARS GALAXY" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS MALTESERS" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS GALAXY" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS TWIX" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS IMP" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS SNICKERS" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS BOUNTY" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS UK" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS M&M S" AND category_name = "CONFECTIONERY")
                    OR (brand = "MARS DOLMIO" AND category_name = "COOKING SAUCES & AIDS")
                    OR (brand = "MARS UNCLE BEN S" AND category_name = "COOKING SAUCES & AIDS")
                    OR (brand = "MARS M&M S" AND category_name = "CRISPS, SNACKS & DRIED FRUITS")
                    OR (brand = "MARS" AND category_name = "DAIRY")
                    OR (brand = "MARS GALAXY" AND category_name = "DAIRY")
                    OR (brand = "MARS MALTESERS" AND category_name = "DAIRY")
                    OR (brand = "MARS IMP" AND category_name = "DAIRY")
                    OR (brand = "MARS BOUNTY" AND category_name = "DAIRY")
                    OR (brand = "MARS SNICKERS" AND category_name = "DAIRY")
                    OR (brand = "MARS M&M S" AND category_name = "DAIRY")
                    OR (brand = "MARS UK" AND category_name = "DAIRY")
                    OR (brand = "MARS TWIX" AND category_name = "DAIRY")
                    OR (brand = "MARS SNICKERS" AND category_name = "DIABETIC & HEALTH")
                    OR (brand = "MARS UNCLE BEN S" AND category_name = "ETHNIC FOODS")
                    OR (brand = "MARS DOLMIO" AND category_name = "ETHNIC FOODS")
                    OR (brand = "MARS GALAXY" AND category_name = "FRESH JUICE & DESSERTS")
                    OR (brand = "MARS GALAXY" AND category_name = "HOME BAKING")
                    OR (brand = "MARS GALAXY" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS SNICKERS" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS BOUNTY" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS MALTESERS" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS TWIX" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS M&M S" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS IMP" AND category_name = "ICE CREAM & DESSERTS")
                    OR (brand = "MARS GALAXY" AND category_name = "LONG LIFE DAIRY PRODUCTS")
                    OR (brand = "MARS IMP" AND category_name = "LONG LIFE DAIRY PRODUCTS")
                    OR (brand = "MARS M&M S" AND category_name = "LONG LIFE DAIRY PRODUCTS")
                    OR (brand = "MARS MALTESERS" AND category_name = "LONG LIFE DAIRY PRODUCTS")
                    OR (brand = "MARS" AND category_name = "LONG LIFE DAIRY PRODUCTS")
                    OR (brand = "MARS GALAXY" AND category_name = "MOBILE ACCESSORIES")
                    OR (brand = "MARS BOUNTY" AND category_name = "NON DAIRY MILK")
                    OR (brand = "MARS DOLMIO" AND category_name = "PASTA & NOODLE")
                    OR (brand = "MARS CATSAN" AND category_name = "PET CARE")
                    OR (brand = "MARS THOMAS" AND category_name = "PET CARE")
                    OR (brand = "MARS KITE KAT" AND category_name = "PET FOOD")
                    OR (brand = "MARS WHISKAS" AND category_name = "PET FOOD")
                    OR (brand = "MARS SHEBA" AND category_name = "PET FOOD")
                    OR (brand = "MARS PEDIGREE" AND category_name = "PET FOOD")
                    OR (brand = "MARS THOMAS" AND category_name = "PET FOOD")
                    OR (brand = "MARS CESAR" AND category_name = "PET FOOD")
                    OR (brand = "MARS DREAMIES" AND category_name = "PET FOOD")
                    OR (brand = "MARS TRILL" AND category_name = "PET FOOD")
                    OR (brand = "MARS MALTESERS" AND category_name = "PRESERVATIVES & SPREADS")
                    OR (brand = "MARS GALAXY" AND category_name = "PRESERVATIVES & SPREADS")
                    OR (brand = "MARS SNICKERS" AND category_name = "PRESERVATIVES & SPREADS")
                    OR (brand = "MARS M&M S" AND category_name = "PRESERVATIVES & SPREADS")
                    OR (brand = "MARS UK" AND category_name = "PRESERVATIVES & SPREADS")
                    OR (brand = "MARS UNCLE BEN S" AND category_name = "PULSES & SPICES & HERBS")
                    OR (brand = "MARS UNCLE BEN S" AND category_name = "RICE")
                    OR (brand = "MARS DOLMIO" AND category_name = "SAUCES & PICKLES")
                    OR (brand = "MARS IMP" AND category_name = "SPORTS NUTRITION")
                    OR (brand = "MARS UK" AND category_name = "SPORTS NUTRITION")
                    OR (brand = "MARS" AND category_name = "SPORTS NUTRITION")
                    OR (brand = "MARS M&M S" AND category_name = "SPORTS NUTRITION")
                    OR (brand = "MARS BOUNTY" AND category_name = "SPORTS NUTRITION")
                  then 'MARS'
              else 'OTHERS'
          end as business
      from gold.material_master),

  customer_profile as (
      select distinct account_key, gender, nationality_desc, nationality_group, 
          age, age_group, LHRDATE
      from gold.customer_profile
      where mobile is not null),

  rfm_segment as (
      select customer_id, segment 
      from analytics.customer_segments
      where key = 'rfm'
      and country = 'uae'
      and channel = 'pos'
      and month_year = '202401')

  select base.*, mm.material_name, mm.material_group_id, mm.material_group_name,
      mm.category_id, mm.category_name, mm.department_name, 
      case when mm.brand is null then 'OTHERS' else mm.brand end as brand,
      case when mm.business is null then 'OTHERS' else mm.business end as business, 
      sm.store_name, sm.region_name, sm.country_id,
      cp.nationality_desc, cp.age, cp.LHRDATE, 
      case when LHRDATE is not null then 'LOYALTY'
          else 'NON LOYALTY'
      end as loyalty_member,
      case when cp.gender is null then 'UNKNOWN' else cp.gender end as gender,
      case when (cp.nationality_group is null or cp.nationality_group = 'NA') then 'UNKNOWN'
          else cp.nationality_group end as nationality_group,
      case when cp.age_group is null then 'UNKNOWN' else cp.age_group end as age_group,
      case when rfm.segment is null then 'No Mapping' else rfm.segment end as segment
  from base
  left join mat_master mm
  on base.material_id = mm.material_id
  left join gold.store_master sm
  on base.store_id = sm.store_id
  left join customer_profile cp
  on base.customer_id = cp.account_key
  left join rfm_segment rfm
  ON base.customer_id = rfm.customer_id
