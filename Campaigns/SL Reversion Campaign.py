# Databricks notebook source
query = """SELECT mobile, card_key
FROM gold.customer_profile AS t1
JOIN analytics.customer_segments AS t2
ON t1.account_key = t2.customer_id
WHERE LHRDATE IS NOT NULL
AND key = 'rfm'
AND channel = 'pos'
AND t2.country = 'uae'
AND month_year = '202311'
AND segment = 'Slipping Loyalist'
GROUP BY mobile, card_key"""

df = spark.sql(query)
df[['mobile']].toPandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/dec_campaigns/sl_reversion/' +'test_set_sl_mobile.csv', sep = '|', index = False)
df[['mobile', 'card_key']].toPandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/dec_campaigns/sl_reversion/' +'test_set_sl_cardkey.csv', sep = '|', index = False)

# COMMAND ----------

query = """SELECT mobile
FROM gold.customer_profile AS t1
JOIN analytics.customer_segments AS t2
ON t1.account_key = t2.customer_id
WHERE LHRDATE IS NULL
AND key = 'rfm'
AND channel = 'pos'
AND t2.country = 'uae'
AND month_year = '202311'
AND segment = 'Slipping Loyalist'
GROUP BY mobile"""

df = spark.sql(query)
df[['mobile']].toPandas().to_csv('/dbfs/mnt/cdp-customers/gold-layer/adhoc/campaigns/dec_campaigns/sl_reversion/' +'control_set_sl_mobile.csv', sep = '|', index = False)

# COMMAND ----------



# COMMAND ----------


