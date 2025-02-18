# Databricks notebook source
# MAGIC %md
# MAGIC #ABU DHABI (Updated)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dev.sandbox.pj_ao_reco
# MAGIC SET recommendation = 'Observe'
# MAGIC WHERE
# MAGIC     material_id = 1342729
# MAGIC     AND region_name = 'ABU DHABI'

# COMMAND ----------

# MAGIC %md
# MAGIC #DUBAI (Updated)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dev.sandbox.pj_ao_reco
# MAGIC SET recommendation = 'Observe'
# MAGIC WHERE
# MAGIC     material_id = 2220203
# MAGIC     AND region_name = 'DUBAI'

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_ao_dashboard_delist_reco_skus AS (
#     SELECT * FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus
#     UNION
#     SELECT * FROM dev.sandbox.pj_ao_dashboard_delist_reco_skus_analysis WHERE material_group_name IN ('RICE & OAT CAKE', 'VINEGAR')
# )
