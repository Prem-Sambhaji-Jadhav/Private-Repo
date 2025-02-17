# Databricks notebook source
# DBTITLE 1,Sandboxes to be Refreshed
po_sandboxes = ['pj_hhc_final_elasticities', 'pj_hhc_imputation_metrics', 'pj_po_actual_pred_data_promo_hhc_master', 'pj_po_hhc_apc_cluster_mapping', 'pj_po_imputed_promo_master_hhc', 'pj_po_model_results_promo_hhc_master_best_model', 'pj_poc_mds_final_hhc_master']


ao_sandboxes = ['pj_ao_attributes', 'pj_ao_customer_data', 'pj_ao_dashboard_delist_reco_skus', 'pj_ao_dashboard_delist_reco_skus_analysis', 'pj_ao_dashboard_reco', 'pj_ao_framework_data', 'pj_ao_gp', 'pj_ao_material_store_data', 'pj_ao_reco', 'pj_ao_v2', 'pj_ao_v2_new_listings', 'pj_ao_weekly_data']

# ao_sandboxes = ['pj_ao_dashboard_region_view', 'pj_ao_lfl_calendar', 'pj_ao_dashboard_cy_region_view', 'pj_ao_dashboard_py_region_view']


campaign_sandboxes = ['campaign_customer_details', 'campaign_test_cardkeys']


analytics_tables = ['kuwait_campaign_customer_details', 'qatar_campaign_customer_details', 'bahrain_campaign_customer_details']


sandbox_list = po_sandboxes + ao_sandboxes + campaign_sandboxes
sandbox_list = sorted(sandbox_list)
analytics_list = sorted(analytics_tables)

i = 1
print('Sandbox Tables:')
for table in sandbox_list:
    print(f"{i}. {table}")
    i += 1

print('\nAnalytics Tables:')
for table in analytics_list:
    print(f"{i}. {table}")
    i += 1

# COMMAND ----------

# DBTITLE 1,Sandboxes not Required for Refresh
df = spark.sql("SHOW TABLES IN dev.sandbox").toPandas()[['tableName']]

pj_cols = [col[0] for col in df.values if col[0].startswith('pj_')]

del_tables = [item for item in pj_cols if item not in sandbox_list]

i = 1
for del_table in del_tables:
    print(f"{i}. {del_table}")
    i += 1

# COMMAND ----------

# DBTITLE 1,Refreshing Sandboxes
i = 1
print('Sandbox Tables:')
for table in sandbox_list:
    query = f"""
    CREATE OR REPLACE TABLE dev.sandbox.{table} AS (
        SELECT * FROM dev.sandbox.{table}
    )
    """
    spark.sql(query)
    print(f"{i}. {table}")
    i += 1

print('\nAnalytics Tables:')
for table in analytics_list:
    query = f"""
    CREATE OR REPLACE TABLE dev.analytics.{table} AS (
        SELECT * FROM dev.analytics.{table}
    )
    """
    spark.sql(query)
    print(f"{i}. {table}")
    i += 1
