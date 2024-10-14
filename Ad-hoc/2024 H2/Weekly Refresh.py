# Databricks notebook source
# DBTITLE 1,List of Sandboxes to be Refreshed
po_sandboxes = ['pj_po_actual_pred_data_promo_hhc_apc_master', 'pj_po_actual_pred_data_promo_hhc_dis_master', 'pj_po_actual_pred_data_promo_hhc_wup_master', 'pj_po_final_elasticities_promo_hhc_master', 'pj_po_hhc_apc_cluster_mapping', 'pj_po_imputed_metrics_promo_master_hhc_apc', 'pj_po_imputed_metrics_promo_master_hhc_dis', 'pj_po_imputed_metrics_promo_master_hhc_wup', 'pj_po_imputed_promo_master_hhc_apc', 'pj_po_imputed_promo_master_hhc_dis', 'pj_po_imputed_promo_master_hhc_wup', 'pj_po_model_results_promo_hhc_apc_master', 'pj_po_model_results_promo_hhc_apc_master_best_model', 'pj_po_model_results_promo_hhc_dis_master', 'pj_po_model_results_promo_hhc_dis_master_best_model', 'pj_po_model_results_promo_hhc_wup_master', 'pj_po_model_results_promo_hhc_wup_master_best_model', 'pj_poc_mds_final_hhc_master']

ao_sandboxes = ['pj_ao_v2', 'pj_ao_v2_attribute_sales', 'pj_ao_v2_mds', 'pj_ao_attributes', 'pj_ao_customer_data', 'pj_ao_dashboard_reco', 'pj_ao_framework_data', 'pj_ao_gp_12months', 'pj_ao_gp_3months', 'pj_ao_material_store_data', 'pj_ao_weekly_data', 'pj_ao_dashboard_delist_reco_skus']

# ao_sandboxes = ['pj_assortment_dashboard_region_view', 'tbl_assortment_lfl_calendar', 'pj_assortment_dashboard_cy_region_view', 'pj_assortment_dashboard_py_region_view']

sandbox_list = po_sandboxes + ao_sandboxes

sandbox_list = sorted(sandbox_list)

i = 0
for sandbox in sandbox_list:
    i += 1
    print(f"{i}. {sandbox}")

# COMMAND ----------

# DBTITLE 1,List of Sandboxes not Required for Refresh
df = spark.sql("SHOW TABLES IN dev.sandbox").toPandas()[['tableName']]
pj_cols = [col[0] for col in df.values if col[0].startswith('pj_')]

del_tables = [item for item in pj_cols if item not in sandbox_list]
i = 0
for del_table in del_tables:
    i += 1
    print(f"{i}. {del_table}")

# COMMAND ----------

# DBTITLE 1,Refreshing Sandboxes
i = 0
for sandbox in sandbox_list:
    df = spark.sql(f'SELECT * FROM dev.sandbox.{sandbox}')
    df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f'dev.sandbox.{sandbox}')
    i += 1
    print(f"{i}. {sandbox}")
