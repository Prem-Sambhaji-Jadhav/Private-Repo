# Databricks notebook source
# MAGIC %md
# MAGIC #Actual vs Predicted Results

# COMMAND ----------

# query = """
# SELECT
#     t1.demand_group,
#     t1.region_name,
#     t1.material_id,
#     t1.week_number,
#     t1.quantity_actual,
#     t1.quantity_pred
# FROM dev.sandbox.pj_po_actual_pred_data_promo_hhc_apc_master AS t1
# JOIN dev.sandbox.pj_po_model_results_promo_hhc_apc_master_best_model AS t2
#     ON t1.demand_group = t2.demand_group
#     AND t1.region_name = t2.region_name
#     AND t1.material_id = t2.material_id
# WHERE
#     t1.model = t2.best_model
#     AND t2.model = t2.best_model
# ORDER BY 1, 2, 3, 4
# """

# df = spark.sql(query).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #MDS Master Sandbox

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_poc_mds_final_hhc_master AS (
#     SELECT "ALL PURPOSE CLEANER" AS material_group_name, "hhc_apc_dg_1" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_apc_dg1
#     UNION
#     SELECT "ALL PURPOSE CLEANER" AS material_group_name, "hhc_apc_dg_2" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_apc_dg2
#     UNION
#     SELECT "ALL PURPOSE CLEANER" AS material_group_name, "hhc_apc_dg_3" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_apc_dg3
#     UNION
#     SELECT "ALL PURPOSE CLEANER" AS material_group_name, "hhc_apc_dg_4" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_apc_dg4
#     UNION
#     SELECT "WASHING UP" AS material_group_name, "hhc_wup_dg_1" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_wup_dg1
#     UNION
#     SELECT "WASHING UP" AS material_group_name, "hhc_wup_dg_2" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_wup_dg2
#     UNION
#     SELECT "DISINFECTANTS" AS material_group_name, "hhc_dis_dg_1" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_dis_dg1
#     UNION
#     SELECT "DISINFECTANTS" AS material_group_name, "hhc_dis_dg_2" AS demand_group, region_name, material_id, week_number, quantity, avg_unit_price
#     FROM dev.sandbox.pj_poc_mds_final_hhc_dis_dg2
# )

# COMMAND ----------

# MAGIC %md
# MAGIC #Scatter Plots

# COMMAND ----------

import plotly.express as px
import pandas as pd

ALL_REGIONS = ('SHARJAH', 'DUBAI', 'AL AIN', 'ABU DHABI')
all_regions_iter = ('ALL',) + ALL_REGIONS
MG_LIST = ['ALL PURPOSE CLEANER', 'DISINFECTANTS', 'WASHING UP']

def plot_elasticity_scatter_plot(region_name, MG_NAME):
    if region_name == 'ALL':
        region_tuple = ALL_REGIONS
    else:
        region_tuple = (region_name,)
    
    if len(region_tuple) == 1:
        region_sql = f"('{region_tuple[0]}')"
    else:
        region_sql = str(region_tuple)
    
    query_final_elasticity = f"""
    WITH elasticities AS (
        SELECT
            material_group_name,
            demand_group,
            region_name,
            material_id,
            ROUND(CASE WHEN price_elasticity_top_3_avg < -2 THEN -2 ELSE price_elasticity_top_3_avg END, 3) AS price_elasticity,
            ROUND(promo_elasticity_top_3_avg, 3) AS promo_elasticity
        FROM dev.sandbox.pj_po_final_elasticities_promo_hhc_master
        WHERE
            material_group_name = '{MG_NAME}'
            AND region_name IN {region_sql}
    ),

    weekly_sales AS (
        SELECT
            *,
            (quantity * avg_unit_price) AS sales
        FROM dev.sandbox.pj_poc_mds_final_hhc_master
        WHERE week_number >= '2023-W31'
            AND region_name IN {region_sql}
            AND material_group_name = '{MG_NAME}'
    ),

    sales_data AS (
        SELECT
            demand_group,
            region_name,
            material_id,
            SUM(sales) AS total_sales,
            SUM(SUM(sales)) OVER () AS overall_sales,
            (total_sales / overall_sales) AS sales_contri,
            SUM(SUM(sales)/SUM(SUM(sales)) OVER()) OVER(ORDER BY SUM(sales)/SUM(SUM(sales)) OVER() DESC) AS cumulative_sales_contri
        FROM weekly_sales
        GROUP BY 1, 2, 3
    )

    SELECT
        t1.*,
        COALESCE(t2.total_sales, 0) AS sales
    FROM elasticities t1
    LEFT JOIN sales_data t2
        ON t1.demand_group = t2.demand_group
        AND t1.region_name = t2.region_name
        AND t1.material_id = t2.material_id
    ORDER BY material_id, region_name
    """

    df_final_elasticities = spark.sql(query_final_elasticity).toPandas()
    num_materials = df_final_elasticities.material_id.nunique()

    if region_name == 'ALL':
        df_agg = df_final_elasticities.groupby('material_id').apply(
            lambda x: pd.Series({
                'sales': x['sales'].sum(),
                'price_elasticity': (x['price_elasticity'] * x['sales']).sum() / x['sales'].sum(),
                'promo_elasticity': (x['promo_elasticity'] * x['sales']).sum() / x['sales'].sum()
            })
        ).reset_index()
    else:
        df_agg = df_final_elasticities[['material_id', 'price_elasticity', 'promo_elasticity', 'sales']].round(2)

    # Cummulative sales contribution calculation
    df_agg = df_agg.sort_values(by = 'sales', ascending = False).reset_index(drop = True)
    df_agg['sales_contri'] = df_agg['sales'] / df_agg['sales'].sum()
    df_agg['cumulative_contri'] = df_agg['sales_contri'].cumsum()

    # Prepare buckets based on sales contribution
    if region_name == 'AL AIN' and MG_NAME == 'WASHING UP':
        bin_edges = [0, 0.32, 0.6, 1]
        bin_labels = ['Top 30%', 'Middle 30%', 'Bottom 40%']
        df_agg['sales_bin'] = pd.cut(df_agg['cumulative_contri'], bins=bin_edges, labels=bin_labels, include_lowest=True)
        df_agg = df_agg[['material_id', 'price_elasticity', 'promo_elasticity', 'sales_bin']]
    else:
        bin_edges = [0, 0.2, 0.5, 1]
        bin_labels = ['Top 20%', 'Middle 30%', 'Bottom 50%']
        df_agg['sales_bin'] = pd.cut(df_agg['cumulative_contri'], bins=bin_edges, labels=bin_labels, include_lowest=True)
        df_agg = df_agg[['material_id', 'price_elasticity', 'promo_elasticity', 'sales_bin']]

    # Replace outliers with the second maximum value
    if (MG_NAME == 'ALL PURPOSE CLEANER' and region_name in ['AL AIN', 'ABU DHABI']) \
        or (MG_NAME == 'WASHING UP' and region_name in ['AL AIN', 'ABU DHABI', 'DUBAI', 'SHARJAH']) \
            or (MG_NAME == 'DISINFECTANTS' and region_name in ['AL AIN', 'ABU DHABI', 'DUBAI', 'SHARJAH', 'ALL']):
        max_val = df_agg['promo_elasticity'].max()
        second_max_val = df_agg['promo_elasticity'][df_agg['promo_elasticity'] != max_val].max()
        df_agg['promo_elasticity'] = df_agg['promo_elasticity'].replace(max_val, second_max_val)
    
    fig = px.scatter(
        df_agg,
        x = 'promo_elasticity',
        y = 'price_elasticity',
        hover_data = ['material_id'],
        color = 'sales_bin',
        color_discrete_map={"Top 20%": "#32CD32", "Middle 30%": "#4682B4", "Bottom 50%": "#FF6F61", "Top 30%": "#32CD32", "Bottom 40%": "#FF6F61"},
        labels = {'promo_elasticity': 'Promo Elasticity', 'price_elasticity': 'Price Elasticity'},
        title = f"MG: {MG_NAME}, Price vs Promo Elasticity Numbers, {'All Regions' if region_name == 'ALL' else region_name}"
    )

    fig.update_layout(
        height = 600,
        width = 1000,
        title_x = 0.5,
        legend_title_text='Sales Buckets'
    )

    print(f'MG: {MG_NAME}, Region: {region_name}, Num SKUs: {num_materials}')
    fig.update_yaxes(range = [0, df_agg['price_elasticity'].min()-0.25])

    fig.add_hline(y = -1, line_width=1, line_dash="dash", line_color="red")
    if df_agg['promo_elasticity'].max() >= 0.25:
        fig.add_vline(x = 0.25, line_width=1, line_dash="dash", line_color="red")
    else:
        fig.add_vline(x = df_agg.promo_elasticity.quantile(0.75), line_width=1, line_dash="dash", line_color="red")

    fig.show()

    temp = df_agg.copy()
    temp.insert(0, 'material_group_name', MG_NAME)
    temp.insert(0, 'region_name', region_name)
    temp = temp.groupby(['material_group_name', 'region_name']).agg(promo_elasticity_min=('promo_elasticity', 'min'), promo_elasticity_max=('promo_elasticity', 'max')).reset_index()
    return temp

min_max_df = pd.DataFrame()
for MG_NAME in MG_LIST:
    for region_name in all_regions_iter:
        min_max_df = pd.concat([min_max_df, plot_elasticity_scatter_plot(region_name = region_name, MG_NAME = MG_NAME)])

# COMMAND ----------

# MAGIC %md
# MAGIC #Min-Max DF

# COMMAND ----------

min_max_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Private Label

# COMMAND ----------

import plotly.express as px
import pandas as pd

# ALL_REGIONS = ('SHARJAH', 'DUBAI', 'AL AIN', 'ABU DHABI')
all_regions_iter = ('ALL',)
MG_LIST = ['ALL PURPOSE CLEANER', 'DISINFECTANTS', 'WASHING UP']

def plot_elasticity_scatter_plot(region_name, MG_NAME):
    if region_name == 'ALL':
        region_tuple = ALL_REGIONS
    else:
        region_tuple = (region_name,)
    
    if len(region_tuple) == 1:
        region_sql = f"('{region_tuple[0]}')"
    else:
        region_sql = str(region_tuple)
    
    query_final_elasticity = f"""
    WITH elasticities AS (
        SELECT
            t1.material_group_name,
            t1.demand_group,
            t1.region_name,
            t1.material_id,
            t2.material_name,
            ROUND(CASE WHEN t1.price_elasticity_top_3_avg < -2 THEN -2 ELSE t1.price_elasticity_top_3_avg END, 3) AS price_elasticity,
            ROUND(t1.promo_elasticity_top_3_avg, 3) AS promo_elasticity
        FROM dev.sandbox.pj_po_final_elasticities_promo_hhc_master AS t1
        JOIN gold.material.material_master AS t2 ON t1.material_id = t2.material_id
        WHERE
            t1.material_group_name = '{MG_NAME}'
            AND t1.region_name IN {region_sql}
            AND t2.brand = 'LULU PRIVATE LABEL'
    ),

    weekly_sales AS (
        SELECT
            *,
            (quantity * avg_unit_price) AS sales
        FROM dev.sandbox.pj_poc_mds_final_hhc_master
        WHERE week_number >= '2023-W31'
            AND region_name IN {region_sql}
            AND material_group_name = '{MG_NAME}'
    ),

    sales_data AS (
        SELECT
            demand_group,
            region_name,
            material_id,
            SUM(sales) AS total_sales,
            SUM(SUM(sales)) OVER () AS overall_sales,
            (total_sales / overall_sales) AS sales_contri,
            SUM(SUM(sales)/SUM(SUM(sales)) OVER()) OVER(ORDER BY SUM(sales)/SUM(SUM(sales)) OVER() DESC) AS cumulative_sales_contri
        FROM weekly_sales
        GROUP BY 1, 2, 3
    )

    SELECT
        t1.*,
        COALESCE(t2.total_sales, 0) AS sales
    FROM elasticities t1
    LEFT JOIN sales_data t2
        ON t1.demand_group = t2.demand_group
        AND t1.region_name = t2.region_name
        AND t1.material_id = t2.material_id
    ORDER BY t1.material_id, t1.region_name
    """

    df_final_elasticities = spark.sql(query_final_elasticity).toPandas()
    num_materials = df_final_elasticities.material_id.nunique()

    if region_name == 'ALL':
        df_agg = df_final_elasticities.groupby(['material_id', 'material_name']).apply(
            lambda x: pd.Series({
                'sales': x['sales'].sum(),
                'price_elasticity': (x['price_elasticity'] * x['sales']).sum() / x['sales'].sum(),
                'promo_elasticity': (x['promo_elasticity'] * x['sales']).sum() / x['sales'].sum()
            })
        ).reset_index()
    else:
        df_agg = df_final_elasticities[['material_id', 'material_name', 'price_elasticity', 'promo_elasticity', 'sales']].round(2)

    # Replace outliers with the second maximum value
    # if (MG_NAME == 'ALL PURPOSE CLEANER' and region_name in ['AL AIN', 'ABU DHABI']) \
    #     or (MG_NAME == 'WASHING UP' and region_name in ['AL AIN', 'ABU DHABI', 'DUBAI', 'SHARJAH']) \
    #         or (MG_NAME == 'DISINFECTANTS' and region_name in ['AL AIN', 'ABU DHABI', 'DUBAI', 'SHARJAH', 'ALL']):
    #     max_val = df_agg['promo_elasticity'].max()
    #     second_max_val = df_agg['promo_elasticity'][df_agg['promo_elasticity'] != max_val].max()
    #     df_agg['promo_elasticity'] = df_agg['promo_elasticity'].replace(max_val, second_max_val)
    
    fig = px.scatter(
        df_agg,
        x = 'promo_elasticity',
        y = 'price_elasticity',
        hover_data = ['material_id', 'material_name'],
        text='material_name',
        labels = {'promo_elasticity': 'Promo Elasticity', 'price_elasticity': 'Price Elasticity'},
        title = f"MG: {MG_NAME}, Price vs Promo Elasticity Numbers, {'All Regions' if region_name == 'ALL' else region_name}"
    )

    fig.update_layout(
        height = 600,
        width = 1000,
        title_x = 0.5
    )

    print(f'MG: {MG_NAME}, Region: {region_name}, Num SKUs: {num_materials}')
    fig.update_yaxes(range = [0, df_agg['price_elasticity'].min()-0.25])

    fig.add_hline(y = -1, line_width=1, line_dash="dash", line_color="red")
    if df_agg['promo_elasticity'].max() >= 0.25:
        fig.add_vline(x = 0.25, line_width=1, line_dash="dash", line_color="red")
    else:
        fig.add_vline(x = df_agg.promo_elasticity.quantile(0.75), line_width=1, line_dash="dash", line_color="red")

    fig.show()

    temp = df_agg.copy()
    temp.insert(0, 'material_group_name', MG_NAME)
    temp.insert(0, 'region_name', region_name)
    temp = temp.groupby(['material_group_name', 'region_name']).agg(promo_elasticity_min=('promo_elasticity', 'min'), promo_elasticity_max=('promo_elasticity', 'max')).reset_index()
    return temp

min_max_df = pd.DataFrame()
for MG_NAME in MG_LIST:
    for region_name in all_regions_iter:
        min_max_df = pd.concat([min_max_df, plot_elasticity_scatter_plot(region_name = region_name, MG_NAME = MG_NAME)])

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


