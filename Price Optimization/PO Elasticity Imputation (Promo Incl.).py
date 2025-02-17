# Databricks notebook source
# MAGIC %md
# MAGIC #Inputs

# COMMAND ----------

query_region_set_promo_elas = """
select
  material_group_name,
  mg_demand_group,
  collect_set(region_name) as region_set_promo_elas
from
  dev.analytics.pg_hhc_master_sku_summary
where
  g_r2_g_pe_g_pre > 0
group by
  material_group_name,
  mg_demand_group
order by
  material_group_name,
  mg_demand_group

"""
pdf_query_region_set_promo_elas = spark.sql(query_region_set_promo_elas).toPandas()

pdf_query_region_set_promo_elas.head()

# COMMAND ----------

def get_valid_region_set(df_valid_regions, mg_name, mg_demand_group):

    region_set = df_valid_regions[(df_valid_regions.material_group_name==mg_name) & (df_valid_regions.mg_demand_group==mg_demand_group)].region_set_promo_elas.values[0]

    region_list = list(region_set)

    return region_list


mg_name, mg_demand_group = 'BATH ROOM CLEANERS', 'DG-1'

valid_regions = get_valid_region_set(pdf_query_region_set_promo_elas, mg_name, mg_demand_group)

print(valid_regions)

# COMMAND ----------

material_group_name = 'DISINFECTANTS'
mg_name_abbr = 'dis'
cluster_idx = 2

# List of regions for which good SKUs are present for promo elasticity
# (This can be automated if the final result numbers from the modelling notebook are saved in a sandbox)
region_filters_promo = ['ABU DHABI', 'AL AIN', 'DUBAI', 'SHARJAH']

cluster_table_name = 'analytics.pricing.hhc_cluster_sku_info_disinfectants'

##########################################################################################

demand_group = f"hhc_{mg_name_abbr}_dg_{str(cluster_idx)}"

# COMMAND ----------

# MAGIC %md
# MAGIC #Function Initializations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist, squareform
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
pd.set_option('display.max_columns', None)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pivot DF & Merging Final Data

# COMMAND ----------

def pivot_df(df, metric):
    df = df.reset_index()

    # Melt the dataframe to convert wide format to long format
    df_melted = df.melt(id_vars='material_id', var_name='reference_material_id', value_name='value')

    # Add metric column
    df_melted['metric'] = metric

    # Reordering columns for better readability
    df_melted = df_melted[['material_id', 'reference_material_id', 'metric', 'value']].sort_values(by = ['material_id', 'reference_material_id']).reset_index(drop = True)

    return df_melted

# COMMAND ----------

def merge_data(content_price_df_norm, corr_dist_norm, cosine_dist_df_norm, df_region, region, mat_1_perc):
    # Pivot the dataframes to bring the materials from columns to rows and merging the dataframes
    content_price_df_norm_pivot = pivot_df(content_price_df_norm, 'content_price_dist')
    corr_dist_norm_pivot = pivot_df(corr_dist_norm, 'qty_correlation')
    cosine_dist_df_norm_pivot = pivot_df(cosine_dist_df_norm, 'cosine_similarity')

    merged_df = pd.concat([content_price_df_norm_pivot, corr_dist_norm_pivot, cosine_dist_df_norm_pivot], ignore_index = True)

    merged_df = pd.merge(merged_df, df_region[['material_id', 'price_elasticity', 'promo_elasticity']], left_on='reference_material_id', right_on='material_id', how='left', suffixes=('', '_del'))
    merged_df.drop(columns = 'material_id_del', inplace=True)

    merged_df = merged_df.sort_values(by = ['material_id', 'reference_material_id']).reset_index(drop = True)

    # Calculate average of the metrics for each material_id and reference_material_id pair
    temp = merged_df.groupby(['material_id', 'reference_material_id'])[['value']].mean().reset_index()
    merged_df = merged_df.merge(temp, on=['material_id', 'reference_material_id'], how='left', suffixes=('', '_avg'))
    
    # Store merged_df in a separate df to be saved in a sandbox table
    merged_df_metrics = merged_df.copy()

    # Filter for the minimum average distance calculated
    merged_df = merged_df[['material_id', 'reference_material_id', 'value_avg', 'price_elasticity', 'promo_elasticity']].drop_duplicates().reset_index(drop = True)
    merged_df_filtered = merged_df.loc[merged_df.groupby('material_id')['value_avg'].idxmin()]
    merged_df_filtered = merged_df_filtered[['material_id', 'price_elasticity', 'promo_elasticity']]

    # Take the average elasticity of the top 3 SKUs having minimum distance for each material_id
    merged_df_top_3 = merged_df.groupby('material_id').apply(lambda x: x.nsmallest(3, 'value_avg')).reset_index(drop=True)
    merged_df_top_3 = merged_df_top_3.groupby('material_id').agg(
        price_elasticity_dict=('price_elasticity', lambda x: str(dict(zip(merged_df_top_3['reference_material_id'], x)))),
        price_elasticity_avg=('price_elasticity', 'mean'),
        promo_elasticity_dict=('promo_elasticity', lambda x: str(dict(zip(merged_df_top_3['reference_material_id'], x)))),
        promo_elasticity_avg=('promo_elasticity', 'mean')
    ).reset_index()

    final_df = pd.merge(merged_df_filtered, merged_df_top_3, how='left', on='material_id')
    
    # Add the region name, demand group, and case number to identify SKU rejection case
    final_df.insert(0, 'region_name', region)
    final_df.insert(0, 'demand_group', demand_group)
    merged_df_metrics.insert(0, 'region_name', region)
    merged_df_metrics.insert(0, 'demand_group', demand_group)
    if mat_1_perc >= 0.77:
        final_df.insert(3, 'elasticity_criteria', 'imputed: case 1')
    elif mat_1_perc >= 0.36:
        final_df.insert(3, 'elasticity_criteria', 'imputed: case 2')
    else:
        final_df.insert(3, 'elasticity_criteria', 'imputed: case 3')

    return final_df, merged_df_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ##Normalization

# COMMAND ----------

def normalize_dataframe(df):
    # Find the global minimum and maximum
    min_val = df.min().min()
    max_val = df.max().max()
    
    df_normalized = (df - min_val) / (max_val - min_val)
    
    return df_normalized

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cosine Similarity

# COMMAND ----------

def cosine_sim(df_region, categorical_cols, acceptable_flag = 'acceptable_model_flag'):
    # Calculate the cosine similarity of the categorical attributes

    catg_df = df_region[['material_id', acceptable_flag] + categorical_cols]

    # Drop columns having only 1 distinct value
    catg_df = catg_df.loc[:, (catg_df != catg_df.iloc[0]).any()]
    
    common_cols = list(set(categorical_cols) & set(catg_df.columns))

    # If acceptable_model_flag gets dropped, then re-include it
    if acceptable_flag not in catg_df.columns:
        catg_df = catg_df.merge(df_region[['material_id', acceptable_flag]], on = 'material_id', how = 'left')

    # Dummification of attribute columns
    catg_df_encoded = pd.get_dummies(catg_df, columns=common_cols)

    catg_df_encoded_0 = catg_df_encoded[catg_df_encoded[acceptable_flag] == 0]
    catg_df_encoded_1 = catg_df_encoded[catg_df_encoded[acceptable_flag] == 1]

    X_0 = catg_df_encoded_0.drop(columns = ['material_id', acceptable_flag])
    X_1 = catg_df_encoded_1.drop(columns = ['material_id', acceptable_flag])

    # Compute the cosine similarity between each row of df1 and each row of df2
    df1_array = X_0.values
    df2_array = X_1.values
    cosine_sim_matrix = cosine_similarity(df1_array, df2_array)
    cosine_sim_df = pd.DataFrame(cosine_sim_matrix, index=catg_df_encoded_0['material_id'], columns=catg_df_encoded_1['material_id'])

    # Subtract the cosine similarity values from 1 so that we can convert it into a distance metric
    cosine_dist_df = cosine_sim_df.copy()
    cosine_dist_df.iloc[:, :] = 1 - cosine_dist_df.iloc[:, :]

    # Normalize the distance values
    cosine_dist_df_norm = normalize_dataframe(cosine_dist_df)

    return cosine_dist_df_norm

# COMMAND ----------

# MAGIC %md
# MAGIC ##Content & Price Distance

# COMMAND ----------

def content_price_dist(attributes_df, region, acceptable_flag = 'acceptable_model_flag'):
    # Create ordinal values of content column

    df_region = attributes_df[attributes_df['region_name'] == region].reset_index(drop = True)
    df_region.drop(columns = 'region_name', inplace = True)

    def create_ordinal(df, column_name):
        # Get distinct values and assign ordinal values
        unique_vals = df[column_name].unique()
        ordinal_mapping = {val: idx + 1 for idx, val in enumerate(sorted(unique_vals))}
        
        # Set ordinal values in the df
        df[column_name + '_ordinal'] = df[column_name].map(ordinal_mapping)

        return df

    df_region = create_ordinal(df_region, 'content')


    # Create ordinal values of price column
    query = f"""
    SELECT
        CAST(material_id AS INT) AS material_id,
        material_group_name
    FROM {cluster_table_name}
    WHERE
        material_group_name = '{material_group_name}'
        AND cluster_idx = {str(cluster_idx)}
        AND weight_scheme = 'w1'
    """
    df_sku_attrs_base = spark.sql(query).toPandas()

    df_region['avg_unit_price_int'] = round(df_region['avg_unit_price'])
    df_sku_attrs_base = df_sku_attrs_base.merge(df_region[['material_id', 'avg_unit_price_int']], on='material_id', how='left')

    def create_ordinal_category(group):
        col = 'avg_unit_price_int'
        unique_values = group[col].nunique()
        if unique_values == 1:
            group[f'{col}_ordinal'] = 1
        else:
            try:
                group[f'{col}_ordinal'] = pd.qcut(group[col], q=min(unique_values, 4), labels=False, duplicates='drop') + 1
            except ValueError:
                group[f'{col}_ordinal'] = group[col].rank(method='dense', ascending=True).astype(int)
                group[f'{col}_ordinal'] = pd.cut(group[f'{col}_ordinal'], bins=min(4, unique_values), labels=False) + 1
        return group
    
    df_sku_attrs_base_trans = df_sku_attrs_base.groupby('material_group_name', group_keys=True).apply(create_ordinal_category)
    df_sku_attrs_base_trans = df_sku_attrs_base_trans.reset_index(drop=True)
    
    df_region = df_region.merge(df_sku_attrs_base_trans[['material_id', 'avg_unit_price_int_ordinal']], on='material_id', how='left')

    # Calculate euclidean distance
    def euclidean_distance(df, feature_column):
        df_0 = df[df[acceptable_flag] == 0].set_index('material_id')
        df_1 = df[df[acceptable_flag] == 1].set_index('material_id')
        
        distances = cdist(df_0[[feature_column]], df_1[[feature_column]], metric='euclidean')
        
        distance_df = pd.DataFrame(distances, index=df_0.index, columns=df_1.index)
        
        return distance_df

    content_distance_df = euclidean_distance(df_region, 'content_ordinal')
    price_distance_df = euclidean_distance(df_region, 'avg_unit_price_int_ordinal')

    # Calculate weighted average of content and price distance
    weight_content = 0.2
    weight_price = 0.8
    content_price_df = (content_distance_df * weight_content + price_distance_df * weight_price) / 2

    # Normalize the distance values
    content_price_df_norm = normalize_dataframe(content_price_df)

    return df_region, content_price_df_norm

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quantity Correlation

# COMMAND ----------

def quantity_corr(qty_df, region, acceptable_flag = 'acceptable_model_flag'):
    df_region = qty_df[qty_df['region_name'] == region].reset_index(drop = True)

    materials_0 = df_region[df_region[acceptable_flag] == 0]['material_id'].unique()
    materials_1 = df_region[df_region[acceptable_flag] == 1]['material_id'].unique()

    corr_matrix = pd.DataFrame(index=materials_0, columns=materials_1)
    for mat0 in materials_0:
        for mat1 in materials_1:
            # Ensure that only intersecting weeks are used
            df_mat_0 = df_region[df_region['material_id'] == mat0][['material_id', 'week_number', 'quantity']].reset_index(drop = True)
            df_mat_1 = df_region[df_region['material_id'] == mat1][['material_id', 'week_number', 'quantity']].reset_index(drop = True)
            df_full = pd.merge(df_mat_0, df_mat_1, on = 'week_number', how = 'inner', suffixes = ('_mat_0', '_mat_1'))

            if df_full.empty:
                corr_matrix.loc[mat0, mat1] = np.nan
            
            else:
                # Extract quantity columns and set week_number as the index
                qty_mat0 = df_full[df_full['material_id_mat_0'] == mat0][['week_number', 'quantity_mat_0']].set_index('week_number')
                qty_mat1 = df_full[df_full['material_id_mat_1'] == mat1][['week_number', 'quantity_mat_1']].set_index('week_number')
                aligned = qty_mat0.join(qty_mat1)

                # Store correlation in a dataframe
                corr_matrix.loc[mat0, mat1] = aligned['quantity_mat_0'].corr(aligned['quantity_mat_1'])
    
    # Subtract the correlation values from 1 so that we can convert it into a distance metric
    corr_dist = corr_matrix.copy()
    corr_dist.iloc[:, :] = 1 - corr_dist.iloc[:, :]
    corr_dist.index.name = 'material_id'
    
    # Normalize the distance values
    corr_dist_norm = normalize_dataframe(corr_dist)

    return corr_dist_norm

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Data

# COMMAND ----------

def read_data():
    file_name = 'hhc_cluster_sku_info_final_master.csv'
    file_path = f'/Workspace/Repos/piyush@loyalytics.in/lulu_notebooks/temp_data_files/{file_name}'
    df_sku_cluster_info = pd.read_csv(file_path)
    spark.createDataFrame(df_sku_cluster_info).createOrReplaceTempView("sku_cluster_view")

    query = f"""
    WITH attributes AS (
        SELECT
            t1.region_name,
            CAST(t1.material_id AS INT) AS material_id,
            t1.price_elasticity,
            t1.promo_elasticity,
            t2.content,
            t2.brand,
            t2.pack_type,
            t2.regular_pack_flag,
            t2.strength,
            t2.target_surface,
            t2.environment,
            t2.form,
            CASE WHEN t1.r2 >= 0.6 AND t1.price_elasticity < 0 THEN 1 ELSE 0 END AS acceptable_model_flag,
            CASE WHEN t1.r2 >= 0.6 AND t1.price_elasticity < 0 AND t1.promo_elasticity > 0 THEN 1 ELSE 0 END AS acceptable_model_flag_promo
        FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master_best_model AS t1
        JOIN sku_cluster_view AS t2 ON t1.material_id = t2.material_id
        WHERE
            t1.demand_group = '{demand_group}'
            AND t1.model = t1.best_model
            AND t2.weight_scheme = 'w1'
    ),

    sales AS (
        SELECT
            region_name,
            CAST(material_id AS INT) AS material_id,
            week_number,
            quantity,
            (avg_unit_price * quantity) AS sales
        FROM dev.sandbox.pj_poc_mds_final_hhc_{mg_name_abbr}_dg{str(cluster_idx)}
    ),

    unit_price AS (
        SELECT
            region_name,
            material_id,
            ROUND(SUM(sales)/ SUM(quantity), 2) AS avg_unit_price
        FROM sales
        GROUP BY 1, 2
    )

    SELECT
        t1.*,
        t2.avg_unit_price
    FROM attributes AS t1
    JOIN unit_price AS t2
        ON t1.region_name = t2.region_name
        AND t1.material_id = t2.material_id
    ORDER BY t1.region_name, t1.acceptable_model_flag, t1.material_id
    """
    attributes_df = spark.sql(query).toPandas()

    query = f"""
    SELECT
        t1.region_name,
        CAST(t1.material_id AS INT) AS material_id,
        t1.week_number,
        t1.quantity,
        CASE WHEN t2.r2 >= 0.6 AND t2.price_elasticity < 0 THEN 1 ELSE 0 END AS acceptable_model_flag,
        CASE WHEN t2.r2 >= 0.6 AND t2.price_elasticity < 0 AND t2.promo_elasticity > 0 THEN 1 ELSE 0 END AS acceptable_model_flag_promo
    FROM dev.sandbox.pj_poc_mds_final_hhc_{mg_name_abbr}_dg{str(cluster_idx)} AS t1
    JOIN dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master_best_model AS t2
        ON t1.region_name = t2.region_name
        AND t1.material_id = t2.material_id
    WHERE t2.model = t2.best_model
    ORDER BY 1, 2, 3
    """
    qty_df = spark.sql(query).toPandas()

    return attributes_df, qty_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Price Elasticity Imputation

# COMMAND ----------

attributes_df, qty_df = read_data()

# COMMAND ----------

attributes_df.display()

# COMMAND ----------

qty_df.display()

# COMMAND ----------

final_df_all_regions = pd.DataFrame()
merged_df_metrics_all_regions = pd.DataFrame()

regions = qty_df[qty_df['acceptable_model_flag'] == 0]['region_name'].unique()
for region in regions:
    mat_1_count = qty_df[(qty_df['acceptable_model_flag'] == 1) & (qty_df['region_name'] == region)]['material_id'].nunique()
    total_mats = qty_df[qty_df['region_name'] == region]['material_id'].nunique()
    mat_1_perc = mat_1_count / total_mats

    # Extract the quantity correlation for one region at a time
    corr_dist_norm = quantity_corr(qty_df, region)

    # Calculate the content and price distance
    df_region, content_price_df_norm = content_price_dist(attributes_df, region)

    # Calculate the cosine similarity of the categorical attributes
    categorical_cols = ['brand', 'pack_type', 'regular_pack_flag', 'strength', 'target_surface', 'environment', 'form']
    cosine_dist_df_norm = cosine_sim(df_region, categorical_cols)

    # Merge the dataframes and calculate the elasticity for the closest SKU
    final_df, merged_df_metrics = merge_data(content_price_df_norm, corr_dist_norm, cosine_dist_df_norm, df_region, region, mat_1_perc)

    # Concatenate the df with the final dataframe to store data for all regions
    final_df_all_regions = pd.concat([final_df_all_regions, final_df])
    merged_df_metrics_all_regions = pd.concat([merged_df_metrics_all_regions, merged_df_metrics])

# COMMAND ----------

final_df_all_regions.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Promo Elasticity Imputation

# COMMAND ----------

final_df_all_regions_promo = pd.DataFrame()
merged_df_metrics_all_regions_promo = pd.DataFrame()

regions = qty_df[qty_df['acceptable_model_flag_promo'] == 0]['region_name'].unique()
for region in regions:
    if region in region_filters:
        mat_1_count = qty_df[(qty_df['acceptable_model_flag'] == 1) & (qty_df['region_name'] == region)]['material_id'].nunique()
        total_mats = qty_df[qty_df['region_name'] == region]['material_id'].nunique()
        mat_1_perc = mat_1_count / total_mats

        # Extract the quantity correlation for one region at a time
        corr_dist_norm = quantity_corr(qty_df, region, 'acceptable_model_flag_promo')

        # Calculate the content and price distance
        df_region, content_price_df_norm = content_price_dist(attributes_df, region, 'acceptable_model_flag_promo')

        # Calculate the cosine similarity of the categorical attributes
        categorical_cols = ['brand', 'pack_type', 'regular_pack_flag', 'strength', 'target_surface', 'environment', 'form']
        cosine_dist_df_norm = cosine_sim(df_region, categorical_cols, 'acceptable_model_flag_promo')

        # Merge the dataframes and calculate the elasticity for the closest SKU
        final_df, merged_df_metrics = merge_data(content_price_df_norm, corr_dist_norm, cosine_dist_df_norm, df_region, region, mat_1_perc)

        # Concatenate the df with the final dataframe to store data for all regions
        final_df_all_regions_promo = pd.concat([final_df_all_regions_promo, final_df])
        merged_df_metrics_all_regions_promo = pd.concat([merged_df_metrics_all_regions_promo, merged_df_metrics])

# COMMAND ----------

final_df_all_regions_promo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Save to Sandbox

# COMMAND ----------

final_df_all_regions = final_df_all_regions[['demand_group', 'region_name', 'material_id', 'elasticity_criteria', 'price_elasticity', 'price_elasticity_dict', 'price_elasticity_avg']]
final_df_all_regions_promo = final_df_all_regions_promo[['demand_group', 'region_name', 'material_id', 'elasticity_criteria', 'promo_elasticity', 'promo_elasticity_dict', 'promo_elasticity_avg']]
final_df_all_regions_master = final_df_all_regions_promo.merge(final_df_all_regions, on = ['demand_group', 'region_name', 'material_id', 'elasticity_criteria'], how = 'outer')

merged_df_metrics_all_regions.drop(columns = 'promo_elasticity', inplace = True)
merged_df_metrics_all_regions_promo.drop(columns = 'price_elasticity', inplace = True)
merged_df_metrics_all_regions_master = merged_df_metrics_all_regions_promo.merge(merged_df_metrics_all_regions, on = ['demand_group', 'region_name', 'material_id', 'reference_material_id', 'metric', 'value', 'value_avg'], how = 'outer')

# COMMAND ----------

spark_df = spark.createDataFrame(final_df_all_regions_master)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_imputed_promo_hhc_{mg_name_abbr}_dg{str(cluster_idx)}")

spark_df = spark.createDataFrame(merged_df_metrics_all_regions_master)
spark_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_imputed_metrics_promo_hhc_{mg_name_abbr}_dg{str(cluster_idx)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Master Sandboxes
# MAGIC Run the below cells only after the notebook has been executed for all demand groups of the given material group.
# MAGIC <br><br>
# MAGIC Each DG sub-table in the below two cells has to be manually written for as many DGs present in the material group.

# COMMAND ----------

# query = f"""
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_imputed_promo_master_hhc_{mg_name_abbr} AS (
#     SELECT * FROM dev.sandbox.pj_po_imputed_promo_hhc_{mg_name_abbr}_dg1
#     UNION
#     SELECT * FROM dev.sandbox.pj_po_imputed_promo_hhc_{mg_name_abbr}_dg2
# )
# """

# spark.sql(query)

# COMMAND ----------

# query = f"""
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_imputed_metrics_promo_master_hhc_{mg_name_abbr} AS (
#     SELECT * FROM dev.sandbox.pj_po_imputed_metrics_promo_hhc_{mg_name_abbr}_dg1
#     UNION
#     SELECT * FROM dev.sandbox.pj_po_imputed_metrics_promo_hhc_{mg_name_abbr}_dg2
# )
# """

# spark.sql(query)

# COMMAND ----------

# query = f"""
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_final_elasticities_promo_hhc_{mg_name_abbr} AS (
#     WITH model_results_price AS (
#         SELECT
#             demand_group,
#             region_name,
#             material_id,
#             "accepted set" AS elasticity_criteria,
#             price_elasticity AS price_elasticity_top_1,
#             price_elasticity AS price_elasticity_top_3_avg
#         FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master_best_model
#         WHERE
#             model = best_model
#             AND r2 >= 0.6
#             AND price_elasticity < 0
#     ),

#     imputed_results_price AS (
#         SELECT
#             demand_group,
#             region_name,
#             material_id,
#             elasticity_criteria,
#             price_elasticity AS price_elasticity_top_1,
#             price_elasticity_avg AS price_elasticity_top_3_avg
#         FROM dev.sandbox.pj_po_imputed_promo_master_hhc_{mg_name_abbr}
#         WHERE price_elasticity_avg IS NOT NULL
#     ),

#     combined_price AS (
#         SELECT * FROM model_results_price
#         UNION
#         SELECT * FROM imputed_results_price
#     ),

#     model_results_promo AS (
#         SELECT
#             demand_group,
#             region_name,
#             material_id,
#             "accepted set" AS elasticity_criteria,
#             promo_elasticity AS promo_elasticity_top_1,
#             promo_elasticity AS promo_elasticity_top_3_avg
#         FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master_best_model
#         WHERE
#             model = best_model
#             AND r2 >= 0.6
#             AND price_elasticity < 0
#             AND promo_elasticity > 0
#     ),

#     imputed_results_promo AS (
#         SELECT
#             demand_group,
#             region_name,
#             material_id,
#             elasticity_criteria,
#             promo_elasticity AS promo_elasticity_top_1,
#             promo_elasticity_avg AS promo_elasticity_top_3_avg
#         FROM dev.sandbox.pj_po_imputed_promo_master_hhc_{mg_name_abbr}
#         WHERE promo_elasticity_avg IS NOT NULL
#     ),

#     combined_promo AS (
#         SELECT * FROM model_results_promo
#         UNION
#         SELECT * FROM imputed_results_promo
#     )

#     SELECT
#         t1.*,
#         COALESCE(t2.promo_elasticity_top_1, AVG(t2.promo_elasticity_top_1) OVER (PARTITION BY t1.demand_group)) AS promo_elasticity_top_1,
#         COALESCE(t2.promo_elasticity_top_3_avg, AVG(t2.promo_elasticity_top_3_avg) OVER (PARTITION BY t1.demand_group)) AS promo_elasticity_top_3_avg
#     FROM combined_price AS t1
#     LEFT JOIN combined_promo AS t2
#         ON t1.region_name = t2.region_name
#         AND t1.material_id = t2.material_id
#     ORDER BY demand_group, region_name, elasticity_criteria
# )
# """

# spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #Final Elasticities Sandbox (All MGs)
# MAGIC Combining final elasticities for all the material groups into one sandbox.
# MAGIC <br><br>
# MAGIC Run the below cells only after the above cell for final elasticities has been executed for all material groups.
# MAGIC <br><br>
# MAGIC Each MG sub-table in the below cell has to be manually written for as many material groups whose elasticities are calculated.

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_final_elasticities_promo_hhc_master AS (
#     SELECT "ALL PURPOSE CLEANER" AS material_group_name, * FROM dev.sandbox.pj_po_final_elasticities_promo_hhc_apc
#     UNION
#     SELECT "WASHING UP" AS material_group_name, * FROM dev.sandbox.pj_po_final_elasticities_promo_hhc_wup
#     UNION
#     SELECT "DISINFECTANTS" AS material_group_name, * FROM dev.sandbox.pj_po_final_elasticities_promo_hhc_dis
# )
