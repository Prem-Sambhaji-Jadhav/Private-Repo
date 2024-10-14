# Databricks notebook source
# MAGIC %md
# MAGIC #Inputs

# COMMAND ----------

mg_name = 'DISINFECTANTS'
mg_name_abbr = 'dis'
cluster_idx = 2


###############################################

drop_event_idx = False
using_monthly_data = False
using_promo_data = True

demand_group = f"hhc_{mg_name_abbr}_dg_{str(cluster_idx)}"

# COMMAND ----------

# MAGIC %md
# MAGIC #Function Initializations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Libraries

# COMMAND ----------

# pip install xgboost lightgbm catboost optuna

# COMMAND ----------

import numpy as np
import pandas as pd
import ast
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
import optuna
from sklearn.inspection import partial_dependence
from sklearn.preprocessing import MinMaxScaler
pd.set_option('display.max_columns', None)

from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor

import sys
import os
stderr = sys.stderr
sys.stderr = open(os.devnull, 'w')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Weekly Aggregation

# COMMAND ----------

def weekly_aggr(df_aggr, df_price_comp, df_price_ratio_all):
    # Calculate sales
    df_aggr['sales'] = df_aggr['quantity'] * df_aggr['avg_unit_price']

    # Aggregate all possible columns
    df_aggr_new = df_aggr.groupby(['demand_group', 'region_name', 'week_number']).agg({'quantity': 'sum', 'sales': 'sum', 'has_holidays': 'mean', 'price_inc_flag': 'mean', 'discount_amount': 'sum', 'avg_temp_norm': 'mean', 'total_rainfall_norm': 'mean', 'seasonality_idx': 'mean', 'new_launch_idx': 'mean'}).reset_index()

    # Calculate avg_unit_unit_price
    df_aggr_new['avg_unit_price'] = round(df_aggr_new['sales'] / df_aggr_new['quantity'], 2)

    # Calculate lag quantities
    df_aggr_new['lag_quantity_1w'] = df_aggr_new.groupby('region_name')['quantity'].shift(1)
    df_aggr_new['lag_quantity_2w'] = df_aggr_new.groupby('region_name')['quantity'].shift(2)
    df_aggr_new['lag_quantity_1w'] = df_aggr_new.groupby('region_name', group_keys=False)['lag_quantity_1w'].apply(lambda x: x.fillna(x.mean()))
    df_aggr_new['lag_quantity_2w'] = df_aggr_new.groupby('region_name', group_keys=False)['lag_quantity_2w'].apply(lambda x: x.fillna(x.mean()))

    # Calculate price ratio (moving average) for 15 days and 30 days
    df_aggr_new['avg_unit_price_ma_15d'] = round(df_aggr_new.groupby('region_name')['avg_unit_price'].transform(lambda x: x.rolling(2, min_periods = 2).mean()), 2)
    df_aggr_new['avg_unit_price_ma_30d'] = round(df_aggr_new.groupby('region_name')['avg_unit_price'].transform(lambda x: x.rolling(4, min_periods = 4).mean()), 2)
    df_aggr_new['price_ratio_ma_15d'] = round(df_aggr_new['avg_unit_price'] / df_aggr_new['avg_unit_price_ma_15d'], 2)
    df_aggr_new['price_ratio_ma_30d'] = round(df_aggr_new['avg_unit_price'] / df_aggr_new['avg_unit_price_ma_30d'], 2)
    df_aggr_new[['price_ratio_ma_15d', 'price_ratio_ma_30d']] = df_aggr_new[['price_ratio_ma_15d', 'price_ratio_ma_30d']].fillna(1)
    df_aggr_new = df_aggr_new.drop(columns = ['avg_unit_price_ma_15d', 'avg_unit_price_ma_30d'])

    # Calculate price ratio with individual materials
    temp = df_price_ratio_all[['region_name', 'week_number', 'material_proxy_identifier', 'avg_unit_price']].sort_values(by = ['region_name', 'week_number', 'material_proxy_identifier']).reset_index(drop = True)
    materials = temp['material_proxy_identifier'].unique()
    df_price_ratio = df_aggr_new[['region_name', 'week_number', 'avg_unit_price']].copy()

    for material in materials:
        df_merged = df_aggr_new[['region_name', 'week_number', 'avg_unit_price']].merge(temp[temp['material_proxy_identifier'] == material], on=['region_name', 'week_number'], suffixes=('', f'_{material}'))

        df_price_ratio = df_price_ratio.merge(df_merged[['region_name', 'week_number', f'avg_unit_price_{material}']], on = ['region_name', 'week_number'], how = 'left')
        df_price_ratio[f'price_ratio_{material}'] = df_price_ratio['avg_unit_price'] / df_price_ratio[f'avg_unit_price_{material}']

    price_ratio_cols = ['price_ratio_' + item for item in materials]
    price_ratio_cols = sorted(price_ratio_cols, key=lambda x: int(x.split('_')[-1]))
    df_price_ratio = df_price_ratio[['region_name', 'week_number'] + price_ratio_cols]
    df_aggr_new = df_aggr_new.merge(df_price_ratio, on = ['region_name', 'week_number'], how = 'left')
    for col in price_ratio_cols:
        df_aggr_new[col].fillna(df_aggr_new[col].mean(), inplace = True)

    # Calculate price comp ratio
    df_aggr_new = df_aggr_new.merge(df_price_comp, on = ['region_name', 'week_number'], how = 'left')
    df_aggr_new['price_comp'] = (df_aggr_new['total_sales'] - df_aggr_new['sales']) / (df_aggr_new['total_quantity'] - df_aggr_new['quantity'])
    df_aggr_new['price_ratio_comp'] = round(df_aggr_new['avg_unit_price'] / df_aggr_new['price_comp'], 3)
    df_aggr_new = df_aggr_new.drop(columns = ['total_sales', 'total_quantity', 'price_comp'])

    # Aggregate event columns
    event_cols = [column for column in df_aggr.columns if column.startswith('ev_')]
    if len(event_cols) > 0:
        temp = df_aggr.groupby(['region_name', 'week_number'])[event_cols].mean().reset_index()
        df_aggr_new = df_aggr_new.merge(temp, on = ['region_name', 'week_number'], how = 'left')
    else:
        temp = df_aggr.groupby(['region_name', 'week_number'])['event_presence_idx'].mean().reset_index()
        df_aggr_new = df_aggr_new.merge(temp, on = ['region_name', 'week_number'], how = 'left')

    # Extract year and week_number_val columns
    df_aggr_new[['year', 'week_number_val']] = df_aggr_new['week_number'].str.split('-', expand=True)

    # Store all the material id and proxy identifiers for a given region and week pair in a column
    df_grouped = df_aggr.groupby(['region_name', 'week_number'])['material_proxy_identifier'].apply(lambda x: ', '.join(x)).reset_index()
    df_aggr_new = df_aggr_new.merge(df_grouped, on = ['region_name', 'week_number'], how = 'left')
    df_grouped = df_aggr.groupby(['region_name', 'week_number'])['material_id'].apply(lambda x: ', '.join(x)).reset_index()
    df_aggr_new = df_aggr_new.merge(df_grouped, on = ['region_name', 'week_number'], how = 'left')
    df_aggr_new.rename(columns = {'material_id': 'material_ids', 'material_proxy_identifier': 'material_proxy_identifiers'}, inplace = True)

    # Calculate self numeric distribution and the ratio with all other products' numeric distribution
    temp = df_aggr.merge(df_aggr_new[['region_name', 'week_number', 'material_proxy_identifiers']], on = ['region_name', 'week_number'], how = 'left')
    numeric_dist_cols = [column for column in temp.columns if column.startswith('numeric_dist_')]
    temp = temp[['region_name', 'week_number', 'material_proxy_identifiers'] + numeric_dist_cols].drop_duplicates().reset_index(drop = True)

    numeric_dist = []
    numeric_dist_comp = []
    for i in range(len(temp)):
        ids = temp.loc[i, 'material_proxy_identifiers']
        ids = ids.split(', ')
        ids = ['numeric_dist_' + id for id in ids]
        comp_dists = [item for item in numeric_dist_cols if item not in ids]
        numeric_dist.append(temp.loc[i, ids].mean())
        numeric_dist_comp.append(temp.loc[i, comp_dists].mean())
    temp['numeric_dist'] = numeric_dist
    temp['numeric_dist_comp'] = numeric_dist_comp
    temp['numeric_dist_ratio'] = round(temp['numeric_dist'] / temp['numeric_dist_comp'], 3)
    temp = temp[['region_name', 'week_number', 'numeric_dist', 'numeric_dist_ratio']]
    df_aggr_new = df_aggr_new.merge(temp, on = ['region_name', 'week_number'], how = 'left')

    # Round off columns
    round_zero_cols = ['has_holidays', 'price_inc_flag']
    df_aggr_new[round_zero_cols] = df_aggr_new[round_zero_cols].round(0)
    round_two_cols = ['lag_quantity_1w', 'lag_quantity_2w', 'price_ratio_ma_15d', 'price_ratio_ma_30d', 'discount_amount']
    df_aggr_new[round_two_cols] = df_aggr_new[round_two_cols].round(2)
    round_three_cols = price_ratio_cols + ['new_launch_idx', 'avg_temp_norm', 'total_rainfall_norm', 'seasonality_idx', 'numeric_dist'] + event_cols
    df_aggr_new[round_three_cols] = df_aggr_new[round_three_cols].round(3)

    # Drop sales column
    df_aggr_new = df_aggr_new.drop(columns = 'sales')

    return df_aggr_new

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales Metrics Calculation

# COMMAND ----------

def final_metrics(table_name, df, demand_group):
    query = f"""
    SELECT
        t1.product_id AS material_id,
        t2.region_name,
        ROUND(SUM(amount) / SUM(quantity), 2) AS avg_unit_price_l12_mds
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.store.store_master AS t2 ON t1.store_id = t2.store_id
    WHERE
        t1.business_day BETWEEN "2023-08-01" AND "2024-07-31"
        AND product_id IN (SELECT DISTINCT material_id AS product_id
                            FROM {table_name})
        AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2
    """
    avg_price_df_jul = spark.sql(query).toPandas()

    query = f"""
    SELECT
        t1.product_id AS material_id,
        t2.region_name,
        ROUND(SUM(amount) / SUM(quantity), 2) AS avg_unit_price_l12
    FROM gold.transaction.uae_pos_transactions AS t1
    JOIN gold.store.store_master AS t2 ON t1.store_id = t2.store_id
    WHERE
        t1.business_day BETWEEN "2023-09-01" AND "2024-08-31"
        AND product_id IN (SELECT DISTINCT material_id AS product_id
                            FROM {table_name})
        AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
        AND t1.amount > 0
        AND t1.quantity > 0
    GROUP BY 1, 2
    """
    avg_price_df_aug = spark.sql(query).toPandas()

    query = f"""
    WITH all_quantity AS (
        SELECT
            t1.product_id AS material_id,
            t2.region_name,
            CONCAT(YEAR(t1.business_day), '-W', LPAD(WEEKOFYEAR(t1.business_day), 2, '0')) as week_number,
            SUM(t1.quantity) AS qty
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.store.store_master AS t2 ON t1.store_id = t2.store_id
        WHERE
            t1.business_day BETWEEN "2023-09-01" AND "2024-08-31"
            AND product_id IN (SELECT DISTINCT material_id AS product_id
                                FROM {table_name})
            AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
            AND t1.amount > 0
            AND t1.quantity > 0
        GROUP BY 1, 2, 3
    )

    SELECT
        material_id,
        region_name,
        ROUND(AVG(qty), 2) AS avg_weekly_qty_l12
    FROM all_quantity
    GROUP BY 1, 2
    """
    avg_weekly_qty_df_aug = spark.sql(query).toPandas()
    
    final_metrics_df = df[['material_id', 'region_name', 'material_name', 'material_proxy_identifier']].drop_duplicates().reset_index(drop = True)
    
    final_metrics_df['demand_group'] = demand_group
    
    final_metrics_df = pd.merge(final_metrics_df, avg_price_df_jul, on = ['region_name', 'material_id'], how = 'left')
    final_metrics_df = pd.merge(final_metrics_df, avg_price_df_aug, on = ['region_name', 'material_id'], how = 'left')
    final_metrics_df = pd.merge(final_metrics_df, avg_weekly_qty_df_aug, on = ['region_name', 'material_id'], how = 'left')
    
    temp_weekly = df.copy()
    temp_weekly['sales'] = temp_weekly['avg_unit_price'] * temp_weekly['quantity']
    
    if using_monthly_data:
        l12_start = '2023-M08'
        text_val = 'monthly'
    else:
        l12_start = '2023-W31'
        text_val = 'weekly'

    del_df = temp_weekly[temp_weekly[period_val] >= l12_start].groupby(['material_id', 'region_name'])['quantity'].mean().reset_index()
    del_df.rename(columns = {'quantity': f'avg_{text_val}_qty_l12_mds'}, inplace = True)
    temp_weekly = pd.merge(temp_weekly, del_df, on = ['material_id', 'region_name'], how = 'left')
    
    temp_overall = temp_weekly[temp_weekly[period_val] >= l12_start].groupby(['region_name', 'material_id'])[['sales', 'discount_amount']].mean().reset_index()
    temp_overall.rename(columns = {'sales': f'avg_{text_val}_sales_l12_mds', 'discount_amount': f'avg_{text_val}_discount_l12_mds'}, inplace = True)
    temp_overall = pd.merge(temp_overall, temp_weekly[['material_id', 'region_name', f'avg_{text_val}_qty_l12_mds']].drop_duplicates(), on = ['material_id', 'region_name'], how = 'left')
    
    final_metrics_df = pd.merge(final_metrics_df, temp_overall, on = ['material_id', 'region_name'], how = 'left')
    
    temp_disc_overall = temp_weekly[temp_weekly[period_val] >= l12_start].groupby(['region_name', 'material_id']).agg({'quantity': 'sum', 'discount_amount': 'sum', period_val: 'count'}).reset_index()
    temp_disc_overall['avg_unit_discount_l12_mds_overall'] = temp_disc_overall['discount_amount'] / temp_disc_overall['quantity']

    temp_disc_conditional = temp_weekly[(temp_weekly[period_val] >= l12_start) & (temp_weekly['discount_amount'] != 0)].groupby(['region_name', 'material_id'])[['discount_amount', 'quantity']].sum().reset_index()
    temp_disc_conditional['avg_unit_discount_l12_mds_conditional'] = temp_disc_conditional['discount_amount'] / temp_disc_conditional['quantity']
    
    final_metrics_df = pd.merge(final_metrics_df, temp_disc_overall[['region_name', 'material_id', 'avg_unit_discount_l12_mds_overall']], on = ['material_id', 'region_name'], how = 'left')

    final_metrics_df = pd.merge(final_metrics_df, temp_disc_conditional[['region_name', 'material_id', 'avg_unit_discount_l12_mds_conditional']], on = ['material_id', 'region_name'], how = 'left')
    
    final_metrics_df['avg_unit_discount_l12_mds_conditional'] = final_metrics_df['avg_unit_discount_l12_mds_conditional'].fillna(0)

    query = f"""
    WITH all_quantity AS (
        SELECT
            t1.product_id AS material_id,
            t2.region_name,
            MONTH(t1.business_day) AS month,
            SUM(t1.quantity) AS qty
        FROM gold.transaction.uae_pos_transactions AS t1
        JOIN gold.store.store_master AS t2 ON t1.store_id = t2.store_id
        WHERE
            t1.business_day BETWEEN "2023-09-01" AND "2024-08-31"
            AND product_id IN (SELECT DISTINCT material_id AS product_id
                                FROM {table_name})
            AND t1.transaction_type IN ("SALE", "SELL_MEDIA")
            AND t1.amount > 0
            AND t1.quantity > 0
        GROUP BY 1, 2, 3
    )

    SELECT
        material_id,
        region_name,
        ROUND(AVG(qty), 2) AS avg_monthly_qty_l12
    FROM all_quantity
    GROUP BY 1, 2
    """
    avg_monthly_qty_df_aug = spark.sql(query).toPandas()

    # Count of Months = Count of Weeks / 4.345
    if period_val == 'week_number':
        multiplier = 4.345
    else:
        multiplier = 1
    temp_disc_overall['avg_monthly_qty_l12_mds'] = temp_disc_overall['quantity'] * multiplier / temp_disc_overall[period_val]

    final_metrics_df = pd.merge(final_metrics_df, avg_monthly_qty_df_aug, on = ['region_name', 'material_id'], how = 'left')
    final_metrics_df = pd.merge(final_metrics_df, temp_disc_overall[['region_name', 'material_id', 'avg_monthly_qty_l12_mds']], on = ['region_name', 'material_id'], how = 'left')
    
    cols = [f'avg_{text_val}_sales_l12_mds', 'avg_monthly_qty_l12_mds', 'avg_monthly_qty_l12']
    final_metrics_df[cols] = round(final_metrics_df[cols])
    cols = [f'avg_{text_val}_qty_l12_mds']
    final_metrics_df[cols] = round(final_metrics_df[cols], 2)
    cols = [f'avg_{text_val}_discount_l12_mds', 'avg_unit_discount_l12_mds_overall', 'avg_unit_discount_l12_mds_conditional']
    final_metrics_df[cols] = round(final_metrics_df[cols], 4)

    return final_metrics_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Model Initialization & Hyperparameters

# COMMAND ----------

def get_model(name, params):
    if name == 'RandomForestRegressor':
        model = RandomForestRegressor(random_state = 42, **params)
    elif name == 'AdaBoostRegressor':
        model = AdaBoostRegressor(random_state = 42, **params)
    elif name == 'LGBMRegressor':
        model = LGBMRegressor(verbosity = -1, silent = True, random_state = 42, **params)
    elif name == 'GradientBoostingRegressor':
        model = GradientBoostingRegressor(random_state = 42, **params)
    elif name == 'CatBoostRegressor':
        model = CatBoostRegressor(verbose = 0, random_state = 42, **params)
    elif name == 'XGBRegressor':
        model = XGBRegressor(random_state = 42, **params)
    elif name == 'LinearRegression' or name == 'Log-LogLinearRegression':
        model = LinearRegression(**params)
    elif name == 'LassoRegression':
        model = Lasso(**params)
    elif name == 'RidgeRegression':
        model = Ridge(**params)
    elif name == 'ElasticNetRegression':
        model = ElasticNet(**params)
    else:
        model = 'None'
    
    return model

# COMMAND ----------

def get_params_range(trial, name):
    if name == 'RandomForestRegressor':
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 500),
            'max_depth': trial.suggest_int('max_depth', 3, 20),
            'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
            'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 20),
            'max_features': trial.suggest_categorical('max_features', ['auto', 'sqrt', 'log2']),
            'bootstrap': trial.suggest_categorical('bootstrap', [True, False]),
        }
        
        if params['bootstrap']:
            params['oob_score'] = trial.suggest_categorical('oob_score', [True, False])
        else:
            params['oob_score'] = False

    elif name == 'AdaBoostRegressor':
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 500),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 1.0, log=True),
            'loss': trial.suggest_categorical('loss', ['linear', 'square', 'exponential'])
        }

    elif name == 'LGBMRegressor':
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 1000),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 1.0, log=True),
            'num_leaves': trial.suggest_int('num_leaves', 31, 256),
            'max_depth': trial.suggest_int('max_depth', -1, 50),
            'min_data_in_leaf': trial.suggest_int('min_data_in_leaf', 20, 100),
            'lambda_l1': trial.suggest_float('lambda_l1', 0.0, 10.0),
            'lambda_l2': trial.suggest_float('lambda_l2', 0.0, 10.0),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 10),
        }

    elif name == 'GradientBoostingRegressor':
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 500),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 1.0, log=True),
            'max_depth': trial.suggest_int('max_depth', 3, 20),
            'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
            'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 20),
            'subsample': trial.suggest_float('subsample', 0.5, 1.0),
            'max_features': trial.suggest_categorical('max_features', ['auto', 'sqrt', 'log2'])
        }

    elif name == 'CatBoostRegressor':
        params = {
            'iterations': trial.suggest_int('iterations', 100, 1000),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 1.0, log=True),
            'depth': trial.suggest_int('depth', 4, 16),
            'l2_leaf_reg': trial.suggest_float('l2_leaf_reg', 1e-2, 10.0, log=True),
            'bagging_temperature': trial.suggest_float('bagging_temperature', 0.0, 1.0),
            'border_count': trial.suggest_int('border_count', 32, 255),
            'random_strength': trial.suggest_float('random_strength', 0.0, 10.0),
            'grow_policy': trial.suggest_categorical('grow_policy', ['SymmetricTree', 'Depthwise', 'Lossguide']),
        }

    elif name == 'XGBRegressor':
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 500),
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 1e-4, 0.1, log=True),
            'subsample': trial.suggest_float('subsample', 0.5, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
            'gamma': trial.suggest_float('gamma', 0, 5),
            'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 1.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 1.0, log=True),
        }

    elif name == 'RidgeRegression':
        params = {
            'alpha': trial.suggest_float('alpha', 1e-4, 10.0, log=True),
            'fit_intercept': trial.suggest_categorical('fit_intercept', [True, False]),
            'normalize': trial.suggest_categorical('normalize', [True, False]),
            'solver': trial.suggest_categorical('solver', ['auto', 'svd', 'cholesky', 'lsqr', 'sparse_cg', 'sag', 'saga', 'lbfgs']),
        }

    elif name == 'LassoRegression':
        params = {
            'alpha': trial.suggest_float('alpha', 1e-4, 10.0, log=True),
            'fit_intercept': trial.suggest_categorical('fit_intercept', [True, False]),
            'normalize': trial.suggest_categorical('normalize', [True, False]),
            'max_iter': trial.suggest_int('max_iter', 1000, 5000),
            'tol': trial.suggest_float('tol', 1e-5, 1e-2, log=True),
        }

    elif name == 'ElasticNetRegression':
        params = {
            'alpha': trial.suggest_float('alpha', 1e-4, 10.0, log=True),
            'l1_ratio': trial.suggest_float('l1_ratio', 0.0, 1.0),
            'fit_intercept': trial.suggest_categorical('fit_intercept', [True, False]),
            'normalize': trial.suggest_categorical('normalize', [True, False]),
            'max_iter': trial.suggest_int('max_iter', 1000, 5000),
            'tol': trial.suggest_float('tol', 1e-5, 1e-2, log=True),
        }
        
    else:
        params = 'None'
    
    return params

# COMMAND ----------

# MAGIC %md
# MAGIC ##Optuna

# COMMAND ----------

def optuning(df2, name):
    best_params_df = pd.DataFrame()
    for i in range(len(df2)):
        df3 = pd.DataFrame([df2.iloc[i]])
        df3 = df3.merge(df, on = ['region_name', 'material_id'], how = 'left')
        df3, identifier, dynamic_predictors = model_data_prep(df3)

        X = df3.drop(columns=['quantity', period_val])
        y = df3['quantity']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

        def objective(trial):
            params = get_params_range(trial, name)
            model = get_model(name, params)
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            return r2_score(y_test, y_pred)

        study = optuna.create_study(direction = 'maximize', sampler = optuna.samplers.TPESampler(seed = 42))
        optuna.logging.set_verbosity(optuna.logging.CRITICAL)
        study.optimize(objective, n_trials = 50)

        # best_model = get_model(name, study.best_params)
        # best_model.fit(X_train, y_train)
        # y_pred = best_model.predict(X_test)

        # print(f"R-squared: {r2_score(y_test, y_pred):.4f}")
        # print(f"MAE: {mean_absolute_error(y_test, y_pred):.4f}")
        # print(f"RMSE: {np.sqrt(mean_squared_error(y_test, y_pred)):.4f}")
        # print(f"MAPE: {mean_absolute_percentage_error(y_test, y_pred):.4f}")

        temp = pd.DataFrame([study.best_params])
        best_params_df = pd.concat([best_params_df, temp], ignore_index = True)

    int_cols = ['min_data_in_leaf', 'bagging_freq', 'iterations', 'border_count', 'n_estimators', 'min_samples_split', 'min_samples_leaf', 'num_leaves', 'max_depth', 'depth']
    float_cols = ['learning_rate', 'lambda_l1', 'lambda_l2', 'feature_fraction', 'bagging_fraction', 'subsample', 'l2_leaf_reg', 'bagging_temperature', 'random_strength', 'gamma', 'reg_alpha', 'reg_lambda', 'colsample_bytree']
    text_cols = ['max_features', 'loss', 'grow_policy', 'bootstrap', 'oob_score']

    params_avg = {}
    for col in list(set(int_cols) & set(best_params_df.columns)):
        params_avg[col] = round(best_params_df[col].mean())
    for col in list(set(float_cols) & set(best_params_df.columns)):
        params_avg[col] = best_params_df[col].mean()
    for col in list(set(text_cols) & set(best_params_df.columns)):
        params_avg[col] = best_params_df[col].mode()[0]

    params_avg_df = pd.DataFrame({'model': [name], 'hyperparameters': [str(params_avg)]})

    return params_avg_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results & Feature Importance

# COMMAND ----------

def results_calculate(table_name, results, df):
    results = pd.merge(results, df[['material_id', 'material_name']].drop_duplicates(), on = 'material_id', how = 'left')

    results['acceptable_model_flag'] = np.where((results.r2_test >= 0.6) & (results.price_elasticity < 0), 1, 0)

    # results['promo_elasticity'] = results['promo_elasticity'].astype(str)
    # results = results.drop(columns = 'promo_elasticity')

    results = results.drop(columns = 'r2_train')
    results.rename(columns = {'r2_test': 'r2'}, inplace = True)
    
    return results

# COMMAND ----------

def feature_importances(name, model, X_train):
    features = X_train.columns.tolist()

    if name == 'LinearRegression':
        importance_df = pd.DataFrame({
            'feature': features,
            'importance': model.coef_
        })
    elif name == 'CatBoostRegressor':
        importance_df = pd.DataFrame({
            'feature': features,
            'importance': model.get_feature_importance()
        })
    else:
        importance_df = pd.DataFrame({
            'feature': features,
            'importance': model.feature_importances_
        })

    importance_df = importance_df.sort_values(by = 'importance', ascending = False).reset_index(drop = True)
    importance_dct = dict(zip(importance_df['feature'], importance_df['importance']))

    return str(importance_dct)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Elasticity Calculation

# COMMAND ----------

def calculate_elasticities(model, X, method='pdp', price_col='avg_unit_price', discount_col='discount_perc_adj'):
    valid_methods = ['pdp', 'marginal']

    if method not in valid_methods:
        raise ValueError(f"Invalid method. Choose from {valid_methods}")

    if price_col not in X.columns:
        raise ValueError(f"{price_col} not found in the dataset")

    elasticities = {'price_elasticity': None, 'promo_elasticity': None}

    if method == 'pdp':
        if price_col in X.columns:
            pdp_price = partial_dependence(model, X, [X.columns.get_loc(price_col)], grid_resolution=100)
            prices, quantities = pdp_price['values'][0], pdp_price['average'][0]
            
            price_changes = np.gradient(prices)
            quantity_changes = np.gradient(quantities)
            price_elasticities = np.divide(quantity_changes / quantities, price_changes / prices, 
                                        out=np.zeros_like(quantity_changes), where=price_changes != 0)
            elasticities['price_elasticity'] = np.round(np.mean(price_elasticities),3)
        else:
            elasticities['price_elasticity'] = None
        
        if discount_col in X.columns:
            X_discount = X[X[discount_col] != 0].copy()
            X_discount = X_discount[X_discount[discount_col] != X_discount[discount_col].shift()]
            if len(X_discount) > 0 and X_discount[discount_col].nunique() >= 5:
                pdp_discount = partial_dependence(model, X_discount, [X_discount.columns.get_loc(discount_col)], grid_resolution=100)
                discounts, quantities = pdp_discount['values'][0], pdp_discount['average'][0]
                
                discount_changes = np.gradient(discounts)
                quantity_changes = np.gradient(quantities)
                discount_elasticities = np.divide(quantity_changes / quantities, discount_changes / discounts, 
                out=np.zeros_like(quantity_changes), where=discount_changes != 0)
                elasticities['promo_elasticity'] = np.round(np.mean(discount_elasticities),3)
            else:
                elasticities['promo_elasticity'] = None
        else:
            elasticities['promo_elasticity'] = None

    elif method == 'marginal':
        if price_col in X.columns:
            y_pred = model.predict(X)
            delta_price = X[price_col].std() * 0.01
            X_plus_delta = X.copy()
            X_plus_delta[price_col] += delta_price
            price_elasticity = np.mean((model.predict(X_plus_delta) - y_pred) / y_pred / (delta_price / X[price_col]))
            elasticities['price_elasticity'] = np.round(price_elasticity,3)
        else:
            elasticities['price_elasticity'] = None
        
        if discount_col in X.columns:
            X_discount = X[X[discount_col] != 0].copy()
            X_discount = X_discount[X_discount[discount_col] != X_discount[discount_col].shift()]
            
            if len(X_discount) > 0 and X_discount[discount_col].nunique() >= 5:
                y_pred_discount = model.predict(X_discount)
                delta_discount = X_discount[discount_col].std() * 0.01
                X_discount_plus_delta = X_discount.copy()
                X_discount_plus_delta[discount_col] += delta_discount
                promo_elasticity = np.mean((model.predict(X_discount_plus_delta) - y_pred_discount) / y_pred_discount / (delta_discount / X_discount[discount_col]))
                elasticities['promo_elasticity'] = np.round(promo_elasticity,3)
            else:
                elasticities['promo_elasticity'] = None
        else:
            elasticities['promo_elasticity'] = None
    
    return elasticities

# COMMAND ----------

# MAGIC %md
# MAGIC ##Model Data Prep

# COMMAND ----------

def get_top_negative_corr(df_final_mds_base):
    price_columns = [column for column in df_final_mds_base.columns if column.startswith('price_ratio_p')]
    price_columns.insert(0, 'quantity')

    df_final_mds_base = df_final_mds_base[price_columns]

    # top_negative_corr = df_final_mds_base.corr().iloc[:, 0].sort_values().head(4)[1:4].index.tolist()
    top_negative_corr = df_final_mds_base.corr().iloc[:, 0].sort_values().head(2)[1:].index.tolist()[0]

    return top_negative_corr

# COMMAND ----------

def model_data_prep(data):
    # Drop event index or individual event columns
    if drop_event_idx:
        data = data.drop(columns = 'event_presence_idx')
        ev_cols = [column for column in data.columns if column.startswith('ev_')]
    else:
        ev_cols = [column for column in data.columns if column.startswith('ev_')]
        data = data.drop(columns = ev_cols)
        ev_cols = ['event_presence_idx']

    # Drop self price ratio
    identifier = data['material_proxy_identifier'].iloc[0]
    data = data.drop(columns = [f'price_ratio_{identifier}'])

    # Select only top 1 negative price ratios correlating with quantity
    top_negative_corr = get_top_negative_corr(data)
    price_columns = [column for column in data.columns if column.startswith('price_ratio_p')]
    price_columns = [x for x in price_columns if x != top_negative_corr]
    data = data.drop(columns = price_columns)

    # Normalize columns
    scaler = MinMaxScaler()
    columns_to_normalize = ['avg_unit_price', 'seasonality_idx', 'avg_temp', 'total_rainfall', top_negative_corr] + ev_cols
    data[columns_to_normalize] = scaler.fit_transform(data[columns_to_normalize])

    # adding numeric dist idx and dropping other numeric dist columns
    self_numeric_dist_col = f'numeric_dist_{identifier}'
    numeric_dist_cols = [col for col in data.columns if col.startswith('numeric_dist_') and col != self_numeric_dist_col]
    # data['numeric_dist_comp_idx'] = data[self_numeric_dist_col] / data[numeric_dist_cols].mean(axis=1)
    data.drop(columns=numeric_dist_cols, inplace=True)
    data.rename(columns={self_numeric_dist_col: 'numeric_dist'}, inplace=True)

    # Drop lag_quantity if both are highly correlated
    if using_monthly_data:
        quantity_val1 = 'lag_quantity_1m'
        quantity_val2 = 'lag_quantity_2m'
        price_ratio_ma_val1 = 'price_ratio_ma_2m'
        price_ratio_ma_val2 = 'price_ratio_ma_3m'
        price_ratio_comp_val = 'price_ratio_comp'
    else:
        quantity_val1 = 'lag_quantity_1w'
        quantity_val2 = 'lag_quantity_2w'
        price_ratio_ma_val1 = 'price_ratio_ma_15d'
        price_ratio_ma_val2 = 'price_ratio_ma_30d'
        price_ratio_comp_val = 'price_ratio_comp'
    # corr_val_lags = data[quantity_val1].corr(data[quantity_val2])
    # if corr_val_lags >= 0.8:
    #     data.drop(columns=[quantity_val2], inplace=True)

    # Extract the dynamic predictors
    # dynamic_predictors = '_'.join([top_negative_corr[0]] + [item.split('_')[-1] for item in top_negative_corr[1:]])
    dynamic_predictors = top_negative_corr

    # Drop categorical columns
    if using_monthly_data:
        temp_val = 'month_number_val'
    else:
        temp_val = 'week_number_val'
    data = data.drop(columns = ['region_name', 'material_id', 'material_proxy_identifier', 'material_name', 'year', temp_val])

    # Dropping all unnecessary columns
    data = data.drop(columns = ['new_launch_idx', 'has_holidays', 'price_inc_flag', 'discount_amount', 'discount_perc', 'unit_discount', price_ratio_ma_val1, price_ratio_ma_val2, price_ratio_comp_val, quantity_val1, quantity_val2])

    if not using_promo_data:
        data = data.drop(columns = 'discount_perc_adj')

    # Drop columns having a constant value
    data = data.loc[:, (data != data.iloc[0]).any()]

    return data, identifier, dynamic_predictors

# COMMAND ----------

# MAGIC %md
# MAGIC ##Modelling

# COMMAND ----------

def model_testing(df, params_df = pd.DataFrame()):
    if using_monthly_data:
        models = {
            'LinearRegression': LinearRegression(),
            'Log-LogLinearRegression': LinearRegression()
        }
    else:
        models = {
            'LinearRegression': LinearRegression(),
            # 'DecisionTreeRegressor': DecisionTreeRegressor(random_state = 42),
            'RandomForestRegressor': RandomForestRegressor(random_state = 42),
            # 'GradientBoostingRegressor': GradientBoostingRegressor(random_state = 42),
            # 'SVR': SVR(random_state = 42),
            # 'KNeighborsRegressor': KNeighborsRegressor(random_state = 42),
            'AdaBoostRegressor': AdaBoostRegressor(random_state = 42),
            'XGBRegressor': XGBRegressor(random_state = 42),
            # 'LGBMRegressor': LGBMRegressor(verbosity = -1, silent = True, random_state = 42),
            'CatBoostRegressor': CatBoostRegressor(verbose = 0, random_state = 42)
            # 'Log-LogLinearRegression': LinearRegression()
        }
    
    if not params_df.empty:
        for name, model in models.items():
            if name != 'LinearRegression' and name != 'Log-LogLinearRegression':
                best_params = ast.literal_eval(params_df[params_df['model'] == name]['hyperparameters'].iloc[0])
                model.set_params(**best_params)
                # if best_params != 'None':
                    # model.set_params(**best_params)
                # else:
                    # print(f"No parameters set for this model: {name}")

    results = pd.DataFrame(columns = ['demand_group', 'region_name', 'material_id', 'material_proxy_identifier', 'model', 'r2_train', 'r2_test', 'rmse', 'price_elasticity', 'promo_elasticity', 'dynamic_predictors', 'feature_importance'])

    scatter_plot_df = pd.DataFrame(columns = ['demand_group', 'model', 'model_set', 'region_name', 'material_id', 'avg_unit_price', 'quantity_actual', 'quantity_pred', 'material_proxy_identifier', period_val])

    materials = df['material_id'].unique()

    i = 0
    for material in materials:
        i += 1
        print(f"{i}. {material} processing...", end = ' ')
        regions = df[df['material_id'] == material]['region_name'].unique()

        for region in regions:
            print(f"{region}...", end = ' ')
            df2 = df[(df['material_id'] == material) & (df['region_name'] == region)].reset_index(drop=True)

            df2, identifier, dynamic_predictors = model_data_prep(df2)
            # print(len(df[(df['material_id'] == material) & (df['region_name'] == region)]['avg_unit_price']))
            # print(len(df2))

            X = df2.drop(columns='quantity')
            y = df2['quantity']

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Store all weeks in a variable
            weeks_train = X_train[period_val].values.tolist()
            weeks_test = X_test[period_val].values.tolist()

            X = X.drop(columns = period_val)
            X_train = X_train.drop(columns = period_val)
            X_test = X_test.drop(columns = period_val)

            for name, model in models.items():
                if name == 'Log-LogLinearRegression':
                    X_train = np.log(X + 1e-6)
                    X_test = np.log(X + 1e-6)
                    y_train = np.log(y)
                    y_test = np.log(y)
                
                model.fit(X_train, y_train)
                
                y_train_pred = model.predict(X_train)
                y_test_pred = model.predict(X_test)
                
                train_score = r2_score(y_train, y_train_pred)
                test_score = r2_score(y_test, y_test_pred)
                rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))

                elasticities = calculate_elasticities(model, X)
                price_elasticity = elasticities['price_elasticity']
                promo_elasticity = elasticities['promo_elasticity']

                importance_dct = feature_importances(name, model, X_train)
                
                results.loc[len(results)] = [demand_group, region, material, identifier, name, train_score, test_score, rmse, price_elasticity, promo_elasticity, dynamic_predictors, importance_dct]
                
                total_len = len(weeks_test) + len(weeks_train)
                demand_group_lst = [demand_group] * total_len
                name_lst = [name] * total_len
                model_set_lst = ['train'] * len(weeks_train) + ['test'] * len(weeks_test)
                region_lst = [region] * total_len
                material_lst = [material] * total_len
                price_lst = X_train['avg_unit_price'].tolist() + X_test['avg_unit_price'].tolist()
                y_lst = y_train.tolist() + y_test.tolist()
                y_pred_lst = y_train_pred.tolist() + y_test_pred.tolist()
                identifier_lst = [identifier] * total_len
                weeks_lst = weeks_train + weeks_test

                if name == 'Log-LogLinearRegression':
                    price_lst = X_train['avg_unit_price'].tolist()
                    y_lst = y_train.tolist()
                    y_pred_lst = y_train_pred.tolist()

                temp = pd.DataFrame({'demand_group': demand_group_lst,
                                     'model': name_lst,
                                     'model_set': model_set_lst,
                                     'region_name': region_lst,
                                     'material_id': material_lst,
                                     'avg_unit_price': price_lst,
                                     'quantity_actual': y_lst,
                                     'quantity_pred': y_pred_lst,
                                     'material_proxy_identifier': identifier_lst,
                                     period_val: weeks_lst})
                scatter_plot_df = pd.concat([scatter_plot_df, temp], ignore_index = True)
        print(f"done")

    scatter_plot_df = scatter_plot_df.sort_values(by = ['model', 'region_name', 'material_id', period_val]).reset_index(drop = True)
    scatter_plot_df = pd.merge(scatter_plot_df, df[['material_id', 'material_name']].drop_duplicates(), on = 'material_id', how = 'left')

    results[['r2_train', 'r2_test']] = round(results[['r2_train', 'r2_test']], 3)

    return scatter_plot_df, results

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Data

# COMMAND ----------

table_sub_name = f"hhc_{mg_name_abbr}_dg{str(cluster_idx)}"

if using_monthly_data:
    table_name = f'dev.sandbox.pj_poc_mds_final_monthly_{table_sub_name}'
    period_val = 'month_number'
else:
    period_val = 'week_number'
    table_name = f'dev.sandbox.pj_poc_mds_final_{table_sub_name}'

df = spark.sql(f'SELECT * FROM {table_name}').toPandas()

df['unit_discount'] = df['discount_amount'] / df['quantity']
df['discount_perc_adj'] = 1 - df['avg_unit_price']/(df['avg_unit_price'] + df['unit_discount'])
df['discount_perc_adj'] = df['discount_perc_adj'].clip(lower=0, upper=1)

# COMMAND ----------

df.shape

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Model Testing (Baseline)

# COMMAND ----------

scatter_plot_df, results = model_testing(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Hyperparameter Tuning (Optuna)

# COMMAND ----------

df2 = df[['region_name', 'material_id']].drop_duplicates()
df2 = df2.groupby('region_name').apply(lambda x: x.sample(1, random_state = 42)).reset_index(drop=True)

names = ['RandomForestRegressor', 'AdaBoostRegressor', 'CatBoostRegressor', 'XGBRegressor']
tuned_hyperparameters = pd.DataFrame(columns = ['model', 'hyperparameters'])

for name in names:
    tuned_hyperparameters = pd.concat([tuned_hyperparameters, optuning(df2, name)], ignore_index = True)
    print(f"{name} done")

# COMMAND ----------

# Set oob_score to False if bootstrap is also False for RandomForestRegressor
def update_oob_score(row):
    if row['model'] == 'RandomForestRegressor':
        hyperparameters = ast.literal_eval(row['hyperparameters'])
        if not hyperparameters.get('bootstrap', True):
            hyperparameters['oob_score'] = False
        return str(hyperparameters)
    return row['hyperparameters']

tuned_hyperparameters['hyperparameters'] = tuned_hyperparameters.apply(update_oob_score, axis=1)

# Run models with the final hyperparameters
scatter_plot_df, results = model_testing(df, tuned_hyperparameters)

results = results.merge(tuned_hyperparameters, on = 'model', how = 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC #Results Calculation

# COMMAND ----------

results = results_calculate(table_name, results, df)

fixed_predictors = ['avg_unit_price', 'self_numeric_dist', 'seasonality_idx', 'total_rainfall', 'avg_temp', 'event_presence_idx']
print(f"Fixed Predictors:\n{fixed_predictors}")

results.sort_values(by = ['region_name', 'material_id', 'r2'], ascending=[True, True, False]).display()

# COMMAND ----------

scatter_plot_df = pd.merge(scatter_plot_df, df[[period_val, 'region_name', 'material_id', 'avg_unit_price']], on = [period_val, 'region_name', 'material_id'], how = 'left', suffixes=('', '_mds'))

scatter_plot_df['avg_unit_price_mds'] = scatter_plot_df['avg_unit_price_mds'].astype(int)
temp = scatter_plot_df.groupby(['region_name', 'material_id'])['avg_unit_price_mds'].nunique().reset_index().copy()
temp.rename(columns={'avg_unit_price_mds': 'num_price_changes'}, inplace=True)

scatter_plot_df = pd.merge(scatter_plot_df, temp, on = ['region_name', 'material_id'], how = 'left')


scatter_plot_df = pd.merge(scatter_plot_df, results[['model', 'region_name', 'material_id', 'acceptable_model_flag']], on = ['model', 'region_name', 'material_id'], how = 'left')

test_pred_df = scatter_plot_df.copy()

test_pred_df = test_pred_df.sort_values(by = ['region_name', 'material_id', period_val, 'model']).reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Final Metrics Calculation

# COMMAND ----------

# final_metrics_df = final_metrics(table_name, df, demand_group)
# final_metrics_df = final_metrics_df[['demand_group', 'region_name', 'material_id', 'avg_weekly_sales_l12_mds', 'avg_weekly_qty_l12_mds', 'avg_weekly_qty_l12', 'avg_unit_price_l12_mds', 'avg_monthly_qty_l12_mds', 'avg_monthly_qty_l12', 'avg_weekly_discount_l12_mds', 'avg_unit_discount_l12_mds_overall', 'avg_unit_discount_l12_mds_conditional', 'material_name', 'material_proxy_identifier']]
# final_metrics_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Save to Sandbox

# COMMAND ----------

spark_df1 = spark.createDataFrame(results)
spark_df2 = spark.createDataFrame(test_pred_df)
# spark_df3 = spark.createDataFrame(final_metrics_df)

if drop_event_idx:
    if using_monthly_data:
        if using_promo_data:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_monthly_ev_promo_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_monthly_ev_promo_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_monthly_ev_promo_{table_sub_name}")
        else:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_monthly_ev_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_monthly_ev_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_monthly_ev_{table_sub_name}")
    else:
        if using_promo_data:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_ev_promo_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_ev_promo_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_ev_promo_{table_sub_name}")
        else:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_ev_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_ev_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_ev_{table_sub_name}")

else:
    if using_monthly_data:
        if using_promo_data:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_monthly_promo_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_monthly_promo_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_monthly_promo_{table_sub_name}")
        else:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_monthly_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_monthly_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_monthly_{table_sub_name}")
    else:
        if using_promo_data:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_promo_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_promo_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_promo_{table_sub_name}")
        else:
            spark_df1.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_model_results_{table_sub_name}")
            spark_df2.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_actual_pred_data_{table_sub_name}")
            # spark_df3.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_final_metrics_{table_sub_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating MG level final tables

# COMMAND ----------

# query = f"""
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master AS (
#     SELECT
#         demand_group, region_name, material_id, material_proxy_identifier,
#         model, r2, rmse, price_elasticity, promo_elasticity, acceptable_model_flag
#     FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_dg1
#     UNION
#     SELECT
#         demand_group, region_name, material_id, material_proxy_identifier,
#         model, r2, rmse, price_elasticity, promo_elasticity, acceptable_model_flag
#     FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_dg2
# )
# """

# spark.sql(query)

# COMMAND ----------

# query = f"""
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_actual_pred_data_promo_hhc_{mg_name_abbr}_master AS (
#     SELECT * FROM dev.sandbox.pj_po_actual_pred_data_promo_hhc_{mg_name_abbr}_dg1
#     UNION
#     SELECT * FROM dev.sandbox.pj_po_actual_pred_data_promo_hhc_{mg_name_abbr}_dg2
# )
# """

# spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #Actual vs Predicted Plot

# COMMAND ----------

# model = 'Log-LogLinearRegression' #LinearRegression, Log-LogLinearRegression
# material_id = '1012327'
# region_name = 'ABU DHABI'

# # query = f"""
# # SELECT *
# # FROM dev.sandbox.pj_po_actual_pred_data_monthly_ev_apc_dg1
# # WHERE
# #     model = '{model}'
# #     AND material_id = '{material_id}'
# #     AND region_name = '{region_name}'
# # """
# # scatter_plot_df = spark.sql(query).toPandas()

# scatter_plot_df2 = scatter_plot_df[(scatter_plot_df['model'] == model) & (scatter_plot_df['region_name'] == region_name) & (scatter_plot_df['material_id'] == material_id)]

# df_melted = scatter_plot_df2.melt(id_vars=period_val, value_vars=['quantity_actual', 'quantity_pred'], var_name='Quantity', value_name='Value')

# import plotly.express as px
# fig = px.scatter(df_melted, x=period_val, y='Value', color='Quantity', 
#                  title='Actual vs Predicted Plot by Weeks')
# fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Quantity vs Price Plot

# COMMAND ----------

# model = 'Log-LogLinearRegression' #LinearRegression, Log-LogLinearRegression
# material_id = '1012327'
# region_name = 'ABU DHABI'

# # query = f"""
# # SELECT *
# # FROM dev.sandbox.pj_po_actual_pred_data_monthly_apc_dg1
# # WHERE
# #     model = '{model}'
# #     AND material_id = '{material_id}'
# #     AND region_name = '{region_name}'
# # """
# # scatter_plot_df = spark.sql(query).toPandas()

# scatter_plot_df2 = scatter_plot_df[(scatter_plot_df['model'] == model) & (scatter_plot_df['region_name'] == region_name) & (scatter_plot_df['material_id'] == material_id)]
# temp = scatter_plot_df2[['region_name', 'material_id', period_val, 'avg_unit_price', 'quantity_actual']]
# temp = temp[(temp['region_name'] == region_name) & (temp['material_id'] == material_id)]
# temp.sort_values(period_val, inplace = True)
# temp.plot(x = 'month_number', y = 'quantity_actual', title = 'Log-Log Price vs Quantity Plot')
# temp.plot(x = 'month_number', y = 'avg_unit_price', title = 'Log-Log Price vs Quantity Plot')

# COMMAND ----------

# df_ps = df[['region_name', 'material_id', 'month_number', 'avg_unit_price', 'quantity']]
# df_ps2 = df_ps[(df_ps['region_name'] == 'ABU DHABI') & (df_ps['material_id'] == '1012327')]
# df_ps2.sort_values('month_number', inplace = True)
# df_ps2.plot(x = 'avg_unit_price', y = 'quantity', title = 'Price vs Quantity Plot')

# COMMAND ----------

# df_ps = df[['region_name', 'material_id', 'month_number', 'avg_unit_price', 'quantity']]
# df_ps2 = df_ps[(df_ps['region_name'] == 'ABU DHABI') & (df_ps['material_id'] == '1012327')]
# df_ps2.sort_values('month_number', inplace=True)
# df_ps2.plot(x='month_number', y='quantity', title='Quantity Plot')
# df_ps2.plot(x='month_number', y='avg_unit_price', title='Quantity Plot')

# COMMAND ----------

# MAGIC %md
# MAGIC #Non-acceptable Flags Aggregation

# COMMAND ----------

# query = f"""
# SELECT
#     "hhc_{demand_group}" AS demand_group,
#     t1.*
# FROM dev.sandbox.pj_poc_mds_final_hhc_apc_dg{str(cluster_idx)} AS t1
# JOIN dev.sandbox.pg_final_model_results_hhc_apc_dg_{str(cluster_idx)} AS t2
#     ON t1.material_id = t2.material_id
#     AND t1.region_name = t2.region_name
# WHERE t2.acceptable_flag = 0
# """
# df_aggr = spark.sql(query).toPandas()

# query = f"""
# WITH cte AS (
#     SELECT
#         region_name,
#         material_id,
#         week_number,
#         quantity,
#         (quantity * avg_unit_price) AS sales
#     FROM dev.sandbox.pj_poc_mds_final_hhc_apc_dg{str(cluster_idx)}
# )

# SELECT
#     region_name,
#     week_number,
#     SUM(sales) AS total_sales,
#     SUM(quantity) AS total_quantity
# FROM cte
# GROUP BY 1, 2
# ORDER BY 1, 2
# """
# df_price_comp = spark.sql(query).toPandas()

# query = f"""
# SELECT *
# FROM dev.sandbox.pj_poc_mds_final_hhc_apc_dg{str(cluster_idx)}
# """
# df_price_ratio_all = spark.sql(query).toPandas()

# COMMAND ----------

# df_aggr_new = weekly_aggr(df_aggr, df_price_comp, df_price_ratio_all)

# spark.createDataFrame(df_aggr_new).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"dev.sandbox.pj_po_mds_final_weekly_hhc_{table_sub_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.sandbox.pj_po_mds_final_weekly_hhc_apc_dg1

# COMMAND ----------

# MAGIC %md
# MAGIC #Table Joins for Power BI

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_hhc_apc_results_master AS (
#     SELECT
#         t1.demand_group,
#         t1.region_name,
#         t1.material_id,
#         t1.model,
#         t1.r2,
#         t1.rmse,
#         t1.price_elasticity,
#         t1.promo_elasticity,
#         t1.acceptable_flag,
#         t2.best_model,
#         t3.avg_weekly_sales_l12_mds,
#         t3.avg_weekly_qty_l12_mds,
#         t3.avg_weekly_qty_l12,
#         t3.avg_unit_price_l12_mds,
#         t3.avg_monthly_qty_l12_mds,
#         t3.avg_monthly_qty_l12,
#         t3.avg_weekly_discount_l12_mds,
#         t3.avg_unit_discount_l12_mds_overall,
#         t3.avg_unit_discount_l12_mds_conditional,
#         t3.material_name,
#         t1.material_proxy_identifier
#     FROM dev.sandbox.pg_po_hhc_apc_master_model_results_all_dg AS t1
#     JOIN dev.sandbox.pg_po_hhc_apc_best_models_all_dg AS t2
#         ON t1.demand_group = t2.demand_group
#         AND t1.region_name = t2.region_name
#     JOIN dev.sandbox.pg_po_hhc_apc_sales_info_all_dg AS t3
#         ON t1.demand_group = t3.demand_group
#         AND t1.region_name = t3.region_name
#         AND t1.material_id = t3.material_id
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev.sandbox.pj_po_hhc_apc_results_master

# COMMAND ----------

# MAGIC %md
# MAGIC #Best Model Selection

# COMMAND ----------

query = f"""
SELECT
    demand_group,
    region_name,
    model,
    COUNT(material_id) AS num_skus,
    ROUND(AVG(r2), 2) AS avg_r2,
    ROUND(MIN(price_elasticity), 3) AS min_price_elasticity,
    ROUND(MAX(price_elasticity), 3) AS max_price_elasticity,
    ROUND(AVG(price_elasticity), 3) AS avg_price_elasticity
FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master
WHERE
    r2 >= 0.6
    AND price_elasticity < 0
GROUP BY 1, 2, 3
ORDER BY 1, 2, 4 DESC, 5 DESC
"""

spark.sql(query).toPandas().display()

# COMMAND ----------

# query = f"""
# CREATE OR REPLACE TABLE dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master_best_model AS (
#     SELECT
#         *,
#         CASE WHEN demand_group = "hhc_{mg_name_abbr}_dg_1" AND region_name = "ABU DHABI" THEN "CatBoostRegressor"
#             WHEN demand_group = "hhc_{mg_name_abbr}_dg_1" AND region_name = "AL AIN" THEN "XGBRegressor"
#             WHEN demand_group = "hhc_{mg_name_abbr}_dg_1" AND region_name = "DUBAI" THEN "LinearRegression"
#             WHEN demand_group = "hhc_{mg_name_abbr}_dg_1" AND region_name = "SHARJAH" THEN "XGBRegressor"
#             WHEN demand_group = "hhc_{mg_name_abbr}_dg_2" AND region_name = "ABU DHABI" THEN "AdaBoostRegressor"
#             WHEN demand_group = "hhc_{mg_name_abbr}_dg_2" AND region_name = "AL AIN" THEN "XGBRegressor"
#             WHEN demand_group = "hhc_{mg_name_abbr}_dg_2" AND region_name = "DUBAI" THEN "AdaBoostRegressor"
#             WHEN demand_group = "hhc_{mg_name_abbr}_dg_2" AND region_name = "SHARJAH" THEN "XGBRegressor"
#         END AS best_model
#     FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master
# )
# """

# spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #Final Result Numbers

# COMMAND ----------

query = f"""
WITH model_results AS (
    SELECT
        demand_group,
        region_name,
        COUNT(DISTINCT material_id) AS total_pairs,
        COUNT(DISTINCT CASE WHEN r2 >= 0.6 AND price_elasticity < 0 THEN material_id END) AS g_r2_g_pe,
        COUNT(DISTINCT CASE WHEN r2 >= 0.6 AND price_elasticity >= 0 THEN material_id END) AS g_r2_b_pe,
        COUNT(DISTINCT CASE WHEN r2 < 0.6 AND price_elasticity >= 0 THEN material_id END) AS b_r2_b_pe,
        COUNT(DISTINCT CASE WHEN r2 < 0.6 AND price_elasticity < 0 THEN material_id END) AS b_r2_g_pe,
        COUNT(DISTINCT CASE WHEN r2 >= 0.6 AND price_elasticity < 0 AND promo_elasticity > 0 THEN material_id END) AS g_r2_g_pe_g_pre
    FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master_best_model
    WHERE model = best_model
    GROUP BY 1, 2
),

price_variations AS (
    SELECT
        demand_group,
        region_name,
        COUNT(DISTINCT material_id) AS low_price_variation_skus
    FROM dev.sandbox.pj_po_actual_pred_data_promo_hhc_{mg_name_abbr}_master
    WHERE num_price_changes < 5
    GROUP BY 1, 2
)

SELECT
    t1.*,
    t2.low_price_variation_skus
FROM model_results t1
LEFT JOIN price_variations t2
    ON t1.demand_group = t2.demand_group
    AND t1.region_name = t2.region_name
ORDER BY t1.demand_group, t1.region_name
"""

spark.sql(query).toPandas().display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


