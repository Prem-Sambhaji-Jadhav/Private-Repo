# Databricks notebook source
# MAGIC %md
# MAGIC #Function Initializations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Libraries

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
# MAGIC ##Model Initialization & Hyperparameters

# COMMAND ----------

def get_model(name, params):
    if name == 'RandomForestRegressor':
        model = RandomForestRegressor(random_state = 42, **params)
    elif name == 'AdaBoostRegressor':
        model = AdaBoostRegressor(random_state = 42, **params)
    elif name == 'CatBoostRegressor':
        model = CatBoostRegressor(verbose = 0, random_state = 42, **params)
    elif name == 'XGBRegressor':
        model = XGBRegressor(random_state = 42, **params)
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

        X = df3.drop(columns=['quantity', 'week_number'])
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

    top_negative_corr = df_final_mds_base.corr().iloc[:, 0].sort_values().head(2)[1:].index.tolist()[0]

    return top_negative_corr

# COMMAND ----------

def model_data_prep(data):
    # Drop individual event columns
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
    data.drop(columns=numeric_dist_cols, inplace=True)
    data.rename(columns={self_numeric_dist_col: 'numeric_dist'}, inplace=True)

    dynamic_predictors = top_negative_corr

    # Drop categorical columns
    data = data.drop(columns = ['region_name', 'material_id', 'material_proxy_identifier', 'material_name', 'year', 'week_number_val'])

    # Dropping all unnecessary columns
    data = data.drop(columns = ['new_launch_idx', 'has_holidays', 'price_inc_flag', 'discount_amount', 'discount_perc', 'unit_discount', 'price_ratio_ma_15d', 'price_ratio_ma_30d', 'price_ratio_comp', 'lag_quantity_1w', 'lag_quantity_2w'])

    # Drop columns having a constant value
    data = data.loc[:, (data != data.iloc[0]).any()]

    return data, identifier, dynamic_predictors

# COMMAND ----------

# MAGIC %md
# MAGIC ##Modelling

# COMMAND ----------

def model_testing(df, params_df = pd.DataFrame()):
    models = {
        'LinearRegression': LinearRegression(),
        'RandomForestRegressor': RandomForestRegressor(random_state = 42),
        'AdaBoostRegressor': AdaBoostRegressor(random_state = 42),
        'XGBRegressor': XGBRegressor(random_state = 42),
        'CatBoostRegressor': CatBoostRegressor(verbose = 0, random_state = 42)
    }
    
    if not params_df.empty:
        for name, model in models.items():
            if name != 'LinearRegression' and name != 'Log-LogLinearRegression':
                best_params = ast.literal_eval(params_df[params_df['model'] == name]['hyperparameters'].iloc[0])
                model.set_params(**best_params)

    results = pd.DataFrame(columns = ['demand_group', 'region_name', 'material_id', 'material_proxy_identifier', 'model', 'r2_train', 'r2_test', 'rmse', 'price_elasticity', 'promo_elasticity', 'dynamic_predictors', 'feature_importance'])

    scatter_plot_df = pd.DataFrame(columns = ['demand_group', 'model', 'model_set', 'region_name', 'material_id', 'avg_unit_price', 'quantity_actual', 'quantity_pred', 'material_proxy_identifier', 'week_number'])

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

            X = df2.drop(columns='quantity')
            y = df2['quantity']

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Store all weeks in a variable
            weeks_train = X_train['week_number'].values.tolist()
            weeks_test = X_test['week_number'].values.tolist()

            X = X.drop(columns = 'week_number')
            X_train = X_train.drop(columns = 'week_number')
            X_test = X_test.drop(columns = 'week_number')

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
                                     'week_number': weeks_lst})
                scatter_plot_df = pd.concat([scatter_plot_df, temp], ignore_index = True)
        print(f"done")

    scatter_plot_df = scatter_plot_df.sort_values(by = ['model', 'region_name', 'material_id', 'week_number']).reset_index(drop = True)
    scatter_plot_df = pd.merge(scatter_plot_df, df[['material_id', 'material_name']].drop_duplicates(), on = 'material_id', how = 'left')

    results[['r2_train', 'r2_test']] = round(results[['r2_train', 'r2_test']], 3)

    return scatter_plot_df, results

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Data

# COMMAND ----------

query_temp = """
select material_group_name, collect_set(demand_group) as demand_groups
from analytics.pricing.hhc_mds_master
group by material_group_name
order by material_group_name
"""

df_mg_dg = spark.sql(query_temp)

MG_DG_DICT = {row['material_group_name']: row['demand_groups'] for row in df_mg_dg.collect()}
MG_LIST = list(MG_DG_DICT.keys())

df_temp_agg = df_mg_dg.toPandas()
df_temp_agg['demand_groups'] = df_temp_agg.demand_groups.apply(lambda x: sorted(x))

df_temp_agg.head()

# COMMAND ----------

# def get_mg_dg_mds(material_group_name, mg_demand_group):

#     sdf_mds_master = spark.table('analytics.pricing.hhc_mds_master')

#     sdf_mg_dg_mds = sdf_mds_master.filter(f"material_group_name = '{material_group_name}' and demand_group = '{mg_demand_group}'")

#     df = sdf_mg_dg_mds.toPandas()

#     # remove all columns (price ratios/numeric dist) where the entire columns are null
#     df = df.dropna(axis=1, how='all')

#     # removing the additional material_group_name and demand_group columns
#     df.drop(columns=['material_group_name', 'demand_group'], inplace=True)

#     df['unit_discount'] = df['discount_amount'] / df['quantity']
#     df['discount_perc_adj'] = 1 - df['avg_unit_price']/(df['avg_unit_price'] + df['unit_discount'])
#     df['discount_perc_adj'] = df['discount_perc_adj'].clip(lower=0, upper=1)

#     return df


# # DRIVER CODE (for data fetch)
# material_group_name = 'BATH ROOM CLEANERS'
# cluster_idx = 1

# mg_demand_group = f'DG-{cluster_idx}'

# mg_name_abbr = material_group_name.replace(" ", "_").lower()
# demand_group = f"hhc_{mg_name_abbr}_dg_{str(cluster_idx)}"

# df = get_mg_dg_mds(material_group_name=material_group_name, mg_demand_group=mg_demand_group)

# # create a temp view from the final raw mds (df)
# table_name = 'mg_dg_mds_view'
# spark.createDataFrame(df).createOrReplaceTempView(table_name)

# num_sku = df.material_id.nunique()
# print(f'Total Distinct SKUs: {num_sku}\n')

# df.display()

# COMMAND ----------

def get_mg_dg_mds(material_group_name, mg_demand_group):

    sdf_mds_master = spark.table('analytics.pricing.hhc_mds_master')

    sdf_mg_dg_mds = sdf_mds_master.filter(f"material_group_name = '{material_group_name}' and demand_group = '{mg_demand_group}'")

    df = sdf_mg_dg_mds.toPandas()

    # remove all columns (price ratios/numeric dist) where the entire columns are null
    df = df.dropna(axis=1, how='all')

    # removing the additional material_group_name and demand_group columns
    df.drop(columns=['material_group_name', 'demand_group'], inplace=True)

    df['unit_discount'] = df['discount_amount'] / df['quantity']
    df['discount_perc_adj'] = 1 - df['avg_unit_price']/(df['avg_unit_price'] + df['unit_discount'])
    df['discount_perc_adj'] = df['discount_perc_adj'].clip(lower=0, upper=1)

    return df


def get_all_model_results(material_group_name, cluster_idx):

    mg_demand_group = f'DG-{cluster_idx}'

    mg_name_abbr = material_group_name.replace(" ", "_").lower()
    demand_group = f"hhc_{mg_name_abbr}_dg_{str(cluster_idx)}"

    df = get_mg_dg_mds(material_group_name=material_group_name, mg_demand_group=mg_demand_group)

    # create a temp view from the final raw mds (df)
    table_name = 'mg_dg_mds_view'
    spark.createDataFrame(df).createOrReplaceTempView(table_name)

    num_sku = df.material_id.nunique()
    print(f'Total Distinct SKUs: {num_sku}\n')


    # hyperparameter tuning step
    print('\nStarting Hyperparameter Tuning..')
    df2 = df[['region_name', 'material_id']].drop_duplicates()
    df2 = df2.groupby('region_name').apply(lambda x: x.sample(1, random_state = 42)).reset_index(drop=True)

    names = ['RandomForestRegressor', 'AdaBoostRegressor', 'CatBoostRegressor', 'XGBRegressor']
    tuned_hyperparameters = pd.DataFrame(columns = ['model', 'hyperparameters'])

    for name in names:
        tuned_hyperparameters = pd.concat([tuned_hyperparameters, optuning(df2, name)], ignore_index = True)
        print(f"{name} done")

    print('\tHyperparameter Tuning Complete..')

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
    print('Running Model Iterations..')
    scatter_plot_df, results = model_testing(df, tuned_hyperparameters)
    results = results.merge(tuned_hyperparameters, on = 'model', how = 'left')
    print('\t Done.')


    #collate results
    print('Collating Results...')
    results = results_calculate(table_name, results, df)

    fixed_predictors = ['avg_unit_price', 'self_numeric_dist', 'seasonality_idx', 'total_rainfall', 'avg_temp', 'event_presence_idx']

    print('\t Done.')


    # prepare prediction df
    print('Preparing test pred df...')
    scatter_plot_df = pd.merge(scatter_plot_df, df[['week_number', 'region_name', 'material_id', 'avg_unit_price']], on = ['week_number', 'region_name', 'material_id'], how = 'left', suffixes=('', '_mds'))

    scatter_plot_df['avg_unit_price_mds'] = scatter_plot_df['avg_unit_price_mds'].astype(int)
    temp = scatter_plot_df.groupby(['region_name', 'material_id'])['avg_unit_price_mds'].nunique().reset_index().copy()
    temp.rename(columns={'avg_unit_price_mds': 'num_price_changes'}, inplace=True)

    scatter_plot_df = pd.merge(scatter_plot_df, temp, on = ['region_name', 'material_id'], how = 'left')

    scatter_plot_df = pd.merge(scatter_plot_df, results[['model', 'region_name', 'material_id', 'acceptable_model_flag']], on = ['model', 'region_name', 'material_id'], how = 'left')

    test_pred_df = scatter_plot_df.copy()
    test_pred_df = test_pred_df.sort_values(by = ['region_name', 'material_id', 'week_number', 'model']).reset_index(drop = True)


    # attaching the mg name and demand group markers to final tables
    results['material_group_name'] = material_group_name
    results['mg_demand_group'] = mg_demand_group

    test_pred_df['material_group_name'] = material_group_name
    test_pred_df['mg_demand_group'] = mg_demand_group

    print('\t Done.')

    return results, test_pred_df


# MASTER DRIVER CODE

# MG_DG_DICT = {'BATH ROOM CLEANERS': ['DG-3', 'DG-1', 'DG-2'], 'BLEACH': ['DG-2', 'DG-1'], 'DISHWASHER DETERGENT': ['DG-3', 'DG-2', 'DG-1'], 'GLASS CLEANERS': ['DG-2', 'DG-1'], 'INSECTICIDES': ['DG-3', 'DG-2', 'DG-1'], 'KITCHEN CLEANERS': ['DG-2', 'DG-1'], 'SCOURING CREAMS': ['DG-3', 'DG-2', 'DG-1'], 'SPECIALIST CLEANERS': ['DG-3', 'DG-2', 'DG-1'], 'TOILET BLOCKS': ['DG-3', 'DG-2', 'DG-1'], 'TOILET CLEANERS': ['DG-3', 'DG-2', 'DG-1']}

MG_DG_DICT = {'BATH ROOM CLEANERS': ['DG-3', 'DG-1', 'DG-2']}


target_table_name_results = 'analytics.pricing.hhc_all_model_results_raw'
target_table_name_test_pred = 'analytics.pricing.hhc_all_model_test_preds'


for mg_name in list(MG_DG_DICT.keys()):
    for cluster_idx_ in range(len(MG_DG_DICT[mg_name])):

        cluster_idx = cluster_idx_+1

        print(f'Starting Iteration for {mg_name}, DG-{cluster_idx}')

        results, test_pred_df = get_all_model_results(material_group_name=mg_name, cluster_idx=cluster_idx)

        print(f'Finished {mg_name}, DG-{cluster_idx}')

        print('Writing to Target Tables...')

        sdf_results = spark.createDataFrame(results)
        sdf_test_pred = spark.createDataFrame(test_pred_df)

        sdf_results.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(target_table_name_results)

        sdf_test_pred.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(target_table_name_test_pred)
        print('\t Table Write complete.\n\n')

# COMMAND ----------

# debug code

def get_mg_dg_mds(material_group_name, mg_demand_group):

    sdf_mds_master = spark.table('analytics.pricing.hhc_mds_master')

    sdf_mg_dg_mds = sdf_mds_master.filter(f"material_group_name = '{material_group_name}' and demand_group = '{mg_demand_group}'")

    df = sdf_mg_dg_mds.toPandas()

    # remove all columns (price ratios/numeric dist) where the entire columns are null
    df = df.dropna(axis=1, how='all')

    # removing the additional material_group_name and demand_group columns
    df.drop(columns=['material_group_name', 'demand_group'], inplace=True)

    df['unit_discount'] = df['discount_amount'] / df['quantity']
    df['discount_perc_adj'] = 1 - df['avg_unit_price']/(df['avg_unit_price'] + df['unit_discount'])
    df['discount_perc_adj'] = df['discount_perc_adj'].clip(lower=0, upper=1)

    return df


MG_DG_DICT = {'BATH ROOM CLEANERS': ['DG-3', 'DG-1', 'DG-2']}

mg_name_temp = 'BATH ROOM CLEANERS'
demand_group_temp = 'DG-2'

print(f'Starting Iteration for {mg_name_temp}, {demand_group_temp}')

df_mds_temp = get_mg_dg_mds(material_group_name=mg_name_temp, mg_demand_group=demand_group_temp)

temp_cols = ['week_number', 'region_name', 'material_id', 'quantity', 'avg_unit_price', 'material_proxy_identifier']


num_sku_temp = df_mds_temp.material_id.nunique()

print(f'Total Distinct SKUs: {num_sku_temp}\n')

df_mds_temp[temp_cols]


# COMMAND ----------

# debug code

# group by region_name and count the distinct SKUs in each region (pandas df)
num_sku_temp.groupby('region_name').

# COMMAND ----------

target_table_name_results = 'analytics.pricing.hhc_all_model_results_raw'
target_table_name_test_pred = 'analytics.pricing.hhc_all_model_test_preds'


spark.table(target_table_name_test_pred).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating MG level final tables
# MAGIC Run the below cells only after the notebook has been executed for all demand groups of the given material group.
# MAGIC <br><br>
# MAGIC Each DG sub-table in the below two cells has to be manually written for as many DGs present in the material group.

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
# MAGIC #Best Model Selection
# MAGIC Run the below cells only after the above master tables are created

# COMMAND ----------

# query = f"""
# SELECT
#     demand_group,
#     region_name,
#     model,
#     COUNT(material_id) AS num_skus,
#     ROUND(AVG(r2), 2) AS avg_r2,
#     ROUND(MIN(price_elasticity), 3) AS min_price_elasticity,
#     ROUND(MAX(price_elasticity), 3) AS max_price_elasticity,
#     ROUND(AVG(price_elasticity), 3) AS avg_price_elasticity
# FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master
# WHERE
#     r2 >= 0.6
#     AND price_elasticity < 0
# GROUP BY 1, 2, 3
# ORDER BY 1, 2, 4 DESC, 5 DESC
# """

# spark.sql(query).toPandas().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Best model is being assigned to each demand group and region below manually.
# MAGIC <br><br>
# MAGIC Each DG has to be manually mentioned in the case statement in the below query for as many DGs present in the material group.

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
# MAGIC Run the below cell only after the above sandbox for best model is created

# COMMAND ----------

# query = f"""
# WITH model_results AS (
#     SELECT
#         demand_group,
#         region_name,
#         COUNT(DISTINCT material_id) AS total_pairs,
#         COUNT(DISTINCT CASE WHEN r2 >= 0.6 AND price_elasticity < 0 THEN material_id END) AS g_r2_g_pe,
#         COUNT(DISTINCT CASE WHEN r2 >= 0.6 AND price_elasticity >= 0 THEN material_id END) AS g_r2_b_pe,
#         COUNT(DISTINCT CASE WHEN r2 < 0.6 AND price_elasticity >= 0 THEN material_id END) AS b_r2_b_pe,
#         COUNT(DISTINCT CASE WHEN r2 < 0.6 AND price_elasticity < 0 THEN material_id END) AS b_r2_g_pe,
#         COUNT(DISTINCT CASE WHEN r2 >= 0.6 AND price_elasticity < 0 AND promo_elasticity > 0 THEN material_id END) AS g_r2_g_pe_g_pre
#     FROM dev.sandbox.pj_po_model_results_promo_hhc_{mg_name_abbr}_master_best_model
#     WHERE model = best_model
#     GROUP BY 1, 2
# ),

# price_variations AS (
#     SELECT
#         demand_group,
#         region_name,
#         COUNT(DISTINCT material_id) AS low_price_variation_skus
#     FROM dev.sandbox.pj_po_actual_pred_data_promo_hhc_{mg_name_abbr}_master
#     WHERE num_price_changes < 5
#     GROUP BY 1, 2
# )

# SELECT
#     t1.*,
#     t2.low_price_variation_skus
# FROM model_results t1
# LEFT JOIN price_variations t2
#     ON t1.demand_group = t2.demand_group
#     AND t1.region_name = t2.region_name
# ORDER BY t1.demand_group, t1.region_name
# """

# spark.sql(query).toPandas().display()
