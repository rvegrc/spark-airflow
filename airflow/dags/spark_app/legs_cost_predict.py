def run():
    import json
    import pandas as pd
    import os
    import sys
    import sklearn
    import datetime
    from datetime import datetime as dt
    import numpy as np

    import io
    import sys

    import clickhouse_connect

    from pprint import pprint
    import pytz

    from pydantic import BaseModel, Field
    from typing import List, Dict, Any, Optional

    import logging


    # # eda
    # import phik


    # visualization
    import matplotlib.pyplot as plt
    import seaborn as sns

    # ml
    # import xgboost as xgb
    import catboost as ctb
    # import lightgbm as lgb

    import joblib

    # import optuna
    # from optuna.visualization.matplotlib import plot_param_importances

    import mlflow
    import mlflow.sklearn
    from mlflow.models.signature import infer_signature


    # start mlflow server in terminal `mlflow server`
    # client = mlflow.MlflowClient(tracking_uri='http://127.0.0.1:8888') # for saving mlruns in local webserver
    # mlflow.set_tracking_uri='http://127.0.0.1:8888'

    # root_path = "/all/mlruns"# for docker container folder




    # set mlflow tracking uri
    # your_mlflow_tracking_uri = f'{root_path}/mlruns'




    from sklearn.model_selection import cross_val_score
    from sklearn.model_selection import train_test_split

    from sklearn.pipeline import Pipeline as skl_pipeline

    from sklearn.base import BaseEstimator, TransformerMixin

    # from imblearn.pipeline import Pipeline as imb_pipeline
    # from imblearn.over_sampling import SMOTE

    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import OneHotEncoder, PowerTransformer, OrdinalEncoder, StandardScaler, RobustScaler

    sklearn.set_config(transform_output='pandas')

    # load metrics
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

    # turn off warnings
    import warnings
    warnings.filterwarnings('ignore')


    # set all columns to be displayed
    pd.set_option('display.max_columns', None)


    # from dotenv import load_dotenv
    # load_dotenv()

    # constants
    RAND_ST = 345
    CH_USER = os.getenv("CH_USER")
    CH_PASS = os.getenv("CH_PASS")
    CH_IP = os.getenv('CH_IP')
    # AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    # AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    # AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
    # MLFLOW_S3_ENDPOINT_URL = os.getenv('MLFLOW_S3_ENDPOINT_URL')
    # AWS_S3_ENDPOINT_URL=os.getenv('AWS_S3_ENDPOINT_URL')
    # os.environ["AWS_S3_SIGNATURE_VERSION"] = "s3v4"


    MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI')
    # MLFLOW_TRACKING_URI = 'http://localhost:15000'
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


    # Define the timezone
    EXP_TIMEZONE = pytz.timezone('Etc/GMT-3')


    # get connection to db
    client = clickhouse_connect.get_client(host=CH_IP, port=8123, username=CH_USER, password=CH_PASS)

    # import tools
    # Get the parent directory
    parent_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))

    # Add parent directory to sys.path
    sys.path.append(parent_dir)


    from tools import pd_tools, db_tools, paths
    from tools.db_tools import DbTools
    from custom_transformers import SafePowerTransformer

    db_tools = DbTools(data_path, tmp_path, client)

    
    # import paths
    from paths import Paths
    root_path = '.' # for local folder
    paths = Paths(root_path)
    data_path = paths.data_path
    tmp_path = paths.tmp_path
    prod_data_path = paths.prod_data_path
    dev_data_path = paths.dev_data_path
    prod_db = paths.prod_db
    dev_db = paths.dev_db



    ## Load data from db

    lc_result = client.query_df(f'select * from {dev_db}.legs_costs_result')


    ## Select cat and num cols

    # find columns with dtypes=bool
    bin_cols = lc_result.select_dtypes(include='bool').columns

    # find columns with dtypes=numeric
    num_cols_all = lc_result.select_dtypes(exclude=['bool', 'object']).columns


    target = 'ce_cost'

    cat_cols = [
        'l_fk_transp_stages'
        ,'l_fk_modality'
        ,'ltc_fk_vat_rates'
        ,'pr_code_dep'
        ,'pr_fk_point_types_dep'
        ,'pr_code_dest'
        ,'pr_fk_point_types_dest'
        ,'ts_fk_margins'
        ,'cr_fk_services_rail'
        ,'cr_fk_measure_units'
        ,'cr_fk_currencies'
        ,'ce_fk_counterparty'
        ,'ef_code_fk_gng_freights'
        ,'cargo_guarded'
        ,'ts_date_start_year'
        ,'ts_date_start_month'
        ,'ts_date_start_day'
        ,'ts_date_finish_year'
        ,'ts_date_finish_month'
        ,'ts_date_finish_day'
    ] + bin_cols.tolist()


    num_cols = num_cols_all.difference(cat_cols + [target]).to_list()


    def cols_check(df: pd.DataFrame, cols: list) -> list:
        '''Return cols are only in df'''
        return [col for col in cols if col in df.columns]

    def cols_check_diff(df: pd.DataFrame, cat_cols: list, num_cols: list) -> tuple[list]:
        '''Check if columns are in df and return checked columns and errors'''
        cols_err = []
        cat_cols_checked = cols_check(df, cat_cols)
        num_cols_checked = cols_check(df, num_cols)
        feature_cols = num_cols_checked + cat_cols_checked    
        cols_err.append([(f'col={col} not in df') for col in feature_cols if col not in df.columns])
        cols_err.append([(f'col={col} not in feature_cols') for col in df.columns if col not in (feature_cols + [target])])

        return cat_cols_checked, num_cols_checked, cols_err


    ## Train test split

    def tt_split(df: pd.DataFrame, target: str, test_size: float, rand_st: int)-> tuple:
        """Split data to train and test and stratify by target binned column

        Args:
            df (dataframe): dataframe with target column
            target (str): name of target column in df
            test_size (float, optional): size of test data. Defaults to 0.2.
            rand_st (int, optional): random state. 

        Returns:
            tuple: train and test data
        """  

    

        # bin the continuous values of pe_price into categories
        for n in range(0, 81):
            quintile = 20 + n
            y_binned = pd.qcut(df[target], q=quintile, labels=False, duplicates='drop') # q=20 binned by 5%
            # find populated class in y has only 1 member
            if min(y_binned.value_counts()) == 1 and quintile < 100:
                continue
            elif quintile < 100:
                y_binned = pd.qcut(df[target], q=quintile, labels=False, duplicates='drop') # if q=20 then binned by 5%
                X = df.drop(target, axis=1)
                y = df[target]
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=rand_st, stratify=y_binned)
                break  # Exit the loop after successful split
            elif quintile == 100:
                # stratify not necessary
                X = df.drop(target, axis=1)
                y = df[target]
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=rand_st)
                break  # Exit the loop after successful split
        
        print(quintile)
        return X_train, X_test, y_train, y_test

    ## Make pipeline for training

    def clear_col_names(df: pd.DataFrame) -> pd.DataFrame:
        '''Clear column names from special characters "num" and "reminder" from transformation'''
        cols = []
        for col in df.columns:
            cols.append(col.replace('num__', '').replace('reminder__', ''))
        df.columns = cols
        return df

    def pipe_m_s(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series, y_test: pd.Series
                ,cat_cols_checked: list, num_cols_checked: list) -> tuple:
        '''Create training pipeline for CatBoostRegressor'''

        # preprocess data
        # create transformers
        num_transformer = skl_pipeline(steps=[
            ('safepower', SafePowerTransformer())
                        ]
                        )

        # create preprocessor
        preprocessor = ColumnTransformer(
            remainder='passthrough',
            transformers=[
                ('num', num_transformer, num_cols_checked),
                # ('cat', OneHotEncoder(handle_unknown='ignore'), cat_cols_checked)
            ]
        )

        # Preprocess the training data for catboost
        X_train_prep = preprocessor.fit_transform(X_train)
        X_test_prep = preprocessor.transform(X_test)


        # list of loss func if you find optimal loss function
        loss_funcs = [
            'MAPE'
            # ,'MAPE' # for analyze
            # ,'Poisson'
            # ,'Quantile' # for analyze
            # ,'MultiQuantile'
            # ,'RMSE' # for analyze
            # ,'RMSEWithUncertainty'
            # ,'LogLinQuantile'
            # ,'Lq'
            # ,'Huber'
            # ,'Expectile'
            # ,'Tweedie'
            # ,'LogCosh'   # for analyze
            # ,'SurvivalAft'
        ]

        # models = {} # dict for saving models
        # best = [] # list for saving best iterations

        # # back loop for loss_func analyze

        # for loss_func in loss_funcs:

            # # back loop for loss_func analyze

        # catboost params
        params = {
            'random_state': RAND_ST
            ,'cat_features': ['remainder__' + col for col in cat_cols_checked]
            ,'n_estimators': 1000 # 500
            ,'depth':10 # 16
            ,'use_best_model': True
            ,'loss_function': 'RMSE'
            # ,f'custom_metric': ['RMSE' # metric for best iteration
            #                     ,'MAPE'
            #                     ,'Poisson'
            #                     ,'Quantile'                                                
            #                     ,'RMSEWithUncertainty'
            #                     ,'LogLinQuantile']
            ,'eval_metric': 'RMSE'
            # ,'learning_rate': 0.5
            # ,'grow_policy': 'Depthwise'  
        }

        X_train = clear_col_names(X_train_prep)
        X_test = clear_col_names(X_test_prep)

    

        print(f'Loss function: {loss_funcs}')
        model = ctb.CatBoostRegressor() # create new pipe
        params['loss_function'] = loss_funcs[0] # update loss function in params for pipe
        model.set_params(**params) # set updated params in pipe
        model.fit(X_train, y_train, eval_set=(X_test, y_test))

        best_iteration = {f"best_iteration_{params['loss_function']}": model.get_best_iteration()}
        try: 
            best_score_rmse = (mean_squared_error(y_test, model.predict(X_test)))**0.5 # count RMSE for predicted testing data
        except:
            best_score_rmse = (mean_squared_error(y_test, 
                                                pd.DataFrame(model.predict(X_test)).apply(lambda x: x.mean(), axis=1))
                                )**0.5 # for RMSEWithUncertainty
        best_iteration['loss_function_pipe'] = loss_funcs
        best_iteration[f'best_score_rmse'] = best_score_rmse
        # best.append(best_iteration)
        # models[f'ctb_reg_model_name_{loss_func}'] = ctb_reg_pipe_name # save model in dict

        best_iteration = pd.DataFrame(best_iteration, index=[0])
    
        return model, preprocessor, best_iteration

    def target_balance(df: pd.DataFrame, target: str, measure: int, service: int) -> pd.DataFrame:
        """Balance of target column in df and save it to csv file

        Args:
            df (dataframe): dataframe with target column
            target (str): name of target column in df
            measure (int): measure unit
            service (int): service id
        """        
        target_valc = df[target].value_counts()
        counts_sum = target_valc.values.sum()
        percent = target_valc.values / counts_sum
        target_valc_balance = pd.DataFrame({'measure': measure, 'service': service, target: target_valc.index.tolist(), 
                                    'counts': target_valc.values.tolist(), 'percent_counts': percent.tolist()})
        return target_valc_balance


    ## Train models

    # drop cols with only one unique value
    lc_prep = lc_result.drop(columns=lc_result.columns[lc_result.nunique() == 1])

    def check_and_log_artifact(artifact_path):
        """
        Check if the artifact file exists and log it to MLflow if it does.
        If the file does not exist, print a warning message.
        """
        if os.path.exists(artifact_path):
            mlflow.log_artifact(artifact_path)
        else:
            print(f"Warning: File does not exist and will not be logged: {artifact_path}")

    def mlflow_start_run(measure: int, service: int, model: BaseEstimator, preprocessor: BaseEstimator
                        ,metrics: dict, tmp_path: str, err_name: str, target: str):
        """
        Start an MLflow run, log the model, preprocessor, parameters, metrics, and artifacts.
        """
        model_name = model.__class__.__name__    

        with mlflow.start_run(run_name=f'{measure}_{service}_{model_name}') as run:
            # Log the preprocessor and model
            mlflow.sklearn.log_model(preprocessor, 'preprocessor')
            mlflow.sklearn.log_model(model, model_name)
            
            # Log model parameters and metrics
            mlflow.log_params(model.get_params())
            mlflow.log_metrics(metrics)
            
            # Define artifacts to log
            artifacts_to_log = [
                f'{tmp_path}/{measure}_{service}_feature_importance.png',
                f'{tmp_path}/{measure}_{service}_feature_importance.csv',
                f'{tmp_path}/{measure}_{service}_{err_name}_hist.png',
                f'{tmp_path}/{measure}_{service}_best_iter_rmse.csv',
                f'{tmp_path}/{measure}_{service}_{target}_balance.csv',
                f'{tmp_path}/{measure}_{service}_rel_errors_ms.csv',
                f'{tmp_path}/{measure}_services_value_counts.csv'
            ]
            
            # Log each artifact if it exists
            for artifact_path in artifacts_to_log:
                check_and_log_artifact(artifact_path)
        
        return run.info.run_id

    def log_to_mlflow(measure: int, service: int, model: BaseEstimator, preprocessor: BaseEstimator
                    ,metrics: dict, tmp_path: str, err_name: str, target: str, show_output: bool = True):
        """
        Log the model, preprocessor, metrics, and artifacts to MLflow.
        If `show_output` is False, suppress the output.
        """
        
        # Start MLflow run and log everything (output will be show or suppressed)
        if show_output == True:
            # Start MLflow run and log everything (output will be shown)
            run_id = mlflow_start_run(
                measure, service, model, preprocessor, metrics, tmp_path, err_name, target
            )
        else:
            print("Output is not shown")
            # Redirect stdout to suppress output
            old_stdout = sys.stdout
            sys.stdout = io.StringIO()

            # Start MLflow run and log everything
            run_id = mlflow_start_run(
                measure, service, model, preprocessor, metrics, tmp_path, err_name, target
            )
            # Reset stdout
            sys.stdout = old_stdout
        
        return run_id
    
    def save_plot(path: str):
        plt.savefig(path)
        plt.close()
        # clear plot
        plt.clf()

    def calculate_relative_errors(y_true: pd.Series, y_pred: pd.Series) -> tuple:
        '''Calculate relative errors'''
        rel_err_abs = (abs(y_true - y_pred) / y_true)
        rel_err = ((y_true - y_pred) / y_true)
            
        return rel_err_abs, rel_err


    mlflow.set_experiment(f'{dt.now(EXP_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")}')


    # load existing ms_mod_val from db
    ms_mod_val_exists = client.query_df(f'''
    select
        m.measure
        ,m.service
        ,m.model_run_id
        ,m.value
        ,first_value(m.mape) over (
            partition by m.measure, m.service 
            order by m.mape, m.date desc
        ) as mape
        ,m.flag
        ,m.date  
    from {dev_db}.ms_mod_val m  
        ''')



    ms_mod_val_new = []

    measurements = lc_prep['cr_fk_measure_units'].unique().tolist()

    # check columns
    cat_cols_checked, num_cols_checked, cols_err = cols_check_diff(lc_prep, cat_cols, num_cols)

    if (len(cols_err[0]) + len(cols_err[1])) > 0:
        print(cols_err)
    else:
        print('cat_cols and num_cols check passed')

        rel_errs_ams = [] # list of relative errors for all measures and services


        for measure in measurements[:]:
            lc_meas = lc_prep.query(f'cr_fk_measure_units == {measure}')
            # list of services
            services = lc_meas['cr_fk_services_rail'].unique().tolist()

            # balance of services
            serv_valc = lc_meas['cr_fk_services_rail'].value_counts()
            services_value_counts = pd.DataFrame({'measure': measure, 'service': serv_valc.index.tolist(), 'service_counts': serv_valc.values.tolist()})
            services_value_counts.to_csv(f'{tmp_path}/{measure}_services_value_counts.csv', index=False)
    
            
            for service in services[:]:
            
                lc_meas_serv = lc_meas.query(f'cr_fk_services_rail == {service}').copy()

                # balance of target column
                target_valc_balance = target_balance(lc_meas_serv, target, measure, service)
                target_valc_balance.to_csv(f'{tmp_path}/{measure}_{service}_{target}_balance.csv', index=False)

                # check if price is not unique
                if lc_meas_serv[target].nunique() > 1:
    
                    print(measure, service)
                    
                    # split data to train and test
                    X_train, X_test, y_train, y_test = tt_split(lc_meas_serv, target, test_size=0.2, rand_st=RAND_ST)

                    # create model + preprocessor
                    model, preprocessor, best_iteration = pipe_m_s(X_train, X_test, y_train, y_test
                                                ,cat_cols_checked, num_cols_checked
                                                )     


                    # save best iteration to csv
                    best_iteration.to_csv(f'{tmp_path}/{measure}_{service}_best_iter_rmse.csv', index=False)

                    X_test_prep = preprocessor.transform(X_test)
                    X_test_prep_rn = clear_col_names(X_test_prep)
                    predict_test = model.predict(X_test_prep_rn)


                    # Define input example and infer signature for model and preprocessor
                    inp_examp_prep = X_test.iloc[0:1]
                    # sign_prep = infer_signature(inp_examp_prep, X_test_prep_rn.iloc[0:1])
                    inp_examp_model = X_test_prep_rn.iloc[0:1]
                    # sign_model = infer_signature(inp_examp_model, pd.DataFrame(predict_test))
                

                    # save ts_id for future analyze and drop index
                    ts_id = lc_prep.loc[y_test.index]['ts_id'].reset_index(drop=True)

                    # create dataframe because some target has zero values
                    tmp_df = pd.concat([pd.DataFrame(y_test).reset_index(drop=True), pd.DataFrame(predict_test)], axis='columns')

                    tmp_df = tmp_df.rename(columns={0: f'{target}_predicted'})

                    # for target = 0 relative errors will be infinities. Thats why we need to divide dataset to 2 parts

                    try:
                        # plus 1 to avoid division by zero

                        # check if zeroes in y_test
                        if tmp_df[f'{target}'].min() == 0:                        
                            # divide dataset to 2 parts
                            tmp_df_0 = tmp_df[tmp_df[target] == 0]
                            tmp_df_1 = tmp_df[tmp_df[target] != 0]

                            # count relative errors
                            rel_err_abs_0 = abs(tmp_df_0[f'{target}_predicted'])
                            rel_err_0 = tmp_df_0[f'{target}_predicted']
                            
                            rel_err_abs_1, rel_err_1 = calculate_relative_errors(tmp_df_1[target], tmp_df_1[f'{target}_predicted'])
                            # concat relative errors
                            rel_err_abs = pd.concat([rel_err_abs_0, rel_err_abs_1])
                            rel_err = pd.concat([rel_err_0, rel_err_1])

                        else:           
                            rel_err_abs, rel_err = calculate_relative_errors(y_test, predict_test)

                    except:
                        predict_test = pd.DataFrame(predict_test).apply(lambda x: x.mean(), axis=1) # for RMSEWithUncertainty
                        rel_err_abs, rel_err = calculate_relative_errors(y_test, predict_test)

                    # drop index for correct concat
                    rel_err_abs = rel_err_abs.reset_index(drop=True)
                    rel_err = rel_err.reset_index(drop=True)


                    dict_errs = {'ts_id': ts_id,'measure': measure, 'service': service, 'rel_err_abs': rel_err_abs, 'rel_err': rel_err}

                    rel_errs_ms = pd.DataFrame(dict_errs)

                    # save relative errors to csv for artifact
                    rel_errs_ms.to_csv(f'{tmp_path}/{measure}_{service}_rel_errors_ms.csv', index=False)
    
                    rel_errs_ms_descr_abs = rel_errs_ms['rel_err_abs'].describe()
                    rel_errs_ms_descr = rel_errs_ms['rel_err'].describe()


                            
                    # feature_importances and save plot to png and save data to csv
                    # pd_tools.get_feature_importances(model, X_test_prep, target, tmp_path)

                    feature_importances = pd_tools.get_feature_importances(model, X_test)

                    # save feature importances to csv
                    feature_importances.to_csv(f'{tmp_path}/{measure}_{service}_feature_importance.csv', index=False)
                    
                    # plot feature importances and save to png
                    plt.figure(figsize=(10, 10))
                    sns.barplot(x=feature_importances['importance'], y=feature_importances['feature'], palette='husl')
                    # Adjust layout to prevent clipping
                    plt.tight_layout()
                    save_plot(f'{tmp_path}/{measure}_{service}_feature_importance.png')              
                    
                    err_name = 'relative_error_abs'

                    # plot histogram of relative error and save to png
                    pd_tools.plot_hist_mm_lines(rel_errs_ms['rel_err_abs'], err_name, 'numeric')

                    save_plot(f'{tmp_path}/{measure}_{service}_{err_name}_hist.png')

                    metrics = {
                        'mse': mean_squared_error(y_test, predict_test)    
                        ,'mape': (abs(y_test - predict_test) / y_test).mean()
                        ,'r2': r2_score(y_test, predict_test)
                        # Log relative error descriptions
                        ,"mean_rel_err": rel_errs_ms_descr['mean']
                        ,"std_rel_err": rel_errs_ms_descr['std']
                        ,"min_rel_err": rel_errs_ms_descr['min']
                        ,"q1_rel_err": rel_errs_ms_descr['25%']
                        ,"q2_rel_err": rel_errs_ms_descr['50%']
                        ,"q3_rel_err": rel_errs_ms_descr['75%']
                        ,"max_rel_err": rel_errs_ms_descr['max']
                        # Log absolute relative error descriptions
                        ,"mean_rel_err_abs": rel_errs_ms_descr_abs['mean']
                        ,"std_rel_err_abs": rel_errs_ms_descr_abs['std']
                        ,"min_rel_err_abs": rel_errs_ms_descr_abs['min']
                        ,"q1_rel_err_abs": rel_errs_ms_descr_abs['25%']
                        ,"q2_rel_err_abs": rel_errs_ms_descr_abs['50%']
                        ,"q3_rel_err_abs": rel_errs_ms_descr_abs['75%']
                        ,"max_rel_err_abs": rel_errs_ms_descr_abs['max']
                        
                    }
        
                    # log to mlflow
                    run_id = log_to_mlflow(
                        measure, service, model, preprocessor
                        #    ,model, inp_examp_model
                        #    ,preprocessor, inp_examp_prep
                        ,metrics, tmp_path, err_name, target
                        ,show_output=False                    
                    )


                    mape_new = rel_errs_ms['rel_err_abs'].mean()
                    # check if mape exists in db for measure and service
                    if ms_mod_val_exists.query(f'measure == {measure} and service == {service}')['mape'].empty:
                        ms_mod_val_new.append({'measure': measure, 'service': service, 'model_run_id': run_id, 
                                            'value': -100,  'mape': mape_new, 'flag': 1, 
                                            'date': pd.to_datetime(dt.now(EXP_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"))})
                    else:                              
                        mape_exist = ms_mod_val_exists.query(f'measure == {measure} and service == {service}')['mape'].values[0]
                    
                    # check if mape for new model is less than mape for existing model and set threshold for mape
                        if mape_new / mape_exist < 0.99:
                            # add model id from mlflow
                            ms_mod_val_new.append({'measure': measure, 'service': service, 'model_run_id': run_id, 
                                                'value': -100,  'mape': mape_new, 'flag': 1, 
                                                'date': pd.to_datetime(dt.now(EXP_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"))})

                    rel_errs_ms['run_id'] = run_id
                    
                    # add relative errors
                    rel_errs_ams.append(rel_errs_ms)

                    # sleep(60)   
                    
                else:
                    # add service with only one price
                    # check if price doesn't exists in db for measure and service and add it
                    if ms_mod_val_exists.query(f'measure == {measure} and service == {service}').empty:
                        ms_mod_val_new.append({'measure': measure, 'service': service, 'model_run_id': 'no_model', 
                                        'value': lc_meas_serv[target].unique()[0], 'mape': 100, 'flag': 0, 
                                        'date': pd.to_datetime(dt.now(EXP_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"))})
        

        # pd.DataFrame(ms_mod_val).to_csv(f'{tmp_path}/ms_mod_val.csv', index=False)
    
    #    upload new id models and values to db if it's not empty
    if len(ms_mod_val_new) > 0:
        db_tools.upload_to_clickhouse(pd.DataFrame(ms_mod_val_new)
                                        ,f'{dev_db}' ,'ms_mod_val', 'add', iana_timezone='Etc/GMT-3')
    

    rel_errs_ams = pd.concat(rel_errs_ams)

    # upload to db
    db_tools.upload_to_clickhouse(rel_errs_ams
                                        ,'tmp','rel_errs_ams', 'add', iana_timezone='Etc/GMT-3')
    return

# start only if you run the file, if the file would be instead imported the run command will be ignored 
if __name__ == "__main__":
    run()