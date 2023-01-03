import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import json
import datetime as dt


def pull_xcoms_to_df(xcom_value, response_type ):
    logging.debug('xcom_value')
    key_value = json.loads(xcom_value) 
    logging.debug(type(key_value))
    
    logging.debug(key_value)
    if response_type == 'SimpleDict':
        df_key_value = pd.json_normalize([key_value])
    elif response_type == 'ListSimpleDIct':
        df_key_value = pd.json_normalize(key_value)        
    logging.debug(df_key_value.head())
    df_key_value.rename(columns=lambda x: x.replace(':@','').replace('.','_'), inplace=True)
    logging.debug(df_key_value.columns)
    return df_key_value 


def push_df_to_pg(df, table_nm,column_list, schema='af_local_raw'):
    pg_sql_hook= PostgresHook(postgres_conn_id='postgres_airflow', schema='booster') 
    pg_conn_engine= pg_sql_hook.get_sqlalchemy_engine()
    # postgres_sql_upload.insert_rows(table=table_nm, rows=df)
    # df.reset_index(drop=True, inplace=True)
    if len(column_list) > 0 :
        df_to_pg = df[column_list]
    else:
        df_to_pg = df       
    logging.debug(type(df))
    df_to_pg['loaded_at'] = dt.datetime.now()
    df_to_pg.to_sql(table_nm, pg_conn_engine, schema=schema, if_exists='append', index=False)
