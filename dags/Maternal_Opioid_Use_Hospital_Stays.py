from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator, HttpHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import pandas as pd
import logging , json
import utils as ut
from airflow.models import Variable


default_args = {
    'owner': 'data',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG('Maternal_Opioid_Use_Hospital_Stays', description='Dag to pull data from for maternal opioid usage and process the data pipeline',
          schedule_interval='0 12 * * *',
          start_date=datetime(2023, 1, 1), catchup=True, max_active_runs=3,
          default_args=default_args)

get_data = SimpleHttpOperator( 
        task_id='get_data',
        method='GET',
        endpoint='/resource/r2n4-n2i4.json',
        http_conn_id = 'pa_gov',
        dag=dag,
        do_xcom_push=True      
    )          

def pull_response_load_df_to_pg(**kwargs):
    ti = kwargs['ti']
    key = kwargs['key']
    response_type = kwargs['response_type']
    table_nm = kwargs['table_nm']
    column_list = kwargs['column_list']


    xcom_value = ti.xcom_pull(key=None, task_ids=kwargs['task_id'])
    logging.debug(xcom_value)
        
    response_df = ut.pull_xcoms_to_df(xcom_value, response_type)
    logging.debug(type(response_df))
    ut.push_df_to_pg(df=response_df, table_nm=table_nm , column_list=column_list) 

start = EmptyOperator(task_id= "start", dag=dag)

end = EmptyOperator(task_id= "end", dag=dag) 


load_stats_to_pg = PythonOperator( 
    task_id='load_stats_to_pg',
    provide_context=True, #provide context is for getting the TI (task instance ) parameters
    dag=dag,
    python_callable=pull_response_load_df_to_pg,
    op_kwargs={'task_id': 'get_data',
                'key' : '' ,
                'response_type': 'ListSimpleDIct',
                'table_nm': 'maternal_opioid_use_hospital_stays',
                'column_list': []
                })  
config_file = '/opt/airflow/include/.kube/config'
in_cluster = False

dbt_run = KubernetesPodOperator(
    name="dbt_run",
    image="ipsingh88/local_dbt",
    cmds=["bash", "-cx"],
    arguments=["dbt run --project-dir /dbt/ -s maternal_opioid_use_hospital_stays --profiles-dir /dbt/profile"],
    labels={"app": "dbt"},
    task_id="dbt_run",
    in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
    cluster_context="docker-desktop",  # is ignored when in_cluster is set to True
    config_file=config_file,
    is_delete_operator_pod=True,
    get_logs=True,
    namespace='default',
    env_vars={
            'DBT_DBNAME_PRD': Variable.get("DBT_DBNAME_PRD"),
            'DBT_SCHEMA': Variable.get("DBT_SCHEMA"),
            'DBT_USER_PRD': Variable.get("DBT_USER_PRD"),
            'DBT_ENV_SECRET_PWD_PRD': Variable.get("DBT_ENV_SECRET_PWD_PRD")
        },
)

start >> get_data >> load_stats_to_pg >> dbt_run >> end
