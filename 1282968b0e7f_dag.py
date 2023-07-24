from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

#Defining the parameters for the Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/amymallett8@gmail.com/Pintrest Pipeline Project',
}


#Defining the parameters for the Run Now Operator
notebook_params = {
    "Variable":5
}

default_args = {
    'owner': 'Amy Mallett',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('1282968b0e7f_dag',
    #datetime format
    start_date=datetime(2023, 7, 24),
    #check out possible intervals, to be a string
    schedule_interval='*/1 * * * *',
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run