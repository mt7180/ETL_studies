
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'mira',
    'start_date': days_ago(0),
    'email': 'mira.theidel@gmx.de',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'access_log_processing',
    default_args=default_args,
    description='ETL Server Access Log Processing',
    schedule_interval=timedelta(days=1),
)

download = BashOperator(
    task_id='download_data',
    bash_command='curl -O https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt',
    dag = dag,
)

extract = BashOperator(
    task_id='extract_data',
    bash_command='cut -d"#" -f1,4 web-server-access-log.txt > extracted-data.txt',
    dag=dag,
)

transform = BashOperator(
    task_id='transform_data',
    bash_command='tr "#" "," < extracted-data.txt | tr "[:lower:]" "[:upper:]" > transformed-data.txt',
    dag=dag,
)

load = BashOperator(
    task_id='load_data',
    bash_command='zip log.zip transformed-data.txt',
    dag=dag,
)

download >> extract >> transform >> load



# Note!
# There is a new way of writing dags since the introduction of the TaskFlow API in 2020 (instead of Bash- or PythonOperator)
# https://airflow.apache.org/docs/apache-airflow/2.1.2/_modules/airflow/example_dags/tutorial_taskflow_api_etl.html


# import json

# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago


# # These args will get passed on to each operator
# # You can override them on a per-task basis during operator initialization
# default_args = {
#     'owner': 'airflow',
# }


# @dag(
#     default_args=default_args, 
#     schedule_interval=None, 
#     start_date=days_ago(2), tags=['example']
# )
# def tutorial_taskflow_api_etl():
   
#     @task()
#     def extract():
#         ...
        
#     @task(multiple_outputs=True)
#     def transform(order_data_dict: dict):
#         ...
    
#     @task()
#     def load(total_order_value: float):
#         ...

#     # e.g. :
#     order_data = extract()
#     order_summary = transform(order_data)
#     load(order_summary["total_order_value"])

# tutorial_etl_dag = tutorial_taskflow_api_etl()

