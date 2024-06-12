
# Note:
# There is a new way of writing dags since the introduction of the TaskFlow API in 2020 (instead of BashOperator)
# https://airflow.apache.org/docs/apache-airflow/2.1.2/_modules/airflow/example_dags/tutorial_taskflow_api_etl.html

from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'test',
    'start_date': days_ago(0),
    'email': 'test@email.de',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

working_dir = '/home/project/airflow/dags'

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xf {working_dir}/finalassignment/tolldata.tgz -C {working_dir}',
    dag=dag,
)

extract_data_csv = BashOperator(
    task_id='extract_data_csv',
    bash_command=f'cut -d"," -f1-4 {working_dir}/vehicle-data.csv > {working_dir}/csv_data.csv',
    dag=dag,
)

extract_data_tsv = BashOperator(
    task_id='extract_data_tsv',
    bash_command=f'cut -f5-7 --output-delimiter="," {working_dir}/tollplaza-data.tsv > {working_dir}/tsv_data.csv',
    dag=dag,
)

extract_data_fixed_width = BashOperator(
    task_id='extract_data_fixed_width',
    bash_command=(
        f'cat {working_dir}/payment-data.txt | tr -s "[:space:]" '
        f'| cut -d" " -f11-12 --output-delimiter="," '
        f'> {working_dir}/fixed_width_data.csv'
    ),
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        f'paste -d "," {working_dir}/csv_data.csv '
        f'{working_dir}/tsv_data.csv '
        f'{working_dir}/fixed_width_data.csv '
        f'| tr -d "\\r" '
        f'> {working_dir}/extracted_data.csv '
    ),
    dag=dag,
)

transform_data =  BashOperator(
    task_id='transform_data',
    bash_command=(
        f'cat {working_dir}/extracted_data.csv '
        f'| cut -d "," -f4 '
        f'| tr "[:lower:]" "[:upper:]" '
        f'> {working_dir}/finalassignment/staging/transformed_data.csv'
    ),
    dag=dag,
)



unzip_data >> extract_data_csv >> extract_data_tsv \
>> extract_data_fixed_width >> consolidate_data >> transform_data
