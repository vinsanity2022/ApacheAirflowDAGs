# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG 
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator 
# This makes scheduling easy
from airflow.utils.dates import days_ago


# Define DAG arguments
default_args = {
    'owner': 'vins',
    'start_date': days_ago(0),
    'email': ['managbanagmelvin6@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
}

# Define DAG
dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)


# Define task

# Task 1.3
unzip_data = BashOperator(
    task_id= 'Unziping_data',
    bash_command= 'tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

#Task 1.4
extract_data_from_csv = BashOperator(
    task_id= 'extracting_data_from_csv',
    bash_command= 'cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

# Task 1.5
extract_data_from_tsv = BashOperator(
    task_id= 'extracting_data_from_tsv',
    bash_command= 'cut -d"\t" -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr "\t" ","  > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

#Task 1.6
extract_data_from_fixed_width = BashOperator(
    task_id= 'extracting_data_from_fixed_width',
    bash_command= 'cut -d" " -f10,11 /home/project/airflow/dags/finalassignment/payment-data.txt | tr " " "," > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

#Task 1.7
consolidate_data = BashOperator(
    task_id= 'consolidated_data',
    bash_command= 'paste -d"," /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# Task 1.8
transform_data = BashOperator(
    task_id= 'transforming_data',
    bash_command= 'cut -d"," -f4 | tr [a-z] [A-Z] < /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)


# Task 1.9
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

