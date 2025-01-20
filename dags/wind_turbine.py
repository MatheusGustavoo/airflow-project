from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

default_args = {
    'owner': 'Matheus',
    'depends_on_past': False,
    'email':['Seuemail@gmail.com'], 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
}

dag = DAG(
    'wind_turbine', 
    description='Data from wind turbine',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    doc_md="# Data from wind turbine"
    )

group_database = TaskGroup("group_database", dag=dag)
group_check = TaskGroup("group_check", dag=dag)


file_sensor_task = FileSensor(
    task_id='file_sensor',
    filepath=Variable.get('path_file'),      
    fs_conn_id='fs_default',
    mode='poke',
    poke_interval=5,
    dag=dag
)

@task
def process_file(**kwargs):
    with open(Variable.get('path_file')) as f:
        data = json.load(f)
        kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
        kwargs['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
        kwargs['ti'].xcom_push(key='hydraulicpressure', value=data['hydraulicpressure'])
        kwargs['ti'].xcom_push(key='timestamp', value=data['timestamp'])
        kwargs['ti'].xcom_push(key='temperature', value=data['temperature'])
    
    os.remove(Variable.get('path_file'))
    


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',
    sql='''
        CREATE TABLE IF NOT EXISTS sensors (
            idtemp VARCHAR(255),
            powerfactor VARCHAR(255),
            hydraulicpressure VARCHAR(255),
            timestamp VARCHAR(255),
            temperature VARCHAR(255)
        );
    ''',
    task_group=group_database,
    dag=dag
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgress',
    parameters=('{{ ti.xcom_pull(task_ids="process_file", key="idtemp") }}',
                '{{ ti.xcom_pull(task_ids="process_file", key="powerfactor") }}',
                '{{ ti.xcom_pull(task_ids="process_file", key="hydraulicpressure") }}',
                '{{ ti.xcom_pull(task_ids="process_file", key="timestamp") }}',
                '{{ ti.xcom_pull(task_ids="process_file", key="temperature") }}'),
    sql='''INSERT INTO sensors (idtemp, powerfactor, hydraulicpressure, timestamp, temperature) VALUES (%s, %s, %s, %s, %s);''',
    task_group=group_database,
    dag=dag
)

send_email_alert = EmailOperator(
    task_id='send_email_alert',
    to='Seuemail@gmail.com',
    subject='Airflow alert',
    html_content = '''
    <h3> Alerta de Temperatura. </h3>
    <p> DAg: wind_turbine </p>
    ''',
    task_group=group_check,
    dag =dag
)

send_email = EmailOperator(
    task_id='send_email',
    to='Seuemail@gmail.com',
    subject='Airflow advise',
    html_content = '''
    <h3> Temperatura dentro do padrão. </h3>
    <p> DAg: wind_turbine </p>
    ''',
    task_group=group_check,
    dag =dag
)


@task.branch(task_id='check_temp', task_group=group_check)
def check_temp(ti=None):
    nummber = float(ti.xcom_pull(task_ids='process_file', key='temperature'))
    if nummber >= 24:
        return 'group_check.send_email_alert'
    else:
        return 'group_check.send_email'
    

check = check_temp()
process = process_file()

with group_check:
    check >> [send_email, send_email_alert]
    
with group_database:
    create_table >> insert_data

file_sensor_task >> process
process >> group_check
process >> group_database