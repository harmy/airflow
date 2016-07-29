"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from pprint import pprint

dag_id = 'payout'
schedule_interval = '*/1 * * * *'
default_args = {
    'owner': 'gpay',
    'depends_on_past': False,
    'start_date': datetime(2016, 7, 20),
    'email': ['zhengzhoumin@mobifun365.net'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval)


def print_context(**kwargs):
    pprint(kwargs)
    return 'Whatever you return gets printed in the logs'

# t1, t2 and t3 are examples of tasks created by instatiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

t3 = PostgresOperator(
    task_id='get_connections',
    sql='test.sql',
    dag=dag
)

t4 = PythonOperator(
    task_id='print_template',
    provide_context=True,
    python_callable=print_context,
    templates_dict={'hi': 'HI!'},
    dag=dag
)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t1)