from datetime import datetime, timedelta
# from airflow import DAG
from airflow.sdk import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python import PythonOperator

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


with DAG(
    "basic_dag_example",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "eric.cho@robos.one",
        "depends_on_past": False,    # 과거 실행은 무시함
        #  "start_date": datetime(2025, 8, 22),
        # "end_date": datetime(2030, 12, 31),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="tutorial_basic_operators",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 8, 22),
    catchup=False,
    tags=["example", "basic", "days"]
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )



# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }


# dag = DAG(
#     'hello_world',
#     default_args=default_args,
#     description='A simple hello world DAG',
#     schedule=timedelta(days=1),
#     catchup=False,
#     tags=['example', 'hello_world'],
# )





# def print_hello():
#     print("Hello World from Airflow!")
#     return "Hello World!"


# def print_date():
#     print(f"Current date and time: {datetime.now()}")
#     return datetime.now()


# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'hello_world',
#     default_args=default_args,
#     description='A simple hello world DAG',
#     schedule=timedelta(days=1),
#     catchup=False,
#     tags=['example', 'hello_world'],
# )

# hello_task = PythonOperator(
#     task_id='print_hello',
#     python_callable=print_hello,
#     dag=dag,
# )

# date_task = PythonOperator(
#     task_id='print_date',
#     python_callable=print_date,
#     dag=dag,
# )

# bash_task = BashOperator(
#     task_id='print_bash',
#     bash_command='echo "Hello from Bash!"',
#     dag=dag,
# )

# hello_task >> date_task >> bash_task
