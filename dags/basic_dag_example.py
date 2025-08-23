"""
# Basic DAG Example

이 DAG는 Airflow의 기본적인 기능들을 보여주는 예시입니다.

## 포함된 태스크들:
- **print_date**: 현재 날짜를 출력하는 간단한 Bash 명령어
- **sleep**: 5초간 대기하는 태스크 (재시도 3회 설정)

## 특징:
- 매일 실행되는 스케줄 (daily)
- 과거 실행은 무시 (catchup=False)
- 소유자: eric.cho@robos.one
- 재시도: 1회 (5분 간격)

## 의존성:
print_date → sleep (순차 실행)
"""

import json
import pendulum
import textwrap
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 22, tz="UTC"),
    catchup=False,
    tags=["example", "basic", "days"],
    default_args={
        "owner": "eric.cho@robos.one",
        "depends_on_past": False,  # 과거 실행은 무시함
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="tutorial_basic_operators",
    doc_md=__doc__,
)
def basic_dag_example():

    @task.bash
    def print_date():
        """
        - print_date 태스크
        현재 시스템 날짜와 시간을 출력합니다.

        - 사용 명령어: `date`
        """
        return "date"

    @task()
    def extract():
        """
        - Extract task
        A simple Extract task to get data ready for the rest of the data pipeline. In this case, getting data is simulated by reading from a hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        - Transform task
        A simple Transform task which takes in the collection of order data and computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        """
        - Load task
        A simple Load task which takes in the result of the Transform task and instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")

    print_date()
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


basic_dag_example()


# with DAG(
#     "basic_dag_example",
#     # These args will get passed on to each operator
#     # You can override them on a per-task basis during operator initialization
#     default_args={
#         "owner": "eric.cho@robos.one",
#         "depends_on_past": False,    # 과거 실행은 무시함
#         #  "start_date": datetime(2025, 8, 22),
#         # "end_date": datetime(2030, 12, 31),
#         "retries": 1,
#         "retry_delay": timedelta(minutes=5),
#         "email_on_failure": False,
#         "email_on_retry": False,
#         # 'queue': 'bash_queue',
#         # 'pool': 'backfill',
#         # 'priority_weight': 10,
#         # 'end_date': datetime(2016, 1, 1),
#         # 'wait_for_downstream': False,
#         # 'execution_timeout': timedelta(seconds=300),
#         # 'on_failure_callback': some_function, # or list of functions
#         # 'on_success_callback': some_other_function, # or list of functions
#         # 'on_retry_callback': another_function, # or list of functions
#         # 'sla_miss_callback': yet_another_function, # or list of functions
#         # 'on_skipped_callback': another_function, #or list of functions
#         # 'trigger_rule': 'all_success'
#     },
#     description="tutorial_basic_operators",
#     # schedule=timedelta(days=1),
#     schedule=None,
#     start_date=datetime(2025, 8, 22),
#     catchup=False,
#     tags=["example", "basic", "days"]
# ) as dag:

#     dag.doc_md = __doc__

#     # t1, t2 and t3 are examples of tasks created by instantiating operators
#     t1 = BashOperator(
#         task_id="print_date",
#         bash_command="date",
#     )

#     # 다양한 문서화 방법들
#     t1.doc_md = textwrap.dedent(
#     """\
#     #### print_date 태스크
#     현재 시스템 날짜와 시간을 출력합니다.

#     **사용 명령어**: `date`
#     """
#     )


#     t2 = BashOperator(
#         task_id="sleep",
#         depends_on_past=False,
#         bash_command="sleep 5",
#         retries=3,
#     )

#     t2.doc_md = """
#     ### sleep 태스크
#     5초간 대기하는 태스크입니다.

#     - **대기 시간**: 5초
#     - **재시도**: 3회
#     - **용도**: 시간 지연 테스트
#     """

#     templated_command = textwrap.dedent(
#     """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#     {% endfor %}
#     """
#     )

#     t3 = BashOperator(
#         task_id="templated",
#         depends_on_past=False,
#         bash_command=templated_command,
#     )

#     t1 >> [t2, t3]
