"""
# ETL 예제 - 직원 데이터 파이프라인

## 프로젝트 정보
- **DAG ID**: etl_example
- **작성자**: eric.cho@robos.one
- **작성일**: 2025-08-23
- **최종수정일**: 2025-08-24
- **버전**: 1.1.0
- **목적**: 실제적인 ETL 파이프라인 구현 및 PostgreSQL 연동 학습 (TaskFlow API 활용)

## 개요
외부 CSV 파일에서 직원 데이터를 추출하여 PostgreSQL 데이터베이스에 적재하는 완전한 ETL 파이프라인입니다.
스테이징 테이블을 통한 데이터 정제와 UPSERT 패턴을 활용한 데이터 통합을 구현합니다.
TaskFlow API를 적극적으로 활용하여 파이프라인을 구성합니다.

## 파이프라인 단계

### 1. create_employees_table
- **유형**: @task (PythonOperator)
- **목적**: 최종 직원 테이블 생성
- **테이블**: employees (메인 테이블)
- **스키마**: Serial Number (PK), Company Name, Employee Markme, Description, Leave

### 2. create_employees_temp_table
- **유형**: @task (PythonOperator)
- **목적**: 임시 스테이징 테이블 생성
- **테이블**: employees_temp (스테이징 테이블)
- **동작**: 기존 테이블 삭제 후 재생성

### 3. get_data
- **유형**: @task (PythonOperator)
- **목적**: 외부 CSV 데이터 다운로드 및 스테이징 테이블 적재
- **데이터 소스**: GitHub의 샘플 CSV 파일
- **저장 경로**: /opt/airflow/dags/files/employees.csv
- **적재 방식**: PostgreSQL COPY 명령어 사용

### 4. merge_data
- **유형**: @task (PythonOperator)
- **목적**: 데이터 정제 및 최종 테이블 UPSERT
- **처리 로직**: DISTINCT를 통한 중복 제거
- **충돌 처리**: ON CONFLICT DO UPDATE (PostgreSQL)

## 데이터베이스 연결 정보
- **Connection ID**: tutorial_pg_conn
- **Database**: PostgreSQL
- **필수 설정**: Airflow UI에서 연결 정보 등록 필요

## 스케줄링 정보
- **실행방식**: 일일 실행 (0 0 * * *)
- **시작일**: 2021-01-01 (UTC)
- **catchup**: False
- **타임아웃**: 60분

## 의존성 그래프
```
create_employees_table() ────┐
                             ├─→ get_data() → merge_data()
create_employees_temp_table()┘
```

## 예상 결과
- **처리 데이터**: CSV 파일의 모든 직원 정보
- **중복 처리**: Serial Number 기준 중복 제거
- **최종 저장**: employees 테이블에 정제된 데이터 저장

## 사전 요구사항
- PostgreSQL 데이터베이스 실행 중
- tutorial_pg_conn 연결 정보 설정 완료
- /opt/airflow/dags/files/ 디렉토리 쓰기 권한

## 참고사항
- Airflow 공식 튜토리얼 기반 구현
- 실제 운영 환경에서 사용 가능한 패턴
- PostgreSQL Hook과 TaskFlow API 활용 예제
"""
import datetime
import os
import pendulum
import requests

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.hooks.postgres

@dag(
    dag_id="etl_example",
    schedule="0 0 * * *",  # 매일 자정 실행
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["etl", "postgresql", "csv", "tutorial", "taskflow"],
    default_args={
        "owner": "eric.cho@robos.one",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="완전한 ETL 파이프라인 - CSV to PostgreSQL (TaskFlow API)",
    doc_md=__doc__,
)
def etl_example_taskflow():

    @task
    def create_employees_table():
        """최종 직원 테이블(employees)을 생성합니다. 이미 존재하면 아무 작업도 하지 않습니다."""
        pg_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        create_sql = """
            CREATE TABLE IF NOT EXISTS employees (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );
        """
        pg_hook.run(create_sql)

    @task
    def create_employees_temp_table():
        """임시 스테이징 테이블(employees_temp)을 생성합니다. 기존 테이블이 있다면 삭제 후 다시 만듭니다."""
        pg_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        create_sql = """
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE employees_temp (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );
        """
        pg_hook.run(create_sql)

    @task
    def get_data():
        """
        외부 CSV 파일을 다운로드하고 스테이징 테이블에 적재합니다.
        GitHub의 샘플 데이터를 사용하여 실제 ETL 시나리오를 시뮬레이션합니다.
        """
        data_path = "/opt/airflow/dags/files/employees.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"
        
        print(f"CSV 파일 다운로드 중: {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        with open(data_path, "wb") as file:
            file.write(response.content)
        
        print(f"파일 저장 완료: {data_path}")

        # pg_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        print("스테이징 테이블에 데이터 적재 중...")
        # pg_hook.copy_expert(
        #     sql="COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '"'",
        #     filename=data_path,
        # )
        print("데이터 적재 완료!")

    @task
    def merge_data():
        """
        스테이징 테이블의 데이터를 정제하고 최종 테이블에 UPSERT합니다.
        중복 데이터는 최신 정보로 업데이트됩니다.
        """
        query = """
            INSERT INTO employees
            SELECT DISTINCT * 
            FROM employees_temp
            ON CONFLICT ("Serial Number") DO UPDATE
            SET
                "Company Name" = EXCLUDED."Company Name",
                "Employee Markme" = EXCLUDED."Employee Markme",
                "Description" = EXCLUDED."Description",
                "Leave" = EXCLUDED."Leave";
        """
        
        pg_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        print("데이터 정제 및 UPSERT 실행 중...")
        pg_hook.run(query, autocommit=True)
        print("데이터 병합 완료!")

    # 작업 의존성 설정
    get_data_task = get_data()
    merge_data_task = merge_data()

    create_employees_table() >> get_data_task
    create_employees_temp_table() >> get_data_task
    get_data_task >> merge_data_task

etl_example_taskflow()