"""
# 기본 DAG 예제 - ETL 파이프라인 데모

## 프로젝트 정보
- **DAG ID**: basic_dag_example
- **작성자**: eric.cho@robos.one
- **작성일**: 2025-08-22
- **최종수정일**: 2025-08-23
- **버전**: 1.0.0
- **목적**: Apache Airflow TaskFlow API 학습 및 기본 ETL 패턴 데모

## 개요
Apache Airflow TaskFlow API를 활용한 기본적인 ETL(Extract, Transform, Load) 파이프라인 구현 예제입니다.
샘플 주문 데이터를 처리하여 총 주문 금액을 계산하는 간단한 데이터 처리 워크플로우를 보여줍니다.

## 작업 상세 정보

### 1. print_date
- **유형**: BashOperator (@task.bash)
- **목적**: 시스템 날짜 출력 (독립 작업)
- **입력값**: 없음
- **출력값**: 현재 시스템 날짜/시간
- **소요시간**: ~1초

### 2. extract
- **유형**: PythonOperator (@task)
- **목적**: 샘플 주문 데이터 추출
- **입력값**: 하드코딩된 JSON 문자열
- **출력값**: dict 형태의 주문 데이터 (order_id: amount)
- **데이터 예시**: {"1001": 301.27, "1002": 433.21, "1003": 502.22}

### 3. transform
- **유형**: PythonOperator (@task)
- **목적**: 주문 데이터 변환 및 총액 계산
- **입력값**: extract 작업의 dict 출력
- **처리로직**: 모든 주문 금액 합계 계산
- **출력값**: {"total_order_value": float}

### 4. load
- **유형**: PythonOperator (@task)
- **목적**: 최종 결과 출력 (로그)
- **입력값**: transform 작업의 total_order_value
- **출력값**: 콘솔 로그 출력
- **출력형식**: "총 주문 금액: {금액:.2f}"

## 스케줄링 정보
- **실행방식**: 수동 트리거 (schedule=None)
- **시작일**: 2025-08-22 (UTC)
- **catchup**: False (과거 실행 무시)
- **재시도**: 1회 (5분 간격)
- **타임아웃**: 기본값 사용

## 의존성 그래프
```
print_date (독립 실행)

extract → transform → load
```

## 예상 실행 결과
- **총 처리 시간**: 약 3-5초

## 태그
- example, basic, taskflow, etl, demo

## 참고사항
- 학습 목적의 예제 DAG입니다
- 실제 데이터 소스 연결 없이 하드코딩된 샘플 데이터 사용
- TaskFlow API 패턴 학습에 최적화되어 있습니다
"""

import json
import pendulum
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 22, tz="UTC"),
    catchup=False,
    tags=["example", "basic", "taskflow"],
    default_args={
        "owner": "eric.cho@robos.one",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="TaskFlow API를 사용한 기본 DAG 예제",
    doc_md=__doc__,
)
def basic_dag_example():

    @task.bash
    def print_date():
        """
        현재 시스템 날짜와 시간을 출력합니다.
        """
        return "date"

    @task()
    def extract():
        """
        JSON 문자열에서 샘플 주문 데이터를 추출합니다.
        ETL 파이프라인을 위한 데이터 추출을 시뮬레이션합니다.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        총 주문 금액을 계산하여 주문 데이터를 변환합니다.
        """
        total_order_value = 0
        for value in order_data_dict.values():
            total_order_value += value
        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        """
        계산된 총 주문 금액을 출력하는 로드 작업입니다.
        """
        print(f"총 주문 금액: {total_order_value:.2f}")

    # 작업 의존성
    print_date()
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


basic_dag_example()
