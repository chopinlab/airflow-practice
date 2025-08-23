

# 버전
- 파이썬: 3.12(airflow가 3.12를 지원)


# 로컬 설치

## 기초 세팅

```bash
# uv 세팅
uv init
uv venv

# 파이썬 세팅
uv python list
uv python install cpython-3.12.11-linux-x86_64-gnu
uv python pin 3.12
uv sync
```

## vscode extension 설치
airflow - necati arslan



# Airflow Executors

## Executor 종류

### 1. SequentialExecutor (기본값 - standalone 모드)
- **단일 프로세스**, 순차 실행
- 개발/테스트용으로만 사용
- 병렬 처리 불가능

### 2. LocalExecutor
- **단일 머신**, 멀티프로세스
- 병렬 태스크 실행 가능
- CPU 코어 수만큼 워커 프로세스 생성
- 현재 프로젝트 설정

**주요 설정:**
- `AIRFLOW__CORE__PARALLELISM=32`: 전체 동시 태스크 수
- `AIRFLOW__CORE__DAG_CONCURRENCY=16`: DAG당 동시 태스크 수

### 3. CeleryExecutor
- **다중 머신** 분산 처리
- Redis/RabbitMQ 메시지 브로커 필요
- 여러 서버에서 워커 실행 가능
- 진정한 분산 처리

### 4. KubernetesExecutor
- 각 태스크를 별도 Kubernetes Pod에서 실행
- 동적 리소스 할당
- 클라우드 환경에 최적

## 현재 설정

```yaml
# LocalExecutor 사용
environment:
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__CORE__PARALLELISM=32
  - AIRFLOW__CORE__DAG_CONCURRENCY=16

# 모든 컴포넌트 실행
command: >
  bash -c "
    airflow db migrate &&
    airflow scheduler &
    airflow triggerer &
    airflow api-server
  "
```

## Airflow 컴포넌트

- **Scheduler**: DAG 파싱, 태스크 스케줄링
- **API Server**: 웹 UI 및 REST API 제공 (구 webserver)
- **Triggerer**: Deferrable 태스크 처리 (비동기 이벤트 대기)
- **Executor**: 태스크 실행 방식 결정 (Scheduler 내부 컴포넌트)

# 커스텀 이미지 생성
추가적인 플로그인이나 provider, 라이브러리 추가
```dockerfile
FROM apache/airflow:3.0.5-python3.12
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


# 
# RUN pip install --no-cache-dir \
#     "apache-airflow==${AIRFLOW_VERSION}" \
#     apache-airflow-providers-apache-spark==5.1.1


USER airflow

```

# 개발 환경 설정

## 프로젝트 개요

Apache Airflow 실습 프로젝트 (Python 3.13+). `uv`를 패키지 매니저로 사용합니다.

## 개발 설정

- `uv`를 의존성 관리 도구로 사용
- Python 버전: >=3.13 (pyproject.toml에 명시)
- 메인 진입점: `main.py`

## 주요 명령어

### 패키지 관리
- 의존성 설치: `uv sync`
- 새 의존성 추가: `uv add <package>`
- Python 스크립트 실행: `uv run python main.py`

### Docker
- 서비스 시작: `docker-compose up`
- 백그라운드 시작: `docker-compose up -d`
- 서비스 중지: `docker-compose down`

## 아키텍처

현재 최소한의 Python 프로젝트로 구성:
- `main.py`의 단일 진입점
- 컨테이너화된 개발을 위한 Docker Compose 설정
- pyproject.toml을 사용한 표준 Python 프로젝트 구조

이 프로젝트는 Airflow 실험과 학습을 위한 초기 설정 단계입니다.

# Airflow DAG 스케줄링

## Schedule 파라미터 설정 방법

### 1. timedelta 방식 (현재 사용 중)
```python
from datetime import timedelta

schedule=timedelta(days=1)      # 매일
schedule=timedelta(hours=6)     # 6시간마다  
schedule=timedelta(minutes=30)  # 30분마다
schedule=timedelta(weeks=1)     # 매주
```

### 2. Cron 표현식 (가장 유연함)
```python
schedule="0 2 * * *"        # 매일 오전 2시
schedule="30 14 * * 1-5"    # 평일 오후 2:30
schedule="0 9 1 * *"        # 매월 1일 오전 9시
schedule="0 0 * * 0"        # 매주 일요일 자정
```

### 3. 사전 정의된 상수
```python
from airflow.timetables.trigger import CronTriggerTimetable

schedule="@daily"       # 매일 자정
schedule="@hourly"      # 매시간
schedule="@weekly"      # 매주 일요일 자정  
schedule="@monthly"     # 매월 1일 자정
schedule="@yearly"      # 매년 1월 1일 자정
schedule="@once"        # 한 번만 실행
```

### 4. 수동 실행만
```python
schedule=None           # 수동 트리거만
```

### 5. Dataset 기반 스케줄링 (최신 기능)
```python
from airflow import Dataset

schedule=[Dataset("s3://bucket/data")]  # 데이터셋 업데이트 시 실행
```

**추천:** 정확한 시간이 중요하면 cron, 간격이 중요하면 timedelta를 사용하세요.

