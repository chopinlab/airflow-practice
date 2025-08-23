# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow practice project using Python 3.12+. The project uses `uv` as the package manager and dependency resolver.

## Development Setup

- This project uses `uv` for dependency management
- Python version: >=3.12 (specified in pyproject.toml)
- Main DAG entry point: `dags/basic_dag_example.py`
- Uses TaskFlow API (@dag decorator) for modern Airflow development

## Common Commands

### Package Management
- Install dependencies: `uv sync`
- Add new dependency: `uv add <package>`
- Add dev dependency: `uv add --dev <package>`
- Format code: `uv run black .`
- Check formatting: `uv run black --check .`

### Docker
- Start services: `docker-compose up`
- Start in background: `docker-compose up -d`
- Stop services: `docker-compose down`

## Architecture

This is an Apache Airflow practice project with:
- DAG definitions in `dags/` directory
- TaskFlow API implementation using @dag and @task decorators
- Docker Compose configuration for containerized Airflow development
- Standard Python project structure using pyproject.toml
- Black formatter for code styling

## DAG Development

### 구현된 DAG 목록

#### 1. **basic_dag_example.py**
- Primary DAG: TaskFlow API 기본 예제
- Uses TaskFlow API (@dag decorator) for modern Airflow development
- Task documentation using docstrings for Airflow UI integration
- UTC timezone usage with pendulum for consistent scheduling

#### 2. **etl_example.py** 
- ETL pipeline example (extract, transform, load tasks)
- PostgreSQL 연동 및 Asset 기반 데이터 lineage 추적
- CSV → PostgreSQL 데이터 적재 파이프라인
- Asset 정의:
  - `csv_data_asset`: 입력 CSV 파일 추적
  - `employee_data_asset`: PostgreSQL 테이블 출력 추적

#### 3. **minio_asset_example.py** ⭐️
- **핵심**: Airflow 3.0의 Asset 기반 이벤트 드리븐 파이프라인
- 구성: Producer DAG + Consumer DAG + Path Watcher DAG
- Producer: `@asset` 데코레이터 사용한 매일 데이터 업데이트
- Path Watcher: 5분마다 새 파일 생성 감지
- Consumer: Asset 변경 감지 시 자동 실행
- **학습 포인트**: Asset-based scheduling의 실제 구현 예제

#### 4. **mlops_pipeline_example.py** 🚀
- **목적**: 완전한 MLOps 훈련 파이프라인 구현
- **특징**: 
  - 파라미터 기반 동적 실행 (데이터셋/모델 선택 가능)
  - MLflow 통합 (훈련→평가→검증→등록 일괄 처리)
  - Airflow 의사결정 로직 (성능 기반 배포 결정)
  - Asset 기반 데이터 lineage 추적
- **지원**: iris/titanic/housing 데이터셋, XGBoost/LightGBM/Scikit-learn 모델
- **아키텍처**: MLflow + Airflow 역할 분리 (실험 관리 vs 워크플로우 제어)

#### 5. **model_serving_pipeline.py** 🍱
- **목적**: BentoML 기반 모델 서빙 및 배포 자동화
- **배포 방식**: Docker 컨테이너 (Kubernetes 대신 단순화)
- **특징**:
  - BentoML 서비스 자동 생성 (OpenAPI, 메트릭, 헬스체크 내장)
  - Docker Compose + Nginx 로드밸런서
  - Asset 기반 자동 트리거 (Production 모델 등록 시)
  - 완전한 서빙 파이프라인 (배포→모니터링→헬스체크→알림)

## MLOps 아키텍처 패턴

### 전체 플로우
```
MinIO 데이터 → Asset 이벤트 → MLOps 훈련 파이프라인 → MLflow 통합
                                                          ↓
BentoML 서빙 ← Asset 이벤트 ← 모델 레지스트리 ← Airflow 배포 결정
```

### 핵심 구현 패턴

#### 1. **Asset 기반 이벤트 드리븐**
- 데이터 변경 감지 → 자동 파이프라인 실행
- 데이터 lineage 추적 및 시각화
- 파일 생성 감지를 통한 실시간 처리

#### 2. **MLflow + Airflow 역할 분리**
- **MLflow**: 실험 추적, 모델 관리, 메트릭 수집
- **Airflow**: 워크플로우 제어, 의사결정, 배포 관리
- **통합 실행**: MLflow가 훈련+평가+검증을 일괄 처리하고 결과 리턴

#### 3. **BentoML 모델 서빙**
- 자동 API 생성 (OpenAPI/Swagger UI)
- 내장 모니터링 (Prometheus 메트릭)
- Docker 기반 간편 배포 및 스케일링

#### 4. **파라미터 기반 동적 파이프라인**
- 런타임 데이터셋/모델 선택
- 하이퍼파라미터 튜닝 지원
- A/B 테스트를 위한 다중 모델 관리

## 기술 스택 및 의존성

### Core MLOps Stack
```toml
dependencies = [
    "apache-airflow~=3.0.0",
    "apache-airflow-providers-postgres>=6.2.3", 
    "requests>=2.32.5",
]
```

### Additional Components (예상 추가 필요)
- **MLflow**: 실험 추적 및 모델 관리
- **BentoML**: 모델 서빙 프레임워크  
- **MinIO**: S3 호환 객체 스토리지
- **Nginx**: 로드밸런서 및 프록시

## 학습 포인트

### Airflow 3.0 신기능 활용
- **Asset-based Scheduling**: 데이터 중심 워크플로우
- **@asset 데코레이터**: 간편한 asset 정의
- **통합 MLOps**: MLflow와의 seamless 연동
- **이벤트 드리븐**: 시간 기반에서 데이터 기반 스케줄링으로 진화

### 실무 적용 가능한 패턴
- **완전한 MLOps 파이프라인**: 훈련부터 서빙까지 end-to-end
- **마이크로서비스 아키텍처**: 각 컴포넌트의 명확한 책임 분리  
- **컨테이너 기반 배포**: Docker를 활용한 실용적 배포 전략
- **모니터링 및 관찰성**: 메트릭, 로깅, 헬스체크 통합