"""
# 모델 서빙 파이프라인 - 프로덕션 배포 및 서빙 관리

## 프로젝트 정보
- **DAG ID**: model_serving_pipeline
- **작성자**: eric.cho@robos.one
- **작성일**: 2025-08-24
- **목적**: 훈련된 모델의 프로덕션 배포, 서빙, 모니터링 관리

## 개요
훈련 파이프라인과 분리된 별도의 서빙 전용 DAG로:
1. MLflow Model Registry에서 승인된 모델 감지
2. 컨테이너 이미지 빌드 및 배포
3. API 서버 배포 (FastAPI/Flask)
4. 헬스체크 및 성능 모니터링
5. Blue-Green 배포 관리

## 트리거 조건
- MLflow Model Registry의 모델이 "Production" stage로 전환될 때
- 수동 트리거 (긴급 배포 시)
- 정기적인 서빙 상태 체크 (매시간)

## 서빙 아키텍처
```
MLflow Registry → Docker Build → K8s Deploy → Load Balancer → Monitoring
                                     ↓
                            Health Check → Rollback (필요시)
```
"""
import datetime
import pendulum
from typing import Dict, Any

from airflow.sdk import dag, task, Asset
from airflow.models.param import Param

# 모델 서빙 관련 Assets
production_model_asset = Asset(
    uri="mlflow://models/production_model",
    name="production_ready_model", 
    extra={"stage": "Production", "env": "prod"}
)

serving_endpoint_asset = Asset(
    uri="https://api.company.com/ml/predict",
    name="ml_serving_endpoint",
    extra={"type": "rest_api", "framework": "fastapi"}
)

@dag(
    dag_id="model_serving_pipeline",
    # 모델이 Production stage로 전환되면 자동 실행
    schedule=[production_model_asset],  
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlops", "serving", "production", "kubernetes"],
    params={
        "deployment_strategy": Param(
            default="blue_green",
            type="string",
            enum=["blue_green", "rolling_update", "canary"],
            description="배포 전략 선택"
        ),
        "replicas": Param(
            default=3,
            type="integer",
            description="서빙 인스턴스 수"
        ),
        "cpu_limit": Param(
            default="500m",
            type="string", 
            description="CPU 제한 (Kubernetes)"
        ),
        "memory_limit": Param(
            default="1Gi",
            type="string",
            description="메모리 제한 (Kubernetes)"
        ),
        "auto_scaling": Param(
            default=True,
            type="boolean",
            description="오토스케일링 활성화"
        )
    },
    default_args={
        "owner": "ml-platform-team@company.com",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="프로덕션 모델 서빙 배포 및 관리 파이프라인",
    doc_md=__doc__,
)
def model_serving_pipeline():

    @task(inlets=[production_model_asset])
    def fetch_production_model():
        """
        MLflow Model Registry에서 Production stage 모델 정보를 가져옵니다.
        """
        print("🔍 MLflow Model Registry 조회 중...")
        
        # 실제로는 MLflow Client 사용
        # from mlflow.tracking import MlflowClient
        # client = MlflowClient()
        # models = client.search_model_versions("stage='Production'")
        
        # 예제용 더미 데이터
        production_models = [
            {
                "name": "iris_best_model",
                "version": "3",
                "model_uri": "s3://ml-bucket/models/iris_xgboost_v3",
                "stage": "Production",
                "description": "XGBoost classifier for iris dataset",
                "accuracy": 0.96,
                "created_timestamp": "2025-08-24T07:00:00Z",
                "tags": {"dataset": "iris", "model_type": "xgboost"}
            }
        ]
        
        if not production_models:
            print("❌ Production stage 모델이 없습니다!")
            raise ValueError("No production models found")
        
        latest_model = production_models[0]  # 최신 모델 선택
        
        print(f"✅ Production 모델 발견:")
        print(f"   📛 모델명: {latest_model['name']}")
        print(f"   🔢 버전: {latest_model['version']}")
        print(f"   🎯 정확도: {latest_model['accuracy']}")
        print(f"   📍 경로: {latest_model['model_uri']}")
        
        return latest_model

    @task
    def create_bentoml_service(model_info: Dict[str, Any], **context):
        """
        BentoML을 사용하여 모델 서빙 서비스를 생성합니다.
        """
        model_name = model_info['name']
        model_version = model_info['version']
        model_uri = model_info['model_uri']
        model_type = model_info['tags'].get('model_type', 'sklearn')
        
        print(f"🍱 BentoML 서비스 생성 시작")
        print(f"📦 서비스명: {model_name}_service")
        print(f"🤖 모델 타입: {model_type}")
        
        # BentoML 서비스 정의 파일 생성
        bentofile_content = f"""
service: "service.py:svc"
labels:
  owner: ml-team
  stage: production
include:
  - "service.py"
  - "requirements.txt"
python:
  requirements_txt: requirements.txt
"""
        
        requirements_content = """
bentoml>=1.2.0
mlflow>=2.8.0
numpy>=1.21.0
pandas>=1.3.0
scikit-learn>=1.0.0
xgboost>=1.7.0
lightgbm>=3.3.0
"""

        # BentoML 서비스 코드 생성
        if model_type == "xgboost":
            service_content = f"""
import bentoml
import numpy as np
import pandas as pd
from typing import List

# MLflow 모델을 BentoML 모델로 임포트
mlflow_model = bentoml.mlflow.import_model(
    "iris_xgboost_model",
    model_uri="{model_uri}",
    labels={{"version": "{model_version}"}},
)

@bentoml.service(
    resources={{"cpu": "500m", "memory": "1Gi"}},
    traffic={{"timeout": 20}},
)
class {model_name.title()}Service:
    
    bento_model = bentoml.models.get("iris_xgboost_model:latest")
    
    @bentoml.api
    def predict(self, input_data: List[List[float]]) -> List[int]:
        \"\"\"
        XGBoost 모델 예측 API
        \"\"\"
        df = pd.DataFrame(input_data)
        predictions = self.bento_model.predict(df)
        return predictions.tolist()
    
    @bentoml.api  
    def predict_proba(self, input_data: List[List[float]]) -> List[List[float]]:
        \"\"\"
        예측 확률 반환 API
        \"\"\"
        df = pd.DataFrame(input_data)
        probabilities = self.bento_model.predict_proba(df)
        return probabilities.tolist()
    
    @bentoml.api
    def batch_predict(self, batch_data: List[List[float]]) -> List[int]:
        \"\"\"
        배치 예측 API (자동 배치 최적화)
        \"\"\"
        df = pd.DataFrame(batch_data)
        predictions = self.bento_model.predict(df)
        return predictions.tolist()

# 서비스 인스턴스
svc = {model_name.title()}Service()
"""
        else:  # sklearn or lightgbm
            service_content = f"""
import bentoml
import numpy as np
import pandas as pd
from typing import List, Dict

# MLflow 모델을 BentoML 모델로 임포트
mlflow_model = bentoml.mlflow.import_model(
    "{model_name}_model",
    model_uri="{model_uri}",
    labels={{"version": "{model_version}", "framework": "{model_type}"}},
)

@bentoml.service(
    resources={{"cpu": "500m", "memory": "1Gi"}},
    traffic={{"timeout": 20}},
)
class {model_name.title()}Service:
    
    bento_model = bentoml.models.get("{model_name}_model:latest")
    
    @bentoml.api
    def predict(self, input_data: List[List[float]]) -> List[int]:
        \"\"\"
        단일/다중 예측 API
        입력: [[feature1, feature2, feature3, feature4], ...]
        출력: [prediction1, prediction2, ...]
        \"\"\"
        df = pd.DataFrame(input_data)
        predictions = self.bento_model.predict(df)
        return predictions.tolist()
    
    @bentoml.api
    def predict_single(self, features: List[float]) -> Dict:
        \"\"\"
        단일 샘플 예측 (상세 결과 포함)
        \"\"\"
        df = pd.DataFrame([features])
        prediction = self.bento_model.predict(df)[0]
        
        if hasattr(self.bento_model, 'predict_proba'):
            probabilities = self.bento_model.predict_proba(df)[0]
            return {{
                "prediction": int(prediction),
                "probabilities": probabilities.tolist(),
                "confidence": float(max(probabilities))
            }}
        else:
            return {{
                "prediction": float(prediction) if hasattr(prediction, 'dtype') else prediction
            }}
    
    @bentoml.api
    def health_check(self) -> Dict:
        \"\"\"
        서비스 헬스체크
        \"\"\"
        return {{
            "status": "healthy",
            "model_name": "{model_name}",
            "model_version": "{model_version}",
            "framework": "{model_type}",
            "bento_version": bentoml.__version__
        }}

# 서비스 인스턴스  
svc = {model_name.title()}Service()
"""
        
        print(f"📄 BentoML 서비스 파일 생성완료")
        print(f"   - bentofile.yaml")
        print(f"   - service.py") 
        print(f"   - requirements.txt")
        
        # BentoML 빌드 시뮬레이션
        bento_tag = f"{model_name}_service:{model_version}"
        print(f"🔨 Bento 빌드 진행 중...")
        print(f"   bentoml build")
        print(f"   bentoml containerize {bento_tag}")
        
        # 자동 생성되는 특징들
        auto_features = [
            "🌐 OpenAPI/Swagger UI 자동 생성",
            "📊 Prometheus 메트릭 내장",
            "🔄 자동 배치 처리 최적화", 
            "🏥 헬스체크 엔드포인트",
            "📈 성능 모니터링",
            "🔀 A/B 테스트 지원"
        ]
        
        print(f"✨ BentoML 자동 기능들:")
        for feature in auto_features:
            print(f"   {feature}")
        
        print(f"✅ BentoML 서비스 생성 완료!")
        
        return {
            "bento_tag": bento_tag,
            "service_name": f"{model_name}_service",
            "bentofile": bentofile_content,
            "service_code": service_content,
            "requirements": requirements_content,
            "auto_features": auto_features,
            "swagger_url": f"http://localhost:3000/docs",  # BentoML 기본 포트
            "metrics_url": f"http://localhost:3000/metrics"
        }

    @task 
    def deploy_bento_as_docker_container(model_info: Dict[str, Any], bento_info: Dict[str, Any], **context):
        """
        BentoML 서비스를 Docker 컨테이너로 직접 배포합니다.
        """
        params = context['params']
        replicas = params['replicas']
        cpu_limit = params['cpu_limit']
        memory_limit = params['memory_limit']
        
        model_name = model_info['name']
        bento_tag = bento_info['bento_tag']
        service_name = bento_info['service_name']
        
        print(f"🐳 BentoML Docker 컨테이너 배포 시작")
        print(f"🍱 Bento 태그: {bento_tag}")
        print(f"📊 컨테이너 수: {replicas}개")
        print(f"💻 리소스: CPU={cpu_limit}, Memory={memory_limit}")
        
        # Docker Compose 설정 생성
        docker_compose_content = f"""
version: '3.8'
services:
  {service_name}:
    image: {bento_tag}
    container_name: {service_name}-main
    ports:
      - "8080:3000"  # 외부:내부 포트 매핑
    environment:
      - BENTOML_PORT=3000
      - BENTOML_PRODUCTION=true
      - BENTOML_METRICS_ENABLED=true
    deploy:
      resources:
        limits:
          cpus: '{cpu_limit.replace("m", "").rstrip("0")}0'  # 500m -> 0.5
          memory: {memory_limit}
        reservations:
          cpus: '0.2'
          memory: 512M
    volumes:
      - bento_logs:/opt/bentoml/logs
      - bento_cache:/tmp/bentoml
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:3000/health_check"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s

  nginx:
    image: nginx:alpine
    container_name: {service_name}-nginx
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - nginx_logs:/var/log/nginx
    depends_on:
      - {service_name}
    restart: unless-stopped

volumes:
  bento_logs:
  bento_cache:
  nginx_logs:

networks:
  default:
    name: {service_name}-network
"""
        
        # Nginx 로드밸런서 설정 (여러 컨테이너 인스턴스용)
        nginx_config = f"""
events {{
    worker_connections 1024;
}}

http {{
    upstream bentoml_backend {{
        # 여러 BentoML 컨테이너 인스턴스
        server {service_name}:3000;
        # server {service_name}-2:3000;  # 필요시 추가
        # server {service_name}-3:3000;
    }}

    # 로그 형식
    log_format main '$remote_addr - $remote_user [$time_local] "$request" '
                   '$status $body_bytes_sent "$http_referer" '
                   '"$http_user_agent" "$http_x_forwarded_for"';

    server {{
        listen 80;
        server_name localhost;
        
        # 액세스 로그
        access_log /var/log/nginx/access.log main;
        error_log /var/log/nginx/error.log;

        # 예측 API
        location /predict {{
            proxy_pass http://bentoml_backend/predict;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 60s;
            proxy_send_timeout 60s;
            proxy_read_timeout 60s;
        }}

        # 헬스체크
        location /health {{
            proxy_pass http://bentoml_backend/health_check;
            proxy_set_header Host $host;
        }}

        # Swagger UI
        location /docs {{
            proxy_pass http://bentoml_backend/docs;
            proxy_set_header Host $host;
        }}

        # Prometheus 메트릭
        location /metrics {{
            proxy_pass http://bentoml_backend/metrics;
            proxy_set_header Host $host;
        }}

        # 모든 API 경로
        location / {{
            proxy_pass http://bentoml_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        }}
    }}
}}
"""

        print("📄 Docker 배포 설정 생성완료:")
        print("   ✅ docker-compose.yml")
        print("   ✅ nginx.conf (로드밸런서)")
        
        # 실제 Docker 명령어들
        docker_commands = [
            f"# BentoML 이미지 빌드",
            f"bentoml build",
            f"bentoml containerize {bento_tag}",
            "",
            f"# Docker Compose로 서비스 시작", 
            f"docker-compose up -d",
            "",
            f"# 또는 개별 컨테이너 실행",
            f"docker run -d \\",
            f"  --name {service_name}-1 \\",
            f"  --restart unless-stopped \\",
            f"  -p 8080:3000 \\",
            f"  -e BENTOML_PORT=3000 \\",
            f"  -e BENTOML_PRODUCTION=true \\",
            f"  --cpus='{cpu_limit.replace('m', '').rstrip('0')}0' \\",
            f"  --memory={memory_limit} \\",
            f"  -v bento_logs:/opt/bentoml/logs \\",
            f"  --health-cmd='curl -f http://localhost:3000/health_check' \\",
            f"  --health-interval=30s \\",
            f"  --health-timeout=10s \\",
            f"  --health-retries=3 \\",
            f"  {bento_tag}",
        ]
        
        print(f"\n🐳 Docker 배포 명령어:")
        for cmd in docker_commands:
            print(f"   {cmd}")
        
        # 다중 인스턴스 실행 (스케일링)
        if replicas > 1:
            print(f"\n📊 스케일링: {replicas}개 인스턴스 생성")
            for i in range(1, replicas + 1):
                port = 8080 + (i - 1)
                scale_cmd = f"docker run -d --name {service_name}-{i} -p {port}:3000 {bento_tag}"
                print(f"   {scale_cmd}")

        # 컨테이너 관리 명령어들
        management_commands = {
            "상태 확인": f"docker ps -f name={service_name}",
            "로그 확인": f"docker logs {service_name}-main",
            "헬스체크": f"curl http://localhost:8080/health_check",
            "중지": f"docker-compose down",
            "재시작": f"docker-compose restart",
            "스케일링": f"docker-compose up -d --scale {service_name}={replicas}",
            "정리": f"docker system prune -f"
        }
        
        print(f"\n🛠️ 컨테이너 관리 명령어:")
        for desc, cmd in management_commands.items():
            print(f"   {desc}: {cmd}")
        
        print(f"\n✅ BentoML Docker 컨테이너 배포 완료!")
        
        # 서비스 엔드포인트 정보
        endpoints = {
            "main_api": "http://localhost:8080",
            "predict": "http://localhost:8080/predict",
            "predict_single": "http://localhost:8080/predict_single", 
            "batch_predict": "http://localhost:8080/batch_predict",
            "health_check": "http://localhost:8080/health_check",
            "docs": "http://localhost:8080/docs",  # Swagger UI
            "metrics": "http://localhost:8080/metrics",  # Prometheus
            "nginx_status": "http://localhost:80"  # Nginx를 통한 액세스
        }
        
        return {
            "container_name": f"{service_name}-main",
            "service_name": service_name,
            "replicas": replicas,
            "docker_compose": docker_compose_content,
            "nginx_config": nginx_config,
            "endpoints": endpoints,
            "management_commands": management_commands,
            "bento_tag": bento_tag,
            "ports": {
                "main": 8080,
                "nginx": 80,
                "https": 443
            }
        }

    @task(outlets=[serving_endpoint_asset])
    def setup_monitoring_and_alerts(model_info: Dict[str, Any], k8s_info: Dict[str, Any]):
        """
        서빙 모델의 모니터링 및 알림을 설정합니다.
        """
        model_name = model_info['name']
        service_name = k8s_info['service_name']
        
        print(f"📊 모니터링 설정 시작")
        print(f"🔍 대상 서비스: {service_name}")
        
        # Prometheus 메트릭 설정
        monitoring_config = {
            "metrics": [
                "http_requests_total",
                "http_request_duration_seconds", 
                "ml_prediction_latency",
                "ml_prediction_accuracy",
                "kubernetes_pod_cpu_usage",
                "kubernetes_pod_memory_usage"
            ],
            "alerts": [
                {
                    "name": "HighErrorRate",
                    "condition": "error_rate > 0.05",
                    "severity": "warning"
                },
                {
                    "name": "HighLatency", 
                    "condition": "p95_latency > 1000ms",
                    "severity": "critical"
                },
                {
                    "name": "ModelDrift",
                    "condition": "prediction_drift > 0.1",
                    "severity": "warning"
                }
            ]
        }
        
        print(f"📈 Prometheus 메트릭 설정:")
        for metric in monitoring_config['metrics']:
            print(f"   - {metric}")
            
        print(f"🚨 알림 규칙 설정:")
        for alert in monitoring_config['alerts']:
            print(f"   - {alert['name']}: {alert['condition']} ({alert['severity']})")
        
        # Grafana 대시보드 설정
        dashboard_config = {
            "title": f"ML Serving - {model_name}",
            "panels": [
                "Request Rate",
                "Response Time",
                "Error Rate", 
                "Prediction Distribution",
                "Resource Usage",
                "Model Performance"
            ]
        }
        
        print(f"📊 Grafana 대시보드 생성: {dashboard_config['title']}")
        
        # 서빙 엔드포인트 정보
        endpoint_url = f"https://api.company.com/ml/{model_name}/predict"
        
        print(f"✅ 모니터링 설정 완료!")
        print(f"🌐 서빙 엔드포인트: {endpoint_url}")
        
        return {
            "endpoint_url": endpoint_url,
            "monitoring_enabled": True,
            "dashboard_url": f"https://grafana.company.com/d/ml-serving-{model_name}",
            "alerts_configured": len(monitoring_config['alerts'])
        }

    @task
    def run_health_checks(model_info: Dict[str, Any], monitoring_info: Dict[str, Any]):
        """
        배포된 모델의 헬스체크를 수행합니다.
        """
        model_name = model_info['name']
        endpoint_url = monitoring_info['endpoint_url']
        
        print(f"🏥 헬스체크 시작")
        print(f"🎯 대상 엔드포인트: {endpoint_url}")
        
        health_checks = [
            {"name": "API 응답성", "status": "OK", "latency": "45ms"},
            {"name": "모델 로딩", "status": "OK", "memory": "312MB"}, 
            {"name": "예측 정확성", "status": "OK", "accuracy": "96.2%"},
            {"name": "리소스 사용량", "status": "OK", "cpu": "23%"},
            {"name": "로그 수집", "status": "OK", "rate": "100/min"}
        ]
        
        print(f"🔍 헬스체크 결과:")
        all_healthy = True
        for check in health_checks:
            status_emoji = "✅" if check['status'] == 'OK' else "❌"
            print(f"   {status_emoji} {check['name']}: {check['status']}")
            if check['status'] != 'OK':
                all_healthy = False
        
        if all_healthy:
            print(f"🎉 모든 헬스체크 통과! 서빙 준비 완료!")
            deployment_status = "HEALTHY"
        else:
            print(f"⚠️ 일부 헬스체크 실패. 검토 필요.")
            deployment_status = "UNHEALTHY"
        
        return {
            "overall_status": deployment_status,
            "health_checks": health_checks,
            "endpoint_ready": all_healthy,
            "last_check_time": pendulum.now().isoformat()
        }

    @task
    def send_deployment_notification(
        model_info: Dict[str, Any], 
        k8s_info: Dict[str, Any], 
        monitoring_info: Dict[str, Any],
        health_status: Dict[str, Any]
    ):
        """
        배포 완료 알림을 전송합니다.
        """
        model_name = model_info['name']
        model_version = model_info['version']
        endpoint_url = monitoring_info['endpoint_url']
        dashboard_url = monitoring_info['dashboard_url']
        status = health_status['overall_status']
        
        print(f"📢 배포 완료 알림 발송")
        
        if status == "HEALTHY":
            message = f"""
🎉 모델 서빙 배포 성공!

📊 모델 정보:
   - 모델명: {model_name}
   - 버전: {model_version}  
   - 정확도: {model_info['accuracy']:.2%}

🚀 배포 정보:
   - 엔드포인트: {endpoint_url}
   - 레플리카: {k8s_info['replicas']}개
   - 오토스케일링: {'활성화' if k8s_info['auto_scaling_enabled'] else '비활성화'}

📊 모니터링:
   - 대시보드: {dashboard_url}
   - 알림 규칙: {monitoring_info['alerts_configured']}개 설정됨

✅ 모든 헬스체크 통과 - 서비스 준비 완료!
"""
        else:
            message = f"""
⚠️ 모델 서빙 배포 완료 (주의 필요)

모델: {model_name} v{model_version}
상태: {status}
엔드포인트: {endpoint_url}

일부 헬스체크가 실패했습니다. 검토가 필요합니다.
"""
        
        print(message)
        
        # 실제로는 Slack, Teams, PagerDuty 등으로 알림
        # slack_webhook = "https://hooks.slack.com/..."
        # requests.post(slack_webhook, json={"text": message})
        
        return {
            "notification_sent": True,
            "message": message,
            "recipients": ["ml-team@company.com", "platform-team@company.com"],
            "deployment_successful": status == "HEALTHY"
        }

    # Task 의존성 정의 - BentoML Docker 서빙 파이프라인
    model_fetched = fetch_production_model()
    bento_service_created = create_bentoml_service(model_fetched)
    docker_deployed = deploy_bento_as_docker_container(model_fetched, bento_service_created)
    monitoring_setup = setup_monitoring_and_alerts(model_fetched, docker_deployed)
    health_checked = run_health_checks(model_fetched, monitoring_setup)
    notification_sent = send_deployment_notification(
        model_fetched, docker_deployed, monitoring_setup, health_checked
    )
    
    # 의존성 체인 - Docker 컨테이너 배포 방식
    model_fetched >> bento_service_created >> docker_deployed >> monitoring_setup >> health_checked >> notification_sent

# DAG 인스턴스 생성
model_serving_pipeline()