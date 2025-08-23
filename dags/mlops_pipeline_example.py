"""
# MLOps 파이프라인 예제 - 다중 데이터셋과 모델 선택

## 프로젝트 정보
- **DAG ID**: mlops_training_pipeline
- **작성자**: eric.cho@robos.one
- **작성일**: 2025-08-24
- **목적**: MinIO 다중 데이터셋과 MLflow를 활용한 동적 ML 훈련 파이프라인

## 개요
1. MinIO에서 선택된 데이터셋 로드
2. 파라미터로 모델 타입과 하이퍼파라미터 전달
3. MLflow로 모델 훈련 및 실험 추적
4. 모델 성능 평가 및 등록

## 워크플로우
```
Dataset Selection → Data Load → Feature Engineering → Model Training → Evaluation → Model Registry
```

## 지원 데이터셋
- iris: 붓꽃 분류 데이터
- titanic: 타이타닉 생존 예측
- housing: 주택 가격 예측

## 지원 모델
- xgboost: XGBoost 분류/회귀
- lightgbm: LightGBM 분류/회귀
- sklearn: Scikit-learn 모델들
"""
import datetime
import pendulum
import json
from typing import Dict, Any

from airflow.sdk import dag, task, Asset
from airflow.models.param import Param

# MinIO 데이터셋 Assets
datasets_assets = {
    "iris": Asset(
        uri="s3://ml-bucket/datasets/iris.csv",
        name="iris_dataset",
        extra={"type": "classification", "features": 4, "classes": 3}
    ),
    "titanic": Asset(
        uri="s3://ml-bucket/datasets/titanic.csv", 
        name="titanic_dataset",
        extra={"type": "classification", "features": 8, "classes": 2}
    ),
    "housing": Asset(
        uri="s3://ml-bucket/datasets/housing.csv",
        name="housing_dataset", 
        extra={"type": "regression", "features": 13, "target": "price"}
    )
}

# 훈련된 모델 Asset
model_asset = Asset(
    uri="s3://ml-bucket/models/trained_model",
    name="trained_ml_model",
    extra={"registry": "mlflow", "stage": "staging"}
)

@dag(
    dag_id="mlops_training_pipeline",
    schedule="@manual",  # 수동 실행 (파라미터 전달용)
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlops", "mlflow", "minio", "machine-learning"],
    params={
        "dataset": Param(
            default="iris",
            type="string", 
            enum=["iris", "titanic", "housing"],
            description="훈련에 사용할 데이터셋 선택"
        ),
        "model_type": Param(
            default="xgboost",
            type="string",
            enum=["xgboost", "lightgbm", "sklearn"],
            description="사용할 모델 타입 선택"
        ),
        "hyperparams": Param(
            default={"n_estimators": 100, "max_depth": 6, "learning_rate": 0.1},
            type="object",
            description="모델 하이퍼파라미터"
        ),
        "experiment_name": Param(
            default="ml_experiment_001",
            type="string",
            description="MLflow 실험명"
        )
    },
    default_args={
        "owner": "eric.cho@robos.one",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
    },
    description="동적 ML 훈련 파이프라인 - 데이터셋과 모델을 파라미터로 선택",
    doc_md=__doc__,
)
def mlops_training_pipeline():

    @task
    def validate_parameters(**context):
        """
        입력된 파라미터들의 유효성을 검증합니다.
        """
        params = context['params']
        dataset = params['dataset']
        model_type = params['model_type'] 
        hyperparams = params['hyperparams']
        
        print(f"🔍 파라미터 검증 시작")
        print(f"📊 선택된 데이터셋: {dataset}")
        print(f"🤖 선택된 모델: {model_type}")
        print(f"⚙️  하이퍼파라미터: {json.dumps(hyperparams, indent=2)}")
        
        # 데이터셋별 모델 타입 호환성 체크
        dataset_info = datasets_assets[dataset].extra
        if dataset_info['type'] == 'classification' and model_type == 'sklearn':
            print("✅ 분류 작업 - sklearn 호환")
        elif dataset_info['type'] == 'regression':
            print("✅ 회귀 작업 호환")
            
        print("✅ 모든 파라미터 검증 완료!")
        return {
            "dataset": dataset,
            "model_type": model_type, 
            "hyperparams": hyperparams,
            "dataset_info": dataset_info
        }

    @task(inlets=[datasets_assets["iris"], datasets_assets["titanic"], datasets_assets["housing"]])
    def load_dataset_from_minio(validated_params: Dict[str, Any]):
        """
        MinIO에서 선택된 데이터셋을 로드합니다.
        """
        dataset_name = validated_params['dataset']
        dataset_info = validated_params['dataset_info']
        
        print(f"📥 MinIO에서 데이터셋 로드 중: {dataset_name}")
        print(f"🗃️ 데이터셋 정보: {dataset_info}")
        
        # 실제로는 MinIO client 사용
        # from minio import Minio
        # client = Minio('localhost:9000', access_key='...', secret_key='...')
        # data = client.get_object('ml-bucket', f'datasets/{dataset_name}.csv')
        
        # 예제용 더미 데이터
        if dataset_name == "iris":
            sample_data = {
                "features": [[5.1, 3.5, 1.4, 0.2], [4.9, 3.0, 1.4, 0.2]],
                "target": [0, 0],
                "feature_names": ["sepal_length", "sepal_width", "petal_length", "petal_width"],
                "target_names": ["setosa", "versicolor", "virginica"]
            }
        elif dataset_name == "titanic":
            sample_data = {
                "features": [[22, 1, 0, 0, 7.25], [38, 1, 1, 0, 71.28]],
                "target": [0, 1],
                "feature_names": ["age", "sex", "pclass", "sibsp", "fare"],
                "target_names": ["died", "survived"]
            }
        else:  # housing
            sample_data = {
                "features": [[0.00632, 18.0, 2.31, 0, 0.538], [0.02731, 0.0, 7.07, 0, 0.469]],
                "target": [24.0, 21.6],
                "feature_names": ["crim", "zn", "indus", "chas", "nox"],
                "target_names": ["price"]
            }
        
        print(f"✅ 데이터 로드 완료! 샘플 수: {len(sample_data['features'])}")
        
        return {
            **validated_params,
            "data": sample_data,
            "data_path": f"s3://ml-bucket/datasets/{dataset_name}.csv"
        }

    @task
    def feature_engineering(dataset_info: Dict[str, Any]):
        """
        특징 공학을 수행합니다.
        """
        dataset_name = dataset_info['dataset']
        data = dataset_info['data']
        
        print(f"🔧 특징 공학 시작: {dataset_name}")
        
        # 기본적인 전처리
        processed_features = []
        for features in data['features']:
            # 정규화 예제
            normalized = [f / max(data['features'][0]) if max(data['features'][0]) > 0 else f for f in features]
            processed_features.append(normalized)
        
        print(f"✅ 특징 공학 완료! 처리된 특징 수: {len(processed_features[0])}")
        
        return {
            **dataset_info,
            "processed_features": processed_features,
            "original_features": data['features']
        }

    @task(outlets=[model_asset])
    def run_complete_mlflow_pipeline(processed_data: Dict[str, Any], **context):
        """
        MLflow가 훈련→평가→검증→등록을 통합으로 수행하고 최종 결과를 리턴합니다.
        Airflow는 이 결과를 받아서 배포 결정만 수행합니다.
        """
        params = context['params']
        experiment_name = params['experiment_name']
        model_type = processed_data['model_type']
        hyperparams = processed_data['hyperparams']
        dataset_name = processed_data['dataset']
        
        print(f"🚀 MLflow 통합 파이프라인 시작")
        print(f"📋 실행 단계: 훈련 → 평가 → 검증 → 등록")
        print(f"🧪 실험명: {experiment_name}")
        print(f"🤖 모델 타입: {model_type}")
        print(f"📊 데이터셋: {dataset_name}")
        
        # 실제로는 이런 식으로 MLflow 통합 실행
        # import subprocess
        # result = subprocess.run([
        #     "mlflow", "run", ".",
        #     "-P", f"dataset={dataset_name}",
        #     "-P", f"model_type={model_type}",
        #     "-P", f"hyperparams={json.dumps(hyperparams)}",
        #     "-P", "run_full_pipeline=true"  # 훈련+평가+검증+등록 모두 실행
        # ], capture_output=True, text=True)
        
        print("=" * 60)
        print("🔄 STEP 1: 모델 훈련 중...")
        
        # 훈련 시뮬레이션
        if model_type == "xgboost":
            print("🌳 XGBoost 모델 훈련")
            model_info = {"type": "XGBClassifier", "params": hyperparams}
        elif model_type == "lightgbm":
            print("💡 LightGBM 모델 훈련")
            model_info = {"type": "LGBMClassifier", "params": hyperparams}
        else:  # sklearn
            print("🔬 Scikit-learn 모델 훈련")
            model_info = {"type": "RandomForestClassifier", "params": hyperparams}
        
        print("✅ 훈련 완료!")
        
        print("🔄 STEP 2: 모델 평가 중...")
        # 평가 시뮬레이션 (실제로는 MLflow가 수행)
        accuracy = 0.85 + (hash(str(hyperparams)) % 100) / 1000
        precision = accuracy - 0.02
        recall = accuracy - 0.01
        f1_score = 2 * (precision * recall) / (precision + recall)
        
        print(f"📊 평가 결과:")
        print(f"   - Accuracy: {accuracy:.4f}")
        print(f"   - Precision: {precision:.4f}")
        print(f"   - Recall: {recall:.4f}")
        print(f"   - F1-Score: {f1_score:.4f}")
        
        print("🔄 STEP 3: 모델 검증 중...")
        # 검증 기준 (실제로는 더 복잡한 검증 수행)
        validation_passed = accuracy >= 0.8 and precision >= 0.78
        
        print("🔄 STEP 4: 모델 등록 처리 중...")
        if validation_passed:
            print("✅ 검증 통과! 모델 레지스트리에 등록 중...")
            registration_status = "REGISTERED"
            model_stage = "Staging"  # 먼저 Staging에 등록
        else:
            print("❌ 검증 실패! 등록하지 않음")
            registration_status = "REJECTED"
            model_stage = None
        
        # MLflow 통합 파이프라인 완료 - 모든 정보를 포함한 결과 리턴
        final_result = {
            # 모델 정보
            "model_info": model_info,
            "model_uri": f"s3://ml-bucket/models/{dataset_name}_{model_type}",
            "experiment_name": experiment_name,
            
            # 성능 메트릭 (MLflow에서 수집됨)
            "metrics": {
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "f1_score": f1_score
            },
            
            # 검증 결과 (MLflow에서 수행됨)
            "validation": {
                "passed": validation_passed,
                "threshold_accuracy": 0.8,
                "threshold_precision": 0.78
            },
            
            # 등록 결과 (MLflow Model Registry)
            "registration": {
                "status": registration_status,
                "stage": model_stage,
                "model_name": f"{dataset_name}_best_model"
            },
            
            # 배포 추천사항 (MLflow가 판단)
            "deployment_recommendation": {
                "action": "deploy_to_staging" if validation_passed else "retrain_required",
                "reason": "성능 기준 충족" if validation_passed else "성능 기준 미달",
                "confidence": "high" if accuracy > 0.9 else "medium" if accuracy > 0.85 else "low"
            }
        }
        
        print("=" * 60)
        print(f"🎯 MLflow 통합 파이프라인 완료!")
        print(f"📊 최종 성능: {accuracy:.4f}")
        print(f"✅ 검증 결과: {'통과' if validation_passed else '실패'}")
        print(f"📝 등록 상태: {registration_status}")
        print(f"🚀 배포 추천: {final_result['deployment_recommendation']['action']}")
        
        return final_result

    @task
    def decide_deployment_action(mlflow_result: Dict[str, Any]):
        """
        MLflow 통합 파이프라인 결과를 바탕으로 배포 액션을 결정합니다.
        이것이 Airflow의 핵심 역할 - 의사결정!
        """
        recommendation = mlflow_result['deployment_recommendation']
        metrics = mlflow_result['metrics']
        validation = mlflow_result['validation']
        registration = mlflow_result['registration']
        
        print(f"🤔 배포 결정 분석 중...")
        print(f"📊 MLflow 추천: {recommendation['action']}")
        print(f"💪 성능: {metrics['accuracy']:.4f} (임계값: {validation['threshold_accuracy']})")
        print(f"✅ 검증: {'통과' if validation['passed'] else '실패'}")
        
        # Airflow의 비즈니스 로직 적용
        if recommendation['action'] == "deploy_to_staging":
            if metrics['accuracy'] > 0.95:
                final_action = "deploy_to_production"
                print(f"🚀 결정: 성능 우수 - 바로 프로덕션 배포!")
            else:
                final_action = "deploy_to_staging"
                print(f"📊 결정: 스테이징 환경에서 A/B 테스트")
                
        elif recommendation['action'] == "retrain_required":
            final_action = "schedule_retrain"
            print(f"🔄 결정: 다른 하이퍼파라미터로 재훈련 스케줄")
        else:
            final_action = "manual_review"
            print(f"👤 결정: 수동 검토 필요")
        
        return {
            "action": final_action,
            "mlflow_recommendation": recommendation,
            "model_uri": mlflow_result['model_uri'],
            "model_stage": registration['stage'],
            "confidence": recommendation['confidence']
        }

    @task
    def send_notification(deployment_decision: Dict[str, Any], mlflow_result: Dict[str, Any], **context):
        """
        최종 배포 결정 결과를 알림으로 전송합니다.
        """
        params = context['params']
        dataset = params['dataset']
        model_type = params['model_type']
        
        action = deployment_decision['action']
        confidence = deployment_decision['confidence']
        accuracy = mlflow_result['metrics']['accuracy']
        
        print(f"📢 MLOps 파이프라인 완료 알림")
        print(f"📊 데이터셋: {dataset}")
        print(f"🤖 모델 타입: {model_type}")
        print(f"🎯 최종 정확도: {accuracy:.4f}")
        print(f"🚀 배포 결정: {action}")
        print(f"💪 신뢰도: {confidence}")
        
        # 액션별 메시지
        if action == "deploy_to_production":
            message = f"🎉 새로운 고성능 모델이 프로덕션에 배포됩니다! (정확도: {accuracy:.4f})"
        elif action == "deploy_to_staging":
            message = f"📊 새로운 모델이 스테이징에서 A/B 테스트를 시작합니다!"
        elif action == "schedule_retrain":
            message = f"🔄 모델 성능 부족으로 재훈련이 스케줄됩니다."
        else:
            message = f"👤 모델 결과에 대한 수동 검토가 필요합니다."
        
        print(f"💌 알림 메시지: {message}")
        
        # 실제로는 Slack, Teams, Email 등으로 알림 전송
        # slack_webhook = "https://hooks.slack.com/..."
        # requests.post(slack_webhook, json={
        #     "text": message,
        #     "attachments": [{
        #         "fields": [
        #             {"title": "Dataset", "value": dataset, "short": True},
        #             {"title": "Model", "value": model_type, "short": True},
        #             {"title": "Accuracy", "value": f"{accuracy:.4f}", "short": True},
        #             {"title": "Action", "value": action, "short": True}
        #         ]
        #     }]
        # })
        
        return {
            "notification_sent": True,
            "message": message,
            "recipients": ["ml-team@company.com", "devops@company.com"]
        }

    # Task 의존성 정의 - 통합 MLflow 접근법
    validated = validate_parameters()
    dataset_loaded = load_dataset_from_minio(validated)
    features_engineered = feature_engineering(dataset_loaded)
    
    # MLflow가 훈련→평가→검증→등록을 통합으로 수행
    mlflow_pipeline_result = run_complete_mlflow_pipeline(features_engineered)
    
    # Airflow가 MLflow 결과를 바탕으로 배포 결정
    deployment_decision = decide_deployment_action(mlflow_pipeline_result)
    
    # 최종 알림
    notification = send_notification(deployment_decision, mlflow_pipeline_result)
    
    # 의존성 체인 - 더 간단해짐!
    validated >> dataset_loaded >> features_engineered >> mlflow_pipeline_result >> deployment_decision >> notification

# DAG 인스턴스 생성
mlops_training_pipeline()