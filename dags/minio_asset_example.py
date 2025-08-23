"""
# MinIO Asset 업데이트 감지 예제

## 프로젝트 정보
- **DAG ID**: minio_asset_producer, minio_asset_consumer
- **작성자**: eric.cho@robos.one
- **작성일**: 2025-08-23
- **목적**: MinIO asset 업데이트 생성과 감지 예제

## 개요
1. Producer DAG: @asset 데코레이터로 MinIO 파일을 매일 업데이트
2. Consumer DAG: asset 변경을 감지해서 업데이트 메시지 출력

## 워크플로우
```
@asset(매일) → MinIO 파일 업데이트 → asset 이벤트 생성
                                        ↓
Consumer DAG ← asset 이벤트 감지 ← @task(업데이트 메시지 출력)
```
"""
import datetime
import pendulum

from airflow.sdk import dag, task, asset, Asset

# MinIO asset 정의
minio_data_asset = Asset(
    uri="s3://my-bucket/data/sample.json",
    name="minio_sample_data",
    extra={
        "bucket": "my-bucket", 
        "path": "data/sample.json",
        "format": "json",
        "owner": "eric.cho@robos.one"
    }
)

# Producer DAG: @asset 데코레이터 사용
@asset(uri="s3://my-bucket/data/sample.json", schedule="@daily")
def minio_asset_producer():
    """
    MinIO에 샘플 데이터를 업데이트하는 asset producer.
    매일 실행되어 MinIO 파일을 업데이트하고 asset 이벤트를 생성합니다.
    """
    import json
    from datetime import datetime
    
    # 실제로는 MinIO client로 파일 업로드 해야 함
    sample_data = {
        "timestamp": datetime.now().isoformat(),
        "message": "MinIO 데이터가 업데이트되었습니다!",
        "version": "1.0.0",
        "records": [
            {"id": 1, "name": "sample1", "value": 100},
            {"id": 2, "name": "sample2", "value": 200}
        ]
    }
    
    print(f"MinIO 데이터 업데이트 중... {datetime.now()}")
    print(f"업데이트할 데이터: {json.dumps(sample_data, indent=2, ensure_ascii=False)}")
    
    # 실제 환경에서는 여기에 MinIO upload 코드 추가:
    # from minio import Minio
    # client = Minio('localhost:9000', access_key='...', secret_key='...')
    # client.put_object('my-bucket', 'data/sample.json', json.dumps(sample_data))
    
    print("✅ MinIO 파일 업데이트 완료! Asset 이벤트가 생성됩니다.")

# Producer DAG 2: 새로운 path 감지하여 asset 생성
@dag(
    dag_id="minio_path_watcher",
    schedule="*/5 * * * *",  # 5분마다 체크
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["minio", "path", "watcher"],
    default_args={
        "owner": "eric.cho@robos.one",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
    },
    description="새로운 MinIO path 생성을 감지하는 Producer DAG",
)
def minio_path_watcher():
    
    @task(outlets=[minio_data_asset])
    def check_new_path():
        """
        MinIO에서 새로운 path/파일 생성을 체크합니다.
        새 파일이 발견되면 asset 이벤트를 생성합니다.
        """
        import os
        from datetime import datetime
        
        # 실제로는 MinIO client나 S3 client로 체크
        watch_path = "/tmp/minio_watch"  # 실제로는 MinIO bucket path
        
        print(f"🔍 새로운 파일/폴더 생성 체크 중... {datetime.now()}")
        print(f"📂 감시 경로: {watch_path}")
        
        # 로컬 테스트용 (실제로는 MinIO/S3 API 사용)
        if not os.path.exists(watch_path):
            os.makedirs(watch_path, exist_ok=True)
            print("📁 감시 폴더 생성됨")
        
        # 새 파일들 체크
        files = os.listdir(watch_path) if os.path.exists(watch_path) else []
        new_files = [f for f in files if f.startswith("new_")]
        
        if new_files:
            print(f"🆕 새로운 파일 발견: {new_files}")
            print("✅ Asset 이벤트 생성! Consumer DAG가 트리거됩니다.")
            
            # 처리된 파일 이름 변경 (중복 처리 방지)
            for file in new_files:
                old_path = os.path.join(watch_path, file)
                new_path = os.path.join(watch_path, file.replace("new_", "processed_"))
                if os.path.exists(old_path):
                    os.rename(old_path, new_path)
                    print(f"📋 파일 처리 완료: {file} → {file.replace('new_', 'processed_')}")
            
            return f"새 파일 {len(new_files)}개 처리됨"
        else:
            print("📭 새로운 파일 없음")
            return None
    
    check_new_path()

# DAG 인스턴스 생성
minio_path_watcher()

# Consumer DAG: asset 변경 감지
@dag(
    dag_id="minio_asset_consumer",
    schedule=[minio_data_asset],  # asset 변경 시 실행
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["minio", "asset", "consumer"],
    default_args={
        "owner": "eric.cho@robos.one",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
    },
    description="MinIO asset 변경을 감지하는 Consumer DAG",
    doc_md=__doc__,
)
def minio_asset_consumer():
    
    @task(inlets=[minio_data_asset])
    def detect_minio_update():
        """
        MinIO asset 업데이트를 감지하고 메시지를 출력합니다.
        """
        from datetime import datetime
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print("🔔 MinIO Asset 업데이트 감지!")
        print(f"📅 감지 시간: {current_time}")
        print(f"📁 Asset URI: {minio_data_asset.uri}")
        print(f"🏷️  Asset Name: {minio_data_asset.name}")
        print(f"ℹ️  Asset 정보: {minio_data_asset.extra}")
        print("=" * 50)
        print("📊 MinIO 데이터가 성공적으로 업데이트되었습니다!")
        print("🚀 후속 데이터 처리 작업을 시작할 수 있습니다.")
        
        return f"MinIO 업데이트 감지 완료: {current_time}"
    
    @task
    def process_updated_data():
        """
        업데이트된 데이터를 처리하는 후속 작업입니다.
        """
        print("🔄 업데이트된 MinIO 데이터 처리 중...")
        print("   - 데이터 유효성 검증")
        print("   - 데이터 변환")
        print("   - 다운스트림 시스템에 알림")
        print("✅ 데이터 처리 완료!")
        
        return "데이터 처리 완료"
    
    # Task 의존성 설정
    update_detected = detect_minio_update()
    data_processed = process_updated_data()
    
    update_detected >> data_processed

# DAG 인스턴스 생성
minio_asset_consumer()