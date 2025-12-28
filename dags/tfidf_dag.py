from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from mongodb import MongoDBClient
from src.statistics import TimeWindowedTemporalTFIDF
from src.postgresql import PostgreSQLClient
from utils import get_week_range, normalize_datetime, get_week_bucket_start


# 파라미터 초기화
mongo_client = MongoDBClient()
tfidf_calculator = TimeWindowedTemporalTFIDF(mongo_client=mongo_client)
postgres_client = PostgreSQLClient(conn_id='superset_postgres_default')


# 로직 함수 정의
def aggregate_daily_token_counts(**context):
    """하루치 토큰 데이터를 집계해서 count_keyword 컬렉션에 저장"""
    # 파라미터 추출 및 정규화
    execution_date = context.get('execution_date')
    if not execution_date:
        execution_date = datetime.now()
    else:
        execution_date = normalize_datetime(execution_date)
    
    # 하루 범위 계산 (00:00:00 ~ 23:59:59)
    target_date = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # MongoDB 집계 파이프라인으로 일일 토큰 카운트 집계
    daily_counts = mongo_client.aggregate_daily_tokens(target_date=target_date)
    
    if not daily_counts:
        return {'token_count': 0, 'date': target_date.isoformat()}
    
    # 주간 버킷 시작점 계산
    bucket_start = get_week_bucket_start(target_date)
    
    # count_keyword 컬렉션에 저장할 문서 생성
    documents = []
    for count_doc in daily_counts:
        documents.append({
            'date': target_date,
            'token': count_doc['token'],
            'count': count_doc['count'],
            'bucket_start': bucket_start
        })
    
    # 기존 데이터 삭제 (멱등성 보장)
    mongo_client.delete_many_by_filter(
        collection_name='count_keyword',
        filter_dict={'date': target_date}
    )
    
    # 새 데이터 insert
    mongo_client.insert_many(
        collection_name='count_keyword',
        documents=documents
    )
    
    return {
        'token_count': len(documents),
        'date': target_date.isoformat(),
        'bucket_start': bucket_start.isoformat()
    }


def calculate_tfidf_scores(**context):
    """시간 범위 내의 토큰들에 대해 TF-IDF 스코어 계산 및 저장"""
    # 파라미터 추출 및 정규화
    execution_date = context.get('execution_date')
    if not execution_date:
        execution_date = datetime.now()
    else:
        execution_date = normalize_datetime(execution_date)
    
    # 해당 주의 월요일부터 execution_date까지의 범위 계산
    start_time, end_time = get_week_range(execution_date)
    
    # TF-IDF 스코어 계산 (TFIDFScore 모델 리스트 반환)
    tfidf_score_models = tfidf_calculator.calculate(start_date=start_time, end_date=end_time)
    
    if not tfidf_score_models:
        return {'score_count': 0}
    
    # 기존 스코어 삭제 (멱등성 보장)
    mongo_client.delete_many_by_filter(
        collection_name='tfidf_scores',
        filter_dict={
            'start_time': start_time,
        }
    )
    
    # 새 스코어 insert (모델을 딕셔너리로 변환)
    tfidf_scores_dict = [model.to_dict() for model in tfidf_score_models]
    mongo_client.insert_many(
        collection_name='tfidf_scores',
        documents=tfidf_scores_dict
    )
    
    return {
        'score_count': len(tfidf_score_models),
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat()
    }


def migrate_tfidf_to_postgres(**context):
    """MongoDB의 tfidf_scores 데이터를 PostgreSQL로 마이그레이션"""
    # 파라미터 추출 및 정규화
    execution_date = context.get('execution_date')
    if not execution_date:
        execution_date = datetime.now()
    else:
        execution_date = normalize_datetime(execution_date)
    
    # 해당 주의 월요일부터 execution_date까지의 범위 계산
    start_time, end_time = get_week_range(execution_date)
    
    # MongoDB에서 tfidf_scores 조회
    tfidf_scores = mongo_client.find_by_time_range(
        collection_name='tfidf_scores',
        time_field='start_time',
        start_time=start_time,
        end_time=end_time
    )
    
    if not tfidf_scores:
        return {
            'migrated_count': 0,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        }
    
    # PostgreSQL에 저장 (멱등성 보장: start_time, end_time 기준으로 삭제 후 삽입)
    result = postgres_client.upsert_tfidf_scores(
        scores=tfidf_scores,
        table_name='tfidf_scores'
    )
    
    return {
        'migrated_count': result['inserted_count'],
        'deleted_count': result['deleted_count'],
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat()
    }


# DAG 설정
default_args = {
    'owner': 'trendgetter',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

tfidf_dag = DAG(
    'token_aggregation',
    default_args=default_args,
    description='일일 토큰 집계 및 주간 TF-IDF 스코어 계산',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(7),
    catchup=True,
    tags=['trendgetter', 'token', 'aggregation', 'tfidf', 'statistics', 'nlp'],
)


# 태스크 설정
aggregate_daily_task = PythonOperator(
    task_id='aggregate_daily_token_counts',
    python_callable=aggregate_daily_token_counts,
    dag=tfidf_dag,
)

calculate_tfidf_task = PythonOperator(
    task_id='calculate_tfidf_scores',
    python_callable=calculate_tfidf_scores,
    dag=tfidf_dag,
)

migrate_tfidf_task = PythonOperator(
    task_id='migrate_tfidf_to_postgres',
    python_callable=migrate_tfidf_to_postgres,
    dag=tfidf_dag,
)


# 의존성 설정
aggregate_daily_task >> calculate_tfidf_task >> migrate_tfidf_task  # pyright: ignore[reportUnusedExpression]


