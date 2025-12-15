"""TrendGetter ETL DAG"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# src 모듈 import
import sys
import os


from crawler import YoutubeCrawler, InstagramCrawler, CommunityCrawler


def extract_youtube_data(**context):
    """Youtube 데이터 추출"""
    execution_date = context['execution_date']
    
    crawler = YoutubeCrawler()
    result = crawler.scrap(created_at=execution_date)
    
    print(f"Youtube 데이터 추출 완료: {execution_date}")
    print(f"추출된 데이터 수: {len(result) if result else 0}")
    
    return result


def extract_instagram_data(**context):
    """Instagram 데이터 추출"""
    execution_date = context['execution_date']
    
    crawler = InstagramCrawler()
    result = crawler.scrap(created_at=execution_date)
    
    print(f"Instagram 데이터 추출 완료: {execution_date}")
    print(f"추출된 데이터 수: {len(result) if result else 0}")
    
    return result


def extract_community_data(**context):
    """커뮤니티 데이터 추출"""
    execution_date = context['execution_date']
    
    crawler = CommunityCrawler()
    result = crawler.scrap(created_at=execution_date)
    
    print(f"커뮤니티 데이터 추출 완료: {execution_date}")
    print(f"추출된 데이터 수: {len(result) if result else 0}")
    
    return result


def transform_and_load(**context):
    """데이터 변환 및 적재"""
    # 이전 태스크들의 결과를 가져옴
    ti = context['ti']
    
    youtube_data = ti.xcom_pull(task_ids='extract_youtube')
    instagram_data = ti.xcom_pull(task_ids='extract_instagram')
    community_data = ti.xcom_pull(task_ids='extract_community')
    
    all_data = []
    if youtube_data:
        all_data.extend(youtube_data)
    if instagram_data:
        all_data.extend(instagram_data)
    if community_data:
        all_data.extend(community_data)
    
    print(f"총 데이터 수: {len(all_data)}")
    
    # TODO: Transform 및 Load 로직 구현
    # 예: MongoDB에 저장
    # from repository.mongodb import save_data
    # save_data(all_data)
    
    return {
        'total_count': len(all_data),
        'youtube_count': len(youtube_data) if youtube_data else 0,
        'instagram_count': len(instagram_data) if instagram_data else 0,
        'community_count': len(community_data) if community_data else 0
    }


# DAG 기본 설정
default_args = {
    'owner': 'trendgetter',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'trendgetter_etl',
    default_args=default_args,
    description='TrendGetter ETL 파이프라인',
    schedule_interval=timedelta(hours=1),  # 1시간마다 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['trendgetter', 'etl', 'crawler'],
)

# Extract 태스크들 (병렬 실행)
extract_youtube_task = PythonOperator(
    task_id='extract_youtube',
    python_callable=extract_youtube_data,
    dag=dag,
)

extract_instagram_task = PythonOperator(
    task_id='extract_instagram',
    python_callable=extract_instagram_data,
    dag=dag,
)

extract_community_task = PythonOperator(
    task_id='extract_community',
    python_callable=extract_community_data,
    dag=dag,
)

# Transform & Load 태스크 (Extract 태스크들이 모두 완료된 후 실행)
transform_load_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load,
    dag=dag,
)

# 태스크 의존성 설정
[extract_youtube_task, extract_instagram_task, extract_community_task] >> transform_load_task

