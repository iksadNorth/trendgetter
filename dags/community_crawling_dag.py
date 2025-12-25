from datetime import datetime, timedelta, timezone
import traceback

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.api.client.local_client import Client

from crawler import CommunityCrawler
from mongodb import MongoDBClient
from utils import get_timestamp_micro


# 파라미터 초기화
airflow_client      = Client(None, None)
mongo_client        = MongoDBClient()
crawler             = CommunityCrawler()


# 로직 함수 정의
def extract_article_ids(**context):
    # 파라미터 추출
    execution_date = context['execution_date']
    
    # 데이터 추출
    article_ids = crawler.scrap_array_id(created_at=execution_date)
    if not article_ids: return []
    
    return [{'article_id': article_id} for article_id in article_ids]


def trigger_single_detail_dag(**context):
    # 파라미터 추출
    article_id = context.get('article_id')
    if not article_id: raise ValueError("article_id is required")
    
    try:
        # DAG 트리거
        timestamp_micro = get_timestamp_micro()
        run_id = f'triggered__{article_id}__{timestamp_micro}'
        
        airflow_client.trigger_dag(
            dag_id='community_article_detail',
            conf={'article_id': article_id},
            run_id=run_id,
        )
        return f"Successfully triggered for {article_id}"
        
    except Exception as e:
        # 예외 처리
        error_str = str(e).lower()
        if "duplicate key" in error_str:
            return f"DAG run already exists for {article_id} (duplicate key)"
        if "already exists" in error_str:
            return f"DAG run already exists for {article_id} (already exists)"
        if "unique constraint" in error_str:
            return f"DAG run already exists for {article_id} (unique constraint)"
        traceback.print_exc()
        raise


def extract_and_save_article_detail(**context):
    # 파라미터 추출
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        article_id = dag_run.conf.get('article_id')
    else:
        article_id = context['ti'].xcom_pull(key='article_id')
    
    if not article_id: raise ValueError("article_id is required")
    
    # 데이터 추출
    article_data = crawler.scrap_article_data(article_id=article_id)
    if not article_data: return
    
    # MongoDB 저장
    mongo_client.upsert_many(
        collection_name='raw_articles',
        documents=article_data,
        filter_keys=['src_pk', 'src_id', 'comment_id']
    )
    
    return article_data


# DAG 설정
default_args_list = {
    'owner': 'trendgetter',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

community_list_dag = DAG(
    'community_article_list',
    default_args=default_args_list,
    description='커뮤니티 목록에서 article_id 추출 및 상세 DAG 트리거',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['trendgetter', 'community', 'crawler', 'list'],
)

default_args_detail = {
    'owner': 'trendgetter',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

community_detail_dag = DAG(
    'community_article_detail',
    default_args=default_args_detail,
    description='커뮤니티 상세 페이지 크롤링 및 MongoDB 저장',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['trendgetter', 'community', 'crawler', 'detail'],
)


# 태스크 설정
extract_ids_task = PythonOperator(
    task_id='extract_article_ids',
    python_callable=extract_article_ids,
    dag=community_list_dag,
)

trigger_tasks = PythonOperator.partial(
    task_id='trigger_detail_dag',
    python_callable=trigger_single_detail_dag,
    dag=community_list_dag,
).expand(
    op_kwargs=extract_ids_task.output
)

extract_detail_task = PythonOperator(
    task_id='extract_and_save_article_detail',
    python_callable=extract_and_save_article_detail,
    dag=community_detail_dag,
)


# 의존성 설정
#   목록 추출 >> 상세 DAG 트리거
extract_ids_task >> trigger_tasks  # pyright: ignore[reportUnusedExpression]
#   상세 크롤링 >> 저장
extract_detail_task  # pyright: ignore[reportUnusedExpression]
