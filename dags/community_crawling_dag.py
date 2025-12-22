"""Community Crawling DAGs"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from crawler import CommunityCrawler


def extract_article_ids(**context):
    """커뮤니티 목록 페이지에서 article_id 목록 추출"""
    execution_date = context['execution_date']
    
    crawler = CommunityCrawler()
    article_ids = crawler.scrap_array_id(created_at=execution_date)
    
    if not article_ids:
        print(f"No article IDs found for {execution_date}")
        return []
    
    print(f"Found {len(article_ids)} article IDs: {article_ids}")
    
    # XCom에 article_id 목록 저장
    return article_ids


# DAG 1 기본 설정
default_args_list = {
    'owner': 'trendgetter',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 1 정의: 커뮤니티 목록 크롤링
community_list_dag = DAG(
    'community_article_list',
    default_args=default_args_list,
    description='커뮤니티 목록에서 article_id 추출 및 상세 DAG 트리거',
    schedule_interval=timedelta(days=1),  # 1일마다 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['trendgetter', 'community', 'crawler', 'list'],
)

# article_id 목록 추출 태스크
extract_ids_task = PythonOperator(
    task_id='extract_article_ids',
    python_callable=extract_article_ids,
    dag=community_list_dag,
)

# article_id 리스트를 딕셔너리 리스트로 변환하는 헬퍼 함수
def prepare_article_ids_for_expand(**context):
    """XCom에서 article_id 리스트를 가져와서 expand용 딕셔너리 리스트로 변환"""
    ti = context['ti']
    article_ids = ti.xcom_pull(task_ids='extract_article_ids')
    
    if not article_ids:
        return []
    
    # 각 article_id를 딕셔너리로 변환
    return [{'article_id': article_id} for article_id in article_ids]

# article_id 리스트를 딕셔너리 리스트로 변환하는 태스크
prepare_ids_task = PythonOperator(
    task_id='prepare_article_ids',
    python_callable=prepare_article_ids_for_expand,
    dag=community_list_dag,
)

# 단일 article_id에 대해 상세 DAG 트리거 (Dynamic Task Mapping용)
def trigger_single_detail_dag(**context):
    """단일 article_id에 대해 상세 DAG 트리거"""
    # context에서 article_id 가져오기 (op_kwargs로 전달됨)
    article_id = context.get('article_id')
    if not article_id:
        raise ValueError("article_id is required")
    
    from airflow.api.client.local_client import Client
    
    client = Client(None, None)
    
    try:
        run_id = f'triggered__{article_id}__{context["ds_nodash"]}__{context["ts_nodash"]}'
        client.trigger_dag(
            dag_id='community_article_detail',
            conf={'article_id': article_id},
            run_id=run_id,
        )
        print(f"Triggered detail DAG for article_id: {article_id} (run_id: {run_id})")
        return f"Successfully triggered for {article_id}"
    except Exception as e:
        print(f"Failed to trigger DAG for article_id {article_id}: {e}")
        import traceback
        traceback.print_exc()
        raise  # 예외를 다시 발생시켜 태스크 실패로 표시

# Dynamic Task Mapping: 각 article_id마다 독립적인 태스크 생성
# expand()에 딕셔너리 리스트를 전달하면 각 딕셔너리가 op_kwargs로 전달됨
trigger_tasks = PythonOperator.partial(
    task_id='trigger_detail_dag',
    python_callable=trigger_single_detail_dag,
    dag=community_list_dag,
).expand(
    # 딕셔너리 리스트의 각 딕셔너리가 op_kwargs로 전달됨
    op_kwargs=prepare_ids_task.output
)

# 의존성 설정
extract_ids_task >> prepare_ids_task >> trigger_tasks


# ============================================================================
# DAG 2: article_id를 받아서 상세 페이지 크롤링 및 MongoDB 저장
# ============================================================================

def extract_and_save_article_detail(**context):
    """article_id를 받아서 상세 페이지 크롤링 및 MongoDB 저장"""
    # TriggerDagRunOperator의 conf에서 article_id 가져오기
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        article_id = dag_run.conf.get('article_id')
    else:
        # XCom에서 가져오기 (다른 방법)
        ti = context['ti']
        article_id = ti.xcom_pull(key='article_id')
    
    if not article_id:
        raise ValueError("article_id is required")
    
    print(f"Processing article_id: {article_id}")
    
    # 크롤링
    crawler = CommunityCrawler()
    article_data = crawler.scrap_article_data(article_id=article_id)
    
    if not article_data:
        print(f"No data found for article_id: {article_id}")
        return
    
    # MongoDB 저장 (src_pk + seq 복합 키 기준으로 upsert - 멱등성 보장)
    from mongodb import MongoDBClient
    
    mongo_client = MongoDBClient()
    try:
        result = mongo_client.upsert_many(
            collection_name='community_articles',
            documents=article_data,
            filter_keys=['src_pk', 'seq']
        )
        print(f"Upserted {len(article_data)} documents for article {article_id}")
        print(f"Matched: {result['matched_count']}, Modified: {result['modified_count']}, Upserted: {result['upserted_count']}")
    finally:
        mongo_client.close()
    
    return article_data


# DAG 2 기본 설정
default_args_detail = {
    'owner': 'trendgetter',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG 2 정의: 커뮤니티 상세 페이지 크롤링
community_detail_dag = DAG(
    'community_article_detail',
    default_args=default_args_detail,
    description='커뮤니티 상세 페이지 크롤링 및 MongoDB 저장',
    schedule_interval=None,  # 수동 트리거만 (DAG 1에서 트리거)
    start_date=days_ago(1),
    catchup=False,
    tags=['trendgetter', 'community', 'crawler', 'detail'],
)

# 상세 페이지 크롤링 및 저장 태스크
extract_detail_task = PythonOperator(
    task_id='extract_and_save_article_detail',
    python_callable=extract_and_save_article_detail,
    dag=community_detail_dag,
)
