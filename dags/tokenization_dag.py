from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from mongodb import MongoDBClient
from quality_assurance import KiwiTokenizerQualityAssurance
from utils import get_week_range, normalize_datetime


# 파라미터 초기화
mongo_client = MongoDBClient()
qa = KiwiTokenizerQualityAssurance()


# 로직 함수 정의
def extract_raw_articles(**context):
    """시간 범위 계산 및 반환 (XCom 크기 제한 회피)
    
    기본값: execution_date가 속한 주의 월요일부터 execution_date까지
    XCom 크기 제한을 피하기 위해 시간 범위만 반환
    """
    # 파라미터 추출 및 정규화
    execution_date = context.get('execution_date')
    if not execution_date:
        execution_date = datetime.now()
    else:
        execution_date = normalize_datetime(execution_date)
    
    # 해당 주의 월요일부터 execution_date까지의 범위 계산
    start_time, end_time = get_week_range(execution_date)
    
    # 시간 범위만 반환 (XCom 크기 제한 회피)
    return {
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat()
    }


def tokenize_and_save(**context):
    """토큰화 및 메타데이터/토큰 분리 저장 (배치 처리)"""
    # XCom에서 시간 범위 가져오기
    ti = context['ti']
    time_range = ti.xcom_pull(task_ids='extract_raw_articles')
    if not time_range:
        raise ValueError("시간 범위 정보를 가져올 수 없습니다.")
    
    start_time = datetime.fromisoformat(time_range['start_time'])
    end_time = datetime.fromisoformat(time_range['end_time'])
    
    # MongoDB에서 시간 범위로 조회
    articles = mongo_client.find_by_time_range(
        collection_name='raw_articles',
        time_field='created_at',
        start_time=start_time,
        end_time=end_time
    )
    
    if not articles:
        return {'processed_count': 0, 'total_token_count': 0}
    
    processed_count = 0
    total_token_count = 0
    
    # 각 article 처리
    for article in articles:
        # text 필드 추출
        text = article.get('text', '')
        if not text:
            continue
        
        # metadata 생성 (_id와 text 제외)
        metadata = {k: v for k, v in article.items() if k not in ('_id', 'text')}
        
        # metadata_articles에 저장 (text 제외한 정보)
        mongo_client.upsert_many(
            collection_name='metadata_articles',
            documents=[metadata],
            filter_keys=['src_pk', 'src_id', 'comment_id']
        )
        
        # metadata_id 조회 (src_pk, src_id, comment_id로)
        metadata_doc = mongo_client.find_one_by_keys(
            collection_name='metadata_articles',
            filter_dict={
                'src_pk': metadata.get('src_pk'),
                'src_id': metadata.get('src_id'),
                'comment_id': metadata.get('comment_id')
            }
        )
        if not metadata_doc:
            continue
        
        metadata_id = str(metadata_doc['_id'])
        
        # 토큰화
        tokens = qa.tokenize(text)
        if not tokens:
            continue
        
        # 기존 tokens 삭제 (멱등성 보장)
        mongo_client.delete_many_by_filter(
            collection_name='tokens',
            filter_dict={'metadata_id': metadata_id}
        )
        
        # 새 tokens insert
        token_documents = [{
            'metadata_id': metadata_id,
            'token': token,
            'created_at': metadata.get('created_at'),
        } for token in tokens]
        
        if token_documents:
            mongo_client.insert_many(
                collection_name='tokens',
                documents=token_documents
            )
            processed_count += 1
            total_token_count += len(tokens)
    
    return {
        'processed_count': processed_count,
        'total_token_count': total_token_count
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

tokenization_dag = DAG(
    'tokenization',
    default_args=default_args,
    description='raw_articles 컬렉션의 댓글을 토큰화하여 metadata_articles와 tokens 컬렉션에 분리 저장 (기본값: 1주일 단위)',
    schedule_interval=timedelta(days=7),
    start_date=days_ago(1),
    catchup=False,
    tags=['trendgetter', 'tokenization', 'nlp'],
)


# 태스크 설정
extract_articles_task = PythonOperator(
    task_id='extract_raw_articles',
    python_callable=extract_raw_articles,
    dag=tokenization_dag,
)

tokenize_task = PythonOperator(
    task_id='tokenize_and_save',
    python_callable=tokenize_and_save,
    dag=tokenization_dag,
)


# 의존성 설정
#   raw_articles 추출 >> 토큰화 및 저장
extract_articles_task >> tokenize_task  # pyright: ignore[reportUnusedExpression]

