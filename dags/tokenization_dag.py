from datetime import datetime, timedelta
from bson import ObjectId

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
    """raw_articles 컬렉션에서 시간 범위 내 문서 추출
    
    기본값: execution_date가 속한 주의 월요일부터 execution_date까지
    MongoDB 쿼리에서 직접 시간 범위 필터링
    """
    # 파라미터 추출 및 정규화
    execution_date = context.get('execution_date')
    if not execution_date:
        execution_date = datetime.now()
    else:
        execution_date = normalize_datetime(execution_date)
    
    # 해당 주의 월요일부터 execution_date까지의 범위 계산
    start_time, end_time = get_week_range(execution_date)
    
    # MongoDB에서 시간 범위로 조회
    articles = mongo_client.find_by_time_range(
        collection_name='raw_articles',
        time_field='created_at',
        start_time=start_time,
        end_time=end_time
    )
    if not articles: return []
    
    # ObjectId를 문자열로 변환 (XCom JSON 직렬화를 위해)
    result = []
    for article in articles:
        article_copy = article.copy()
        if '_id' in article_copy and isinstance(article_copy['_id'], ObjectId):
            article_copy['_id'] = str(article_copy['_id'])
        result.append({'article': article_copy})
    
    return result


def tokenize_and_save(**context):
    """토큰화 및 메타데이터/토큰 분리 저장"""
    # 파라미터 추출
    article = context.get('article')
    if not article: raise ValueError("article is required")
    
    # text 필드 추출
    text = article.get('text', '')
    if not text: return {'metadata_id': None, 'token_count': 0}
    metadata = {k: v for k, v in article.items() if k != 'text'}
    
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
    if not metadata_doc: return {'metadata_id': None, 'token_count': 0}
    metadata_id = str(metadata_doc['_id'])
    
    # HTML 태그 제거
    text = qa.remove_html_tags(text)
    
    # 토큰화
    tokens = qa.tokenize(text)
    if not tokens: return {'metadata_id': None, 'token_count': 0}
    
    # 기존 tokens 삭제 (멱등성 보장)
    mongo_client.delete_many_by_filter(
        collection_name='tokens',
        filter_dict={'metadata_id': metadata_id}
    )
    
    # 새 tokens insert
    token_documents = [ {
        'metadata_id': metadata_id,
        'token': token,
        'created_at': metadata.get('created_at'),
    } for token in tokens ]
    if not token_documents: return {'metadata_id': metadata_id, 'token_count': 0}
    
    mongo_client.insert_many(
        collection_name='tokens',
        documents=token_documents
    )
    
    return {
        'metadata_id': str(metadata_id),
        'token_count': len(tokens)
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

tokenize_tasks = PythonOperator.partial(
    task_id='tokenize_and_save',
    python_callable=tokenize_and_save,
    dag=tokenization_dag,
).expand(
    op_kwargs=extract_articles_task.output
)


# 의존성 설정
#   raw_articles 추출 >> 토큰화 및 저장
extract_articles_task >> tokenize_tasks  # pyright: ignore[reportUnusedExpression]

