from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from crawler import YoutubeCrawler
from mongodb import MongoDBClient


# 파라미터 초기화
mongo_client = MongoDBClient()
crawler = YoutubeCrawler()

# 댓글 할당량 설정 (Airflow Variable에서 가져오기, 기본값: 1000)
def get_comment_quota():
    """Airflow Variable에서 COMMENT_QUOTA 가져오기 (기본값: 1000)"""
    try:
        quota = Variable.get('COMMENT_QUOTA', default_var=1000)
        return int(quota)
    except Exception:
        return 1000


# 로직 함수 정의
def extract_article_ids(**context):
    """인기 동영상 ID 추출"""
    execution_date = context['execution_date']
    
    article_ids = crawler.scrap_array_id(created_at=execution_date)
    if not article_ids:
        return []
    
    return article_ids


def extract_and_save_details(**context):
    """댓글 크롤링 및 MongoDB 저장 (할당량 제한)"""
    # XCom에서 article_ids 가져오기
    ti = context['ti']
    article_ids = ti.xcom_pull(task_ids='extract_article_ids')
    
    if not article_ids:
        print("추출된 article_ids가 없습니다.")
        return
    
    # 할당량 가져오기
    comment_quota = get_comment_quota()
    print(f"댓글 할당량: {comment_quota}개")
    
    total_comments = 0
    processed_videos = 0
    
    # 각 영상의 댓글 크롤링
    for article_id in article_ids:
        # 할당량 도달 시 중단
        if total_comments >= comment_quota:
            print(f"할당량 도달: {total_comments}개 댓글 수집 완료. 크롤링 중단.")
            break
        
        # 댓글 크롤링
        article_data = crawler.scrap_article_data(article_id=article_id)
        
        if not article_data:
            print(f"영상 {article_id}: 댓글 없음 또는 오류 발생")
            continue
        
        # MongoDB 저장 (upsert)
        upsert_result = mongo_client.upsert_many(
            collection_name='raw_articles',
            documents=article_data,
            filter_keys=['src_pk', 'src_id', 'comment_id']
        )
        
        # 실제로 새로 insert된 댓글 개수 사용
        inserted_count = upsert_result.get('upserted_count', 0)
        total_comments += inserted_count
        processed_videos += 1
        
        print(f"영상 {article_id}: {inserted_count}개 댓글 새로 저장 (누적: {total_comments}/{comment_quota})")
    
    print(f"크롤링 완료: {processed_videos}개 영상 처리, 총 {total_comments}개 댓글 수집")
    return {
        'processed_videos': processed_videos,
        'total_comments': total_comments
    }


# DAG 설정
default_args = {
    'owner': 'trendgetter',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # retry 비활성화 (사용자가 직접 판단하여 트리거)
    'retry_delay': timedelta(minutes=5),
}

youtube_crawling_dag = DAG(
    'youtube_crawling',
    default_args=default_args,
    description='YouTube 인기 동영상 댓글 크롤링 및 MongoDB 저장 (할당량 제한)',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['trendgetter', 'youtube', 'crawler', 'list', 'detail'],
)


# 태스크 설정
extract_ids_task = PythonOperator(
    task_id='extract_article_ids',
    python_callable=extract_article_ids,
    dag=youtube_crawling_dag,
)

extract_detail_task = PythonOperator(
    task_id='extract_and_save_details',
    python_callable=extract_and_save_details,
    dag=youtube_crawling_dag,
)


# 의존성 설정
extract_ids_task >> extract_detail_task  # pyright: ignore[reportUnusedExpression]
