"""PostgreSQL 저장 유틸리티"""
from datetime import datetime
from typing import Dict, Any, Optional, List
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Query
from src.model import TFIDFScore


class PostgreSQLClient:
    """PostgreSQL 클라이언트 (PostgresHook 기반)"""
    
    def __init__(self, conn_id: str = 'superset_postgres_default'):
        """
        Args:
            conn_id: Airflow Connection ID (기본값: 'superset_postgres_default')
        """
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.engine = self.hook.get_sqlalchemy_engine()
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    def _get_session(self) -> Session:
        """세션 생성"""
        return self.SessionLocal()
    
    def upsert_tfidf_scores(
        self,
        scores: List[Dict[str, Any]],
        table_name: str = 'tfidf_scores'
    ) -> Dict[str, Any]:
        """TF-IDF 스코어를 upsert (start_time, end_time 기준으로 기존 데이터 삭제 후 삽입)
        
        멱등성 보장: 동일한 start_time, end_time을 가진 데이터는 모두 삭제 후 재삽입
        
        Args:
            scores: 저장할 TF-IDF 스코어 리스트
            table_name: 테이블 이름 (기본값: 'tfidf_scores', 사용되지 않음 - 모델에서 자동 처리)
        
        Returns:
            삽입 결과 딕셔너리 {'deleted_count': int, 'inserted_count': int}
        """
        if not scores:
            return {'deleted_count': 0, 'inserted_count': 0}
        
        # start_time과 end_time 추출 (모든 레코드가 동일한 시간 범위를 가져야 함)
        first_score = scores[0]
        start_time = first_score.get('start_time')
        end_time = first_score.get('end_time')
        
        if not start_time or not end_time:
            raise ValueError("scores must contain 'start_time' and 'end_time' fields")
        
        # datetime 객체로 변환
        if isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        if isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        session = self._get_session()
        try:
            # 1. 기존 데이터 삭제 (멱등성 보장) - ORM 방식
            deleted_count: int = session.query(TFIDFScore).filter(  # pyright: ignore[reportOptionalCall]
                TFIDFScore.start_time == start_time,
            ).delete(synchronize_session=False)
            
            # 2. 새 데이터 삽입 - ORM 방식
            if scores:
                tfidf_objects = [TFIDFScore.from_dict(score) for score in scores]
                session.add_all(tfidf_objects)
                inserted_count = len(tfidf_objects)
            else:
                inserted_count = 0
            
            # 커밋
            session.commit()
            
            return {
                'deleted_count': deleted_count,
                'inserted_count': inserted_count
            }
        except SQLAlchemyError as e:
            session.rollback()
            raise Exception(f"Failed to upsert TF-IDF scores: {str(e)}")
        finally:
            session.close()
