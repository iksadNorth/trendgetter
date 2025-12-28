from datetime import datetime
from typing import List, Dict, Any, Optional
import math
from mongodb import MongoDBClient
from src.model import TFIDFScore


class TimeWindowedTemporalTFIDF():
    """시간 윈도우 기반 TF-IDF 계산 클래스 (MongoDB 집계 파이프라인 사용)"""
    
    def __init__(self, mongo_client: Optional[MongoDBClient] = None):
        """
        Args:
            mongo_client: MongoDBClient 인스턴스 (None이면 새로 생성)
        """
        self.mongo_client = mongo_client or MongoDBClient()
    
    def calculate(self, start_date: datetime, end_date: datetime) -> List[TFIDFScore]:
        """시간 버킷(1주일) 단위로 TF-IDF 스코어 계산 (count_keyword 컬렉션 기반)
        
        Args:
            start_date: 시작 시간 (시간 버킷 시작점)
            end_date: 종료 시간 (시간 버킷 종료점)
        
        Returns:
            토큰별 TF-IDF 스코어 모델 리스트 (TFIDFScore)
        """
        # 1. 주간 범위의 토큰 카운트 집계 (TF 계산용)
        weekly_token_counts = self.mongo_client.aggregate_weekly_token_counts(
            start_date=start_date,
            end_date=end_date
        )
        
        if not weekly_token_counts:
            return []
        
        # 주간 총 토큰 수 계산
        total_tokens_in_week = sum(weekly_token_counts.values())
        
        if total_tokens_in_week == 0:
            return []
        
        # 2. 전체 주간 버킷 수 조회 (IDF 계산용)
        total_bucket_count = self.mongo_client.get_all_week_buckets_from_counts()
        
        if total_bucket_count == 0:
            return []
        
        # 3. 각 토큰에 대해 TF-IDF 계산
        tfidf_scores = []
        
        for token, token_count in weekly_token_counts.items():
            # TF 계산: 특정 시간 버킷 내에서 토큰 빈도 / 해당 시간 버킷 내의 총 토큰 수
            tf = token_count / total_tokens_in_week if total_tokens_in_week > 0 else 0.0
            
            # IDF 계산: log(전체 시간 버킷 수 / 토큰이 나타난 시간 버킷 수)
            bucket_count_with_token = self.mongo_client.get_token_week_bucket_count(token)
            
            if bucket_count_with_token > 0:
                idf = math.log(total_bucket_count / bucket_count_with_token)
            else:
                idf = 0.0
            
            # TF-IDF 계산
            tfidf_score = tf * idf
            
            # 딕셔너리 생성 후 TFIDFScore 모델로 변환
            score_dict = {
                'token': token,
                'score': tfidf_score,
                'tf': tf,
                'idf': idf,
                'start_time': start_date,
                'end_time': end_date,
                'bucket_count': bucket_count_with_token,
                'total_bucket_count': total_bucket_count
            }
            tfidf_scores.append(TFIDFScore.from_dict(score_dict))
        
        return tfidf_scores
    