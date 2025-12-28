from datetime import datetime
from typing import Dict, Any
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ScrapedData():
    pass


class YoutubeData():
    pass


class InstagramData():
    pass


class CommunityData():
    pass


class TFIDFScore(Base):
    """TF-IDF 스코어 모델"""
    __tablename__ = 'tfidf_scores'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    token = Column(String(255), nullable=False)
    score = Column(Float, nullable=False)
    tf = Column(Float, nullable=False)
    idf = Column(Float, nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    bucket_count = Column(Integer, nullable=False)
    total_bucket_count = Column(Integer, nullable=False)
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'TFIDFScore':
        """딕셔너리에서 TFIDFScore 인스턴스 생성 (정적 메서드)
        
        Args:
            data: TF-IDF 스코어 딕셔너리
                - token: str
                - score: float
                - tf: float
                - idf: float
                - start_time: datetime
                - end_time: datetime
                - bucket_count: int
                - total_bucket_count: int
        
        Returns:
            TFIDFScore 인스턴스
        """
        # datetime 변환
        start_time = data.get('start_time')
        end_time = data.get('end_time')
        
        if isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        if isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # SQLAlchemy 모델은 키워드 인자로 생성
        instance = TFIDFScore()
        instance.token = data.get('token')
        instance.score = float(data.get('score', 0.0))
        instance.tf = float(data.get('tf', 0.0))
        instance.idf = float(data.get('idf', 0.0))
        instance.start_time = start_time
        instance.end_time = end_time
        instance.bucket_count = int(data.get('bucket_count', 0))
        instance.total_bucket_count = int(data.get('total_bucket_count', 0))
        return instance
    
    def to_dict(self) -> Dict[str, Any]:
        """TFIDFScore 인스턴스를 딕셔너리로 변환
        
        Returns:
            TF-IDF 스코어 딕셔너리
        """
        return {
            'id': self.id,
            'token': self.token,
            'score': self.score,
            'tf': self.tf,
            'idf': self.idf,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'bucket_count': self.bucket_count,
            'total_bucket_count': self.total_bucket_count
        }
