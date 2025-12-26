"""MongoDB 저장 유틸리티"""
from datetime import datetime
from typing import Dict, Any, Optional, List
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo.operations import UpdateOne


class MongoDBClient:
    """MongoDB 클라이언트 (MongoHook 기반)"""
    
    def __init__(self, conn_id: str = 'mongodb_default'):
        """
        Args:
            conn_id: Airflow Connection ID (기본값: 'mongodb_default')
        """
        self.hook = MongoHook(mongo_conn_id=conn_id)
    
    def upsert_many(
        self, 
        collection_name: str, 
        documents: List[Dict[str, Any]], 
        filter_keys: Optional[List[str]] = None,
        database_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        여러 문서를 복합 키 기준으로 upsert (멱등성 보장)
        
        Args:
            collection_name: 컬렉션 이름
            documents: 저장할 문서 리스트
            filter_keys: upsert 기준이 되는 필드명 리스트 (기본값: ['src_pk', 'seq'])
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            bulk_write 결과
        """
        if not documents:
            return {'matched_count': 0, 'modified_count': 0, 'upserted_count': 0}
        
        if filter_keys is None:
            filter_keys = []
        
        collection = self.hook.get_collection(collection_name, mongo_db=database_name)
        
        operations = []
        for doc in documents:
            missing_keys = [key for key in filter_keys if key not in doc]
            if missing_keys:
                raise ValueError(f"Document must contain all filter keys {filter_keys}. Missing: {missing_keys}")
            
            filter_dict = {key: doc[key] for key in filter_keys}
            operations.append(
                UpdateOne(
                    filter_dict,
                    {'$set': doc},
                    upsert=True
                )
            )
        
        result = collection.bulk_write(operations, ordered=False)
        
        return {
            'matched_count': result.matched_count,
            'modified_count': result.modified_count,
            'upserted_count': result.upserted_count,
            'upserted_ids': result.upserted_ids
        }
    
    def find_by_time_range(
        self,
        collection_name: str,
        time_field: str,
        start_time: datetime,
        end_time: datetime,
        database_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """시간 범위로 문서 조회
        
        Args:
            collection_name: 컬렉션 이름
            time_field: 시간 필드명 (예: 'created_at')
            start_time: 시작 시간 (포함)
            end_time: 종료 시간 (포함)
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            조회된 문서 리스트
        """
        collection = self.hook.get_collection(collection_name, mongo_db=database_name)
        query = {
            time_field: {
                '$gte': start_time,
                '$lte': end_time
            }
        }
        return list(collection.find(query))
    
    def find_one_by_keys(
        self,
        collection_name: str,
        filter_dict: Dict[str, Any],
        database_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """필터 조건으로 단일 문서 조회
        
        Args:
            collection_name: 컬렉션 이름
            filter_dict: 조회 필터 조건 (예: {'src_pk': '...', 'src_id': '...', 'comment_id': '...'})
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            조회된 문서 또는 None
        """
        collection = self.hook.get_collection(collection_name, mongo_db=database_name)
        return collection.find_one(filter_dict)
    
    def delete_many_by_filter(
        self,
        collection_name: str,
        filter_dict: Dict[str, Any],
        database_name: Optional[str] = None
    ) -> int:
        """필터 조건으로 여러 문서 삭제
        
        Args:
            collection_name: 컬렉션 이름
            filter_dict: 삭제 필터 조건 (예: {'metadata_id': '...'})
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            삭제된 문서 개수
        """
        collection = self.hook.get_collection(collection_name, mongo_db=database_name)
        result = collection.delete_many(filter_dict)
        return result.deleted_count
    
    def insert_many(
        self,
        collection_name: str,
        documents: List[Dict[str, Any]],
        database_name: Optional[str] = None
    ) -> int:
        """여러 문서를 순수 insert (upsert 아님)
        
        Args:
            collection_name: 컬렉션 이름
            documents: 삽입할 문서 리스트
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            삽입된 문서 개수
        """
        if not documents:
            return 0
        
        collection = self.hook.get_collection(collection_name, mongo_db=database_name)
        result = collection.insert_many(documents, ordered=False)
        return len(result.inserted_ids)
    
    def find_tokens_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        database_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """tokens 컬렉션에서 시간 범위로 조회
        
        Args:
            start_time: 시작 시간 (포함)
            end_time: 종료 시간 (포함)
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            조회된 토큰 문서 리스트
        """
        return self.find_by_time_range(
            collection_name='tokens',
            time_field='created_at',
            start_time=start_time,
            end_time=end_time,
            database_name=database_name
        )
    
    def find_all_tokens(
        self,
        database_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """tokens 컬렉션의 모든 토큰 조회 (시간 필터 없음)
        
        Args:
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            전체 토큰 문서 리스트
        """
        collection = self.hook.get_collection('tokens', mongo_db=database_name)
        return list(collection.find({}))
    
    def aggregate_daily_tokens(
        self,
        target_date: datetime,
        database_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """하루치 토큰을 집계해서 토큰별 카운트 반환 (MongoDB 집계 파이프라인 사용)
        
        Args:
            target_date: 집계할 날짜 (하루 전체: 00:00:00 ~ 23:59:59)
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            토큰별 카운트 리스트 [{'token': '...', 'count': 123}, ...]
        """
        collection = self.hook.get_collection('tokens', mongo_db=database_name)
        
        # 하루 범위 계산
        start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': start_time,
                        '$lte': end_time
                    }
                }
            },
            {
                '$group': {
                    '_id': '$token',
                    'count': {'$sum': 1}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'token': '$_id',
                    'count': 1
                }
            }
        ]
        
        return list(collection.aggregate(pipeline))
    
    def aggregate_weekly_token_counts(
        self,
        start_date: datetime,
        end_date: datetime,
        database_name: Optional[str] = None
    ) -> Dict[str, int]:
        """주간 범위의 일일 카운트를 합산해서 토큰별 총 카운트 반환
        
        Args:
            start_date: 시작 날짜 (주간 버킷 시작)
            end_date: 종료 날짜 (주간 버킷 종료)
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            토큰별 총 카운트 딕셔너리 {token: total_count}
        """
        collection = self.hook.get_collection('count_keyword', mongo_db=database_name)
        
        pipeline = [
            {
                '$match': {
                    'date': {
                        '$gte': start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                        '$lte': end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                    }
                }
            },
            {
                '$group': {
                    '_id': '$token',
                    'total_count': {'$sum': '$count'}
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        return {result['_id']: result['total_count'] for result in results}
    
    def get_all_week_buckets_from_counts(
        self,
        database_name: Optional[str] = None
    ) -> int:
        """count_keyword 컬렉션에서 고유한 주간 버킷 수 반환
        
        Args:
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            고유한 주간 버킷 수
        """
        collection = self.hook.get_collection('count_keyword', mongo_db=database_name)
        
        pipeline = [
            {
                '$group': {
                    '_id': '$bucket_start'
                }
            },
            {
                '$count': 'bucket_count'
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        return results[0]['bucket_count'] if results else 0
    
    def get_token_week_bucket_count(
        self,
        token: str,
        database_name: Optional[str] = None
    ) -> int:
        """특정 토큰이 나타난 고유한 주간 버킷 수 반환
        
        Args:
            token: 토큰 문자열
            database_name: 데이터베이스 이름 (None이면 기본값 사용)
        
        Returns:
            토큰이 나타난 고유한 주간 버킷 수
        """
        collection = self.hook.get_collection('count_keyword', mongo_db=database_name)
        
        pipeline = [
            {
                '$match': {
                    'token': token
                }
            },
            {
                '$group': {
                    '_id': '$bucket_start'
                }
            },
            {
                '$count': 'bucket_count'
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        return results[0]['bucket_count'] if results else 0
