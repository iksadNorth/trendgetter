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
