"""MongoDB 저장 유틸리티"""
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
            filter_keys = ['src_pk', 'seq']
        
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
    
    def close(self):
        """연결 종료 (MongoHook이 자동 관리하므로 빈 메서드)"""
        pass

