"""MongoDB 저장 유틸리티"""
from typing import Dict, Any, Optional, List
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.operations import UpdateOne


class MongoDBClient:
    """MongoDB 클라이언트"""
    
    def __init__(self, conn_id: str = 'mongodb_default'):
        """
        Args:
            conn_id: Airflow Connection ID (기본값: 'mongodb_default')
        """
        self.conn_id = conn_id
        self._client: Optional[MongoClient] = None
        self._database: Optional[Database] = None
    
    def _get_connection(self):
        """Airflow Connection에서 MongoDB 연결 정보 가져오기"""
        conn = BaseHook.get_connection(self.conn_id)
        
        # Connection 정보 구성
        host = conn.host or 'localhost'
        port = conn.port or 27017
        login = conn.login
        password = conn.password
        schema = conn.schema or 'admin'  # database name
        
        # MongoDB URI 구성
        if login and password:
            uri = f"mongodb://{login}:{password}@{host}:{port}/{schema}"
        else:
            uri = f"mongodb://{host}:{port}/{schema}"
        
        return uri, schema
    
    def get_client(self) -> MongoClient:
        """MongoDB 클라이언트 반환"""
        if self._client is None:
            uri, _ = self._get_connection()
            self._client = MongoClient(uri)
        return self._client
    
    def get_database(self, database_name: Optional[str] = None) -> Database:
        """데이터베이스 반환"""
        if self._database is None:
            client = self.get_client()
            if database_name:
                self._database = client[database_name]
            else:
                _, default_db = self._get_connection()
                self._database = client[default_db]
        return self._database
    
    def get_collection(self, collection_name: str, database_name: Optional[str] = None) -> Collection:
        """컬렉션 반환"""
        db = self.get_database(database_name)
        return db[collection_name]
    
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
        
        # 기본값 설정
        if filter_keys is None:
            filter_keys = ['src_pk', 'seq']
        
        collection = self.get_collection(collection_name, database_name)
        
        # bulk_write를 위한 UpdateOne 작업 리스트 생성
        operations = []
        for doc in documents:
            # 모든 filter_keys가 문서에 있는지 확인
            missing_keys = [key for key in filter_keys if key not in doc]
            if missing_keys:
                raise ValueError(f"Document must contain all filter keys {filter_keys}. Missing: {missing_keys}")
            
            # 복합 키를 기준으로 upsert
            filter_dict = {key: doc[key] for key in filter_keys}
            operations.append(
                UpdateOne(
                    filter_dict,
                    {'$set': doc},  # 전체 문서를 업데이트
                    upsert=True
                )
            )
        
        # bulk_write 실행
        result = collection.bulk_write(operations, ordered=False)
        
        return {
            'matched_count': result.matched_count,
            'modified_count': result.modified_count,
            'upserted_count': result.upserted_count,
            'upserted_ids': result.upserted_ids
        }
    
    def close(self):
        """연결 종료"""
        if self._client:
            self._client.close()
            self._client = None
            self._database = None

