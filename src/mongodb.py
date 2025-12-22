"""MongoDB 저장 유틸리티"""
from typing import Dict, Any, Optional
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


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
    
    def close(self):
        """연결 종료"""
        if self._client:
            self._client.close()
            self._client = None
            self._database = None

