#!/usr/bin/env python3
"""MongoDB Connection 설정 스크립트 (멱등성 보장)"""
import os
import sys
from sqlalchemy import create_engine, text
from airflow.models import Connection
from airflow.settings import Session
from airflow.configuration import conf
from cryptography.fernet import InvalidToken


def cleanup_corrupted_connections():
    """손상된 Connection을 모두 찾아서 삭제 (Fernet 키 불일치 문제 해결)"""
    try:
        sql_alchemy_conn = conf.get('database', 'sql_alchemy_conn')
        engine = create_engine(sql_alchemy_conn)
        
        # SQL로 모든 connection의 conn_id 가져오기
        with engine.connect() as db_conn:
            result = db_conn.execute(text('SELECT conn_id FROM connection'))
            all_conn_ids = [row[0] for row in result]
        
        if not all_conn_ids:
            print('Connection이 없습니다.')
            return
        
        print(f'총 {len(all_conn_ids)}개의 Connection 확인 중...')
        corrupted_ids = []
        
        # 각 connection의 password 복호화 시도
        # Session을 매번 새로 만들어서 각 connection을 독립적으로 확인
        for conn_id in all_conn_ids:
            try:
                # 새로운 세션으로 각 connection 확인
                session = Session()
                try:
                    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
                    if conn:
                        # password 복호화 시도
                        _ = conn.password  # 이게 실패하면 InvalidToken 발생
                finally:
                    session.close()
            except (InvalidToken, Exception) as e:
                # 복호화 실패 = 손상된 connection
                corrupted_ids.append(conn_id)
                print(f'  손상된 Connection 발견: {conn_id} ({type(e).__name__}: {str(e)[:50]})')
        
        # 손상된 connection 삭제
        if corrupted_ids:
            print(f'\n손상된 Connection {len(corrupted_ids)}개 삭제 중...')
            with engine.begin() as db_conn:
                for conn_id in corrupted_ids:
                    result = db_conn.execute(
                        text('DELETE FROM connection WHERE conn_id = :conn_id'),
                        {'conn_id': conn_id}
                    )
                    print(f'  삭제됨: {conn_id}')
            print(f'\n✅ 총 {len(corrupted_ids)}개의 손상된 Connection 삭제 완료')
        else:
            print('✅ 손상된 Connection이 없습니다.')
            
    except Exception as e:
        print(f'손상된 Connection 정리 중 오류 (무시됨): {e}')
        import traceback
        traceback.print_exc()


def delete_existing_connection(conn_id: str):
    """기존 Connection을 데이터베이스에서 직접 삭제 (Fernet 키 문제 우회)"""
    try:
        sql_alchemy_conn = conf.get('database', 'sql_alchemy_conn')
        engine = create_engine(sql_alchemy_conn)
        
        with engine.begin() as conn:
            result = conn.execute(
                text('DELETE FROM connection WHERE conn_id = :conn_id'),
                {'conn_id': conn_id}
            )
            print(f'삭제된 레코드 수: {result.rowcount}')
    except Exception as e:
        print(f'기존 Connection 삭제 중 오류 (무시됨): {e}')


def create_or_update_connection(
    conn_id: str,
    host: str,
    port: int,
    login: str,
    password: str,
    schema: str
):
    """MongoDB Connection 생성 또는 업데이트"""
    conn = Connection(
        conn_id=conn_id,
        conn_type='generic',
        host=host,
        port=port,
        login=login,
        password=password,
        schema=schema
    )
    
    session = Session()
    try:
        # 기존 connection이 있는지 확인
        existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if existing:
            # 기존 connection 업데이트
            existing.conn_type = conn.conn_type
            existing.host = conn.host
            existing.port = conn.port
            existing.login = conn.login
            existing.set_password(conn.password)
            existing.schema = conn.schema
            print('기존 Connection 업데이트 완료')
        else:
            # 새 connection 추가
            session.add(conn)
            print('새 Connection 생성 완료')
        session.commit()
    except Exception as e:
        session.rollback()
        print(f'오류 발생: {e}')
        raise
    finally:
        session.close()


def main():
    """메인 함수"""
    # 환경 변수에서 값 가져오기
    conn_id = os.getenv('CONN_ID', 'mongodb_default')
    host = os.getenv('MONGODB_HOST', 'mongodb')
    port = int(os.getenv('MONGODB_PORT', '27017'))
    login = os.getenv('MONGODB_USERNAME')
    password = os.getenv('MONGODB_PASSWORD')
    schema = os.getenv('MONGODB_DATABASE')
    
    # 필수 환경 변수 확인
    if not login or not password or not schema:
        print('오류: MONGODB_USERNAME, MONGODB_PASSWORD, MONGODB_DATABASE 환경 변수가 필요합니다.')
        sys.exit(1)
    
    print(f'Connection ID: {conn_id}')
    print(f'Host: {host}:{port}')
    print(f'Database: {schema}')
    print(f'Login: {login}')
    
    # 손상된 Connection 정리 (웹 UI 오류 방지)
    print('\n손상된 Connection 정리 중...')
    cleanup_corrupted_connections()
    
    # 기존 Connection 삭제
    print('\n기존 Connection 삭제 중...')
    delete_existing_connection(conn_id)
    
    # Connection 생성/업데이트
    print('\nMongoDB Connection 생성/업데이트 중...')
    create_or_update_connection(
        conn_id=conn_id,
        host=host,
        port=port,
        login=login,
        password=password,
        schema=schema
    )
    
    print(f'\n✅ MongoDB Connection \'{conn_id}\' 설정 완료!')


if __name__ == '__main__':
    main()

