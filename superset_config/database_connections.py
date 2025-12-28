"""
Superset Database Connections 정의
"""
import os

# PostgreSQL 커넥션 설정
POSTGRES_CONNECTIONS = {
    "postgres": {
        "sqlalchemy_uri": os.getenv(
            "SQLALCHEMY_DATABASE_URI",
            "postgresql+psycopg2://superset:superset@postgres:5432/superset"
        ),
        "display_name": "PostgreSQL",
        "metadata_params": {},
        "engine_params": {
            "pool_size": 5,
            "max_overflow": 10,
            "pool_pre_ping": True,
        }
    }
}

