FROM python:3.11-slim

# 시스템 의존성 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# 작업 디렉토리 설정
WORKDIR /opt/airflow

# 프로젝트 파일 복사
COPY pyproject.toml ./
COPY .env ./.env
COPY src/ ./src/

# airflow 사용자 생성 (기존 Airflow 이미지와 동일한 UID/GID)
RUN groupadd -g 50000 airflow && \
    useradd -u 50000 -g 50000 -ms /bin/bash airflow

# uv sync로 venv 생성 및 모든 의존성 설치 (Airflow 포함)
RUN uv sync && \
    rm -rf /tmp/* /var/tmp/* && \
    find .venv -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find .venv -type f -name "*.pyc" -delete 2>/dev/null || true && \
    chown -R airflow:airflow /opt/airflow

# Airflow 환경 변수 설정
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:/opt/airflow/src

USER airflow
