FROM apache/airflow:2.8.0-python3.11

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# 프로젝트 파일 복사
COPY pyproject.toml ./
COPY src/ ./src/

# 의존성 설치 (pyproject.toml에서 자동으로 읽어서 설치)
RUN uv pip install --system --no-cache --break-system-packages . && \
    rm -rf /tmp/* /var/tmp/* && \
    find /root -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find /root -type f -name "*.pyc" -delete 2>/dev/null || true

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:/opt/airflow/src

WORKDIR /opt/airflow
