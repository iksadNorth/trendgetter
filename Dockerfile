FROM apache/airflow:2.11.0-python3.11

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# 프로젝트 파일 복사
COPY pyproject.toml ./
COPY src/ ./src/

# 의존성 설치 (uv pip install을 사용하여 pyproject.toml의 의존성을 시스템에 설치)
# uv sync는 가상환경을 사용하므로, Docker에서는 uv pip install을 사용
# root로 실행하여 권한 문제 방지
USER root
RUN uv pip install --system --no-cache --break-system-packages -e . && \
    rm -rf /tmp/* /var/tmp/* && \
    find /root -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find /root -type f -name "*.pyc" -delete 2>/dev/null || true
# airflow 사용자로 복원
USER airflow

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:/opt/airflow/src

WORKDIR /opt/airflow
