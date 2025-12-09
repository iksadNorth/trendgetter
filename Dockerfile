FROM apache/airflow:2.8.0-python3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

WORKDIR /opt/airflow
