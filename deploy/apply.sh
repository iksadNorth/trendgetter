#!/bin/bash

# Namespace 적용
kubectl apply -f deploy/namespace.yaml

# .env 파일에서 ConfigMap과 Secret 생성
kubectl create configmap app-env --from-env-file=.env -n trendgetter --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic airflow-secret --from-env-file=.env -n trendgetter --dry-run=client -o yaml | kubectl apply -f -

# 나머지 리소스 적용
kubectl apply -f deploy/pv.yaml
kubectl apply -f deploy/pvc.yaml
kubectl apply -f deploy/redis.yaml
kubectl apply -f deploy/postgres.yaml

# Postgres가 준비될 때까지 대기
echo "Postgres가 준비될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -l app=postgres -n trendgetter --timeout=300s

# Airflow 리소스 적용 (initContainer가 자동으로 DB 초기화)
kubectl apply -f deploy/airflow-scheduler.yaml
kubectl apply -f deploy/airflow-webserver.yaml
kubectl apply -f deploy/airflow-worker.yaml

# Airflow Webserver 접근 정보 출력
kubectl port-forward -n trendgetter svc/airflow-webserver 8080:8080
echo "  그 다음 브라우저에서 http://localhost:8080 접속"

